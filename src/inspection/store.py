"""SQLite 持久层：影像采集索引后端的全部业务数据。

设计原则：
- 只依赖标准库；单连接 + 进程内 RLock，配合 WAL，供 HTTP 线程与后台 worker 共用。
- 时间统一为带时区的 ISO 8601 字符串。
- 校准（calibration）与定位版本（loc_version）只增不改；替换校准 = 新版本。
- 分段文件按 (task_id, sha256) 幂等。
- 缺陷的定位依据（sighting）不可变；报告钉住 sighting，被引用数据拒绝修改。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Conflict(Exception):
    """请求与当前数据状态冲突（状态机非法、引用不可变数据等）。"""


class NotFound(LookupError):
    """引用的实体不存在。"""


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 管线起止井 ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    start_manhole TEXT NOT NULL,
    end_manhole TEXT NOT NULL,
    length_m REAL NOT NULL CHECK (length_m > 0),
    created_at TEXT NOT NULL
);

-- 设备 ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    model TEXT NOT NULL,
    nominal_wheel_mm REAL NOT NULL CHECK (nominal_wheel_mm > 0),
    created_at TEXT NOT NULL
);

-- 校准记录：每次轮径校准变化生成一条；不可修改、不可删除 ----------------------
CREATE TABLE IF NOT EXISTS calibrations (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(id),
    wheel_diameter_mm REAL NOT NULL CHECK (wheel_diameter_mm > 0),
    pulses_per_rev INTEGER NOT NULL CHECK (pulses_per_rev > 0),
    calibrated_at TEXT NOT NULL,          -- 校准实际发生时间（来源时间）
    recorded_at TEXT NOT NULL,            -- 入库时间（接收时间）
    note TEXT NOT NULL DEFAULT '',
    supersedes_id TEXT REFERENCES calibrations(id),
    device_seq INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_calib_device_seq
    ON calibrations(device_id, device_seq);

-- 检测任务 ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
    device_id TEXT NOT NULL REFERENCES devices(id),
    code TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'planned', -- planned|active|completed|cancelled
    recorded_start TEXT,
    recorded_end TEXT,
    created_at TEXT NOT NULL
);

-- 定位版本：把某个锚点（校准/里程零点）固化为可引用的版本；替换校准只能新建版本 --
CREATE TABLE IF NOT EXISTS loc_versions (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    calibration_id TEXT NOT NULL REFERENCES calibrations(id),
    anchor_m REAL NOT NULL,               -- 锚点处管线里程（起点井为 0）
    anchor_pulses INTEGER NOT NULL,       -- 锚点对应编码器读数
    anchor_source TEXT NOT NULL,          -- start_manhole|gps|manual
    created_at TEXT NOT NULL,
    supersedes_id TEXT REFERENCES loc_versions(id),
    note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_locv_task ON loc_versions(task_id);

-- 分段文件：同一盘视频被不同设备/批次分段上传 --------------------------------
CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    upload_batch TEXT NOT NULL,
    filename TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    device_clock_start TEXT,
    device_clock_end TEXT,
    received_at TEXT NOT NULL,
    UNIQUE(task_id, sha256)               -- 重复上传幂等的核心约束
);

-- 分段上传时上报的编码器原始读数（里程事实只在合并阶段按定位版本换算）-----------
CREATE TABLE IF NOT EXISTS segment_raw_readings (
    segment_id TEXT PRIMARY KEY REFERENCES segments(id),
    pulse_start INTEGER,
    pulse_end INTEGER,
    updated_at TEXT NOT NULL
);

-- 合并作业：一个分段最多一个活跃作业（重复上传复用）---------------------------
CREATE TABLE IF NOT EXISTS merge_jobs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    segment_id TEXT NOT NULL REFERENCES segments(id),
    status TEXT NOT NULL,                 -- queued|paused|running|blocked|done|failed
    stage TEXT NOT NULL,                  -- validate|anchor|convert|conflict|index
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    prereq_segment_id TEXT REFERENCES segments(id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON merge_jobs(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_segment
    ON merge_jobs(segment_id)
    WHERE status IN ('queued','paused','running','blocked');

-- 作业与所用定位版本的绑定（入队时锁定，版本不可变）-----------------------------
CREATE TABLE IF NOT EXISTS merge_job_versions (
    merge_job_id TEXT PRIMARY KEY REFERENCES merge_jobs(id),
    loc_version_id TEXT NOT NULL REFERENCES loc_versions(id)
);

-- 全局合并开关（长时间任务暂停/继续）------------------------------------------
CREATE TABLE IF NOT EXISTS merge_control (
    id INTEGER PRIMARY KEY CHECK (id=1),
    paused INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

-- 分段里程区间：仅在合并成功后写入，且必须带定位版本 ---------------------------
CREATE TABLE IF NOT EXISTS segment_ranges (
    segment_id TEXT PRIMARY KEY REFERENCES segments(id),
    loc_version_id TEXT NOT NULL REFERENCES loc_versions(id),
    start_m REAL NOT NULL,
    end_m REAL NOT NULL,
    start_pulses INTEGER NOT NULL,
    end_pulses INTEGER NOT NULL,
    merged_at TEXT NOT NULL,
    CHECK (end_m >= start_m)
);

-- 人工复核：缺失片段 / 坐标冲突 / 校验失败，绝不静默拼接 -----------------------
CREATE TABLE IF NOT EXISTS reviews (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    kind TEXT NOT NULL,                   -- missing_gap|coordinate_conflict|checksum|range_overflow
    status TEXT NOT NULL DEFAULT 'open',  -- open|resolved|dismissed
    detail_json TEXT NOT NULL,
    related_segment_id TEXT REFERENCES segments(id),
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_reviews_task ON reviews(task_id, status);

-- 缺陷人工标记 ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS defects (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    code TEXT NOT NULL,                   -- 缺陷类型码（如 CX-破裂）
    severity TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_defects_task ON defects(task_id);

-- 缺陷定位记录：同一条缺陷可在不同定位版本下复算；已发布的记录不可变 ------------
CREATE TABLE IF NOT EXISTS defect_sightings (
    id TEXT PRIMARY KEY,
    defect_id TEXT NOT NULL REFERENCES defects(id),
    loc_version_id TEXT NOT NULL REFERENCES loc_versions(id),
    segment_id TEXT NOT NULL REFERENCES segments(id),
    file_offset_s REAL NOT NULL,
    pulse_reading INTEGER NOT NULL,
    distance_m REAL NOT NULL,             -- 换算结果（沿管线里程）
    calc_json TEXT NOT NULL,              -- 完整里程换算过程
    state TEXT NOT NULL DEFAULT 'candidate', -- candidate|confirmed|superseded
    published INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(defect_id, loc_version_id)
);
CREATE INDEX IF NOT EXISTS idx_sightings_segment ON defect_sightings(segment_id);

-- 发布报告：钉住具体 sighting；被引用的定位记录进入冻结状态 ---------------------
CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft', -- draft|published|withdrawn
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT
);

CREATE TABLE IF NOT EXISTS report_items (
    report_id TEXT NOT NULL REFERENCES reports(id),
    sighting_id TEXT NOT NULL REFERENCES defect_sightings(id),
    PRIMARY KEY (report_id, sighting_id)
);

-- 发布版本：缺陷对外发布的不可变快照 ------------------------------------------
CREATE TABLE IF NOT EXISTS releases (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    report_id TEXT NOT NULL REFERENCES reports(id),
    version_label TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',-- active|retracted
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_release_label
    ON releases(task_id, version_label);

CREATE TABLE IF NOT EXISTS release_items (
    release_id TEXT NOT NULL REFERENCES releases(id),
    sighting_id TEXT NOT NULL REFERENCES defect_sightings(id),
    PRIMARY KEY (release_id, sighting_id)
);

-- 影像附件：原始片段仅内部角色可见；脱敏截图可对监管发布 ------------------------
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    sighting_id TEXT NOT NULL REFERENCES defect_sightings(id),
    kind TEXT NOT NULL,                   -- raw_frame|redacted
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_assets_sighting ON assets(sighting_id);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # ---- 基础工具 ----------------------------------------------------------

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    @staticmethod
    def row(r: sqlite3.Row | None) -> dict | None:
        return dict(r) if r is not None else None

    def get(self, table: str, row_id: str) -> dict:
        r = self.conn.execute(
            f"SELECT * FROM {table} WHERE id=?", (row_id,)
        ).fetchone()
        if r is None:
            raise NotFound(f"{table} {row_id}")
        return dict(r)

    # ---- 管线 / 设备 --------------------------------------------------------

    def create_pipeline(self, code, start_manhole, end_manhole, length_m) -> dict:
        with self.lock:
            pid = new_id("pipe")
            self.conn.execute(
                "INSERT INTO pipelines VALUES (?,?,?,?,?,?)",
                (pid, code, start_manhole, end_manhole, length_m, now()),
            )
            return self.get("pipelines", pid)

    def create_device(self, code, model, nominal_wheel_mm) -> dict:
        with self.lock:
            did = new_id("dev")
            self.conn.execute(
                "INSERT INTO devices VALUES (?,?,?,?,?)",
                (did, code, model, nominal_wheel_mm, now()),
            )
            return self.get("devices", did)

    # ---- 校准（只增不改）----------------------------------------------------

    def add_calibration(self, device_id, wheel_diameter_mm, pulses_per_rev,
                        calibrated_at, note="", supersedes_id=None) -> dict:
        with self.lock:
            self.get("devices", device_id)
            if supersedes_id:
                old = self.get("calibrations", supersedes_id)
                if old["device_id"] != device_id:
                    raise Conflict("supersedes 校准属于其他设备")
            seq = self.conn.execute(
                "SELECT COALESCE(MAX(device_seq),0)+1 FROM calibrations WHERE device_id=?",
                (device_id,),
            ).fetchone()[0]
            cid = new_id("cal")
            self.conn.execute(
                "INSERT INTO calibrations VALUES (?,?,?,?,?,?,?,?,?)",
                (cid, device_id, wheel_diameter_mm, pulses_per_rev,
                 calibrated_at, now(), note, supersedes_id, seq),
            )
            return self.get("calibrations", cid)

    # ---- 任务 ---------------------------------------------------------------

    def create_task(self, pipeline_id, device_id, code,
                    recorded_start=None, recorded_end=None) -> dict:
        with self.lock:
            self.get("pipelines", pipeline_id)
            self.get("devices", device_id)
            tid = new_id("task")
            self.conn.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
                (tid, pipeline_id, device_id, code, "planned",
                 recorded_start, recorded_end, now()),
            )
            return self.get("tasks", tid)

    def set_task_status(self, task_id: str, status: str) -> dict:
        with self.lock:
            self.get("tasks", task_id)
            self.conn.execute(
                "UPDATE tasks SET status=? WHERE id=?", (status, task_id)
            )
            return self.get("tasks", task_id)

    def create_loc_version(self, task_id, calibration_id, anchor_m, anchor_pulses,
                           anchor_source, note="", supersedes_id=None) -> dict:
        with self.lock:
            task = self.get("tasks", task_id)
            cal = self.get("calibrations", calibration_id)
            if cal["device_id"] != task["device_id"]:
                raise Conflict("校准设备与任务设备不一致")
            if supersedes_id:
                old = self.get("loc_versions", supersedes_id)
                if old["task_id"] != task_id:
                    raise Conflict("supersedes 定位版本属于其他任务")
            vid = new_id("locv")
            self.conn.execute(
                "INSERT INTO loc_versions VALUES (?,?,?,?,?,?,?,?,?)",
                (vid, task_id, calibration_id, anchor_m, anchor_pulses,
                 anchor_source, now(), supersedes_id, note),
            )
            return self.get("loc_versions", vid)

    # ---- 分段上传（幂等）----------------------------------------------------

    def upload_segment(self, task_id, upload_batch, filename, sha256, size_bytes,
                       device_clock_start=None, device_clock_end=None,
                       pulse_start=None, pulse_end=None) -> tuple[dict, bool]:
        """返回 (segment, created)。重复上传复用既有记录与作业。

        脉冲读数作为“设备声称的里程原始量”存放在独立元数据表，
        由合并阶段在特定定位版本下换算，避免上传时偷偷拼接里程。
        """
        with self.lock:
            self.get("tasks", task_id)
            existing = self.conn.execute(
                "SELECT * FROM segments WHERE task_id=? AND sha256=?",
                (task_id, sha256),
            ).fetchone()
            if existing is not None:
                seg = dict(existing)
                # 重复上传只允许补传编码器脉冲读数；文件名/时钟/校验和等文件事实不改写
                self.conn.execute(
                    "UPDATE segment_raw_readings SET pulse_start=COALESCE(?,pulse_start),"
                    "pulse_end=COALESCE(?,pulse_end), updated_at=? WHERE segment_id=?",
                    (pulse_start, pulse_end, now(), seg["id"]),
                )
                return seg, False
            sid = new_id("seg")
            self.conn.execute(
                "INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?)",
                (sid, task_id, upload_batch, filename, sha256, size_bytes,
                 device_clock_start, device_clock_end, now()),
            )
            self.conn.execute(
                "INSERT INTO segment_raw_readings VALUES (?,?,?,?)",
                (sid, pulse_start, pulse_end, now()),
            )
            return self.get("segments", sid), True

    def enqueue_merge(self, segment_id, loc_version_id, prereq_segment_id=None) -> dict:
        with self.lock:
            seg = self.get("segments", segment_id)
            lv = self.get("loc_versions", loc_version_id)
            if lv["task_id"] != seg["task_id"]:
                raise Conflict("定位版本与分段不属于同一任务")
            if prereq_segment_id:
                pre = self.get("segments", prereq_segment_id)
                if pre["task_id"] != seg["task_id"]:
                    raise Conflict("前置分段不属于同一任务")
            active = self.conn.execute(
                "SELECT id FROM merge_jobs WHERE segment_id=? "
                "AND status IN ('queued','paused','running','blocked')",
                (segment_id,),
            ).fetchone()
            if active:
                return self.get("merge_jobs", active["id"])
            # 幂等：同一分段 + 同一定位版本已经合并成功，直接复用既有作业
            done = self.conn.execute(
                "SELECT mj.id FROM merge_jobs mj JOIN merge_job_versions mv "
                "ON mv.merge_job_id=mj.id WHERE mj.segment_id=? "
                "AND mv.loc_version_id=? AND mj.status='done'",
                (segment_id, loc_version_id),
            ).fetchone()
            if done:
                return self.get("merge_jobs", done["id"])
            jid = new_id("job")
            ts = now()
            self.conn.execute(
                "INSERT INTO merge_jobs VALUES (?,?,?,?,?,?,?,?,?,?)",
                (jid, seg["task_id"], segment_id, "queued", "validate", 0,
                 "", prereq_segment_id, ts, ts),
            )
            self.conn.execute(
                "INSERT INTO merge_job_versions VALUES (?,?)", (jid, loc_version_id)
            )
            return self.get("merge_jobs", jid)

    def job_loc_version(self, job_id: str) -> str:
        return self.conn.execute(
            "SELECT loc_version_id FROM merge_job_versions WHERE merge_job_id=?",
            (job_id,),
        ).fetchone()[0]

    # ---- 作业状态流转（worker 调用，全部落盘，重启可恢复）--------------------

    def claim_next_job(self) -> dict | None:
        with self.lock:
            if self.controls_paused():
                return None
            # 一个作业因前置缺失被 block 时，队列里后续作业仍应继续被领取
            while True:
                row = self.conn.execute(
                    "SELECT * FROM merge_jobs WHERE status='queued' "
                    "ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row is None:
                    return None
                job = dict(row)
                blocked = False
                if job["prereq_segment_id"]:
                    pre_job = self.conn.execute(
                        "SELECT id FROM merge_jobs WHERE segment_id=?",
                        (job["prereq_segment_id"],),
                    ).fetchone()
                    pre_done = self.conn.execute(
                        "SELECT 1 FROM segment_ranges WHERE segment_id=?",
                        (job["prereq_segment_id"],),
                    ).fetchone()
                    if not pre_done:
                        # 人工已就该断点填写处理结论（有 resolved 复核单）才放行，
                        # 否则一律阻塞 + 开复核，绝不静默拼接
                        accepted = self.conn.execute(
                            "SELECT 1 FROM reviews WHERE related_segment_id=? "
                            "AND status='resolved' "
                            "AND kind IN ('missing_gap','prerequisite_pending')",
                            (job["segment_id"],),
                        ).fetchone()
                        if not accepted:
                            self._block_job(job, "prerequisite_missing",
                                            "前置分段尚未成功合并，等待人工复核")
                            kind = "prerequisite_pending" if pre_job else "missing_gap"
                            self.open_review(
                                job["task_id"], kind,
                                {"segment_id": job["segment_id"],
                                 "prereq_segment_id": job["prereq_segment_id"],
                                 "reason": "前置分段缺失或未合并，里程链断开"},
                                related_segment_id=job["segment_id"],
                            )
                            blocked = True
                if blocked:
                    continue
                self.conn.execute(
                    "UPDATE merge_jobs SET status='running', attempts=attempts+1, "
                    "updated_at=? WHERE id=?", (now(), job["id"])
                )
                return self.get("merge_jobs", job["id"])

    def update_job_stage(self, job_id: str, stage: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE merge_jobs SET stage=?, updated_at=? WHERE id=?",
                (stage, now(), job_id),
            )

    def _block_job(self, job: dict, error: str, detail: str) -> None:
        self.conn.execute(
            "UPDATE merge_jobs SET status='blocked', last_error=?, updated_at=? WHERE id=?",
            (f"{error}: {detail}", now(), job["id"]),
        )

    def fail_job(self, job_id: str, stage: str, error: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE merge_jobs SET status='failed', stage=?, last_error=?, updated_at=? "
                "WHERE id=?", (stage, error, now(), job_id)
            )

    def pause_job(self, job_id: str) -> dict:
        with self.lock:
            job = self.get("merge_jobs", job_id)
            if job["status"] not in ("queued", "blocked"):
                raise Conflict(f"作业处于 {job['status']}，不能暂停")
            self.conn.execute(
                "UPDATE merge_jobs SET status='paused', updated_at=? WHERE id=?",
                (now(), job_id),
            )
            return self.get("merge_jobs", job_id)

    def resume_job(self, job_id: str) -> dict:
        with self.lock:
            job = self.get("merge_jobs", job_id)
            if job["status"] not in ("paused", "blocked", "failed"):
                raise Conflict(f"作业处于 {job['status']}，不能继续")
            self.conn.execute(
                "UPDATE merge_jobs SET status='queued', updated_at=? WHERE id=?",
                (now(), job_id),
            )
            return self.get("merge_jobs", job_id)

    def set_paused(self, paused: bool) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO merge_control(id, paused, updated_at) VALUES(1,?,?) "
                "ON CONFLICT(id) DO UPDATE SET paused=excluded.paused, updated_at=excluded.updated_at",
                (1 if paused else 0, now()),
            )

    def controls_paused(self) -> bool:
        r = self.conn.execute(
            "SELECT paused FROM merge_control WHERE id=1"
        ).fetchone()
        return bool(r and r[0])

    # ---- 复核 ---------------------------------------------------------------

    def open_review(self, task_id, kind, detail: dict,
                    related_segment_id=None) -> dict:
        with self.lock:
            # 同一分段同一类问题在未处理前只保留一张复核单（重复拦截不刷屏）
            if related_segment_id:
                existing = self.conn.execute(
                    "SELECT id FROM reviews WHERE related_segment_id=? "
                    "AND kind=? AND status='open'",
                    (related_segment_id, kind),
                ).fetchone()
                if existing:
                    return self.get("reviews", existing["id"])
            rid = new_id("rev")
            self.conn.execute(
                "INSERT INTO reviews VALUES (?,?,?,?,?,?,?,?,?)",
                (rid, task_id, kind, "open", json.dumps(detail, ensure_ascii=False),
                 related_segment_id, now(), None, ""),
            )
            return self.get("reviews", rid)

    def resolve_review(self, review_id, resolution) -> dict:
        with self.lock:
            self.get("reviews", review_id)
            self.conn.execute(
                "UPDATE reviews SET status='resolved', resolved_at=?, resolution=? WHERE id=?",
                (now(), resolution, review_id),
            )
            return self.get("reviews", review_id)

    def list_reviews(self, task_id=None, status=None) -> list[dict]:
        sql, params = "SELECT * FROM reviews", []
        where = []
        if task_id:
            where.append("task_id=?")
            params.append(task_id)
        if status:
            where.append("status=?")
            params.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at"
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params)]

    # ---- 合并结果 ------------------------------------------------------------

    def write_segment_range(self, job_id, loc_version_id, start_m, end_m,
                            start_pulses, end_pulses) -> None:
        with self.lock:
            job = self.get("merge_jobs", job_id)
            seg_id = job["segment_id"]
            old = self.conn.execute(
                "SELECT * FROM segment_ranges WHERE segment_id=?", (seg_id,)
            ).fetchone()
            referenced = self.conn.execute(
                "SELECT COUNT(1) FROM defect_sightings WHERE segment_id=?",
                (seg_id,),
            ).fetchone()[0]
            if old and referenced:
                raise Conflict(
                    "该分段区间已被缺陷标记引用，不能随重新合并改写；"
                    "请新建定位版本并复算"
                )
            self.conn.execute(
                "INSERT INTO segment_ranges VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(segment_id) DO UPDATE SET "
                "loc_version_id=excluded.loc_version_id, start_m=excluded.start_m, "
                "end_m=excluded.end_m, start_pulses=excluded.start_pulses, "
                "end_pulses=excluded.end_pulses, merged_at=excluded.merged_at",
                (seg_id, loc_version_id, start_m, end_m,
                 start_pulses, end_pulses, now()),
            )
            self.conn.execute(
                "UPDATE merge_jobs SET status='done', stage='index', last_error='', "
                "updated_at=? WHERE id=?", (now(), job_id)
            )

    # ---- 缺陷标记 ------------------------------------------------------------

    def create_defect(self, task_id, code, severity, created_by) -> dict:
        with self.lock:
            self.get("tasks", task_id)
            did = new_id("def")
            self.conn.execute(
                "INSERT INTO defects VALUES (?,?,?,?,?,?)",
                (did, task_id, code, severity, created_by, now()),
            )
            return self.get("defects", did)

    def segment_range(self, segment_id: str) -> dict | None:
        r = self.conn.execute(
            "SELECT * FROM segment_ranges WHERE segment_id=?", (segment_id,)
        ).fetchone()
        return dict(r) if r else None

    def add_sighting(self, defect_id, loc_version_id, segment_id, file_offset_s,
                     pulse_reading, distance_m, calc: dict) -> dict:
        with self.lock:
            defect = self.get("defects", defect_id)
            seg = self.get("segments", segment_id)
            lv = self.get("loc_versions", loc_version_id)
            if seg["task_id"] != defect["task_id"] or lv["task_id"] != defect["task_id"]:
                raise Conflict("缺陷、分段、定位版本必须属于同一任务")
            rng = self.conn.execute(
                "SELECT * FROM segment_ranges WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if rng is None:
                raise Conflict("分段尚未合并成功，不能在其上标记缺陷")
            if not (rng["start_m"] <= distance_m <= rng["end_m"]):
                raise Conflict(
                    f"换算里程 {distance_m} 落不进分段区间 "
                    f"[{rng['start_m']},{rng['end_m']}]，应进入复核而非强行标记"
                )
            sid = new_id("sgt")
            self.conn.execute(
                "INSERT INTO defect_sightings VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (sid, defect_id, loc_version_id, segment_id, file_offset_s,
                 pulse_reading, distance_m, json.dumps(calc, ensure_ascii=False),
                 "candidate", 0, now()),
            )
            return self.get_sighting(sid)

    def get_sighting(self, sighting_id: str) -> dict:
        r = self.conn.execute(
            "SELECT * FROM defect_sightings WHERE id=?", (sighting_id,)
        ).fetchone()
        if r is None:
            raise NotFound(f"sighting {sighting_id}")
        d = dict(r)
        d["calc"] = json.loads(d["calc_json"])
        return d

    def confirm_sighting(self, sighting_id) -> dict:
        with self.lock:
            s = self.get_sighting(sighting_id)
            if s["state"] == "superseded":
                raise Conflict("已被新定位版本替代的记录不能确认")
            self.conn.execute(
                "UPDATE defect_sightings SET state='confirmed' WHERE id=?",
                (sighting_id,),
            )
            return self.get_sighting(sighting_id)

    def supersede_sightings_for_version(self, defect_id, new_loc_version_id) -> int:
        """新定位版本复算后，旧版本下未发布的 candidate/confirmed 记录标记 superseded。

        已发布（被报告引用）的记录保持冻结，不能随旧数据改变。
        """
        with self.lock:
            cur = self.conn.execute(
                "UPDATE defect_sightings SET state='superseded' "
                "WHERE defect_id=? AND loc_version_id<>? AND published=0 "
                "AND state IN ('candidate','confirmed')",
                (defect_id, new_loc_version_id),
            )
            return cur.rowcount

    # ---- 报告与发布（冻结引用）-----------------------------------------------

    def create_report(self, task_id, title, created_by) -> dict:
        with self.lock:
            self.get("tasks", task_id)
            rid = new_id("rpt")
            self.conn.execute(
                "INSERT INTO reports VALUES (?,?,?,?,?,?,?)",
                (rid, task_id, title, "draft", created_by, now(), None),
            )
            return self.get("reports", rid)

    def attach_sighting_to_report(self, report_id, sighting_id) -> None:
        with self.lock:
            report = self.get("reports", report_id)
            sighting = self.get_sighting(sighting_id)
            defect = self.get("defects", sighting["defect_id"])
            if defect["task_id"] != report["task_id"]:
                raise Conflict("报告与缺陷不属于同一任务")
            if report["status"] != "draft":
                raise Conflict("只有草稿报告可以增减条目")
            self.conn.execute(
                "INSERT OR IGNORE INTO report_items VALUES (?,?)",
                (report_id, sighting_id),
            )

    def publish_report(self, report_id) -> dict:
        with self.lock:
            report = self.get("reports", report_id)
            if report["status"] != "draft":
                raise Conflict(f"报告处于 {report['status']}，不能发布")
            items = self.conn.execute(
                "SELECT sighting_id FROM report_items WHERE report_id=?",
                (report_id,),
            ).fetchall()
            if not items:
                raise Conflict("空报告不能发布")
            for (sid,) in items:
                s = self.get_sighting(sid)
                if s["state"] != "confirmed":
                    raise Conflict(f"sighting {sid} 未确认，不能进入发布报告")
            self.conn.execute(
                "UPDATE reports SET status='published', published_at=? WHERE id=?",
                (now(), report_id),
            )
            for (sid,) in items:
                self.conn.execute(
                    "UPDATE defect_sightings SET published=1 WHERE id=?", (sid,)
                )
            return self.get("reports", report_id)

    def withdraw_report(self, report_id) -> dict:
        with self.lock:
            report = self.get("reports", report_id)
            if report["status"] != "published":
                raise Conflict("只有已发布报告可以撤回")
            self.conn.execute(
                "UPDATE reports SET status='withdrawn' WHERE id=?", (report_id,)
            )
            return self.get("reports", report_id)

    def create_release(self, task_id, report_id, version_label) -> dict:
        with self.lock:
            report = self.get("reports", report_id)
            if report["task_id"] != task_id:
                raise Conflict("报告不属于该任务")
            if report["status"] != "published":
                raise Conflict("只能对已发布报告建立发布版本")
            rid = new_id("rel")
            ts = now()
            self.conn.execute(
                "INSERT INTO releases VALUES (?,?,?,?,?,?)",
                (rid, task_id, report_id, version_label, "active", ts),
            )
            for s in self.conn.execute(
                "SELECT sighting_id FROM report_items WHERE report_id=?",
                (report_id,),
            ):
                self.conn.execute(
                    "INSERT OR IGNORE INTO release_items VALUES (?,?)",
                    (rid, s[0]),
                )
            return self.get("releases", rid)

    def retract_release(self, release_id) -> dict:
        with self.lock:
            self.get("releases", release_id)
            self.conn.execute(
                "UPDATE releases SET status='retracted' WHERE id=?", (release_id,)
            )
            return self.get("releases", release_id)

    # ---- 附件 ----------------------------------------------------------------

    def add_asset(self, sighting_id, kind, sha256, size_bytes, storage_path) -> dict:
        with self.lock:
            self.get_sighting(sighting_id)
            if kind not in ("raw_frame", "redacted"):
                raise Conflict("附件类型必须是 raw_frame 或 redacted")
            aid = new_id("asset")
            self.conn.execute(
                "INSERT INTO assets VALUES (?,?,?,?,?,?,?)",
                (aid, sighting_id, kind, sha256, size_bytes, storage_path, now()),
            )
            return self.get("assets", aid)

    # ---- 查询：一条缺陷的完整索引 ---------------------------------------------

    def defect_detail(self, defect_id: str) -> dict:
        with self.lock:
            defect = self.get("defects", defect_id)
            task = self.get("tasks", defect["task_id"])
            pipeline = self.get("pipelines", task["pipeline_id"])
            sightings = []
            for r in self.conn.execute(
                "SELECT * FROM defect_sightings WHERE defect_id=? ORDER BY created_at",
                (defect_id,),
            ):
                s = dict(r)
                s["calc"] = json.loads(s.pop("calc_json"))
                seg = self.get("segments", s["segment_id"])
                lv = self.get("loc_versions", s["loc_version_id"])
                cal = self.get("calibrations", lv["calibration_id"])
                rng = self.conn.execute(
                    "SELECT * FROM segment_ranges WHERE segment_id=?",
                    (s["segment_id"],),
                ).fetchone()
                report_rows = self.conn.execute(
                    "SELECT r.* FROM reports r JOIN report_items ri ON ri.report_id=r.id "
                    "WHERE ri.sighting_id=?", (s["id"],),
                ).fetchall()
                release_rows = self.conn.execute(
                    "SELECT rel.* FROM releases rel JOIN release_items ri "
                    "ON ri.release_id=rel.id WHERE ri.sighting_id=?", (s["id"],),
                ).fetchall()
                assets = [dict(a) for a in self.conn.execute(
                    "SELECT * FROM assets WHERE sighting_id=?", (s["id"],),
                ).fetchall()]
                sightings.append({
                    "sighting": {k: s[k] for k in (
                        "id", "file_offset_s", "pulse_reading", "distance_m",
                        "state", "published")},
                    "segment": {k: seg[k] for k in (
                        "id", "filename", "upload_batch", "sha256",
                        "device_clock_start", "device_clock_end", "received_at")},
                    "segment_range": dict(rng) if rng else None,
                    "loc_version": {k: lv[k] for k in (
                        "id", "anchor_m", "anchor_pulses", "anchor_source",
                        "created_at", "supersedes_id")},
                    "calibration": {k: cal[k] for k in (
                        "id", "wheel_diameter_mm", "pulses_per_rev",
                        "calibrated_at", "device_seq", "supersedes_id")},
                    "conversion": s["calc"],
                    "assets": assets,
                    "reports": [{k: rr[k] for k in ("id", "title", "status")}
                                for rr in report_rows],
                    "releases": [{k: rr[k] for k in ("id", "version_label", "status")}
                                 for rr in release_rows],
                })
            return {
                "defect": defect,
                "task": {k: task[k] for k in ("id", "code", "status")},
                "pipeline": {k: pipeline[k] for k in (
                    "id", "code", "start_manhole", "end_manhole", "length_m")},
                "sightings": sightings,
            }

    def list_jobs(self, task_id=None) -> list[dict]:
        with self.lock:
            sql = "SELECT * FROM merge_jobs"
            params = []
            if task_id:
                sql += " WHERE task_id=?"
                params.append(task_id)
            sql += " ORDER BY created_at"
            return [dict(r) for r in self.conn.execute(sql, params)]


def open_store(path: str | Path) -> Store:
    return Store(path)
