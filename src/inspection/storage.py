"""SQLite 持久化层。

设计要点：

* 分段文件以 ``(inspection_id, seq)`` 为业务幂等键，重复上传直接命中旧行，
  绝不重复落库；校验和不一致则拒绝。
* 校准版本（calibration_version）只增不改：替换轮径参数只会生成新版本，
  旧版本及其定位结果永久保留。
* 缺陷一旦被发布报告引用即 *冻结*：报告内保存快照 JSON，后续重算、
  复核改判都不会影响报告引用的坐标与换算过程。
* 合并队列、复核项均为可持久化状态机；服务重启后暂停的任务可继续，
  失败原因保留可见。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable

from .clock import now_iso

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS inspections (
    id TEXT PRIMARY KEY,
    pipeline_code TEXT NOT NULL,
    start_manhole TEXT NOT NULL,
    end_manhole TEXT NOT NULL,
    nominal_length_m REAL,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',   -- active|paused|merged|closed
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    nominal_wheel_diameter_mm REAL NOT NULL,
    encoder_ppr INTEGER NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

-- 设备校准版本：只追加。supersedes 指向被替换的版本。
CREATE TABLE IF NOT EXISTS calibration_versions (
    version_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    wheel_diameter_mm REAL NOT NULL,
    effective_from_m REAL NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    supersedes TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(device_id, seq)
);

-- 每次检测实际生效的校准版本序列（一条检测可跨越多次校准）。
CREATE TABLE IF NOT EXISTS inspection_calibrations (
    inspection_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    apply_from_m REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (inspection_id, version_id)
);

CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    blob_path TEXT NOT NULL,
    -- 上传设备自报的编码器里程区间（原始读数，毫米脉冲计数换算前）
    encoder_start REAL NOT NULL,
    encoder_end REAL NOT NULL,
    duration_s REAL,
    uploaded_at TEXT NOT NULL,
    UNIQUE(inspection_id, seq),
    UNIQUE(inspection_id, sha256)
);

-- 定位版本：某个校准版本下，全部分段的里程区间换算结果。
-- 重新合并 = 追加新行，旧行保留，保证历史可追溯。
-- provisional=1 表示该分段位于尚未解决的阻断性复核项之后，坐标为推算值。
CREATE TABLE IF NOT EXISTS positioning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inspection_id TEXT NOT NULL,
    calibration_version_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    chainage_start_m REAL NOT NULL,
    chainage_end_m REAL NOT NULL,
    raw_start REAL NOT NULL,
    raw_end REAL NOT NULL,
    scale REAL NOT NULL,
    provisional INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(calibration_version_id, segment_id)
);

-- 合并任务（长任务，可暂停/继续/重试），重启后可恢复。
CREATE TABLE IF NOT EXISTS merge_jobs (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL,
    calibration_version_id TEXT NOT NULL,
    status TEXT NOT NULL,            -- queued|running|paused|succeeded|failed
    progress INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    fail_reason TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 复核工作流：缺失片段、里程冲突、校验和冲突都进入这里，绝不静默拼接。
CREATE TABLE IF NOT EXISTS reviews (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL,
    kind TEXT NOT NULL,              -- missing_segment|overlap|chainage_conflict|checksum_conflict
    severity TEXT NOT NULL DEFAULT 'blocker',  -- blocker|warning
    status TEXT NOT NULL DEFAULT 'open',       -- open|resolved|rejected
    dedup_key TEXT,
    detail_json TEXT NOT NULL,
    related_segment_ids TEXT NOT NULL DEFAULT '',
    raised_by TEXT NOT NULL DEFAULT 'system',
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT,
    UNIQUE(inspection_id, dedup_key)
);

CREATE TABLE IF NOT EXISTS defects (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    segment_id TEXT NOT NULL,
    calibration_version_id TEXT NOT NULL,
    raw_offset REAL NOT NULL,        -- 分段内原始编码器读数
    chainage_m REAL NOT NULL,        -- 该版本下的管线里程（米）
    scale REAL NOT NULL,             -- 当时的换算系数，留痕
    snap_state TEXT NOT NULL DEFAULT 'draft',  -- draft|confirmed|frozen|superseded
    frozen_by_report_id TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    inspection_id TEXT NOT NULL,
    calibration_version_id TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',      -- draft|published
    snapshot_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT
);

-- 脱敏截图：原始影像经脱敏处理后的可对外版本，带独立校验和。
CREATE TABLE IF NOT EXISTS redacted_snapshots (
    id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    blob_path TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_seg_insp ON segments(inspection_id);
CREATE INDEX IF NOT EXISTS idx_pos_insp ON positioning(inspection_id);
CREATE INDEX IF NOT EXISTS idx_review_insp ON reviews(inspection_id, status);
CREATE INDEX IF NOT EXISTS idx_defect_insp ON defects(inspection_id);
"""


class Store:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._migrate()
            self.conn.commit()

    def _migrate(self) -> None:
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(positioning)")}
        if "provisional" not in cols:
            self.conn.execute(
                "ALTER TABLE positioning ADD COLUMN provisional INTEGER NOT NULL DEFAULT 0"
            )
        review_cols = {r[1] for r in self.conn.execute("PRAGMA table_info(reviews)")}
        if review_cols and "dedup_key" not in review_cols:
            self.conn.execute("ALTER TABLE reviews ADD COLUMN dedup_key TEXT")
        # 旧数据回填
        for row in self.conn.execute(
            "SELECT id, detail_json FROM reviews WHERE dedup_key IS NULL"
        ).fetchall():
            try:
                key = json.loads(row[1]).get("dedup_key")
            except json.JSONDecodeError:
                key = None
            if key:
                self.conn.execute(
                    "UPDATE reviews SET dedup_key=? WHERE id=?", (key, row[0])
                )
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_review_dedup "
            "ON reviews(inspection_id, dedup_key) WHERE dedup_key IS NOT NULL"
        )

    # ---- 基础工具 -------------------------------------------------------

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self.conn.execute(sql, tuple(params))
            self.conn.commit()
            return cur

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(sql, tuple(params)).fetchone()

    @staticmethod
    def row_to_dict(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        return {k: row[k] for k in row.keys()}

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # ---- 恢复支持 -------------------------------------------------------

    def recover_jobs_on_startup(self) -> None:
        """进程崩溃时 running 任务按 paused 恢复；queued 任务保留待继续。"""
        with self._lock:
            self.conn.execute(
                "UPDATE merge_jobs SET status='paused', updated_at=? "
                "WHERE status IN ('running')",
                (now_iso(),),
            )
            self.conn.commit()

    # ---- 便捷读取 -------------------------------------------------------

    def get_inspection(self, inspection_id: str) -> dict | None:
        return self.row_to_dict(
            self.query_one("SELECT * FROM inspections WHERE id=?", (inspection_id,))
        )

    def get_device(self, device_id: str) -> dict | None:
        return self.row_to_dict(
            self.query_one("SELECT * FROM devices WHERE id=?", (device_id,))
        )

    def get_segment(self, segment_id: str) -> dict | None:
        return self.row_to_dict(
            self.query_one("SELECT * FROM segments WHERE id=?", (segment_id,))
        )

    def get_segments(self, inspection_id: str) -> list[dict]:
        return [self.row_to_dict(r) for r in self.query(
            "SELECT * FROM segments WHERE inspection_id=? ORDER BY seq", (inspection_id,)
        )]

    def get_segment_at_seq(self, inspection_id: str, seq: int) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM segments WHERE inspection_id=? AND seq=?", (inspection_id, seq)
        ))

    def get_calibration_version(self, version_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM calibration_versions WHERE version_id=?", (version_id,)
        ))

    def get_latest_calibration(self, device_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM calibration_versions WHERE device_id=? ORDER BY seq DESC LIMIT 1",
            (device_id,),
        ))

    def get_calibration_versions(self, device_id: str) -> list[dict]:
        return [self.row_to_dict(r) for r in self.query(
            "SELECT * FROM calibration_versions WHERE device_id=? ORDER BY seq", (device_id,)
        )]

    def get_positioning(self, version_id: str, segment_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM positioning WHERE calibration_version_id=? AND segment_id=?",
            (version_id, segment_id),
        ))

    def all_positioning(self, inspection_id: str, version_id: str) -> list[dict]:
        return [self.row_to_dict(r) for r in self.query(
            "SELECT p.* FROM positioning p WHERE p.inspection_id=? "
            "AND p.calibration_version_id=? ORDER BY p.chainage_start_m",
            (inspection_id, version_id),
        )]

    def get_job(self, job_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM merge_jobs WHERE id=?", (job_id,)
        ))

    def get_latest_succeeded_job(self, inspection_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM merge_jobs WHERE inspection_id=? AND status='succeeded' "
            "ORDER BY updated_at DESC LIMIT 1", (inspection_id,)
        ))

    def resumeable_jobs(self) -> list[dict]:
        return [self.row_to_dict(r) for r in self.query(
            "SELECT * FROM merge_jobs WHERE status IN ('queued','paused','failed')"
        )]

    def get_open_reviews(self, inspection_id: str) -> list[dict]:
        return [self.row_to_dict(r) for r in self.query(
            "SELECT * FROM reviews WHERE inspection_id=? AND status='open' "
            "ORDER BY created_at", (inspection_id,)
        )]

    def get_defect(self, defect_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM defects WHERE id=?", (defect_id,)
        ))

    def get_report(self, report_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM reports WHERE id=?", (report_id,)
        ))

    def get_snapshot_for_segment(self, segment_id: str) -> dict | None:
        return self.row_to_dict(self.query_one(
            "SELECT * FROM redacted_snapshots WHERE segment_id=?", (segment_id,)
        ))


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def loads(value: str) -> Any:
    return json.loads(value)
