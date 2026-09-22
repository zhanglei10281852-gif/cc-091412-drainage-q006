"""核心业务服务：任务、校准、分段、合并队列、复核、缺陷、快照、报告发布。"""

from __future__ import annotations

import base64
import hashlib
import sqlite3
import threading
import uuid
from pathlib import Path

from .clock import now_iso
from .errors import (
    ChecksumMismatchError,
    ConflictError,
    NotFoundError,
    StateError,
    ValidationError,
)
from .positioning import compute_positioning, explain_point
from .storage import Store, dumps, loads


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class JobControl:
    """合并任务的进程内暂停信号（持久状态在 merge_jobs 表）。"""

    def __init__(self) -> None:
        self._pause: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def request_pause(self, job_id: str) -> None:
        with self._lock:
            self._pause.setdefault(job_id, threading.Event()).set()

    def clear(self, job_id: str) -> None:
        with self._lock:
            self._pause.pop(job_id, None)

    def is_pause_requested(self, job_id: str) -> bool:
        evt = self._pause.get(job_id)
        return evt is not None and evt.is_set()


class Service:
    def __init__(self, store: Store, blob_dir: Path):
        self.store = store
        self.blob_dir = blob_dir
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        self.jobs = JobControl()

    # ================= 设备与校准 =================

    def register_device(self, payload: dict) -> dict:
        device_id = payload.get("id")
        if not device_id:
            raise ValidationError("缺少设备 id")
        if self.store.get_device(device_id):
            raise ConflictError(f"设备 {device_id} 已存在")
        wheel = _require_positive(payload, "nominal_wheel_diameter_mm")
        ppr = _require_positive_int(payload, "encoder_ppr")
        self.store.execute(
            "INSERT INTO devices(id, model, nominal_wheel_diameter_mm, encoder_ppr, "
            "metadata_json, created_at) VALUES(?,?,?,?,?,?)",
            (
                device_id,
                payload.get("model", ""),
                wheel,
                ppr,
                dumps(payload.get("metadata", {})),
                now_iso(),
            ),
        )
        return self.store.get_device(device_id)

    def add_calibration(self, device_id: str, payload: dict) -> dict:
        """登记新校准版本。旧版本永不修改，新版本记录 supersedes。"""
        device = self.store.get_device(device_id)
        if device is None:
            raise NotFoundError(f"设备 {device_id} 不存在")
        wheel = _require_positive(payload, "wheel_diameter_mm")
        effective_from = float(payload.get("effective_from_m", 0))
        if effective_from < 0:
            raise ValidationError("effective_from_m 不能为负")
        previous = self.store.get_latest_calibration(device_id)
        seq = (previous["seq"] + 1) if previous else 1
        if previous and abs(previous["wheel_diameter_mm"] - wheel) < 1e-9:
            raise ConflictError(
                f"轮径 {wheel}mm 与当前版本 v{previous['seq']} 相同，无需新建校准版本"
            )
        version_id = new_id("cal")
        self.store.execute(
            "INSERT INTO calibration_versions(version_id, device_id, seq, "
            "wheel_diameter_mm, effective_from_m, note, supersedes, created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (
                version_id,
                device_id,
                seq,
                wheel,
                effective_from,
                payload.get("note", ""),
                previous["version_id"] if previous else None,
                now_iso(),
            ),
        )
        return self.store.get_calibration_version(version_id)

    # ================= 检测任务 =================

    def register_inspection(self, payload: dict) -> dict:
        insp_id = payload.get("id")
        if not insp_id:
            raise ValidationError("缺少检测任务 id")
        if self.store.get_inspection(insp_id):
            raise ConflictError(f"检测任务 {insp_id} 已存在")
        for key in ("pipeline_code", "start_manhole", "end_manhole", "device_id"):
            if not payload.get(key):
                raise ValidationError(f"缺少 {key}")
        if not self.store.get_device(payload["device_id"]):
            raise ValidationError(f"设备 {payload['device_id']} 未登记")
        ts = now_iso()
        self.store.execute(
            "INSERT INTO inspections(id, pipeline_code, start_manhole, end_manhole, "
            "nominal_length_m, device_id, status, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?, 'active', ?,?)",
            (
                insp_id,
                payload["pipeline_code"],
                payload["start_manhole"],
                payload["end_manhole"],
                payload.get("nominal_length_m"),
                payload["device_id"],
                ts,
                ts,
            ),
        )
        return self.store.get_inspection(insp_id)

    def set_inspection_status(self, insp_id: str, status: str) -> dict:
        insp = self._require_inspection(insp_id)
        if status not in ("active", "paused", "closed"):
            raise ValidationError(f"非法任务状态 {status}")
        self.store.execute(
            "UPDATE inspections SET status=?, updated_at=? WHERE id=?",
            (status, now_iso(), insp_id),
        )
        return self.store.get_inspection(insp_id)

    # ================= 分段上传（幂等） =================

    def upload_segment(self, insp_id: str, payload: dict) -> tuple[dict, bool]:
        """返回 (分段, 是否新建)。重复上传命中旧行，绝不重复落库。"""
        self._require_inspection(insp_id)
        seq = _require_nonnegative_int(payload, "seq")
        file_name = payload.get("file_name")
        if not file_name:
            raise ValidationError("缺少 file_name")
        sha = _require_sha256(payload)
        size = _require_nonnegative_int(payload, "size_bytes")
        enc_start = float(payload.get("encoder_start", 0))
        enc_end = float(payload.get("encoder_end", 0))
        if enc_end < enc_start:
            raise ValidationError("encoder_end 不能小于 encoder_start")

        existing = self.store.get_segment_at_seq(insp_id, seq)
        if existing is not None:
            # 同一序号重复上传：校验和必须一致，否则进冲突复核而非覆盖。
            if existing["sha256"] != sha:
                self._raise_review(
                    insp_id,
                    kind="checksum_conflict",
                    dedup_key=f"checksum:{seq}",
                    detail={
                        "dedup_key": f"checksum:{seq}",
                        "seq": seq,
                        "existing_sha256": existing["sha256"],
                        "incoming_sha256": sha,
                        "existing_file": existing["file_name"],
                        "incoming_file": file_name,
                        "reason": "同一分段序号重复上传但校验和不同，疑似换片/损坏，需人工核对",
                    },
                    related=[existing["id"]],
                )
                raise ChecksumMismatchError(
                    f"序号 {seq} 已存在不同内容的文件，已进入复核流程", "checksum_conflict"
                )
            return existing, False

        # 同内容文件挂到别的序号也是异常，拒绝并提示，不静默当作新片。
        dup = self.store.query_one(
            "SELECT * FROM segments WHERE inspection_id=? AND sha256=?", (insp_id, sha)
        )
        if dup is not None:
            raise ConflictError(
                f"相同校验和的文件已登记为序号 {dup['seq']}（{dup['file_name']}）"
            )

        seg_id = new_id("seg")
        blob_rel = f"{insp_id}/{seg_id}.bin"
        blob_path = self.blob_dir / blob_rel
        content_b64 = payload.get("content_base64")
        if content_b64 is not None:
            data = base64.b64decode(content_b64)
            if len(data) != size:
                raise ValidationError("size_bytes 与内容长度不符")
            _verify_sha256(data, sha)
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            blob_path.write_bytes(data)
        else:
            # 无内容时仅登记元数据（对象存储由外部同步）；合并时会再校验。
            blob_path.parent.mkdir(parents=True, exist_ok=True)

        self.store.execute(
            "INSERT INTO segments(id, inspection_id, seq, file_name, sha256, size_bytes, "
            "blob_path, encoder_start, encoder_end, duration_s, uploaded_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                seg_id,
                insp_id,
                seq,
                file_name,
                sha,
                size,
                blob_rel,
                enc_start,
                enc_end,
                payload.get("duration_s"),
                now_iso(),
            ),
        )
        self.store.execute(
            "UPDATE inspections SET updated_at=? WHERE id=?", (now_iso(), insp_id)
        )
        return self.store.get_segment(seg_id), True

    def add_redacted_snapshot(self, segment_id: str, payload: dict) -> tuple[dict, bool]:
        seg = self.store.get_segment(segment_id)
        if seg is None:
            raise NotFoundError("分段不存在")
        sha = _require_sha256(payload)
        existing = self.store.get_snapshot_for_segment(segment_id)
        if existing is not None:
            if existing["sha256"] == sha:
                return existing, False
            raise ConflictError("该分段已存在不同校验和的脱敏截图，请走复核替换")
        blob_rel = f"redacted/{segment_id}.png"
        blob_path = self.blob_dir / blob_rel
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        if payload.get("content_base64") is not None:
            data = base64.b64decode(payload["content_base64"])
            _verify_sha256(data, sha)
            blob_path.write_bytes(data)
        self.store.execute(
            "INSERT INTO redacted_snapshots(id, segment_id, sha256, blob_path, created_at) "
            "VALUES(?,?,?,?,?)",
            (new_id("snap"), segment_id, sha, blob_rel, now_iso()),
        )
        return self.store.get_snapshot_for_segment(segment_id), True

    # ================= 合并队列 =================

    def create_merge_job(self, insp_id: str, version_id: str | None = None) -> dict:
        insp = self._require_inspection(insp_id)
        if version_id is None:
            latest = self.store.get_latest_calibration(insp["device_id"])
            if latest is None:
                raise StateError("设备尚无校准版本，无法合并")
            version_id = latest["version_id"]
        version = self.store.get_calibration_version(version_id)
        if version is None or version["device_id"] != insp["device_id"]:
            raise ValidationError("校准版本不属于该检测设备")
        if not self.store.get_segments(insp_id):
            raise StateError("尚无分段文件，无法合并")

        active = self.store.query_one(
            "SELECT * FROM merge_jobs WHERE inspection_id=? AND calibration_version_id=? "
            "AND status IN ('queued','running','paused') ORDER BY created_at DESC LIMIT 1",
            (insp_id, version_id),
        )
        if active is not None:
            return self.store.get_job(active["id"])

        job_id = new_id("job")
        ts = now_iso()
        self.store.execute(
            "INSERT INTO merge_jobs(id, inspection_id, calibration_version_id, status, "
            "progress, total, created_at, updated_at) VALUES(?,?,?, 'queued', 0, ?, ?, ?)",
            (job_id, insp_id, version_id, len(self.store.get_segments(insp_id)), ts, ts),
        )
        job = self.store.get_job(job_id)
        self._spawn(job_id)
        return job

    def pause_merge_job(self, job_id: str) -> dict:
        job = self._require_job(job_id)
        if job["status"] not in ("queued", "running"):
            raise StateError(f"任务当前状态 {job['status']} 不可暂停")
        self.jobs.request_pause(job_id)
        # queued 任务可能尚未获得执行片，直接落 paused，worker 启动时会看到。
        if job["status"] == "queued":
            self._set_job_status(job_id, "paused")
        return self.store.get_job(job_id)

    def resume_merge_job(self, job_id: str) -> dict:
        job = self._require_job(job_id)
        if job["status"] not in ("paused", "failed", "queued"):
            raise StateError(f"任务当前状态 {job['status']} 不可继续")
        self.jobs.clear(job_id)
        self._set_job_status(job_id, "queued", fail_reason=None)
        self._spawn(job_id)
        return self.store.get_job(job_id)

    def retry_merge_job(self, job_id: str) -> dict:
        job = self._require_job(job_id)
        if job["status"] != "failed":
            raise StateError("仅失败任务可重试；暂停任务请使用继续")
        self.jobs.clear(job_id)
        self.store.execute(
            "UPDATE merge_jobs SET status='queued', progress=0, fail_reason=NULL, "
            "result_json=NULL, updated_at=? WHERE id=?",
            (now_iso(), job_id),
        )
        self._spawn(job_id)
        return self.store.get_job(job_id)

    def _spawn(self, job_id: str) -> None:
        if getattr(self, "run_inline", False):
            return  # 由 HTTP 层同步驱动，便于测试与单进程脚本
        thread = threading.Thread(
            target=self._run_job_safely, args=(job_id,), daemon=True, name=f"merge-{job_id}"
        )
        thread.start()

    def run_pending(self, job_id: str) -> None:
        """同步执行一个排队中的合并任务（内联模式/脚本使用）。"""
        self._run_job_safely(job_id)

    def _run_job_safely(self, job_id: str) -> None:
        try:
            self._run_merge_job(job_id)
        except Exception as exc:  # 失败原因必须持久可见
            self.store.execute(
                "UPDATE merge_jobs SET status='failed', fail_reason=?, updated_at=? WHERE id=?",
                (f"{type(exc).__name__}: {exc}", now_iso(), job_id),
            )

    def _run_merge_job(self, job_id: str) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            return
        if job["status"] == "paused":
            return
        self._set_job_status(job_id, "running")
        segments = self.store.get_segments(job["inspection_id"])
        total = len(segments)
        self.store.execute(
            "UPDATE merge_jobs SET total=?, updated_at=? WHERE id=?",
            (total, now_iso(), job_id),
        )

        # 分段校验阶段：逐片读取并核对校验和，支持暂停后从断点继续。
        start_index = job["progress"]
        for index in range(start_index, total):
            if self.jobs.is_pause_requested(job_id):
                self._set_job_status(job_id, "paused")
                return
            seg = segments[index]
            blob = self.blob_dir / seg["blob_path"]
            if not blob.exists():
                raise FileNotFoundError(
                    f"序号 {seg['seq']}（{seg['file_name']}）的影像文件缺失: {blob.name}"
                )
            digest = hashlib.sha256()
            with blob.open("rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    digest.update(chunk)
            if digest.hexdigest() != seg["sha256"]:
                raise ChecksumMismatchError(
                    f"序号 {seg['seq']} 校验和与上传登记不一致，文件可能损坏"
                )
            self.store.execute(
                "UPDATE merge_jobs SET progress=?, updated_at=? WHERE id=?",
                (index + 1, now_iso(), job_id),
            )

        # 换算、冲突检测与定位落库（仅基于已校验的分段元数据）
        result = self._recompute_positions(
            job["inspection_id"], job["calibration_version_id"], segments
        )

        self.store.execute(
            "UPDATE merge_jobs SET status='succeeded', progress=?, result_json=?, "
            "updated_at=? WHERE id=?",
            (total, dumps(result.to_jsonable()), now_iso(), job_id),
        )
        # 新版本定位成功：未冻结的旧版缺陷标记为 superseded（已冻结的不动）。
        self.store.execute(
            "UPDATE defects SET snap_state='superseded', updated_at=? "
            "WHERE inspection_id=? AND calibration_version_id<>? "
            "AND snap_state IN ('draft','confirmed')",
            (now_iso(), job["inspection_id"], job["calibration_version_id"]),
        )

    def _recompute_positions(
        self, insp_id: str, version_id: str, segments: list[dict] | None = None
    ):
        """换算里程、同步复核、按 open 阻断项写定位行。返回 PositionResult。

        人工解决复核项后可直接调用，刷新 provisional 标记，无需重读影像文件。
        """
        version = self.store.get_calibration_version(version_id)
        device = self.store.get_device(version["device_id"])
        insp = self.store.get_inspection(insp_id)
        if segments is None:
            segments = self.store.get_segments(insp_id)
        result = compute_positioning(
            segments,
            wheel_diameter_mm=version["wheel_diameter_mm"],
            encoder_ppr=device["encoder_ppr"],
            nominal_length_m=insp["nominal_length_m"],
        )

        self._sync_reviews(insp_id, result.findings)

        open_keys = {
            r["dedup_key"]
            for r in self.store.query(
                "SELECT * FROM reviews WHERE inspection_id=? AND status='open' "
                "AND severity='blocker'",
                (insp_id,),
            )
        }
        active_from_seqs = sorted(
            f.activation_seq
            for f in result.findings
            if f.severity == "blocker"
            and f.dedup_key in open_keys
            and f.activation_seq is not None
        )

        self.store.execute(
            "DELETE FROM positioning WHERE inspection_id=? AND calibration_version_id=?",
            (insp_id, version_id),
        )
        ts = now_iso()
        for row in result.rows:
            provisional = 1 if any(seq <= row.seq for seq in active_from_seqs) else 0
            self.store.execute(
                "INSERT INTO positioning(inspection_id, calibration_version_id, segment_id, "
                "chainage_start_m, chainage_end_m, raw_start, raw_end, scale, provisional, "
                "created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    insp_id,
                    version_id,
                    row.segment_id,
                    row.chainage_start_m,
                    row.chainage_end_m,
                    row.raw_start,
                    row.raw_end,
                    row.scale,
                    provisional,
                    ts,
                ),
            )
        return result

    def _set_job_status(self, job_id: str, status: str, fail_reason: str | None = None) -> None:
        self.store.execute(
            "UPDATE merge_jobs SET status=?, fail_reason=COALESCE(?, fail_reason), "
            "updated_at=? WHERE id=?",
            (status, fail_reason, now_iso(), job_id),
        )

    # ================= 复核 =================

    def _raise_review(
        self,
        insp_id: str,
        *,
        kind: str,
        detail: dict,
        related: list[str] | None = None,
        severity: str = "blocker",
        raised_by: str = "system",
        dedup_key: str | None = None,
    ) -> dict:
        review_id = new_id("rev")
        try:
            self.store.execute(
                "INSERT INTO reviews(id, inspection_id, kind, severity, status, dedup_key, "
                "detail_json, related_segment_ids, raised_by, created_at) "
                "VALUES(?,?,?,?, 'open', ?,?,?,?,?)",
                (
                    review_id,
                    insp_id,
                    kind,
                    severity,
                    dedup_key,
                    dumps(detail),
                    ",".join(related or []),
                    raised_by,
                    now_iso(),
                ),
            )
        except sqlite3.IntegrityError:
            # 并发合并或重跑：相同 dedup_key 已存在，保持既有结论。
            return self.store.query_one(
                "SELECT * FROM reviews WHERE inspection_id=? AND dedup_key=?",
                (insp_id, dedup_key),
            )
        return self.store.query_one("SELECT * FROM reviews WHERE id=?", (review_id,))

    def _sync_reviews(self, insp_id: str, findings) -> None:
        """把本次合并的发现同步到复核表。

        * 新出现且未处理过的发现 -> 建 open 复核项；
        * 仍然存在的 open/resolved 项 -> 不重复建；
        * 之前 open、本次重算已不复现的项 -> 自动关闭并留痕（人工关闭的不动）。

        上传阶段产生的复核项（如 checksum_conflict）没有 dedup_key，
        不参与自动关闭。
        """
        current_keys = {f.dedup_key for f in findings}
        rows = self.store.query(
            "SELECT * FROM reviews WHERE inspection_id=?", (insp_id,)
        )
        for row in rows:
            key = row["dedup_key"]
            if key is None:
                continue
            if row["status"] == "open" and key not in current_keys:
                self.store.execute(
                    "UPDATE reviews SET status='resolved', "
                    "resolution='重新合并后该冲突不再复现，系统自动关闭', resolved_at=? "
                    "WHERE id=?",
                    (now_iso(), row["id"]),
                )
        handled_keys = {
            r["dedup_key"]
            for r in self.store.query(
                "SELECT * FROM reviews WHERE inspection_id=? AND status IN ('open','resolved')",
                (insp_id,),
            )
        }
        for finding in findings:
            if finding.dedup_key in handled_keys:
                continue
            self._raise_review(
                insp_id,
                kind=finding.kind,
                severity=finding.severity,
                detail={**finding.detail, "dedup_key": finding.dedup_key},
                related=finding.related_segment_ids,
                dedup_key=finding.dedup_key,
            )

    def list_reviews(self, insp_id: str, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM reviews WHERE inspection_id=?"
        params: list = [insp_id]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY created_at"
        return [self.store.row_to_dict(r) for r in self.store.query(sql, params)]

    def resolve_review(self, review_id: str, payload: dict, *, rejected: bool = False) -> dict:
        row = self.store.query_one("SELECT * FROM reviews WHERE id=?", (review_id,))
        if row is None:
            raise NotFoundError("复核项不存在")
        if row["status"] != "open":
            raise StateError(f"复核项已为 {row['status']}")
        resolution = payload.get("resolution") or ("驳回" if rejected else "人工核实并接受")
        new_status = "rejected" if rejected else "resolved"
        self.store.execute(
            "UPDATE reviews SET status=?, resolution=?, resolved_at=? WHERE id=?",
            (new_status, resolution, now_iso(), review_id),
        )
        out = self.store.row_to_dict(
            self.store.query_one("SELECT * FROM reviews WHERE id=?", (review_id,))
        )
        # 人工结论立即生效：刷新各已完成定位版本的 provisional 标记。
        for job in self.store.query(
            "SELECT DISTINCT calibration_version_id FROM merge_jobs "
            "WHERE inspection_id=? AND status='succeeded'",
            (row["inspection_id"],),
        ):
            self._recompute_positions(row["inspection_id"], job["calibration_version_id"])
        return out

    # ================= 缺陷标记 =================

    def _current_positioning_context(self, insp_id: str) -> tuple[dict, dict]:
        """返回该检测当前应使用的（校准版本, 合并任务）。"""
        insp = self._require_inspection(insp_id)
        job = self.store.get_latest_succeeded_job(insp_id)
        if job is None:
            raise StateError("该检测尚未完成任何里程合并，无法稳定定位缺陷")
        version = self.store.get_calibration_version(job["calibration_version_id"])
        return version, job

    def mark_defect(self, insp_id: str, payload: dict, *, created_by: str) -> dict:
        seg_id = payload.get("segment_id")
        seg = self.store.get_segment(seg_id) if seg_id else None
        if seg is None or seg["inspection_id"] != insp_id:
            raise NotFoundError("分段不存在或不属于该检测任务")
        raw_offset = float(payload.get("raw_offset"))
        if not (seg["encoder_start"] - 1e-6 <= raw_offset <= seg["encoder_end"] + 1e-6):
            raise ValidationError(
                f"raw_offset {raw_offset} 超出分段编码器区间 "
                f"[{seg['encoder_start']}, {seg['encoder_end']}]"
            )
        code = payload.get("code")
        if not code:
            raise ValidationError("缺少缺陷代码 code")
        version, job = self._current_positioning_context(insp_id)
        pos = self.store.get_positioning(version["version_id"], seg["id"])
        if pos is None:
            raise StateError("当前定位版本缺少该分段的里程数据")
        device = self.store.get_device(version["device_id"])
        explanation = explain_point(
            raw_offset, seg, pos, version["wheel_diameter_mm"], device["encoder_ppr"]
        )
        defect_id = new_id("def")
        state = payload.get("state", "confirmed")
        if state not in ("draft", "confirmed"):
            raise ValidationError("新建标记仅允许 draft/confirmed")
        ts = now_iso()
        self.store.execute(
            "INSERT INTO defects(id, inspection_id, code, description, segment_id, "
            "calibration_version_id, raw_offset, chainage_m, scale, snap_state, "
            "created_by, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                defect_id,
                insp_id,
                code,
                payload.get("description", ""),
                seg["id"],
                version["version_id"],
                raw_offset,
                explanation["chainage_m"],
                explanation["scale_m_per_pulse"],
                state,
                created_by,
                ts,
                ts,
            ),
        )
        out = self.store.get_defect(defect_id)
        out["conversion"] = explanation
        out["warnings"] = (
            ["该分段位于未解决的里程冲突之后，坐标为推算值，需以复核结论为准"]
            if explanation["provisional"]
            else []
        )
        return out

    def defect_detail(self, defect_id: str, *, include_raw: bool) -> dict:
        defect = self.store.get_defect(defect_id)
        if defect is None:
            raise NotFoundError("缺陷不存在")
        insp = self.store.get_inspection(defect["inspection_id"])
        seg = self.store.get_segment(defect["segment_id"])
        version = self.store.get_calibration_version(defect["calibration_version_id"])
        device = self.store.get_device(version["device_id"])
        pos = self.store.get_positioning(version["version_id"], seg["id"])
        explanation = explain_point(
            defect["raw_offset"], seg, pos, version["wheel_diameter_mm"], device["encoder_ppr"]
        )

        open_blockers = [
            r
            for r in self.store.get_open_reviews(defect["inspection_id"])
            if r["severity"] == "blocker"
        ]
        published_report = None
        if defect["frozen_by_report_id"]:
            rep = self.store.get_report(defect["frozen_by_report_id"])
            if rep and rep["status"] == "published":
                published_report = {"id": rep["id"], "title": rep["title"],
                                    "published_at": rep["published_at"]}

        reasons: list[str] = []
        if open_blockers:
            reasons.append("存在未解决的阻断性复核项（缺片/坐标冲突/校验冲突）")
        if explanation["provisional"]:
            reasons.append("缺陷所在分段坐标为冲突后的推算值")
        if not self.store.get_snapshot_for_segment(seg["id"]):
            reasons.append("缺少脱敏截图，无法对外展示")
        if defect["snap_state"] not in ("confirmed", "frozen"):
            reasons.append(f"缺陷状态为 {defect['snap_state']}，尚未确认")
        can_release = not reasons

        detail = {
            "defect": {
                "id": defect["id"],
                "inspection_id": defect["inspection_id"],
                "code": defect["code"],
                "description": defect["description"],
                "chainage_m": defect["chainage_m"],
                "raw_offset": defect["raw_offset"],
                "snap_state": defect["snap_state"],
                "created_by": defect["created_by"],
                "created_at": defect["created_at"],
            },
            "pipeline": {
                "pipeline_code": insp["pipeline_code"],
                "start_manhole": insp["start_manhole"],
                "end_manhole": insp["end_manhole"],
            },
            "segment": self._segment_view(seg, include_raw=include_raw),
            "calibration_version": {
                "version_id": version["version_id"],
                "device_id": version["device_id"],
                "seq": version["seq"],
                "wheel_diameter_mm": version["wheel_diameter_mm"],
                "effective_from_m": version["effective_from_m"],
                "supersedes": version["supersedes"],
                "note": version["note"],
                "created_at": version["created_at"],
            },
            "conversion": explanation,
            "open_blocker_reviews": [
                {"id": r["id"], "kind": r["kind"], "detail": loads(r["detail_json"])}
                for r in open_blockers
            ],
            "release": {
                "can_release_externally": can_release,
                "reasons": reasons,
                "frozen_by_published_report": published_report,
            },
        }
        # 已被新版本定位取代时，附加当前版本下的投影坐标（仅供参考，不改原值）。
        latest_job = self.store.get_latest_succeeded_job(defect["inspection_id"])
        if latest_job and latest_job["calibration_version_id"] != version["version_id"]:
            new_version = self.store.get_calibration_version(latest_job["calibration_version_id"])
            new_pos = self.store.get_positioning(new_version["version_id"], seg["id"])
            if new_pos is not None:
                projection = explain_point(
                    defect["raw_offset"], seg, new_pos,
                    new_version["wheel_diameter_mm"], device["encoder_ppr"],
                )
                detail["latest_version_projection"] = {
                    "calibration_version_id": new_version["version_id"],
                    "seq": new_version["seq"],
                    "chainage_m": projection["chainage_m"],
                    "conversion": projection,
                    "note": "新校准版本下的投影值；已发布报告引用的原始坐标不变",
                }
        return detail

    def _segment_view(self, seg: dict, *, include_raw: bool) -> dict:
        snap = self.store.get_snapshot_for_segment(seg["id"])
        view = {
            "segment_id": seg["id"],
            "seq": seg["seq"],
            "redacted_snapshot": (
                {"id": snap["id"], "sha256": snap["sha256"], "href": f"/media/{snap['blob_path']}"}
                if snap
                else None
            ),
        }
        if include_raw:
            view.update(
                {
                    "file_name": seg["file_name"],
                    "sha256": seg["sha256"],
                    "size_bytes": seg["size_bytes"],
                    "href": f"/media/{seg['blob_path']}",
                    "encoder_start": seg["encoder_start"],
                    "encoder_end": seg["encoder_end"],
                    "duration_s": seg["duration_s"],
                    "uploaded_at": seg["uploaded_at"],
                }
            )
        return view

    # ================= 报告与发布 =================

    def create_report(self, insp_id: str, payload: dict, *, created_by: str) -> dict:
        self._require_inspection(insp_id)
        target_version_id = payload.get("calibration_version_id")
        if target_version_id is None:
            version, _ = self._current_positioning_context(insp_id)
            target_version_id = version["version_id"]
        version = self.store.get_calibration_version(target_version_id)
        if version is None:
            raise ValidationError("校准版本不存在")

        defect_rows = self.store.query(
            "SELECT * FROM defects WHERE inspection_id=? AND calibration_version_id=? "
            "AND snap_state IN ('confirmed','frozen') ORDER BY chainage_m",
            (insp_id, target_version_id),
        )
        items = []
        for row in defect_rows:
            seg = self.store.get_segment(row["segment_id"])
            pos = self.store.get_positioning(target_version_id, seg["id"])
            device = self.store.get_device(version["device_id"])
            snap = self.store.get_snapshot_for_segment(seg["id"])
            items.append(
                {
                    "defect_id": row["id"],
                    "code": row["code"],
                    "description": row["description"],
                    "segment_seq": seg["seq"],
                    "raw_offset": row["raw_offset"],
                    "chainage_m": row["chainage_m"],
                    "conversion": explain_point(
                        row["raw_offset"], seg, pos,
                        version["wheel_diameter_mm"], device["encoder_ppr"],
                    ),
                    "snapshot_id": snap["id"] if snap else None,
                }
            )
        snapshot = {
            "inspection_id": insp_id,
            "calibration_version_id": target_version_id,
            "calibration_seq": version["seq"],
            "wheel_diameter_mm": version["wheel_diameter_mm"],
            "title": payload.get("title", f"{insp_id} 检测缺陷报告"),
            "items": items,
            "generated_at": now_iso(),
        }
        report_id = new_id("rpt")
        self.store.execute(
            "INSERT INTO reports(id, inspection_id, calibration_version_id, title, status, "
            "snapshot_json, created_by, created_at) VALUES(?,?,?,?, 'draft', ?,?,?)",
            (
                report_id,
                insp_id,
                target_version_id,
                snapshot["title"],
                dumps(snapshot),
                created_by,
                now_iso(),
            ),
        )
        return self.store.get_report(report_id)

    def publish_report(self, report_id: str) -> dict:
        report = self.store.get_report(report_id)
        if report is None:
            raise NotFoundError("报告不存在")
        if report["status"] == "published":
            return report
        blockers = [
            r
            for r in self.store.get_open_reviews(report["inspection_id"])
            if r["severity"] == "blocker"
        ]
        if blockers:
            raise StateError(
                f"尚有 {len(blockers)} 个阻断性复核项未处理，缺陷位置无法稳定复现，禁止发布"
            )
        snapshot = loads(report["snapshot_json"])
        if not snapshot["items"]:
            raise StateError("报告不包含任何已确认缺陷，拒绝发布空报告")
        for item in snapshot["items"]:
            if item["conversion"]["provisional"]:
                raise StateError("报告含推算坐标缺陷，需先完成复核")
            seg_row = self.store.query_one(
                "SELECT id FROM segments WHERE inspection_id=? AND seq=?",
                (snapshot["inspection_id"], item["segment_seq"]),
            )
            if seg_row is None or not self.store.get_snapshot_for_segment(seg_row["id"]):
                raise StateError(
                    f"序号 {item['segment_seq']} 缺少脱敏截图，不可对外发布"
                )
        ts = now_iso()
        self.store.execute(
            "UPDATE reports SET status='published', published_at=? WHERE id=?", (ts, report_id)
        )
        for item in snapshot["items"]:
            self.store.execute(
                "UPDATE defects SET snap_state='frozen', frozen_by_report_id=?, updated_at=? "
                "WHERE id=? AND snap_state='confirmed'",
                (report_id, ts, item["defect_id"]),
            )
        return self.store.get_report(report_id)

    def report_view(self, report: dict) -> dict:
        return {
            "id": report["id"],
            "inspection_id": report["inspection_id"],
            "calibration_version_id": report["calibration_version_id"],
            "title": report["title"],
            "status": report["status"],
            "created_by": report["created_by"],
            "created_at": report["created_at"],
            "published_at": report["published_at"],
            "snapshot": loads(report["snapshot_json"]),
        }

    # ================= 公共读取 =================

    def _require_inspection(self, insp_id: str) -> dict:
        insp = self.store.get_inspection(insp_id)
        if insp is None:
            raise NotFoundError(f"检测任务 {insp_id} 不存在")
        return insp

    def _require_job(self, job_id: str) -> dict:
        job = self.store.get_job(job_id)
        if job is None:
            raise NotFoundError("合并任务不存在")
        return job


def _require_positive(payload: dict, key: str) -> float:
    value = payload.get(key)
    if value is None or float(value) <= 0:
        raise ValidationError(f"{key} 必须为正数")
    return float(value)


def _require_positive_int(payload: dict, key: str) -> int:
    value = _require_positive(payload, key)
    if int(value) != value:
        raise ValidationError(f"{key} 必须为整数")
    return int(value)


def _require_nonnegative_int(payload: dict, key: str) -> int:
    value = payload.get(key)
    if value is None or int(value) < 0:
        raise ValidationError(f"{key} 必须为非负整数")
    return int(value)


def _require_sha256(payload: dict) -> str:
    sha = (payload.get("sha256") or "").strip().lower()
    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
        raise ValidationError("sha256 必须为 64 位十六进制字符串")
    return sha


def _verify_sha256(data: bytes, expect: str) -> None:
    actual = hashlib.sha256(data).hexdigest()
    if actual != expect:
        raise ChecksumMismatchError(
            f"内容校验和 {actual} 与登记值 {expect} 不一致"
        )
