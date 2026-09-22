"""后台合并 worker：分段上传后的分步合并管线。

阶段（stage）全部落盘，服务重启后 running 作业回到 queued 继续：
  validate  元数据/编码器读数完整性
  anchor    绑定定位版本与校准
  convert   按校准把脉冲读数换算成沿管线里程
  conflict  与既有区间做几何核对：越界 / 重叠（坐标冲突）/ 缺口（缺失片段）
  index     写入分段里程区间

任何异常都留下可读的 last_error；缺失与冲突一律开复核单并阻塞，绝不静默拼接。
"""

from __future__ import annotations

import math
import threading

from .store import Store, now

# 相邻分段端点小于该距离视为恰好衔接（编码器抖动容差，米）
GAP_TOLERANCE_M = 0.10


class MergeWorker:
    def __init__(self, store: Store):
        self.store = store
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.recover()

    # ---- 生命周期 -----------------------------------------------------------

    def recover(self) -> None:
        """重启恢复：中断在 running 的作业回到队列；失败原因已在库中不丢。"""
        with self.store.lock:
            self.store.execute(
                "UPDATE merge_jobs SET status='queued', updated_at=? "
                "WHERE status='running'", (now(),)
            )

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="merge-worker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=2)

    def notify(self) -> None:
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                job = self.store.claim_next_job()
            except Exception as exc:  # 库级意外不能杀死 worker
                job = None
                self._store_error(exc)
            if job is None:
                self._wake.wait(timeout=0.2)
                self._wake.clear()
                continue
            self._wake.clear()
            self.run_job(job["id"])

    def _store_error(self, exc: Exception) -> None:
        with self.store.lock:
            self.store.execute(
                "UPDATE merge_jobs SET last_error=?, updated_at=? "
                "WHERE status='running'", (f"worker_error: {exc}", now())
            )

    # ---- 里程换算（纯函数，同时供缺陷标记复用）-------------------------------

    @staticmethod
    def convert_pulse(calibration: dict, loc_version: dict,
                      pulse_reading: int) -> tuple[float, dict]:
        diameter_m = calibration["wheel_diameter_mm"] / 1000.0
        circumference_m = math.pi * diameter_m
        pulses_per_rev = calibration["pulses_per_rev"]
        delta_pulses = pulse_reading - loc_version["anchor_pulses"]
        distance_m = (
            loc_version["anchor_m"]
            + delta_pulses * circumference_m / pulses_per_rev
        )
        calc = {
            "formula": "distance_m = anchor_m + (pulse - anchor_pulses) "
                       "* pi * wheel_diameter_m / 1000 / pulses_per_rev",
            "inputs": {
                "wheel_diameter_mm": calibration["wheel_diameter_mm"],
                "pulses_per_rev": pulses_per_rev,
                "anchor_m": loc_version["anchor_m"],
                "anchor_pulses": loc_version["anchor_pulses"],
                "pulse_reading": pulse_reading,
            },
            "steps": [
                f"wheel_circumference_m = pi * {diameter_m:.6f} "
                f"= {circumference_m:.6f}",
                f"delta_pulses = {pulse_reading} - {loc_version['anchor_pulses']} "
                f"= {delta_pulses}",
                f"delta_distance_m = {delta_pulses} * {circumference_m:.6f} "
                f"/ {pulses_per_rev} = {delta_pulses * circumference_m / pulses_per_rev:.6f}",
                f"distance_m = {loc_version['anchor_m']} + "
                f"{delta_pulses * circumference_m / pulses_per_rev:.6f} "
                f"= {distance_m:.6f}",
            ],
            "calibration_id": calibration["id"],
            "loc_version_id": loc_version["id"],
            "distance_m": round(distance_m, 6),
        }
        return round(distance_m, 6), calc

    # ---- 单作业分步执行 ------------------------------------------------------

    def run_job(self, job_id: str) -> None:
        try:
            self._stage_validate(job_id)
            start_m, end_m, p0, p1 = self._stage_anchor_convert(job_id)
            self._stage_conflict(job_id, start_m, end_m)
            self._stage_index(job_id, start_m, end_m, p0, p1)
        except _Blocked as b:
            with self.store.lock:
                job = self.store.get("merge_jobs", job_id)
                self.store.execute(
                    "UPDATE merge_jobs SET status='blocked', stage=?, last_error=?, "
                    "updated_at=? WHERE id=?",
                    (b.stage, f"{b.kind}: {b.detail}", now(), job_id),
                )
                self.store.open_review(
                    job["task_id"], b.kind,
                    {"segment_id": job["segment_id"], "reason": b.detail,
                     **b.extra},
                    related_segment_id=job["segment_id"],
                )
        except Exception as exc:
            self.store.fail_job(job_id, "worker", f"{type(exc).__name__}: {exc}")

    def _stage_validate(self, job_id: str) -> None:
        self.store.update_job_stage(job_id, "validate")
        job = self.store.get("merge_jobs", job_id)
        raw = self.store.execute(
            "SELECT * FROM segment_raw_readings WHERE segment_id=?",
            (job["segment_id"],),
        ).fetchone()
        if raw is None or raw["pulse_start"] is None or raw["pulse_end"] is None:
            raise _Blocked(
                "validate", "missing_gap",
                "分段缺少编码器脉冲起止读数，无法定位里程，等待人工补录",
            )
        if raw["pulse_end"] < raw["pulse_start"]:
            raise _Blocked(
                "validate", "coordinate_conflict",
                f"脉冲读数倒退：start={raw['pulse_start']} end={raw['pulse_end']}",
                pulse_start=raw["pulse_start"], pulse_end=raw["pulse_end"],
            )

    def _stage_anchor_convert(self, job_id: str) -> tuple[float, float, int, int]:
        self.store.update_job_stage(job_id, "anchor")
        job = self.store.get("merge_jobs", job_id)
        lv_id = self.store.job_loc_version(job_id)
        lv = self.store.get("loc_versions", lv_id)
        cal = self.store.get("calibrations", lv["calibration_id"])
        raw = self.store.execute(
            "SELECT * FROM segment_raw_readings WHERE segment_id=?",
            (job["segment_id"],),
        ).fetchone()

        self.store.update_job_stage(job_id, "convert")
        start_m, _ = self.convert_pulse(cal, lv, raw["pulse_start"])
        end_m, _ = self.convert_pulse(cal, lv, raw["pulse_end"])
        return start_m, end_m, raw["pulse_start"], raw["pulse_end"]

    def _stage_conflict(self, job_id: str, start_m: float, end_m: float) -> None:
        self.store.update_job_stage(job_id, "conflict")
        job = self.store.get("merge_jobs", job_id)

        task = self.store.get("tasks", job["task_id"])
        pipeline = self.store.get("pipelines", task["pipeline_id"])
        seg_id = job["segment_id"]

        # 越出管线起止井
        if start_m < -GAP_TOLERANCE_M or end_m > pipeline["length_m"] + GAP_TOLERANCE_M:
            if self._has_open_review(seg_id, "range_overflow"):
                raise _Blocked("conflict", "range_overflow",
                               "里程越出管线起止井范围，复核尚未处理",
                               start_m=start_m, end_m=end_m,
                               pipeline_length_m=pipeline["length_m"])
            if self._has_resolved_review(seg_id, "range_overflow"):
                return
            raise _Blocked("conflict", "range_overflow",
                           "换算里程越出管线起止井，需人工确认锚点/校准",
                           start_m=start_m, end_m=end_m,
                           pipeline_length_m=pipeline["length_m"])

        others = [dict(r) for r in self.store.execute(
            "SELECT sr.* FROM segment_ranges sr JOIN segments s ON s.id=sr.segment_id "
            "WHERE s.task_id=? AND sr.segment_id<>?",
            (job["task_id"], seg_id),
        )]

        # 坐标冲突：与既有区间重叠
        for o in others:
            if start_m < o["end_m"] - GAP_TOLERANCE_M and \
               end_m > o["start_m"] + GAP_TOLERANCE_M:
                if self._has_resolved_review(seg_id, "coordinate_conflict"):
                    return
                raise _Blocked(
                    "conflict", "coordinate_conflict",
                    f"与分段 {o['segment_id']} 里程区间重叠，疑似重复/跳点",
                    start_m=start_m, end_m=end_m,
                    overlap_segment_id=o["segment_id"],
                    overlap_range=[o["start_m"], o["end_m"]],
                )

        # 缺失片段：与最近邻之间存在超出容差的缺口
        predecessors = [o for o in others if o["end_m"] <= start_m + GAP_TOLERANCE_M]
        successors = [o for o in others if o["start_m"] >= end_m - GAP_TOLERANCE_M]
        if predecessors:
            pred = max(predecessors, key=lambda o: o["end_m"])
            gap = start_m - pred["end_m"]
            if gap > GAP_TOLERANCE_M:
                if self._has_resolved_review(seg_id, "missing_gap"):
                    return
                raise _Blocked(
                    "conflict", "missing_gap",
                    f"与前序分段 {pred['segment_id']} 之间存在 {gap:.2f}m 缺口，"
                    "可能有分段缺失，不能自动拼接",
                    gap_m=round(gap, 3), neighbor_segment_id=pred["segment_id"],
                )
        if successors:
            succ = min(successors, key=lambda o: o["start_m"])
            gap = succ["start_m"] - end_m
            if gap > GAP_TOLERANCE_M:
                if self._has_resolved_review(seg_id, "missing_gap"):
                    return
                raise _Blocked(
                    "conflict", "missing_gap",
                    f"与后序分段 {succ['segment_id']} 之间存在 {gap:.2f}m 缺口，"
                    "可能有分段缺失，不能自动拼接",
                    gap_m=round(gap, 3), neighbor_segment_id=succ["segment_id"],
                )

    def _stage_index(self, job_id: str, start_m: float, end_m: float,
                     pulse_start: int, pulse_end: int) -> None:
        self.store.update_job_stage(job_id, "index")
        job = self.store.get("merge_jobs", job_id)
        lv_id = self.store.job_loc_version(job_id)
        self.store.write_segment_range(
            job_id, lv_id, start_m, end_m, pulse_start, pulse_end
        )
        self._auto_close_gap_reviews(job["task_id"])
        self.notify()  # 可能有被阻塞的后继作业可以继续

    def _auto_close_gap_reviews(self, task_id: str) -> None:
        """新分段入库后，原本缺失的缺口若已闭合，自动了结复核单。

        坐标冲突/越界永远不自动关闭——必须人工表态。
        """
        ranges = sorted(
            (dict(r) for r in self.store.execute(
                "SELECT sr.* FROM segment_ranges sr JOIN segments s ON s.id=sr.segment_id "
                "WHERE s.task_id=?", (task_id,))),
            key=lambda r: r["start_m"],
        )
        covered: list[tuple[float, float]] = []
        for r in ranges:
            if covered and r["start_m"] <= covered[-1][1] + GAP_TOLERANCE_M:
                covered[-1] = (covered[-1][0], max(covered[-1][1], r["end_m"]))
            else:
                covered.append((r["start_m"], r["end_m"]))

        for review in self.store.list_reviews(task_id=task_id, status="open"):
            if review["kind"] not in ("missing_gap", "prerequisite_pending"):
                continue
            detail = _safe_json(review["detail_json"])
            seg_id = review.get("related_segment_id")
            if review["kind"] == "prerequisite_pending":
                done = self.store.execute(
                    "SELECT 1 FROM segment_ranges WHERE segment_id=?",
                    (detail.get("prereq_segment_id"),),
                ).fetchone()
                if done:
                    self.store.resolve_review(
                        review["id"], "自动闭合：前置分段已成功合并")
                continue
            # missing_gap：该分段现在已成功入库即视为缺口补齐
            if seg_id and self.store.execute(
                "SELECT 1 FROM segment_ranges WHERE segment_id=?", (seg_id,)
            ).fetchone():
                self.store.resolve_review(review["id"], "自动闭合：缺口相邻分段已到齐")
        self.requeue_blocked(task_id)

    # ---- 复核查询辅助 --------------------------------------------------------

    def _reviews_for(self, seg_id: str, status: str, kind: str):
        return self.store.execute(
            "SELECT * FROM reviews WHERE related_segment_id=? AND status=? AND kind=?",
            (seg_id, status, kind),
        ).fetchall()

    def _has_open_review(self, seg_id: str, kind: str) -> bool:
        return bool(self._reviews_for(seg_id, "open", kind))

    def _has_resolved_review(self, seg_id: str, kind: str) -> bool:
        return bool(self._reviews_for(seg_id, "resolved", kind))


    def requeue_blocked(self, task_id: str) -> None:
        """缺口闭合或复核被解决后，让被阻塞作业重新排队（冲突复核未解决则会再次被拦）。"""
        with self.store.lock:
            rows = self.store.execute(
                "SELECT id FROM merge_jobs WHERE task_id=? AND status='blocked'",
                (task_id,),
            ).fetchall()
            for r in rows:
                self.store.execute(
                    "UPDATE merge_jobs SET status='queued', updated_at=? WHERE id=?",
                    (now(), r["id"]),
                )
        if rows:
            self.notify()


def _safe_json(text: str) -> dict:
    import json
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return {}


class _Blocked(Exception):
    def __init__(self, stage: str, kind: str, detail: str, **extra):
        super().__init__(detail)
        self.stage = stage
        self.kind = kind
        self.detail = detail
        self.extra = extra
