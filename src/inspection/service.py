"""业务服务层：角色可见性、里程换算入口、发布判定。

角色（与 reference/domain.json 对齐）：
  ops 运维人员：可读写；唯一可见原始影像 raw_frame
  dispatcher 调度员：可读写、负责发布；仅见脱敏截图 redacted
  regulator 监管人员：只读；仅见脱敏截图，且只可见已进入有效发布版本的内容
  readonly 只读用户：同监管人员的可见范围
"""

from __future__ import annotations

from urllib.parse import unquote

from .merger import MergeWorker
from .store import Conflict, NotFound, Store  # noqa: F401  (重导出便于 API 层捕获)

ROLES = ("ops", "dispatcher", "regulator", "readonly")
ROLE_LABELS = {
    "ops": "运维人员",
    "dispatcher": "调度员",
    "regulator": "监管人员",
    "readonly": "只读用户",
}
ROLE_ALIASES = {v: k for k, v in ROLE_LABELS.items()} | {
    "operator": "ops", "admin": "ops",
}

WRITE_ROLES = frozenset({"ops", "dispatcher"})
PUBLISH_ROLES = frozenset({"dispatcher"})
RAW_VIDEO_ROLES = frozenset({"ops"})
INTERNAL_ROLES = frozenset({"ops", "dispatcher"})


class Forbidden(Exception):
    """角色无权执行该操作或查看该资源。"""


def normalize_role(raw: str | None) -> str:
    if not raw:
        return "readonly"
    # HTTP 头只允许 latin-1，角色中文名以百分号编码 UTF-8 方式承载
    key = unquote(raw.strip(), encoding="utf-8", errors="replace").lower()
    if key in ROLES:
        return key
    return ROLE_ALIASES.get(key, "readonly")


class InspectionService:
    def __init__(self, store: Store, worker: MergeWorker):
        self.store = store
        self.worker = worker

    # ---- 权限 ---------------------------------------------------------------

    @staticmethod
    def require_write(role: str) -> None:
        if role not in WRITE_ROLES:
            raise Forbidden(f"{ROLE_LABELS.get(role, role)} 无写入权限")

    @staticmethod
    def require_publish(role: str) -> None:
        if role not in PUBLISH_ROLES:
            raise Forbidden("只有调度员可以发布/撤回")

    # ---- 合并队列操作 --------------------------------------------------------

    def pause_job(self, job_id: str, role: str) -> dict:
        self.require_write(role)
        return self.store.pause_job(job_id)

    def resume_job(self, job_id: str, role: str) -> dict:
        self.require_write(role)
        job = self.store.resume_job(job_id)
        self.worker.notify()
        return job

    def set_queue_paused(self, paused: bool, role: str) -> dict:
        self.require_write(role)
        self.store.set_paused(paused)
        if not paused:
            self.worker.notify()
        return {"paused": paused}

    def resolve_review(self, review_id: str, resolution: str, role: str) -> dict:
        self.require_write(role)
        if not resolution or not resolution.strip():
            raise Conflict("复核处理必须填写处理说明，不能无声放行")
        review = self.store.resolve_review(review_id, resolution.strip())
        # 人工表态后，被阻塞作业重新排队；缺口类会通过，冲突类按“已接受”放行
        self.worker.requeue_blocked(review["task_id"])
        return review

    # ---- 缺陷标记 ------------------------------------------------------------

    def mark_sighting(self, defect_id: str, loc_version_id: str, segment_id: str,
                      file_offset_s: float, pulse_reading: int,
                      role: str) -> dict:
        self.require_write(role)
        defect = self.store.get("defects", defect_id)
        seg = self.store.get("segments", segment_id)
        lv = self.store.get("loc_versions", loc_version_id)
        if seg["task_id"] != defect["task_id"] or lv["task_id"] != defect["task_id"]:
            raise Conflict("缺陷、分段、定位版本必须属于同一任务")
        cal = self.store.get("calibrations", lv["calibration_id"])
        distance_m, calc = MergeWorker.convert_pulse(cal, lv, pulse_reading)
        rng = self.store.segment_range(segment_id)
        if rng is None:
            raise Conflict("分段尚未合并成功，不能在其上标记缺陷")
        if not (rng["start_m"] <= distance_m <= rng["end_m"]):
            # 坐标冲突：进复核，不允许硬写
            review = self.store.open_review(
                defect["task_id"], "coordinate_conflict",
                {"defect_id": defect_id, "segment_id": segment_id,
                 "pulse_reading": pulse_reading,
                 "converted_distance_m": distance_m,
                 "segment_range": [rng["start_m"], rng["end_m"]],
                 "reason": "标记里程落不进所属分段区间，疑似轮径跳点/锚点错误"},
                related_segment_id=segment_id,
            )
            raise Conflict(
                f"换算里程 {distance_m} 落不进分段区间，已进入复核 {review['id']}，"
                "不得强行落标记"
            )
        sighting = self.store.add_sighting(
            defect_id, loc_version_id, segment_id, file_offset_s,
            pulse_reading, distance_m, calc,
        )
        # 在新定位版本下复算后，旧版本上未发布的定位记录被替代；
        # 已发布（被报告引用）的记录冻结，不受影响。
        self.store.supersede_sightings_for_version(defect_id, loc_version_id)
        return self.store.get_sighting(sighting["id"])

    # ---- 缺陷完整索引视图（按角色裁剪）---------------------------------------

    def defect_detail(self, defect_id: str, role: str) -> dict:
        detail = self.store.defect_detail(defect_id)
        visible = []
        for item in detail["sightings"]:
            s = item["sighting"]
            in_active_release = any(
                r["status"] == "active" for r in item["releases"])
            if role not in INTERNAL_ROLES and not (s["published"] and in_active_release):
                continue  # 外部角色只能看进入有效发布版本的定位记录
            item["assets"] = self._visible_assets(item["assets"], role)
            item["publish"] = self._publish_assessment(item, detail["task"]["id"])
            visible.append(item)
        if role not in INTERNAL_ROLES and not visible:
            raise Forbidden("该缺陷尚无对外发布内容")
        detail["sightings"] = visible
        detail["externally_visible_now"] = any(
            i["publish"]["in_active_release"] for i in visible
        )
        detail["can_publish_now"] = any(
            i["publish"]["publishable"] for i in visible
        )
        # “当前是否允许对外发布”：已在有效期内发布，或存在可进入新发布版本的定位记录
        detail["externally_publishable"] = (
            detail["externally_visible_now"] or detail["can_publish_now"]
        )
        detail["view_as_role"] = role
        return detail

    @staticmethod
    def _visible_assets(assets: list[dict], role: str) -> list[dict]:
        if role in RAW_VIDEO_ROLES:
            return assets
        return [a for a in assets if a["kind"] == "redacted"]

    def _publish_assessment(self, item: dict, task_id: str) -> dict:
        """该定位记录当前能否对外（已发 / 可发），以及不能发的具体原因。"""
        s = item["sighting"]
        active_release = next(
            (r for r in item["releases"] if r["status"] == "active"), None
        )
        in_active_release = active_release is not None

        reasons: list[str] = []
        if s["state"] == "superseded":
            reasons.append("已被更新定位版本下的复算结果替代")
        elif s["state"] != "confirmed":
            reasons.append(f"定位记录状态为 {s['state']}，需人工确认")
        latest = self.store.execute(
            "SELECT id FROM loc_versions WHERE task_id=? ORDER BY created_at DESC "
            "LIMIT 1", (task_id,),
        ).fetchone()
        if latest and latest["id"] != item["loc_version"]["id"]:
            reasons.append("任务已有更新的定位版本，需在新版本下复算后再发布")
        if not any(a["kind"] == "redacted" for a in item["assets"]):
            reasons.append("缺少脱敏截图，不能以原始影像对外")
        seg_id = item["segment"]["id"]
        open_blocking = self.store.execute(
            "SELECT kind FROM reviews WHERE related_segment_id=? AND status='open' "
            "AND kind IN ('missing_gap','coordinate_conflict','range_overflow')",
            (seg_id,),
        ).fetchall()
        if open_blocking:
            reasons.append("分段存在未处理复核：" + ",".join(r["kind"] for r in open_blocking))

        return {
            "in_active_release": in_active_release,
            "active_release_id": active_release["id"] if active_release else None,
            "published_flag": bool(s["published"]),
            # 可进入“新的”发布版本：未被替代、已确认、版本最新、有脱敏图、无未决复核
            "publishable": not reasons,
            "reasons": reasons,
        }

    # ---- 报告 / 发布 ---------------------------------------------------------

    def publish_report(self, report_id: str, role: str) -> dict:
        self.require_publish(role)
        return self.store.publish_report(report_id)

    def create_release(self, task_id: str, report_id: str, version_label: str,
                       role: str) -> dict:
        self.require_publish(role)
        return self.store.create_release(task_id, report_id, version_label)

    def retract_release(self, release_id: str, role: str) -> dict:
        self.require_publish(role)
        return self.store.retract_release(release_id)
