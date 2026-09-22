"""HTTP 接口（标准库实现，无第三方依赖）。

路由总览
========
鉴权：除 /health 外均需 ``Authorization: Bearer <token>``。

* 设备与校准
    POST   /api/devices
    POST   /api/devices/{id}/calibrations
    GET    /api/devices/{id}/calibrations
* 检测任务
    POST   /api/inspections
    GET    /api/inspections/{id}
    POST   /api/inspections/{id}/status
    PUT    /api/inspections/{id}/segments            （幂等上传）
    GET    /api/inspections/{id}/segments
    POST   /api/segments/{id}/snapshots             （脱敏截图）
* 合并队列
    POST   /api/inspections/{id}/merge-jobs
    GET    /api/merge-jobs/{id}
    POST   /api/merge-jobs/{id}/pause
    POST   /api/merge-jobs/{id}/resume
    POST   /api/merge-jobs/{id}/retry
* 复核
    GET    /api/inspections/{id}/reviews
    POST   /api/reviews/{id}/resolve
    POST   /api/reviews/{id}/reject
* 缺陷
    POST   /api/inspections/{id}/defects
    GET    /api/defects/{id}
* 报告发布
    POST   /api/inspections/{id}/reports
    POST   /api/reports/{id}/publish
    GET    /api/reports/{id}
* 影像
    GET    /media/{blob_path}                       （按角色限制原始/脱敏）
"""

from __future__ import annotations

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .config import Settings
from .errors import AppError, AuthError, PermissionError
from .security import (
    CAN_MANAGE,
    CAN_PUBLISH,
    CAN_VIEW_RAW,
    authenticate,
)
from .service import Service
from .storage import Store

SERVICE_NAME = "pipeline-inspection-index"


class Application:
    def __init__(self, settings: Settings | None = None, *, run_inline: bool = False):
        self.settings = settings or Settings.from_env()
        self.settings.ensure_dirs()
        self.store = Store(self.settings.db_path)
        # 崩溃恢复：running -> paused；重启前已排队的任务继续执行。
        self.store.recover_jobs_on_startup()
        self.service = Service(self.store, self.settings.blob_dir)
        self.run_inline = run_inline
        if run_inline:
            self.service.run_inline = True  # type: ignore[attr-defined]
        else:
            for job in self.store.resumeable_jobs():
                if job["status"] == "queued":
                    self.service._spawn(job["id"])


# ---------------- 路由定义 ----------------

# 每条: (method, compiled_regex, required_roles_or_None, handler_name)
def _compile(routes):
    return [(m, re.compile(p), roles, h) for m, p, roles, h in routes]


class Handler(BaseHTTPRequestHandler):
    server_version = "InspectionIndex/1.0"

    # 在 create_server 时注入到 server 上
    @property
    def app(self) -> Application:
        return self.server.app  # type: ignore[attr-defined]

    def _principal(self):
        return authenticate(self.headers.get("Authorization"))

    def _send_json(self, status: int, body: dict | list) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise AppError(f"请求体不是合法 JSON: {exc}", "bad_json")
        if not isinstance(data, dict):
            raise AppError("请求体必须是 JSON 对象", "bad_json")
        return data

    def _handle(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path
        if method == "GET" and path == "/health":
            self._send_json(200, {"status": "ok", "service": SERVICE_NAME})
            return
        try:
            principal = self._principal()
            for m, pattern, roles, handler_name in ROUTES:
                if m != method:
                    continue
                match = pattern.fullmatch(path)
                if not match:
                    continue
                if roles is not None:
                    principal.require(roles)
                handler = getattr(self, handler_name)
                handler(principal, match.groupdict(), self._read_json())
                return
            self._send_json(404, {"error": "not_found", "message": f"{method} {path}"})
        except AppError as exc:
            self._send_json(exc.status, exc.to_dict())
        except Exception as exc:  # 防御：不泄漏堆栈给客户端
            self._send_json(500, {"error": "internal", "message": str(exc)})

    def do_GET(self):  # noqa: N802
        self._handle("GET")

    def do_POST(self):  # noqa: N802
        self._handle("POST")

    def do_PUT(self):  # noqa: N802
        self._handle("PUT")

    def log_message(self, *_args):  # 安静
        return

    # ================= 业务处理器 =================

    def h_register_device(self, p, groups, body):
        self._send_json(201, self.app.service.register_device(body))

    def h_add_calibration(self, p, groups, body):
        self._send_json(201, self.app.service.add_calibration(groups["device_id"], body))

    def h_list_calibrations(self, p, groups, body):
        device = self.app.store.get_device(groups["device_id"])
        if device is None:
            raise AppError("设备不存在", "not_found", 404)
        self._send_json(
            200,
            {"calibrations": self.app.store.get_calibration_versions(groups["device_id"])},
        )

    def h_register_inspection(self, p, groups, body):
        self._send_json(201, self.app.service.register_inspection(body))

    def h_get_inspection(self, p, groups, body):
        svc = self.app.service
        insp = svc.store.get_inspection(groups["insp_id"])
        if insp is None:
            raise AppError("检测任务不存在", "not_found", 404)
        job = svc.store.get_latest_succeeded_job(insp["id"])
        out = dict(insp)
        if job:
            out["latest_positioning_version_id"] = job["calibration_version_id"]
            out["latest_merge_job_id"] = job["id"]
        out["open_review_count"] = len(svc.store.get_open_reviews(insp["id"]))
        self._send_json(200, out)

    def h_set_status(self, p, groups, body):
        self._send_json(
            200, self.app.service.set_inspection_status(groups["insp_id"], body.get("status"))
        )

    def h_upload_segment(self, p, groups, body):
        segment, created = self.app.service.upload_segment(groups["insp_id"], body)
        self._send_json(201 if created else 200, {"segment": segment, "created": created})

    def h_list_segments(self, p, groups, body):
        svc = self.app.service
        svc._require_inspection(groups["insp_id"])
        segments = svc.store.get_segments(groups["insp_id"])
        include_raw = p.role in CAN_VIEW_RAW
        self._send_json(
            200,
            {"segments": [svc._segment_view(s, include_raw=include_raw) for s in segments]},
        )

    def h_add_snapshot(self, p, groups, body):
        snapshot, created = self.app.service.add_redacted_snapshot(groups["seg_id"], body)
        self._send_json(201 if created else 200, snapshot)

    def h_create_merge_job(self, p, groups, body):
        self._send_json(202, self._submit(
            self.app.service.create_merge_job(groups["insp_id"], body.get("calibration_version_id"))
        ))

    def h_get_job(self, p, groups, body):
        job = self.app.store.get_job(groups["job_id"])
        if job is None:
            raise AppError("合并任务不存在", "not_found", 404)
        self._send_json(200, self._job_view(job))

    def h_pause_job(self, p, groups, body):
        self._send_json(200, self.app.service.pause_merge_job(groups["job_id"]))

    def h_resume_job(self, p, groups, body):
        self._send_json(202, self._submit(self.app.service.resume_merge_job(groups["job_id"])))

    def h_retry_job(self, p, groups, body):
        self._send_json(202, self._submit(self.app.service.retry_merge_job(groups["job_id"])))

    def h_list_reviews(self, p, groups, body):
        svc = self.app.service
        svc._require_inspection(groups["insp_id"])
        reviews = svc.list_reviews(groups["insp_id"], body.get("status"))
        for r in reviews:
            r["detail"] = json.loads(r.pop("detail_json"))
        self._send_json(200, {"reviews": reviews})

    def h_resolve_review(self, p, groups, body):
        self._send_json(200, self.app.service.resolve_review(groups["review_id"], body))

    def h_reject_review(self, p, groups, body):
        self._send_json(200, self.app.service.resolve_review(groups["review_id"], body, rejected=True))

    def h_mark_defect(self, p, groups, body):
        self._send_json(
            201, self.app.service.mark_defect(groups["insp_id"], body, created_by=p.name)
        )

    def h_get_defect(self, p, groups, body):
        svc = self.app.service
        defect = svc.store.get_defect(groups["defect_id"])
        if defect is None:
            raise AppError("缺陷不存在", "not_found", 404)
        include_raw = p.role in CAN_VIEW_RAW
        if not include_raw:
            # 外部角色只能查看已被发布报告冻结的缺陷。
            report = (
                svc.store.get_report(defect["frozen_by_report_id"])
                if defect["frozen_by_report_id"]
                else None
            )
            if not report or report["status"] != "published":
                raise PermissionError("该缺陷尚未随报告发布，外部角色不可见")
        self._send_json(200, svc.defect_detail(groups["defect_id"], include_raw=include_raw))

    def h_create_report(self, p, groups, body):
        report = self.app.service.create_report(groups["insp_id"], body, created_by=p.name)
        self._send_json(201, self.app.service.report_view(report))

    def h_publish_report(self, p, groups, body):
        p.require(CAN_PUBLISH)
        report = self.app.service.publish_report(groups["report_id"])
        self._send_json(200, self.app.service.report_view(report))

    def h_get_report(self, p, groups, body):
        report = self.app.store.get_report(groups["report_id"])
        if report is None:
            raise AppError("报告不存在", "not_found", 404)
        if report["status"] != "published" and p.role not in CAN_MANAGE:
            raise PermissionError("草稿报告仅工程师可见")
        self._send_json(200, self.app.service.report_view(report))

    def h_media(self, p, groups, body):
        svc = self.app.service
        rel = groups["rel"]
        if ".." in Path(rel).parts or rel.startswith("/"):
            raise AppError("非法路径", "forbidden", 403)
        blob = (svc.blob_dir / rel).resolve()
        root = svc.blob_dir.resolve()
        if root not in blob.parents and blob != root:
            raise AppError("非法路径", "forbidden", 403)
        if not blob.is_file():
            raise AppError("媒体不存在", "not_found", 404)
        if rel.startswith("redacted/"):
            pass  # 所有认证角色可见脱敏截图
        else:
            if p.role not in CAN_VIEW_RAW:
                raise PermissionError("原始影像仅项目工程师可见")
        data = blob.read_bytes()
        ctype = "image/png" if rel.endswith(".png") else "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---- 内联/异步执行选择（测试用 run_inline 保证确定性） ----

    def _submit(self, job: dict) -> dict:
        if getattr(self.app.service, "run_inline", False):
            try:
                self.app.service.run_pending(job["id"])
            except Exception:
                pass  # 失败已持久化到任务行
            return self._job_view(self.app.store.get_job(job["id"]))
        return self._job_view(job)

    @staticmethod
    def _job_view(job: dict) -> dict:
        out = dict(job)
        out["result"] = json.loads(job["result_json"]) if job["result_json"] else None
        del out["result_json"]
        return out


ROUTES = _compile([
    ("POST", r"/api/devices", CAN_MANAGE, "h_register_device"),
    ("POST", r"/api/devices/(?P<device_id>[^/]+)/calibrations", CAN_MANAGE, "h_add_calibration"),
    ("GET", r"/api/devices/(?P<device_id>[^/]+)/calibrations", None, "h_list_calibrations"),
    ("POST", r"/api/inspections", CAN_MANAGE, "h_register_inspection"),
    ("GET", r"/api/inspections/(?P<insp_id>[^/]+)", None, "h_get_inspection"),
    ("POST", r"/api/inspections/(?P<insp_id>[^/]+)/status", CAN_MANAGE, "h_set_status"),
    ("PUT", r"/api/inspections/(?P<insp_id>[^/]+)/segments", CAN_MANAGE, "h_upload_segment"),
    ("GET", r"/api/inspections/(?P<insp_id>[^/]+)/segments", None, "h_list_segments"),
    ("POST", r"/api/segments/(?P<seg_id>[^/]+)/snapshots", CAN_MANAGE, "h_add_snapshot"),
    ("POST", r"/api/inspections/(?P<insp_id>[^/]+)/merge-jobs", CAN_MANAGE, "h_create_merge_job"),
    ("GET", r"/api/merge-jobs/(?P<job_id>[^/]+)", None, "h_get_job"),
    ("POST", r"/api/merge-jobs/(?P<job_id>[^/]+)/pause", CAN_MANAGE, "h_pause_job"),
    ("POST", r"/api/merge-jobs/(?P<job_id>[^/]+)/resume", CAN_MANAGE, "h_resume_job"),
    ("POST", r"/api/merge-jobs/(?P<job_id>[^/]+)/retry", CAN_MANAGE, "h_retry_job"),
    ("GET", r"/api/inspections/(?P<insp_id>[^/]+)/reviews", None, "h_list_reviews"),
    ("POST", r"/api/reviews/(?P<review_id>[^/]+)/resolve", CAN_MANAGE, "h_resolve_review"),
    ("POST", r"/api/reviews/(?P<review_id>[^/]+)/reject", CAN_MANAGE, "h_reject_review"),
    ("POST", r"/api/inspections/(?P<insp_id>[^/]+)/defects", CAN_MANAGE, "h_mark_defect"),
    ("GET", r"/api/defects/(?P<defect_id>[^/]+)", None, "h_get_defect"),
    ("POST", r"/api/inspections/(?P<insp_id>[^/]+)/reports", CAN_MANAGE, "h_create_report"),
    ("POST", r"/api/reports/(?P<report_id>[^/]+)/publish", CAN_MANAGE, "h_publish_report"),
    ("GET", r"/api/reports/(?P<report_id>[^/]+)", None, "h_get_report"),
    ("GET", r"/media/(?P<rel>.+)", None, "h_media"),
])


def create_server(
    port: int | None = None,
    host: str | None = None,
    *,
    settings: Settings | None = None,
    run_inline: bool = False,
):
    port = port if port is not None else int(os.environ.get("PORT", "8000"))
    host = host or os.environ.get("HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, port), Handler)
    server.app = Application(settings, run_inline=run_inline)  # type: ignore[attr-defined]
    return server
