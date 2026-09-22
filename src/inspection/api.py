"""HTTP API：仅用标准库 http.server，线程化处理。

所有写接口通过 X-Role 头鉴权（默认 readonly），角色名接受中文或英文标识。
请求/响应均为 JSON；时间字段为带时区 ISO 8601。
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .merger import MergeWorker
from .service import (
    Forbidden,
    InspectionService,
    normalize_role,
)
from .store import Conflict, NotFound, open_store


def _make_routes():
    # 返回 [(method, compiled_path, handler_name), ...]
    rules = [
        ("GET", r"^/health$", "health"),
        ("POST", r"^/pipelines$", "create_pipeline"),
        ("POST", r"^/devices$", "create_device"),
        ("POST", r"^/devices/(?P<device_id>[\w-]+)/calibrations$", "add_calibration"),
        ("GET", r"^/calibrations/(?P<calibration_id>[\w-]+)$", "get_calibration"),
        ("POST", r"^/tasks$", "create_task"),
        ("GET", r"^/tasks/(?P<task_id>[\w-]+)$", "task_overview"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/status$", "set_task_status"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/loc-versions$", "create_loc_version"),
        ("GET", r"^/tasks/(?P<task_id>[\w-]+)/segments$", "list_segments"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/segments$", "upload_segment"),
        ("POST", r"^/segments/(?P<segment_id>[\w-]+)/merge$", "enqueue_merge"),
        ("GET", r"^/jobs$", "list_jobs"),
        ("POST", r"^/jobs/(?P<job_id>[\w-]+)/pause$", "pause_job"),
        ("POST", r"^/jobs/(?P<job_id>[\w-]+)/resume$", "resume_job"),
        ("POST", r"^/queue/pause$", "set_queue_paused"),
        ("GET", r"^/reviews$", "list_reviews"),
        ("POST", r"^/reviews/(?P<review_id>[\w-]+)/resolve$", "resolve_review"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/defects$", "create_defect"),
        ("GET", r"^/defects/(?P<defect_id>[\w-]+)$", "defect_detail"),
        ("POST", r"^/defects/(?P<defect_id>[\w-]+)/sightings$", "add_sighting"),
        ("POST", r"^/sightings/(?P<sighting_id>[\w-]+)/assets$", "add_asset"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/reports$", "create_report"),
        ("POST", r"^/reports/(?P<report_id>[\w-]+)/items$", "attach_report_item"),
        ("POST", r"^/reports/(?P<report_id>[\w-]+)/publish$", "publish_report"),
        ("POST", r"^/reports/(?P<report_id>[\w-]+)/withdraw$", "withdraw_report"),
        ("POST", r"^/tasks/(?P<task_id>[\w-]+)/releases$", "create_release"),
        ("POST", r"^/releases/(?P<release_id>[\w-]+)/retract$", "retract_release"),
    ]
    return [(m, re.compile(p), h) for m, p, h in rules]


ROUTES = _make_routes()


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "InspectionIndex/1.0"

    # ---- 框架 ---------------------------------------------------------------

    def _handle(self, method: str):
        path = self.path.split("?", 1)[0]
        for m, pattern, handler in ROUTES:
            if m != method:
                continue
            match = pattern.match(path)
            if match:
                try:
                    body = self._read_json() if method == "POST" else {}
                    fn = getattr(self, f"h_{handler}")
                    result = fn(body, **match.groupdict())
                except _BadRequest as e:
                    return self._json(400, {"error": "bad_request", "detail": str(e)})
                except Forbidden as e:
                    return self._json(403, {"error": "forbidden", "detail": str(e)})
                except NotFound as e:
                    return self._json(404, {"error": "not_found", "detail": str(e)})
                except Conflict as e:
                    return self._json(409, {"error": "conflict", "detail": str(e)})
                except Exception as e:  # 兜底：不让连接被静默重置
                    import traceback
                    traceback.print_exc()
                    return self._json(500, {
                        "error": "internal",
                        "detail": f"{type(e).__name__}: {e}"})
                if result is None:
                    return self._json(200, {"status": "ok"})
                status, payload = result if isinstance(result, tuple) else (200, result)
                return self._json(status, payload)
        self._json(404, {"error": "not_found"})

    def do_GET(self):  # noqa: N802
        self._handle("GET")

    def do_POST(self):  # noqa: N802
        self._handle("POST")

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise _BadRequest(f"请求体不是合法 JSON：{e}")
        if not isinstance(data, dict):
            raise _BadRequest("请求体必须是 JSON 对象")
        return data

    def _json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        return

    # ---- 便利访问 -----------------------------------------------------------

    @property
    def svc(self) -> InspectionService:
        return self.server.service

    @property
    def store(self):
        return self.server.store

    @property
    def role(self) -> str:
        return normalize_role(self.headers.get("X-Role"))

    @staticmethod
    def require(body: dict, key: str):
        if key not in body or body[key] in (None, ""):
            raise _BadRequest(f"缺少必填字段：{key}")
        return body[key]

    # ---- 健康 ---------------------------------------------------------------

    def h_health(self, _body):
        return {"status": "ok", "service": "pipeline-inspection-index"}

    # ---- 基础数据 ------------------------------------------------------------

    def h_create_pipeline(self, body):
        self.svc.require_write(self.role)
        return 201, self.store.create_pipeline(
            self.require(body, "code"),
            self.require(body, "start_manhole"),
            self.require(body, "end_manhole"),
            float(self.require(body, "length_m")),
        )

    def h_create_device(self, body):
        self.svc.require_write(self.role)
        return 201, self.store.create_device(
            self.require(body, "code"),
            self.require(body, "model"),
            float(self.require(body, "nominal_wheel_mm")),
        )

    def h_add_calibration(self, body, device_id):
        self.svc.require_write(self.role)
        return 201, self.store.add_calibration(
            device_id,
            float(self.require(body, "wheel_diameter_mm")),
            int(self.require(body, "pulses_per_rev")),
            self.require(body, "calibrated_at"),
            body.get("note", ""),
            body.get("supersedes_id"),
        )

    def h_get_calibration(self, _body, calibration_id):
        return self.store.get("calibrations", calibration_id)

    # ---- 任务 / 定位版本 ------------------------------------------------------

    def h_create_task(self, body):
        self.svc.require_write(self.role)
        return 201, self.store.create_task(
            self.require(body, "pipeline_id"),
            self.require(body, "device_id"),
            self.require(body, "code"),
            body.get("recorded_start"),
            body.get("recorded_end"),
        )

    def h_task_overview(self, _body, task_id):
        task = self.store.get("tasks", task_id)
        jobs = self.store.list_jobs(task_id)
        ranges = [dict(r) for r in self.store.execute(
            "SELECT sr.*, s.filename FROM segment_ranges sr "
            "JOIN segments s ON s.id=sr.segment_id WHERE s.task_id=?",
            (task_id,),
        )]
        reviews = self.store.list_reviews(task_id=task_id)
        return {"task": task, "merge_jobs": jobs, "segment_ranges": ranges,
                "reviews": reviews,
                "queue_paused": self.store.controls_paused()}

    def h_set_task_status(self, body, task_id):
        self.svc.require_write(self.role)
        status = self.require(body, "status")
        if status not in ("planned", "active", "completed", "cancelled"):
            raise _BadRequest("非法任务状态")
        return self.store.set_task_status(task_id, status)

    def h_create_loc_version(self, body, task_id):
        self.svc.require_write(self.role)
        return 201, self.store.create_loc_version(
            task_id,
            self.require(body, "calibration_id"),
            float(self.require(body, "anchor_m")),
            int(self.require(body, "anchor_pulses")),
            self.require(body, "anchor_source"),
            body.get("note", ""),
            body.get("supersedes_id"),
        )

    # ---- 分段上传 / 合并队列 --------------------------------------------------

    def h_list_segments(self, _body, task_id):
        rows = [dict(r) for r in self.store.execute(
            "SELECT * FROM segments WHERE task_id=? ORDER BY received_at", (task_id,)
        )]
        raw = {r["segment_id"]: dict(r) for r in self.store.execute(
            "SELECT * FROM segment_raw_readings WHERE segment_id IN "
            "(SELECT id FROM segments WHERE task_id=?)", (task_id,))}
        ranges = {r["segment_id"]: dict(r) for r in self.store.execute(
            "SELECT * FROM segment_ranges WHERE segment_id IN "
            "(SELECT id FROM segments WHERE task_id=?)", (task_id,))}
        for s in rows:
            s["raw_readings"] = raw.get(s["id"])
            s["range"] = ranges.get(s["id"])
        return {"segments": rows}

    def h_upload_segment(self, body, task_id):
        self.svc.require_write(self.role)
        segment, created = self.store.upload_segment(
            task_id,
            self.require(body, "upload_batch"),
            self.require(body, "filename"),
            self.require(body, "sha256"),
            int(self.require(body, "size_bytes")),
            body.get("device_clock_start"),
            body.get("device_clock_end"),
            body.get("pulse_start"),
            body.get("pulse_end"),
        )
        auto_enqueue = body.get("loc_version_id")
        job = None
        if auto_enqueue:
            job = self.store.enqueue_merge(
                segment["id"], auto_enqueue, body.get("prereq_segment_id")
            )
            self.server.worker.notify()
        return (201 if created else 200), {
            "segment": segment, "created": created,
            "idempotent_reuse": not created, "merge_job": job,
        }

    def h_enqueue_merge(self, body, segment_id):
        self.svc.require_write(self.role)
        job = self.store.enqueue_merge(
            segment_id,
            self.require(body, "loc_version_id"),
            body.get("prereq_segment_id"),
        )
        self.server.worker.notify()
        return 201, job

    def h_list_jobs(self, _body):
        from urllib.parse import parse_qs, urlparse
        qs = parse_qs(urlparse(self.path).query)
        return {"jobs": self.store.list_jobs(qs.get("task_id", [None])[0])}

    def h_pause_job(self, _body, job_id):
        return self.svc.pause_job(job_id, self.role)

    def h_resume_job(self, _body, job_id):
        return self.svc.resume_job(job_id, self.role)

    def h_set_queue_paused(self, body):
        paused = bool(self.require(body, "paused"))
        return self.svc.set_queue_paused(paused, self.role)

    # ---- 复核 ----------------------------------------------------------------

    def h_list_reviews(self, _body):
        from urllib.parse import parse_qs, urlparse
        qs = parse_qs(urlparse(self.path).query)
        reviews = self.store.list_reviews(
            qs.get("task_id", [None])[0], qs.get("status", [None])[0]
        )
        for r in reviews:
            r["detail"] = json.loads(r.pop("detail_json"))
        return {"reviews": reviews}

    def h_resolve_review(self, body, review_id):
        return self.svc.resolve_review(
            review_id, self.require(body, "resolution"), self.role
        )

    # ---- 缺陷 ----------------------------------------------------------------

    def h_create_defect(self, body, task_id):
        self.svc.require_write(self.role)
        return 201, self.store.create_defect(
            task_id,
            self.require(body, "code"),
            self.require(body, "severity"),
            body.get("created_by", self.role),
        )

    def h_defect_detail(self, _body, defect_id):
        return self.svc.defect_detail(defect_id, self.role)

    def h_add_sighting(self, body, defect_id):
        sighting = self.svc.mark_sighting(
            defect_id,
            self.require(body, "loc_version_id"),
            self.require(body, "segment_id"),
            float(self.require(body, "file_offset_s")),
            int(self.require(body, "pulse_reading")),
            self.role,
        )
        return 201, sighting

    def h_add_asset(self, body, sighting_id):
        kind = self.require(body, "kind")
        if kind == "raw_frame" and self.role != "ops":
            raise Forbidden("原始影像只允许运维人员登记/查看")
        if kind == "redacted" and self.role not in ("ops", "dispatcher"):
            raise Forbidden("无附件登记权限")
        self.svc.require_write(self.role)
        return 201, self.store.add_asset(
            sighting_id, kind,
            self.require(body, "sha256"),
            int(self.require(body, "size_bytes")),
            self.require(body, "storage_path"),
        )

    # ---- 报告 / 发布 ----------------------------------------------------------

    def h_create_report(self, body, task_id):
        self.svc.require_write(self.role)
        return 201, self.store.create_report(
            task_id, self.require(body, "title"),
            body.get("created_by", self.role),
        )

    def h_attach_report_item(self, body, report_id):
        self.svc.require_write(self.role)
        self.store.attach_sighting_to_report(
            report_id, self.require(body, "sighting_id")
        )
        return {"status": "ok"}

    def h_publish_report(self, _body, report_id):
        return self.svc.publish_report(report_id, self.role)

    def h_withdraw_report(self, _body, report_id):
        self.svc.require_write(self.role)
        return self.store.withdraw_report(report_id)

    def h_create_release(self, body, task_id):
        return 201, self.svc.create_release(
            task_id, self.require(body, "report_id"),
            self.require(body, "version_label"), self.role,
        )

    def h_retract_release(self, _body, release_id):
        return self.svc.retract_release(release_id, self.role)


class _BadRequest(Exception):
    pass


def create_server(db_path: str | None = None, host: str | None = None,
                  port: int | None = None, start_worker: bool = True):
    import os

    db_path = db_path or os.environ.get("INSPECTION_DB", ".runtime/inspection.db")
    host = host or os.environ.get("HOST", "0.0.0.0")
    port = int(port if port is not None else os.environ.get("PORT", "8000"))

    store = open_store(db_path)
    worker = MergeWorker(store)
    service = InspectionService(store, worker)

    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.store = store
    server.worker = worker
    server.service = service
    if start_worker:
        worker.start()
    return server
