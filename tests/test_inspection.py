"""端到端测试：通过 HTTP API 验证全部业务不变量。

每个用例使用独立临时库，不依赖主机隐藏状态。
"""

import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from inspection.api import create_server  # noqa: E402
from inspection.merger import MergeWorker  # noqa: E402
from inspection.service import InspectionService  # noqa: E402
from inspection.store import Conflict, open_store  # noqa: E402

OPS = {"X-Role": "ops"}
DISPATCH = {"X-Role": "dispatcher"}
REGULATOR = {"X-Role": "regulator"}
ZH_OPS = {"X-Role": urllib.parse.quote("运维人员")}


def _wait(predicate, timeout=3.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class ApiClient:
    def __init__(self, server):
        self.port = server.server_port
        self.base = f"http://127.0.0.1:{self.port}"

    def call(self, method, path, body=None, headers=None, expect=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req) as resp:
                payload = json.loads(resp.read())
                status = resp.status
        except urllib.error.HTTPError as e:
            payload = json.loads(e.read())
            status = e.code
        if expect is not None:
            assert status == expect, f"{method} {path} -> {status}: {payload}"
        return status, payload

    def get(self, path, headers=None, expect=200):
        return self.call("GET", path, None, headers, expect)

    def post(self, path, body=None, headers=None, expect=200):
        return self.call("POST", path, body or {}, headers, expect)


class ApiTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "test.db")
        self.server = create_server(
            self.db, host="127.0.0.1", port=0, start_worker=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)
        self.api = ApiClient(self.server)

    def _shutdown(self):
        self.server.worker.stop()
        self.server.shutdown()
        self.server.server_close()

    def wait_job(self, jid, want="done", timeout=3.0):
        ok = _wait(lambda: self.get_job(jid)["status"] == want, timeout=timeout)
        self.assertTrue(ok, f"作业 {jid} 未达到 {want}：{self.get_job(jid)}")
        return self.get_job(jid)

    def get_job(self, jid):
        _, jobs = self.api.get("/jobs")
        return next(j for j in jobs["jobs"] if j["id"] == jid)

    # ---- 造数辅助 -----------------------------------------------------------

    def mk_pipeline(self, length=320.0):
        _, p = self.api.post("/pipelines", {
            "code": "P-" + uuid.uuid4().hex[:10],
            "start_manhole": "起#1", "end_manhole": "止#9",
            "length_m": length}, OPS, 201)
        return p

    def mk_device(self):
        _, d = self.api.post("/devices", {
            "code": "D-" + uuid.uuid4().hex[:10], "model": "Q5",
            "nominal_wheel_mm": 200.0}, OPS, 201)
        return d

    def mk_task(self, pipeline=None, device=None):
        pipeline = pipeline or self.mk_pipeline()
        device = device or self.mk_device()
        _, t = self.api.post("/tasks", {
            "pipeline_id": pipeline["id"], "device_id": device["id"],
            "code": "T-" + uuid.uuid4().hex[:10]}, OPS, 201)
        return t, pipeline, device

    def mk_calibration(self, device, diameter=200.0, supersedes=None):
        body = {"wheel_diameter_mm": diameter, "pulses_per_rev": 1000,
                "calibrated_at": "2026-09-10T08:00:00+08:00"}
        if supersedes:
            body["supersedes_id"] = supersedes
        _, c = self.api.post(f"/devices/{device['id']}/calibrations",
                             body, OPS, 201)
        return c

    def mk_locv(self, task, cal, anchor_m=0.0, anchor_pulses=0,
                source="start_manhole", supersedes=None):
        body = {"calibration_id": cal["id"], "anchor_m": anchor_m,
                "anchor_pulses": anchor_pulses, "anchor_source": source}
        if supersedes:
            body["supersedes_id"] = supersedes
        _, v = self.api.post(f"/tasks/{task['id']}/loc-versions",
                             body, OPS, 201)
        return v


class HealthTest(unittest.TestCase):
    """基线测试：健康检查契约保持不变。"""

    def test_health_returns_service_identity(self):
        tmp = tempfile.TemporaryDirectory()
        server = create_server(str(Path(tmp.name) / "h.db"),
                               host="127.0.0.1", port=0, start_worker=False)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/health") as r:
                self.assertEqual(r.status, 200)
                self.assertEqual(json.load(r),
                                 {"status": "ok",
                                  "service": "pipeline-inspection-index"})
        finally:
            server.shutdown()
            server.server_close()
            server.store.close()
            tmp.cleanup()


class IdempotentUploadTest(ApiTestBase):
    def test_duplicate_upload_returns_same_segment_and_job(self):
        task, _, _ = self.mk_task()
        payload = {"upload_batch": "b1", "filename": "a.mp4",
                   "sha256": "a" * 64, "size_bytes": 10,
                   "pulse_start": 0, "pulse_end": 100}
        s1, first = self.api.post(f"/tasks/{task['id']}/segments",
                                  payload, OPS, 201)
        self.assertTrue(first["created"])
        payload["filename"] = "a-renamed.mp4"  # 同盘同内容再传
        s2, second = self.api.post(f"/tasks/{task['id']}/segments",
                                   payload, OPS, 200)
        self.assertFalse(second["created"])
        self.assertTrue(second["idempotent_reuse"])
        self.assertEqual(first["segment"]["id"], second["segment"]["id"])
        # 文件名等文件事实不被重复上传改写
        self.assertEqual(second["segment"]["filename"], "a.mp4")

        _, segs = self.api.get(f"/tasks/{task['id']}/segments")
        self.assertEqual(len(segs["segments"]), 1)

    def test_same_content_different_tasks_are_distinct(self):
        t1, _, _ = self.mk_task()
        t2, _, _ = self.mk_task()
        body = {"upload_batch": "b", "filename": "x.mp4",
                "sha256": "f" * 64, "size_bytes": 1}
        self.api.post(f"/tasks/{t1['id']}/segments", body, OPS, 201)
        self.api.post(f"/tasks/{t2['id']}/segments", body, OPS, 201)
        _, s1 = self.api.get(f"/tasks/{t1['id']}/segments")
        _, s2 = self.api.get(f"/tasks/{t2['id']}/segments")
        self.assertEqual(len(s1["segments"]), 1)
        self.assertEqual(len(s2["segments"]), 1)
        self.assertNotEqual(s1["segments"][0]["id"], s2["segments"][0]["id"])


class MergeReviewTest(ApiTestBase):
    def _segment(self, task, lv, name, p0, p1, prereq=None):
        import hashlib
        body = {"upload_batch": "b", "filename": name,
                "sha256": hashlib.sha256(name.encode()).hexdigest(),
                "size_bytes": 100, "pulse_start": p0, "pulse_end": p1,
                "loc_version_id": lv["id"]}
        if prereq:
            body["prereq_segment_id"] = prereq
        _, res = self.api.post(f"/tasks/{task['id']}/segments",
                               body, OPS, expect=201)
        return res["segment"], res["merge_job"]

    def test_missing_gap_blocks_and_creates_review_not_silent_join(self):
        task, pipe, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        # 0~100m（200mm 轮，1000 脉冲/转 → 159155 ≈ 100m）
        seg1, job1 = self._segment(task, lv, "p1.mp4", 0, 159155)
        self.assertTrue(_wait(lambda: self._job_done(job1["id"])))
        # 120~220m：20m 缺口
        seg2, job2 = self._segment(task, lv, "p3.mp4", 190986, 350141)

        self.assertTrue(_wait(lambda: self._job_status(job2["id"]) == "blocked"))
        _, reviews = self.api.get(f"/reviews?task_id={task['id']}")
        open_gaps = [r for r in reviews["reviews"]
                     if r["kind"] == "missing_gap" and r["status"] == "open"]
        self.assertEqual(len(open_gaps), 1)
        self.assertIn("缺口", open_gaps[0]["detail"]["reason"])
        # 第二段没有里程区间——没有被悄悄拼接
        _, segs = self.api.get(f"/tasks/{task['id']}/segments")
        by_id = {s["id"]: s for s in segs["segments"]}
        self.assertIsNotNone(by_id[seg1["id"]]["range"])
        self.assertIsNone(by_id[seg2["id"]]["range"])

        # 人工处理复核后作业可继续（有书面 resolution，不是静默放行）
        self.api.post(f"/reviews/{open_gaps[0]['id']}/resolve",
                      {"resolution": "核实该段为低风险明挖段，影像缺失可接受"}, OPS)
        self.assertTrue(_wait(lambda: self._job_done(job2["id"])))
        _, segs = self.api.get(f"/tasks/{task['id']}/segments")
        by_id = {s["id"]: s for s in segs["segments"]}
        self.assertIsNotNone(by_id[seg2["id"]]["range"])

    def test_coordinate_overlap_conflict_blocks(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        _, job1 = self._segment(task, lv, "a.mp4", 0, 159155)       # 0~100m
        self.assertTrue(_wait(lambda: self._job_done(job1["id"])))
        _, job2 = self._segment(task, lv, "b.mp4", 80000, 200000)   # 50~125m 重叠
        self.assertTrue(_wait(lambda: self._job_status(job2["id"]) == "blocked"))
        job = self._job(job2["id"])
        self.assertIn("coordinate_conflict", job["last_error"])
        _, reviews = self.api.get(f"/reviews?task_id={task['id']}&status=open")
        self.assertTrue(any(r["kind"] == "coordinate_conflict"
                            for r in reviews["reviews"]))

    def test_range_overflow_blocks(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        _, job = self._segment(task, lv, "far.mp4", 0, 600000)  # ~377m > 320m
        self.assertTrue(_wait(lambda: self._job_status(job["id"]) == "blocked"))
        self.assertIn("range_overflow", self._job(job["id"])["last_error"])

    def test_missing_pulses_blocks_without_crash(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "np.mp4",
                "sha256": hashlib.sha256(b"np").hexdigest(),
                "size_bytes": 1, "loc_version_id": lv["id"]}
        _, res = self.api.post(f"/tasks/{task['id']}/segments",
                               body, OPS, 201)
        self.assertTrue(_wait(
            lambda: self._job_status(res["merge_job"]["id"]) == "blocked"))

    def test_review_resolution_requires_text(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        _, job = self._segment(task, lv, "g.mp4", 0, 159155)
        self.assertTrue(_wait(lambda: self._job_done(job["id"])))
        _, job2 = self._segment(task, lv, "h.mp4", 190986, 350141)
        self.assertTrue(_wait(lambda: self._job_status(job2["id"]) == "blocked"))
        _, reviews = self.api.get(f"/reviews?task_id={task['id']}")
        rid = reviews["reviews"][0]["id"]
        self.api.post(f"/reviews/{rid}/resolve",
                      {"resolution": "  "}, OPS, expect=409)

    def _job_status(self, jid):
        return self._job(jid)["status"]

    def _job(self, jid):
        _, jobs = self.api.get("/jobs")
        return next(j for j in jobs["jobs"] if j["id"] == jid)

    def _job_done(self, jid):
        return self._job_status(jid) == "done"


class QueueControlTest(ApiTestBase):
    def test_global_pause_and_resume(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        self.api.post("/queue/pause", {"paused": True}, OPS)
        import hashlib
        body = {"upload_batch": "b", "filename": "q.mp4",
                "sha256": hashlib.sha256(b"q").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 15915,
                "loc_version_id": lv["id"]}
        _, res = self.api.post(f"/tasks/{task['id']}/segments",
                               body, OPS, 201)
        jid = res["merge_job"]["id"]
        time.sleep(0.3)
        self.assertEqual(self._get_job(jid)["status"], "queued")
        self.api.post("/queue/pause", {"paused": False}, OPS)
        self.assertTrue(_wait(lambda: self._get_job(jid)["status"] == "done"))

    def test_single_job_pause_and_resume(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        # 先排队并立刻全局暂停，保证作业仍在 queued
        self.api.post("/queue/pause", {"paused": True}, OPS)
        import hashlib
        body = {"upload_batch": "b", "filename": "pj.mp4",
                "sha256": hashlib.sha256(b"pj").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 15915,
                "loc_version_id": lv["id"]}
        _, res = self.api.post(f"/tasks/{task['id']}/segments",
                               body, OPS, 201)
        jid = res["merge_job"]["id"]
        self.api.post(f"/jobs/{jid}/pause", {}, OPS)
        self.assertEqual(self._get_job(jid)["status"], "paused")
        self.api.post("/queue/pause", {"paused": False}, OPS)
        time.sleep(0.3)
        self.assertEqual(self._get_job(jid)["status"], "paused")
        self.api.post(f"/jobs/{jid}/resume", {}, OPS)
        self.assertTrue(_wait(lambda: self._get_job(jid)["status"] == "done"))

    def test_failed_job_resume_keeps_error(self):
        # 直接在 store 层构造一个 failed 作业，验证失败原因可读、可继续
        store = self.server.store
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        seg, _ = store.upload_segment(
            task["id"], "b", "f.mp4",
            hashlib.sha256(b"f").hexdigest(), 1,
            pulse_start=0, pulse_end=100)
        job = store.enqueue_merge(seg["id"], lv["id"])
        store.fail_job(job["id"], "convert", "simulated failure: 编码器离线")
        _, payload = self.api.get("/jobs")
        j = next(x for x in payload["jobs"] if x["id"] == job["id"])
        self.assertEqual(j["status"], "failed")
        self.assertIn("编码器离线", j["last_error"])
        self.api.post(f"/jobs/{job['id']}/resume", {}, OPS)
        self.assertTrue(_wait(lambda: self._get_job(job["id"])["status"] == "done"))

    def _get_job(self, jid):
        _, jobs = self.api.get("/jobs")
        return next(j for j in jobs["jobs"] if j["id"] == jid)


class RestartRecoveryTest(ApiTestBase):
    def test_queue_and_failure_survive_restart(self):
        import hashlib
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)

        # 作业 A：因缺口阻塞（状态须落盘）
        body1 = {"upload_batch": "b", "filename": "r1.mp4",
                 "sha256": hashlib.sha256(b"r1").hexdigest(),
                 "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                 "loc_version_id": lv["id"]}
        _, r1 = self.api.post(f"/tasks/{task['id']}/segments",
                              body1, OPS, 201)
        self.assertTrue(_wait(
            lambda: self._get_job(r1["merge_job"]["id"])["status"] == "done"))
        body2 = {"upload_batch": "b", "filename": "r3.mp4",
                 "sha256": hashlib.sha256(b"r3").hexdigest(),
                 "size_bytes": 1, "pulse_start": 190986, "pulse_end": 350141,
                 "loc_version_id": lv["id"]}
        _, r2 = self.api.post(f"/tasks/{task['id']}/segments",
                              body2, OPS, 201)
        blocked_id = r2["merge_job"]["id"]
        self.assertTrue(_wait(
            lambda: self._get_job(blocked_id)["status"] == "blocked"))

        # 作业 B：failed + 原因
        seg, _ = self.server.store.upload_segment(
            task["id"], "b", "rfail.mp4",
            hashlib.sha256(b"rfail").hexdigest(), 1,
            pulse_start=0, pulse_end=50)
        jfail = self.server.store.enqueue_merge(seg["id"], lv["id"])
        self.server.store.fail_job(jfail["id"], "validate", "boom: 磁盘只读")

        # 重启服务（同库路径）
        self.server.worker.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.server = create_server(self.db, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api = ApiClient(self.server)

        _, jobs = self.api.get("/jobs")
        statuses = {j["id"]: (j["status"], j["last_error"]) for j in jobs["jobs"]}
        self.assertEqual(statuses[blocked_id][0], "blocked")
        self.assertIn("缺口", statuses[blocked_id][1])
        self.assertEqual(statuses[jfail["id"]][0], "failed")
        self.assertIn("磁盘只读", statuses[jfail["id"]][1])

    def _get_job(self, jid):
        _, jobs = self.api.get("/jobs")
        return next(j for j in jobs["jobs"] if j["id"] == jid)


class CalibrationVersioningTest(ApiTestBase):
    def test_replacing_calibration_creates_new_loc_version(self):
        task, _, dev = self.mk_task()
        cal1 = self.mk_calibration(dev, diameter=200.0)
        cal2 = self.mk_calibration(dev, diameter=197.5, supersedes=cal1["id"])
        self.assertNotEqual(cal1["id"], cal2["id"])
        self.assertEqual(cal2["supersedes_id"], cal1["id"])
        # 校准序列只增不改
        _, payload = self.api.get(f"/calibrations/{cal1['id']}")
        self.assertEqual(payload["wheel_diameter_mm"], 200.0)

        lv1 = self.mk_locv(task, cal1)
        lv2 = self.mk_locv(task, cal2, anchor_m=120.0, anchor_pulses=220000,
                           source="manual", supersedes=lv1["id"])
        self.assertEqual(lv2["supersedes_id"], lv1["id"])

    def test_calibration_of_other_device_rejected(self):
        t, _, d1 = self.mk_task()
        d2 = self.mk_device()
        c_other = self.mk_calibration(d2)
        status, _ = self.api.post(f"/tasks/{t['id']}/loc-versions", {
            "calibration_id": c_other["id"], "anchor_m": 0,
            "anchor_pulses": 0, "anchor_source": "start_manhole"}, OPS, 409)


class DefectFreezeTest(ApiTestBase):
    def _ready_defect(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "d.mp4",
                "sha256": hashlib.sha256(b"d").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                "loc_version_id": lv["id"]}
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        seg = r["segment"]
        self.assertTrue(_wait(
            lambda: self._job(r["merge_job"]["id"])["status"] == "done"))
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CX-破裂", "severity": "3"}, OPS, 201)
        _, sgt = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv["id"], "segment_id": seg["id"],
            "file_offset_s": 120.5, "pulse_reading": 79577}, OPS, 201)
        self.assertAlmostEqual(sgt["distance_m"], 50.0, places=1)
        self.api.post(f"/sightings/{sgt['id']}/assets", {
            "kind": "raw_frame", "sha256": "r" * 64,
            "size_bytes": 9, "storage_path": "raw/1.bin"}, OPS, 201)
        self.api.post(f"/sightings/{sgt['id']}/assets", {
            "kind": "redacted", "sha256": "x" * 64,
            "size_bytes": 9, "storage_path": "red/1.png"}, DISPATCH, 201)
        confirmed = self.server.store.confirm_sighting(sgt["id"])
        _, report = self.api.post(f"/tasks/{task['id']}/reports",
                                  {"title": "2026-09 修复设计引用表"}, DISPATCH, 201)
        self.api.post(f"/reports/{report['id']}/items",
                      {"sighting_id": sgt["id"]}, DISPATCH)
        pub = self.api.post(f"/reports/{report['id']}/publish",
                            {}, DISPATCH, 200)[1]
        _, release = self.api.post(f"/tasks/{task['id']}/releases", {
            "report_id": report["id"], "version_label": "v2026.09.1"},
            DISPATCH, 201)
        return task, cal, lv, seg, defect, sgt, report, release

    def _job(self, jid):
        _, jobs = self.api.get("/jobs")
        return next(j for j in jobs["jobs"] if j["id"] == jid)

    def test_defect_query_returns_full_trace(self):
        task, cal, lv, seg, defect, sgt, _, release = self._ready_defect()
        status, detail = self.api.get(f"/defects/{defect['id']}")
        self.assertEqual(status, 200)
        item = detail["sightings"][0]
        # 文件片段
        self.assertEqual(item["segment"]["id"], seg["id"])
        self.assertEqual(item["segment"]["sha256"], seg["sha256"])
        # 校准版本
        self.assertEqual(item["calibration"]["id"], cal["id"])
        self.assertEqual(item["loc_version"]["id"], lv["id"])
        # 里程换算过程
        calc = item["conversion"]
        self.assertIn("formula", calc)
        self.assertEqual(calc["inputs"]["pulse_reading"], 79577)
        self.assertEqual(len(calc["steps"]), 4)
        self.assertAlmostEqual(calc["distance_m"], 50.0, places=1)
        # 当前允许对外发布
        self.assertTrue(detail["externally_publishable"])
        self.assertTrue(item["publish"]["in_active_release"])
        self.assertEqual(
            item["publish"]["active_release_id"], release["id"])

    def test_published_sighting_cannot_change_with_old_data(self):
        import hashlib
        task, cal, lv, seg, defect, sgt, report, release = self._ready_defect()
        # 重新合并同一分段改写区间 -> 被拒绝（已被缺陷引用）
        with self.assertRaises(Conflict):
            self.server.store.write_segment_range(
                self.server.store.execute(
                    "SELECT id FROM merge_jobs WHERE segment_id=?",
                    (seg["id"],)).fetchone()[0],
                lv["id"], 5.0, 105.0, 8000, 168000)
        # 新校准 + 新定位版本下复算：旧记录已发布，必须冻结，不被 supersede
        cal2 = self.mk_calibration(self.server.store.get("devices", task["device_id"]),
                                   diameter=190.0, supersedes=cal["id"])
        lv2 = self.mk_locv(task, cal2, anchor_m=120.0, anchor_pulses=220000,
                           source="manual", supersedes=lv["id"])
        _, s2 = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv2["id"], "segment_id": seg["id"],
            "file_offset_s": 121.0, "pulse_reading": 79577}, OPS, 201)
        _, detail = self.api.get(f"/defects/{defect['id']}", OPS)
        states = {i["sighting"]["id"]: i["sighting"]["state"]
                  for i in detail["sightings"]}
        self.assertEqual(states[sgt["id"]], "confirmed")  # 已发布记录未变
        self.assertIn(s2["id"], states)
        # 已发布报告不能再增删条目
        self.api.post(f"/reports/{report['id']}/items",
                      {"sighting_id": s2["id"]}, DISPATCH, 409)

    def test_unpublished_sighting_is_superseded_on_recalibration(self):
        task, _, dev = self.mk_task()
        cal1 = self.mk_calibration(dev, 200.0)
        lv1 = self.mk_locv(task, cal1)
        import hashlib
        body = {"upload_batch": "b", "filename": "u.mp4",
                "sha256": hashlib.sha256(b"u").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                "loc_version_id": lv1["id"]}
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        self.assertTrue(_wait(
            lambda: self._job(r["merge_job"]["id"])["status"] == "done"))
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CJ-沉积", "severity": "2"}, OPS, 201)
        _, s1 = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv1["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 10, "pulse_reading": 79577}, OPS, 201)
        cal2 = self.mk_calibration(dev, 197.5, supersedes=cal1["id"])
        lv2 = self.mk_locv(task, cal2, anchor_m=0.0, anchor_pulses=0,
                           source="start_manhole", supersedes=lv1["id"])
        self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv2["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 10, "pulse_reading": 79577}, OPS, 201)
        _, detail = self.api.get(f"/defects/{defect['id']}", OPS)
        states = {i["sighting"]["id"]: i["sighting"]["state"]
                  for i in detail["sightings"]}
        self.assertEqual(states[s1["id"]], "superseded")


class RoleVisibilityTest(ApiTestBase):
    def _published_defect(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "v.mp4",
                "sha256": hashlib.sha256(b"v").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                "loc_version_id": lv["id"]}
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        self.wait_job(r["merge_job"]["id"])
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CX", "severity": "2"}, OPS, 201)
        _, sgt = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 3, "pulse_reading": 79577}, OPS, 201)
        self.server.store.confirm_sighting(sgt["id"])
        self.api.post(f"/sightings/{sgt['id']}/assets", {
            "kind": "raw_frame", "sha256": "r" * 64,
            "size_bytes": 1, "storage_path": "raw"}, OPS, 201)
        self.api.post(f"/sightings/{sgt['id']}/assets", {
            "kind": "redacted", "sha256": "x" * 64,
            "size_bytes": 1, "storage_path": "red"}, DISPATCH, 201)
        _, report = self.api.post(f"/tasks/{task['id']}/reports",
                                  {"title": "r"}, DISPATCH, 201)
        self.api.post(f"/reports/{report['id']}/items",
                      {"sighting_id": sgt["id"]}, DISPATCH)
        self.api.post(f"/reports/{report['id']}/publish", {}, DISPATCH)
        _, rel = self.api.post(f"/tasks/{task['id']}/releases", {
            "report_id": report["id"], "version_label": "v1"}, DISPATCH, 201)
        return task, defect, sgt, rel

    def test_ops_sees_raw_and_redacted(self):
        _, defect, _, _ = self._published_defect()
        _, detail = self.api.get(f"/defects/{defect['id']}", OPS)
        kinds = {a["kind"] for a in detail["sightings"][0]["assets"]}
        self.assertEqual(kinds, {"raw_frame", "redacted"})

    def test_dispatcher_sees_only_redacted(self):
        _, defect, _, _ = self._published_defect()
        _, detail = self.api.get(f"/defects/{defect['id']}", DISPATCH)
        kinds = {a["kind"] for a in detail["sightings"][0]["assets"]}
        self.assertEqual(kinds, {"redacted"})

    def test_regulator_sees_only_published_redacted(self):
        _, defect, _, _ = self._published_defect()
        _, detail = self.api.get(f"/defects/{defect['id']}", REGULATOR)
        kinds = {a["kind"] for a in detail["sightings"][0]["assets"]}
        self.assertEqual(kinds, {"redacted"})
        self.assertTrue(detail["externally_publishable"])

    def test_regulator_cannot_see_unpublished_defect(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "hid.mp4",
                "sha256": hashlib.sha256(b"hid").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                "loc_version_id": lv["id"]}
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        self.wait_job(r["merge_job"]["id"])
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CX", "severity": "1"}, OPS, 201)
        self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 1, "pulse_reading": 100}, OPS, 201)
        self.api.get(f"/defects/{defect['id']}", REGULATOR, expect=403)

    def test_regulator_cannot_write_or_publish(self):
        task, _, _ = self.mk_task()
        self.api.post(f"/tasks/{task['id']}/defects",
                      {"code": "x", "severity": "1"}, REGULATOR, 403)

    def test_only_dispatcher_publishes(self):
        _, defect, _, _ = self._published_defect()
        task, _, _ = self.mk_task()
        # ops 无权发布（直接对不存在报告也应先被角色拦下）
        self.api.post("/reports/rpt-x/publish", {}, OPS, expect=403)

    def test_raw_asset_registration_ops_only(self):
        _, defect, sgt, _ = self._published_defect()
        self.api.post(f"/sightings/{sgt['id']}/assets", {
            "kind": "raw_frame", "sha256": "z" * 64,
            "size_bytes": 1, "storage_path": "p"}, DISPATCH, 403)

    def test_chinese_role_header_accepted(self):
        _, defect, _, _ = self._published_defect()
        _, detail = self.api.get(f"/defects/{defect['id']}", ZH_OPS)
        kinds = {a["kind"] for a in detail["sightings"][0]["assets"]}
        self.assertIn("raw_frame", kinds)


class PublishGateTest(ApiTestBase):
    def _defect_with_asset(self, redacted: bool):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "g.mp4",
                "sha256": hashlib.sha256(b"g").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 159155,
                "loc_version_id": lv["id"]}
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        self.wait_job(r["merge_job"]["id"])
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CX", "severity": "2"}, OPS, 201)
        _, sgt = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 2, "pulse_reading": 79577}, OPS, 201)
        if redacted:
            self.api.post(f"/sightings/{sgt['id']}/assets", {
                "kind": "redacted", "sha256": "x" * 64,
                "size_bytes": 1, "storage_path": "red"}, DISPATCH, 201)
        return task, defect, sgt

    def test_candidate_cannot_be_published(self):
        task, defect, sgt = self._defect_with_asset(redacted=True)
        _, report = self.api.post(f"/tasks/{task['id']}/reports",
                                  {"title": "r"}, DISPATCH, 201)
        self.api.post(f"/reports/{report['id']}/items",
                      {"sighting_id": sgt["id"]}, DISPATCH)
        self.api.post(f"/reports/{report['id']}/publish", {}, DISPATCH, 409)
        _, detail = self.api.get(f"/defects/{defect['id']}", DISPATCH)
        self.assertFalse(detail["sightings"][0]["publish"]["publishable"])
        self.assertTrue(any("确认" in x for x in
                            detail["sightings"][0]["publish"]["reasons"]))

    def test_without_redacted_asset_not_externally_publishable(self):
        _, defect, _ = self._defect_with_asset(redacted=False)
        _, detail = self.api.get(f"/defects/{defect['id']}", DISPATCH)
        reasons = detail["sightings"][0]["publish"]["reasons"]
        self.assertTrue(any("脱敏" in x for x in reasons))

    def test_retracted_release_cuts_external_visibility(self):
        task, defect, sgt, rel = self._published()
        self.api.get(f"/defects/{defect['id']}", REGULATOR, expect=200)
        self.api.post(f"/releases/{rel['id']}/retract", {}, DISPATCH)
        self.api.get(f"/defects/{defect['id']}", REGULATOR, expect=403)
        _, detail = self.api.get(f"/defects/{defect['id']}", DISPATCH)
        self.assertFalse(detail["sightings"][0]["publish"]["in_active_release"])

    def _published(self):
        task, defect, sgt = self._defect_with_asset(redacted=True)
        self.server.store.confirm_sighting(sgt["id"])
        _, report = self.api.post(f"/tasks/{task['id']}/reports",
                                  {"title": "r"}, DISPATCH, 201)
        self.api.post(f"/reports/{report['id']}/items",
                      {"sighting_id": sgt["id"]}, DISPATCH)
        self.api.post(f"/reports/{report['id']}/publish", {}, DISPATCH)
        _, rel = self.api.post(f"/tasks/{task['id']}/releases", {
            "report_id": report["id"], "version_label": "v1"}, DISPATCH, 201)
        return task, defect, sgt, rel


class SightingConflictTest(ApiTestBase):
    def test_mark_outside_segment_range_opens_review(self):
        task, _, dev = self.mk_task()
        cal = self.mk_calibration(dev)
        lv = self.mk_locv(task, cal)
        import hashlib
        body = {"upload_batch": "b", "filename": "s.mp4",
                "sha256": hashlib.sha256(b"s").hexdigest(),
                "size_bytes": 1, "pulse_start": 0, "pulse_end": 15915,
                "loc_version_id": lv["id"]}  # 区间约 0~10m
        _, r = self.api.post(f"/tasks/{task['id']}/segments", body, OPS, 201)
        self.wait_job(r["merge_job"]["id"])
        _, defect = self.api.post(f"/tasks/{task['id']}/defects", {
            "code": "CX", "severity": "1"}, OPS, 201)
        status, payload = self.api.post(f"/defects/{defect['id']}/sightings", {
            "loc_version_id": lv["id"], "segment_id": r["segment"]["id"],
            "file_offset_s": 1, "pulse_reading": 79577}, OPS, 409)  # ≈50m
        self.assertIn("复核", payload["detail"])
        _, reviews = self.api.get(f"/reviews?task_id={task['id']}")
        self.assertTrue(any(x["kind"] == "coordinate_conflict"
                            for x in reviews["reviews"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
