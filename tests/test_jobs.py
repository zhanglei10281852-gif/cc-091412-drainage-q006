"""长任务可暂停/继续、失败原因持久化、服务重启后队列恢复。"""

import threading

from httpbase import HttpCase


class JobLifecycleTest(HttpCase):
    def _three(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        for seq, (a, b) in [(0, (0, 20000)), (1, (20000, 40000)), (2, (40000, 60000))]:
            self.upload("insp-pl07", seq, a, b, f"v{seq}".encode())

    def test_pause_then_resume_keeps_progress(self):
        self._three()
        svc = self.server.app.service
        # 内联模式：创建任务后不自动执行（HTTP 之外直接调用）
        job = svc.create_merge_job("insp-pl07")
        self.assertEqual(job["status"], "queued")

        # 在执行前挂起：worker 到达检查点时必须落 paused 而非继续跑完
        svc.jobs.request_pause(job["id"])
        t = threading.Thread(target=svc._run_job_safely, args=(job["id"],))
        t.start()
        t.join(timeout=5)
        paused = svc.store.get_job(job["id"])
        self.assertEqual(paused["status"], "paused")

        # 继续：从断点跑完
        resumed = svc.resume_merge_job(job["id"])
        svc.run_pending(resumed["id"])
        done = svc.store.get_job(job["id"])
        self.assertEqual(done["status"], "succeeded")
        self.assertEqual(done["progress"], 3)
        self.assertIsNone(done["fail_reason"])

    def test_failure_reason_is_persisted_and_retry_succeeds(self):
        self._three()
        store = self.server.app.store
        # 破坏一个影像文件造成校验阶段失败
        seg = store.get_segment_at_seq("insp-pl07", 1)
        blob = self.settings.blob_dir / seg["blob_path"]
        blob.write_bytes(b"corrupted")

        status, job = self.merge("insp-pl07")
        self.assertEqual(status, 202)
        failed = self.request("GET", f"/api/merge-jobs/{job['id']}")[1]
        self.assertEqual(failed["status"], "failed")
        self.assertIn("校验和", failed["fail_reason"])

        # 修复文件后重试成功
        blob.write_bytes(b"v1")
        status, retried = self.request("POST", f"/api/merge-jobs/{job['id']}/retry")
        self.assertEqual(status, 202)
        self.assertEqual(retried["status"], "succeeded")

    def test_missing_blob_failure_survives_restart_and_resumes(self):
        self._three()
        svc = self.server.app.service
        store = self.server.app.store
        job = svc.create_merge_job("insp-pl07")
        # 模拟进程在运行中崩溃：状态停留在 running
        store.execute("UPDATE merge_jobs SET status='running' WHERE id=?", (job["id"],))

        # 重启：新 Application 指向同一数据目录
        from inspection.app import Application
        app2 = Application(self.settings, run_inline=True)
        self.addCleanup(app2.store.close)
        recovered = app2.store.get_job(job["id"])
        self.assertEqual(recovered["status"], "paused")

        # 同时有一个失败任务，失败原因在重启后仍可读
        seg2 = store.get_segment_at_seq("insp-pl07", 2)
        (self.settings.blob_dir / seg2["blob_path"]).unlink()
        store.execute(
            "UPDATE merge_jobs SET status='failed', fail_reason=? WHERE id=?",
            ("FileNotFoundError: 序号 2 的影像文件缺失", job["id"]),
        )
        app3 = Application(self.settings, run_inline=True)
        self.addCleanup(app3.store.close)
        still = app3.store.get_job(job["id"])
        self.assertEqual(still["status"], "failed")
        self.assertIn("缺失", still["fail_reason"])
        # resumeable 队列恢复可见
        self.assertEqual([j["id"] for j in app3.store.resumeable_jobs()], [job["id"]])

    def test_pause_of_running_job_via_state_machine(self):
        self._three()
        svc = self.server.app.service
        job = svc.create_merge_job("insp-pl07")
        # queued 状态直接暂停
        paused = svc.pause_merge_job(job["id"])
        self.assertEqual(paused["status"], "paused")
        # 已暂停不可再暂停
        import unittest
        from inspection.errors import StateError
        with self.assertRaises(StateError):
            svc.pause_merge_job(job["id"])
        # 非失败任务不能 retry
        with self.assertRaises(StateError):
            svc.retry_merge_job(job["id"])
