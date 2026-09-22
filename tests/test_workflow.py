"""端到端主流程：任务/校准/上传/合并/缺陷/发布，以及幂等与版本不变量。"""

import hashlib

from httpbase import HttpCase


class HappyPathTest(HttpCase):
    def _three_segments(self, insp="insp-pl07"):
        for seq, (a, b), key in [
            (0, (0, 20000), b"pl07-seg01"),
            (1, (20000, 40000), b"pl07-seg02"),
            (2, (40000, 60000), b"pl07-seg03"),
        ]:
            status, body = self.upload(insp, seq, a, b, key)
            self.assertEqual(status, 201, body)

    def test_full_workflow_and_defect_query(self):
        self.seed_device()
        cal = self.add_calibration(196.5, note="磨损复测")
        self.seed_inspection()
        self._three_segments()

        status, job = self.merge("insp-pl07", cal["version_id"])
        self.assertEqual(status, 202)
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["progress"], 3)
        self.assertEqual(job["result"]["coverage_complete"], True)

        # 总长 37.04m 与标称 50m 有 warning 但无 blocker
        self.assertTrue(
            all(f["severity"] != "blocker" for f in job["result"]["findings"])
        )

        status, segs = self.request("GET", "/api/inspections/insp-pl07/segments")
        self.assertEqual(status, 200)
        seg1_id = segs["segments"][1]["segment_id"]

        status, snap = self.add_snapshot(seg1_id)
        self.assertEqual(status, 201, snap)
        # 截图重复登记（同校验和）幂等
        status, snap2 = self.add_snapshot(seg1_id)
        self.assertEqual(status, 200)
        self.assertEqual(snap2["id"], snap["id"])

        status, defect = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": seg1_id, "raw_offset": 30000,
             "code": "PL-CRACK", "description": "环裂"},
        )
        self.assertEqual(status, 201, defect)
        self.assertAlmostEqual(defect["chainage_m"], 18.519689, places=5)
        self.assertEqual(defect["warnings"], [])

        status, detail = self.request("GET", f"/api/defects/{defect['id']}")
        self.assertEqual(status, 200)
        # 查询缺陷必须返回：片段、校准版本、换算过程、发布判定
        self.assertEqual(detail["segment"]["seq"], 1)
        self.assertEqual(detail["segment"]["sha256"],
                         hashlib.sha256(b"pl07-seg02").hexdigest())
        self.assertEqual(detail["calibration_version"]["version_id"], cal["version_id"])
        self.assertEqual(detail["calibration_version"]["seq"], 1)
        conv = detail["conversion"]
        self.assertEqual(conv["wheel_diameter_mm"], 196.5)
        self.assertEqual(conv["pulses_into_segment"], 10000)
        self.assertAlmostEqual(conv["chainage_m"], defect["chainage_m"], places=6)
        self.assertFalse(conv["provisional"])
        self.assertTrue(detail["release"]["can_release_externally"])
        self.assertEqual(detail["release"]["reasons"], [])

        status, report = self.request(
            "POST", "/api/inspections/insp-pl07/reports",
            {"title": "PL-07 修复设计引用版"},
        )
        self.assertEqual(status, 201)
        self.assertEqual(len(report["snapshot"]["items"]), 1)
        status, pub = self.request("POST", f"/api/reports/{report['id']}/publish")
        self.assertEqual(status, 200, pub)
        self.assertIsNotNone(pub["published_at"])

        # 发布后缺陷被冻结
        status, detail2 = self.request("GET", f"/api/defects/{defect['id']}")
        self.assertEqual(detail2["defect"]["snap_state"], "frozen")
        self.assertEqual(
            detail2["release"]["frozen_by_published_report"]["id"], report["id"]
        )

    def test_raw_offset_outside_segment_rejected(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        self._three_segments()
        self.merge("insp-pl07")
        status, segs = self.request("GET", "/api/inspections/insp-pl07/segments")
        sid = segs["segments"][0]["segment_id"]
        status, body = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": sid, "raw_offset": 99999, "code": "X"},
        )
        self.assertEqual(status, 422)


class IdempotencyTest(HttpCase):
    def test_duplicate_upload_is_idempotent(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        s1, b1 = self.upload("insp-pl07", 0, 0, 20000, b"same-video")
        self.assertEqual(s1, 201)
        s2, b2 = self.upload("insp-pl07", 0, 0, 20000, b"same-video",
                             file_name="different-name.mp4")
        self.assertEqual(s2, 200)
        self.assertFalse(b2["created"])
        self.assertEqual(b2["segment"]["id"], b1["segment"]["id"])
        status, segs = self.request("GET", "/api/inspections/insp-pl07/segments")
        self.assertEqual(len(segs["segments"]), 1)

    def test_same_seq_different_checksum_enters_review_not_overwrite(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        s1, b1 = self.upload("insp-pl07", 0, 0, 20000, b"original")
        self.assertEqual(s1, 201)
        original_id = b1["segment"]["id"]

        s2, b2 = self.upload("insp-pl07", 0, 0, 20000, b"tampered-bytes")
        self.assertEqual(s2, 409)
        self.assertEqual(b2["error"], "checksum_conflict")

        # 旧分段未被覆盖
        status, segs = self.request("GET", "/api/inspections/insp-pl07/segments")
        self.assertEqual(len(segs["segments"]), 1)
        self.assertEqual(segs["segments"][0]["segment_id"], original_id)
        self.assertEqual(
            segs["segments"][0]["sha256"], hashlib.sha256(b"original").hexdigest()
        )
        # 冲突进入复核队列
        status, reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")
        self.assertEqual(status, 200)
        kinds = [r["kind"] for r in reviews["reviews"]]
        self.assertIn("checksum_conflict", kinds)
        conflict = next(r for r in reviews["reviews"] if r["kind"] == "checksum_conflict")
        self.assertEqual(conflict["status"], "open")
        self.assertEqual(conflict["detail"]["seq"], 0)

    def test_same_content_under_other_seq_rejected(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        self.upload("insp-pl07", 0, 0, 20000, b"dup")
        status, body = self.upload("insp-pl07", 1, 20000, 40000, b"dup")
        self.assertEqual(status, 409)


class CalibrationVersioningTest(HttpCase):
    def test_replacing_calibration_creates_new_version_and_supersedes(self):
        self.seed_device()
        v1 = self.add_calibration(200.0, note="出厂")
        v2 = self.add_calibration(190.0, note="磨损")
        self.assertEqual(v1["seq"], 1)
        self.assertEqual(v2["seq"], 2)
        self.assertEqual(v2["supersedes"], v1["version_id"])

        status, body = self.request(
            "POST", "/api/devices/RBT-01/calibrations", {"wheel_diameter_mm": 190.0}
        )
        self.assertEqual(status, 409)  # 相同轮径不允许制造新版本

        status, listed = self.request("GET", "/api/devices/RBT-01/calibrations")
        self.assertEqual([c["seq"] for c in listed["calibrations"]], [1, 2])
        # 旧版本仍可读取、不可修改（无修改接口）
        self.assertEqual(listed["calibrations"][0]["wheel_diameter_mm"], 200.0)
