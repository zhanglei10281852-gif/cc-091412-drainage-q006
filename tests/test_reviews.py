"""缺失片段/坐标冲突 -> 复核工作流，以及冻结缺陷的版本不变量。"""

from httpbase import HttpCase


class ReviewWorkflowTest(HttpCase):
    def _setup_with_gap_overlap(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=25.0)
        # 0..10000 正常，2 有 gap（15000 脉冲≈9.42m）且与 3 重叠
        self.upload("insp-pl07", 0, 0, 10000, b"seg0")
        self.upload("insp-pl07", 2, 25000, 35000, b"seg2")
        self.upload("insp-pl07", 3, 33000, 40000, b"seg3")
        status, job = self.merge("insp-pl07")
        self.assertEqual(status, 202)
        return job

    def test_gap_and_overlap_generate_open_reviews_and_provisional(self):
        job = self._setup_with_gap_overlap()
        self.assertEqual(job["status"], "succeeded")
        finding_keys = [f["dedup_key"] for f in job["result"]["findings"]]
        self.assertIn("seq-gap:1-1", finding_keys)
        self.assertTrue(any(k.startswith("mileage-gap") for k in finding_keys))
        self.assertTrue(any(k.startswith("overlap") for k in finding_keys))

        status, reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")
        open_items = reviews["reviews"]
        self.assertGreaterEqual(len(open_items), 3)
        self.assertTrue(all(r["status"] == "open" for r in open_items))

        # 冲突之后的分段定位为推算值
        store = self.server.app.store
        rows = store.all_positioning("insp-pl07", job["calibration_version_id"])
        prov = {r["raw_start"]: r["provisional"] for r in rows}
        self.assertEqual(prov[0.0], 0)
        self.assertEqual(prov[25000.0], 1)
        self.assertEqual(prov[33000.0], 1)

    def test_defect_on_provisional_segment_warns_and_prevents_release(self):
        job = self._setup_with_gap_overlap()
        store = self.server.app.store
        seg2 = next(s for s in store.get_segments("insp-pl07") if s["seq"] == 2)
        status, defect = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": seg2["id"], "raw_offset": 30000, "code": "D1"},
        )
        self.assertEqual(status, 201)
        self.assertIn("推算值", defect["warnings"][0])

        status, detail = self.request("GET", f"/api/defects/{defect['id']}")
        self.assertFalse(detail["release"]["can_release_externally"])
        reasons = " ".join(detail["release"]["reasons"])
        self.assertIn("复核项", reasons)
        self.assertIn("推算值", reasons)

        # 没有解决 blocker 时发布被拒
        self.add_snapshot(seg2["id"])
        status, report = self.request(
            "POST", "/api/inspections/insp-pl07/reports", {"title": "t"}
        )
        status, pub = self.request("POST", f"/api/reports/{report['id']}/publish")
        self.assertEqual(status, 409)
        self.assertIn("阻断性复核项", pub["message"])

    def test_human_acceptance_of_conflict_stabilizes_coordinates(self):
        """人工在复核中接受重叠后，provisional 清除，可以正常发布。"""
        job = self._setup_with_gap_overlap()
        store = self.server.app.store

        # 人工接受全部 open blocker
        status, reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")
        blockers = [r for r in reviews["reviews"] if r["severity"] == "blocker"]
        self.assertGreaterEqual(len(blockers), 3)
        for r in blockers:
            status, _ = self.request(
                "POST", f"/api/reviews/{r['id']}/resolve",
                {"resolution": "人工核对：缺片位置无管线附属物，重叠段取主摄像头记录"},
            )
            self.assertEqual(status, 200)

        rows = store.all_positioning("insp-pl07", job["calibration_version_id"])
        self.assertTrue(all(r["provisional"] == 0 for r in rows))

        # 重新创建合并任务会看到冲突仍在（数据本身没变），但 open 项不会重复
        status, job2 = self.merge("insp-pl07", job["calibration_version_id"])
        self.assertEqual(job2["status"], "succeeded")
        status, reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")
        opens = [r for r in reviews["reviews"] if r["status"] == "open"]
        self.assertEqual(opens, [])

        for seq in (0, 2, 3):
            seg = next(s for s in store.get_segments("insp-pl07") if s["seq"] == seq)
            self.add_snapshot(seg["id"], f"png-{seq}".encode())

        seg2 = next(s for s in store.get_segments("insp-pl07") if s["seq"] == 2)
        self.request("POST", "/api/inspections/insp-pl07/defects",
                     {"segment_id": seg2["id"], "raw_offset": 30000, "code": "D1"})
        report = self.request("POST", "/api/inspections/insp-pl07/reports",
                              {"title": "t"})[1]
        status, pub = self.request("POST", f"/api/reports/{report['id']}/publish")
        self.assertEqual(status, 200, pub)

    def test_filled_gap_auto_closes_missing_segment_review(self):
        job = self._setup_with_gap_overlap()
        store = self.server.app.store
        # 解决 overlap；补传 seq1（恰好填补里程空档）
        reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")[1]["reviews"]
        overlap = next(r for r in reviews if r["kind"] == "overlap")
        self.request("POST", f"/api/reviews/{overlap['id']}/resolve",
                     {"resolution": "人工接受重叠"})
        self.upload("insp-pl07", 1, 10000, 25000, b"seg1-filled")
        status, job2 = self.merge("insp-pl07", job["calibration_version_id"])
        self.assertEqual(job2["status"], "succeeded")
        reviews = self.request("GET", "/api/inspections/insp-pl07/reviews")[1]["reviews"]
        statuses = {(r["kind"], loads_key(r)): r["status"] for r in reviews}
        self.assertEqual(statuses.get(("missing_segment", "seq-gap:1-1")), "resolved")
        self.assertTrue(
            any(
                r["status"] == "resolved" and r["kind"] == "missing_segment"
                and loads_key(r).startswith("mileage-gap")
                for r in reviews
            )
        )
        # 没有 open blocker，coverage 可发布
        self.assertEqual(
            [r for r in reviews if r["status"] == "open" and r["severity"] == "blocker"],
            [],
        )


def loads_key(review):
    import json
    return review["detail"]["dedup_key"]


class FreezeInvariantTest(HttpCase):
    def test_published_defect_survives_new_calibration(self):
        self.seed_device()
        v2 = self.add_calibration(196.5)
        self.seed_inspection(nominal=40.0)
        for seq, (a, b), key in [
            (0, (0, 20000), b"a"), (1, (20000, 40000), b"b"), (2, (40000, 60000), b"c"),
        ]:
            self.upload("insp-pl07", seq, a, b, key)
        self.merge("insp-pl07", v2["version_id"])
        segs = self.server.app.store.get_segments("insp-pl07")
        for s in segs:
            self.add_snapshot(s["id"], f"png-{s['seq']}".encode())
        seg1 = next(s for s in segs if s["seq"] == 1)
        defect = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": seg1["id"], "raw_offset": 30000, "code": "D"},
        )[1]
        report = self.request("POST", "/api/inspections/insp-pl07/reports",
                              {"title": "r"})[1]
        self.request("POST", f"/api/reports/{report['id']}/publish")

        chainage_before = defect["chainage_m"]
        version_before = defect["calibration_version_id"] if "calibration_version_id" in defect else v2["version_id"]

        # 新校准（换轮）+ 重新合并
        v3 = self.add_calibration(198.0, note="换新轮")
        job3 = self.merge("insp-pl07", v3["version_id"])[1]
        self.assertEqual(job3["status"], "succeeded")

        store = self.server.app.store
        frozen = store.get_defect(defect["id"])
        self.assertEqual(frozen["snap_state"], "frozen")
        self.assertEqual(frozen["calibration_version_id"], version_before)
        self.assertEqual(frozen["chainage_m"], chainage_before)

        # 报告快照仍是旧坐标
        rpt = store.get_report(report["id"])
        import json
        snapshot = json.loads(rpt["snapshot_json"])
        self.assertEqual(snapshot["calibration_seq"], 1)
        self.assertEqual(snapshot["items"][0]["chainage_m"], chainage_before)

        # 缺陷详情带新版本投影（仅参考）
        detail = self.request("GET", f"/api/defects/{defect['id']}")[1]
        proj = detail["latest_version_projection"]
        self.assertEqual(proj["calibration_version_id"], v3["version_id"])
        self.assertNotEqual(proj["chainage_m"], chainage_before)
        self.assertTrue(can_release_all(detail))

    def test_unconfirmed_defect_becomes_superseded_on_new_positioning(self):
        self.seed_device()
        self.add_calibration(200.0)
        self.seed_inspection(nominal=40.0)
        self.upload("insp-pl07", 0, 0, 20000, b"a")
        self.merge("insp-pl07")
        seg0 = self.server.app.store.get_segments("insp-pl07")[0]
        defect = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": seg0["id"], "raw_offset": 5000, "code": "D",
             "state": "draft"},
        )[1]
        self.add_calibration(195.0)
        self.merge("insp-pl07")
        self.assertEqual(
            self.server.app.store.get_defect(defect["id"])["snap_state"],
            "superseded",
        )


def can_release_all(detail):
    return detail["release"]["can_release_externally"]
