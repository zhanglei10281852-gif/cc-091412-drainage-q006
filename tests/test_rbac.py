"""角色可见范围：原始影像 vs 脱敏截图，草稿/已发布缺陷的可见性。"""

import hashlib

from httpbase import HttpCase


class RbacTest(HttpCase):
    def _published(self):
        self.seed_device()
        self.add_calibration(196.5)
        self.seed_inspection()
        self.upload("insp-pl07", 0, 0, 20000, b"raw-video-bytes")
        self.merge("insp-pl07")
        segs = self.request("GET", "/api/inspections/insp-pl07/segments")[1]["segments"]
        sid = segs[0]["segment_id"]
        self.add_snapshot(sid, b"redacted-png")
        defect = self.request(
            "POST", "/api/inspections/insp-pl07/defects",
            {"segment_id": sid, "raw_offset": 10000, "code": "D"},
        )[1]
        report = self.request("POST", "/api/inspections/insp-pl07/reports",
                              {"title": "r"})[1]
        return sid, defect, report

    def test_write_roles_are_engineer_only(self):
        for role in ("dispatcher", "supervisor", "readonly"):
            status, body = self.request(
                "POST", "/api/devices", {"id": "X"}, role=role
            )
            self.assertEqual(status, 403, body)
        status, body = self.request(
            "POST", "/api/devices", {"id": "X"}, role=None
        )
        self.assertEqual(status, 401)

    def test_non_engineer_sees_only_redacted_not_raw(self):
        sid, defect, report = self._published()

        # 发布前：外部角色不可见缺陷与草稿报告
        status, _ = self.request("GET", f"/api/defects/{defect['id']}", role="dispatcher")
        self.assertEqual(status, 403)
        status, _ = self.request("GET", f"/api/reports/{report['id']}", role="supervisor")
        self.assertEqual(status, 403)

        self.request("POST", f"/api/reports/{report['id']}/publish")

        # 发布后可见，但分段视图不含原始文件信息
        status, detail = self.request(
            "GET", f"/api/defects/{defect['id']}", role="dispatcher"
        )
        self.assertEqual(status, 200)
        seg = detail["segment"]
        self.assertNotIn("sha256", seg)
        self.assertNotIn("file_name", seg)
        self.assertIn("redacted_snapshot", seg)

        # 原始媒体 403，脱敏媒体 200
        store = self.server.app.store
        raw_rel = store.get_segment(sid)["blob_path"]
        snap_rel = store.get_snapshot_for_segment(sid)["blob_path"]
        raw = self._get_media(f"/media/{raw_rel}", role="dispatcher")
        self.assertEqual(raw, 403)
        png = self._get_media(f"/media/{snap_rel}", role="readonly")
        self.assertEqual(png, 200)
        raw_eng = self._get_media(f"/media/{raw_rel}", role="engineer")
        self.assertEqual(raw_eng, 200)

        # 工程师在分段列表仍能看到原始校验和
        segs = self.request("GET", "/api/inspections/insp-pl07/segments",
                            role="engineer")[1]["segments"]
        self.assertEqual(segs[0]["sha256"],
                         hashlib.sha256(b"raw-video-bytes").hexdigest())
        segs_ext = self.request("GET", "/api/inspections/insp-pl07/segments",
                                role="supervisor")[1]["segments"]
        self.assertNotIn("sha256", segs_ext[0])

    def _get_media(self, path, role):
        import urllib.error
        import urllib.request
        req = urllib.request.Request(
            self.base + path,
            headers={"Authorization": f"Bearer {self._token(role)}"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                resp.read()
                return resp.status
        except urllib.error.HTTPError as exc:
            return exc.code

    @staticmethod
    def _token(role):
        return {"engineer": "tok-engineer", "dispatcher": "tok-dispatcher",
                "supervisor": "tok-supervisor", "readonly": "tok-readonly"}[role]
