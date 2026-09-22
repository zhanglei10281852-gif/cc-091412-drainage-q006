"""HTTP 端到端测试基件：临时数据目录 + 随机端口 + 内联执行（确定性）。"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from inspection.app import create_server  # noqa: E402
from inspection.config import Settings  # noqa: E402

TOKENS = {
    "engineer": "tok-engineer",
    "dispatcher": "tok-dispatcher",
    "supervisor": "tok-supervisor",
    "readonly": "tok-readonly",
}


class HttpCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            data_dir=Path(self._tmp.name),
            db_path=Path(self._tmp.name) / "index.db",
            blob_dir=Path(self._tmp.name) / "blobs",
        )
        self.server = create_server(0, "127.0.0.1", settings=self.settings, run_inline=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def _shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server.app.store.close()
        self._tmp.cleanup()

    def request(self, method: str, path: str, body=None, role: str = "engineer"):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if role is not None:
            req.add_header("Authorization", f"Bearer {TOKENS[role]}")
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except json.JSONDecodeError:
                return exc.code, {"raw": raw.decode()}

    # ---- 常用搭建 ----

    def seed_device(self, wheel=200.0, ppr=1000, device_id="RBT-01"):
        status, body = self.request("POST", "/api/devices", {
            "id": device_id, "model": "爬行机器人X1",
            "nominal_wheel_diameter_mm": wheel, "encoder_ppr": ppr,
        })
        self.assertEqual(status, 201, body)
        return body

    def add_calibration(self, wheel, device_id="RBT-01", note=""):
        status, body = self.request(
            "POST", f"/api/devices/{device_id}/calibrations",
            {"wheel_diameter_mm": wheel, "note": note},
        )
        self.assertEqual(status, 201, body)
        return body

    def seed_inspection(self, insp_id="insp-pl07", nominal=50.0, device_id="RBT-01"):
        status, body = self.request("POST", "/api/inspections", {
            "id": insp_id, "pipeline_code": "PL-07",
            "start_manhole": "W101", "end_manhole": "W102",
            "nominal_length_m": nominal, "device_id": device_id,
        })
        self.assertEqual(status, 201, body)
        return body

    def upload(self, insp_id, seq, enc_start, enc_end, content, *, file_name=None):
        import base64
        import hashlib
        data = content if isinstance(content, bytes) else content.encode()
        status, body = self.request("PUT", f"/api/inspections/{insp_id}/segments", {
            "seq": seq,
            "file_name": file_name or f"{insp_id}-{seq:02d}.mp4",
            "sha256": hashlib.sha256(data).hexdigest(),
            "size_bytes": len(data),
            "encoder_start": enc_start,
            "encoder_end": enc_end,
            "duration_s": 60,
            "content_base64": base64.b64encode(data).decode(),
        })
        return status, body

    def merge(self, insp_id, version_id=None):
        return self.request("POST", f"/api/inspections/{insp_id}/merge-jobs",
                            {"calibration_version_id": version_id})

    def add_snapshot(self, segment_id, content=b"PNG"):
        import base64
        import hashlib
        return self.request("POST", f"/api/segments/{segment_id}/snapshots", {
            "sha256": hashlib.sha256(content).hexdigest(),
            "content_base64": base64.b64encode(content).decode(),
        })
