"""演示数据种子：两台设备校准、两条管线（一条完整、一条缺片+重叠）。

用法::

    INSPECTION_DATA_DIR=./data python scripts/seed_demo.py
"""

from __future__ import annotations

import base64
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from inspection.config import Settings  # noqa: E402
from inspection.service import Service  # noqa: E402
from inspection.storage import Store  # noqa: E402


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload(svc: Service, insp: str, seq: int, a: float, b: float, key: str) -> str:
    data = key.encode()
    seg, _ = svc.upload_segment(
        insp,
        {
            "seq": seq,
            "file_name": f"{insp}-seg{seq + 1:02d}.mp4",
            "sha256": sha(data),
            "size_bytes": len(data),
            "encoder_start": a,
            "encoder_end": b,
            "duration_s": 62.5,
            "content_base64": b64(data),
        },
    )
    return seg["id"]


def main() -> None:
    settings = Settings.from_env()
    settings.ensure_dirs()
    store = Store(settings.db_path)
    store.recover_jobs_on_startup()
    svc = Service(store, settings.blob_dir)
    svc.run_inline = True  # 脚本内同步执行，避免与后台线程重复合并

    if store.get_device("RBT-01"):
        print("种子数据已存在，跳过。清空数据目录后可重新播种。")
        return

    # ---- 设备与两次校准（轮径磨损：200mm -> 196.5mm）----
    svc.register_device(
        {
            "id": "RBT-01",
            "model": "爬行机器人 X1",
            "nominal_wheel_diameter_mm": 200.0,
            "encoder_ppr": 1000,
            "metadata": {"vendor": "示例厂商", "commission_date": "2025-11-02"},
        }
    )
    cal1 = svc.add_calibration("RBT-01", {"wheel_diameter_mm": 200.0, "note": "出厂轮径"})
    cal2 = svc.add_calibration(
        "RBT-01", {"wheel_diameter_mm": 196.5, "note": "行驶 80km 后磨损复测"}
    )

    # ---- PL-07：起止井 W101-W102，三片连续，覆盖完整 ----
    svc.register_inspection(
        {
            "id": "insp-pl07",
            "pipeline_code": "PL-07",
            "start_manhole": "W101",
            "end_manhole": "W102",
            "nominal_length_m": 50.0,
            "device_id": "RBT-01",
        }
    )
    s0 = upload(svc, "insp-pl07", 0, 0, 20000, "pl07-seg01")
    s1 = upload(svc, "insp-pl07", 1, 20000, 40000, "pl07-seg02")
    s2 = upload(svc, "insp-pl07", 2, 40000, 60000, "pl07-seg03")
    job = svc.create_merge_job("insp-pl07", cal2["version_id"])
    svc.run_pending(job["id"])
    for idx, sid in enumerate((s0, s1, s2)):
        png = f"redacted-pl07-seg{idx + 1}".encode()
        svc.add_redacted_snapshot(
            sid, {"sha256": sha(png), "content_base64": b64(png)}
        )
    defect = svc.mark_defect(
        "insp-pl07",
        {"segment_id": s1, "raw_offset": 30000, "code": "PL-CRACK",
         "description": "环向裂缝，修复设计引用点"},
        created_by="项目工程师-甲",
    )
    report = svc.create_report(
        "insp-pl07", {"title": "PL-07(W101-W102) 内窥检测报告"}, created_by="项目工程师-甲"
    )
    svc.publish_report(report["id"])
    print(f"PL-07 已发布: report={report['id']} defect={defect['id']}")

    # ---- PL-09：分段 1 缺失，且分段 2/3 里程重叠 -> 全部进复核 ----
    svc.register_inspection(
        {
            "id": "insp-pl09",
            "pipeline_code": "PL-09",
            "start_manhole": "W205",
            "end_manhole": "W206",
            "nominal_length_m": 25.0,
            "device_id": "RBT-01",
        }
    )
    upload(svc, "insp-pl09", 0, 0, 10000, "pl09-seg01")
    upload(svc, "insp-pl09", 2, 25000, 35000, "pl09-seg03")  # 注意：seg02 未上传
    upload(svc, "insp-pl09", 3, 33000, 40000, "pl09-seg03b")  # 与 seg03 重叠
    bad_job = svc.create_merge_job("insp-pl09", cal2["version_id"])
    svc.run_pending(bad_job["id"])
    reviews = svc.list_reviews("insp-pl09")
    print(f"PL-09 合并完成但产生 {len(reviews)} 个待复核项，系统未拼接缺失区间。")

    print("\n种子完成。内置令牌：")
    print("  engineer:   tok-engineer")
    print("  dispatcher: tok-dispatcher")
    print("  supervisor: tok-supervisor")
    print("  readonly:   tok-readonly")


if __name__ == "__main__":
    main()
