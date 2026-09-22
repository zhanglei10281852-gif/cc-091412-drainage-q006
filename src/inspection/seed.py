"""样例数据：管线起止井、设备参数、两轮校准、分段校验和与缺陷标记。

用法：
    python -m inspection.seed                # 写入 INSPECTION_DB 指定的库
    python -m inspection.seed path/to.db     # 写入指定库（幂等，按 code 复用）

场景对应需求：同一盘视频分两段上传，第二段因轮径重新校准出现里程跳点，
其中第二段与第一段之间故意留出缺口，用于演示“缺失片段进复核、不静默拼接”。
"""

from __future__ import annotations

import hashlib
import json
import sys

from .merger import MergeWorker
from .store import open_store


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def seed_demo(store, auto_merge: bool = True) -> dict:
    # 幂等：已存在同 code 实体时直接复用
    pipe = store.execute(
        "SELECT * FROM pipelines WHERE code=?", ("WS-YL-017",)
    ).fetchone()
    if pipe is None:
        pipe = store.create_pipeline(
            "WS-YL-017", "YL-起#12", "YL-止#31", 320.0)
    else:
        pipe = dict(pipe)

    device = store.execute(
        "SELECT * FROM devices WHERE code=?", ("RBT-Q5-03",)
    ).fetchone()
    if device is None:
        device = store.create_device("RBT-Q5-03", "履带式检测机器人 Q5", 200.0)
    else:
        device = dict(device)

    calibs = [dict(r) for r in store.execute(
        "SELECT * FROM calibrations WHERE device_id=? ORDER BY device_seq",
        (device["id"],))]
    if not calibs:
        cal1 = store.add_calibration(
            device["id"], 200.0, 1000, "2026-09-10T08:00:00+08:00",
            "出厂轮径校准")
        # 外业后发现轮径磨损，替换校准——只生成新记录
        cal2 = store.add_calibration(
            device["id"], 197.5, 1000, "2026-09-18T09:30:00+08:00",
            "外业复测：轮径磨损 2.5mm", supersedes_id=cal1["id"])
        calibs = [cal1, cal2]

    task = store.execute(
        "SELECT * FROM tasks WHERE code=?", ("TASK-2026-0918-03",)
    ).fetchone()
    if task is None:
        task = store.create_task(
            pipe["id"], device["id"], "TASK-2026-0918-03",
            "2026-09-18T10:00:00+08:00", "2026-09-18T12:10:00+08:00")
        task = store.set_task_status(task["id"], "active")
    else:
        task = dict(task)

    versions = [dict(r) for r in store.execute(
        "SELECT * FROM loc_versions WHERE task_id=? ORDER BY created_at",
        (task["id"],))]
    if not versions:
        v1 = store.create_loc_version(
            task["id"], calibs[0]["id"], 0.0, 0, "start_manhole",
            "起点井 YL-起#12 对零")
        versions = [v1]

    # 分段：设备 A 批次先传 0~100m；中间缺一段；新校准批次传 120~220m（跳点）
    seg_specs = [
        ("batch-A", "WS-YL-017_part01.mp4", 0, 159155, "10:00:05", "10:24:40", None),
        ("batch-B", "WS-YL-017_part03.mp4", 220000, 381170,
         "11:02:11", "11:41:55", "gap-demo"),
    ]
    segments = []
    jobs = []
    first_id = None
    for batch, name, p0, p1, t0, t1, marker in seg_specs:
        seg, created = store.upload_segment(
            task["id"], batch, name, _sha(name + batch),
            512 * 1024 * 1024,
            device_clock_start=f"2026-09-18T{t0}+08:00",
            device_clock_end=f"2026-09-18T{t1}+08:00",
            pulse_start=p0, pulse_end=p1,
        )
        segments.append(seg)
        if first_id is None:
            first_id = seg["id"]

    if auto_merge:
        worker = MergeWorker(store)
        job1 = store.enqueue_merge(segments[0]["id"], versions[0]["id"])
        worker.run_job(job1["id"])
        # 第二段引用第一段为前置；在新校准下需要新定位版本才能合并
        if len(versions) < 2 and len(calibs) >= 2:
            v2 = store.create_loc_version(
                task["id"], calibs[1]["id"], 120.0, 220000, "manual",
                "按 197.5mm 轮径重新标定锚点（修复跳点）",
                supersedes_id=versions[0]["id"])
            versions.append(v2)
        job2 = store.enqueue_merge(
            segments[1]["id"], versions[-1]["id"], prereq_segment_id=first_id)
        worker.run_job(job2["id"])
        jobs = [store.get("merge_jobs", job1["id"]),
                store.get("merge_jobs", job2["id"])]

    return {
        "pipeline": pipe, "device": device, "calibrations": calibs,
        "task": task, "loc_versions": versions, "segments": segments,
        "merge_jobs": jobs,
    }


def main(argv: list[str]) -> None:
    path = argv[1] if len(argv) > 1 else None
    import os
    path = path or os.environ.get("INSPECTION_DB", ".runtime/inspection.db")
    store = open_store(path)
    try:
        result = seed_demo(store)
        summary = {
            "db": path,
            "pipeline_id": result["pipeline"]["id"],
            "device_id": result["device"]["id"],
            "calibration_ids": [c["id"] for c in result["calibrations"]],
            "task_id": result["task"]["id"],
            "loc_version_ids": [v["id"] for v in result["loc_versions"]],
            "segment_ids": [s["id"] for s in result["segments"]],
            "job_status": [(j["id"], j["status"], j["last_error"])
                           for j in result["merge_jobs"]],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == "__main__":
    main(sys.argv)
