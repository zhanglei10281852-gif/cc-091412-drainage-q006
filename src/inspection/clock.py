"""时间工具：统一带时区的 ISO 8601 字符串（Asia/Shanghai 展示）。"""

from __future__ import annotations

from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("时间必须带时区")
    return dt
