"""里程换算与分段冲突检测（纯函数，便于测试）。

编码器模型
==========
机器人轮式编码器每圈发出 ``encoder_ppr`` 个脉冲，校准轮径为 ``d`` 毫米：

    scale = π · d / (1000 · ppr)         # 每个脉冲对应的米数

所有分段的编码器读数共享同一原点（第一片起点），因此一个原始读数 ``r``
在某个校准版本下的管线里程为：

    chainage(r) = scale · (r - encoder_start_of_first_segment)

同盘视频被拆成多片上传时，相邻两片在里程轴上应当首尾相接。凡是出现：

* 序号不连续（缺片）；
* 相邻区间在里程轴上出现正间隔（有里程却没有影像 = 缺失片段）；
* 相邻区间互相重叠（坐标冲突）；
* 累计里程与管线标称长度明显不符；

都生成 *复核发现*，由调用方写入复核表。冲突位置之后的分片标记为
``provisional``（推算值，未经连续性证实），**绝不静默拼接**。
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field

DEFAULT_TOLERANCE_M = 0.05
LENGTH_DEVIATION_LIMIT = 0.02  # 与标称长度相差 2% 以上给出警告


@dataclass
class PositionRow:
    segment_id: str
    seq: int
    raw_start: float
    raw_end: float
    chainage_start_m: float
    chainage_end_m: float
    scale: float
    provisional: bool


@dataclass
class Finding:
    kind: str  # missing_segment | overlap | chainage_conflict
    severity: str  # blocker | warning
    dedup_key: str
    detail: dict
    related_segment_ids: list[str] = field(default_factory=list)
    # 从哪个分段序号起坐标不可信（冲突之后的分段为推算值）；None 表示不级联
    activation_seq: int | None = None


@dataclass
class PositionResult:
    scale: float
    rows: list[PositionRow]
    findings: list[Finding]
    chainage_origin_raw: float | None
    chainage_end_m: float | None

    @property
    def blockers(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == "blocker"]

    @property
    def coverage_complete(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict:
        return {
            "scale": self.scale,
            "chainage_origin_raw": self.chainage_origin_raw,
            "chainage_end_m": self.chainage_end_m,
            "coverage_complete": self.coverage_complete,
            "rows": [asdict(r) for r in self.rows],
            "findings": [
                {
                    "kind": f.kind,
                    "severity": f.severity,
                    "dedup_key": f.dedup_key,
                    "detail": f.detail,
                    "related_segment_ids": f.related_segment_ids,
                }
                for f in self.findings
            ],
        }


def compute_scale(wheel_diameter_mm: float, encoder_ppr: int) -> float:
    if wheel_diameter_mm <= 0:
        raise ValueError("校准轮径必须为正数")
    if encoder_ppr <= 0:
        raise ValueError("编码器每圈脉冲数必须为正数")
    return math.pi * wheel_diameter_mm / (1000.0 * encoder_ppr)


def compute_positioning(
    segments: list[dict],
    wheel_diameter_mm: float,
    encoder_ppr: int,
    nominal_length_m: float | None = None,
    tolerance_m: float = DEFAULT_TOLERANCE_M,
) -> PositionResult:
    """根据有序分段计算里程区间并产出复核发现。

    ``segments`` 元素至少包含：id, seq, encoder_start, encoder_end。
    """
    scale = compute_scale(wheel_diameter_mm, encoder_ppr)
    ordered = sorted(segments, key=lambda s: s["seq"])
    rows: list[PositionRow] = []
    findings: list[Finding] = []

    if not ordered:
        return PositionResult(scale, [], [], None, None)

    origin = ordered[0]["encoder_start"]
    first_blocker_seen = False

    prev: dict | None = None
    expected_seq = ordered[0]["seq"]
    for seg in ordered:
        # 1) 序号缺口：明确缺片
        if seg["seq"] > expected_seq:
            missing = list(range(expected_seq, seg["seq"]))
            findings.append(
                Finding(
                    kind="missing_segment",
                    severity="blocker",
                    dedup_key=f"seq-gap:{expected_seq}-{seg['seq'] - 1}",
                    detail={
                        "missing_seqs": missing,
                        "after_seq": prev["seq"] if prev else None,
                        "before_seq": seg["seq"],
                        "reason": "分段序号不连续，存在未上传片段",
                    },
                    related_segment_ids=[seg["id"]] + ([prev["id"]] if prev else []),
                    activation_seq=seg["seq"],
                )
            )
            first_blocker_seen = True

        start_m = scale * (seg["encoder_start"] - origin)
        end_m = scale * (seg["encoder_end"] - origin)

        # 单片自身读数倒挂
        if end_m + tolerance_m < start_m:
            findings.append(
                Finding(
                    kind="chainage_conflict",
                    severity="blocker",
                    dedup_key=f"inverted:{seg['seq']}",
                    detail={
                        "seq": seg["seq"],
                        "encoder_start": seg["encoder_start"],
                        "encoder_end": seg["encoder_end"],
                        "reason": "编码器止点读数小于起点读数",
                    },
                    related_segment_ids=[seg["id"]],
                    activation_seq=seg["seq"],
                )
            )
            first_blocker_seen = True

        # 2) 与上一片的里程轴关系
        if prev is not None:
            prev_end_m = scale * (prev["encoder_end"] - origin)
            delta = start_m - prev_end_m
            if delta > tolerance_m:
                findings.append(
                    Finding(
                        kind="missing_segment",
                        severity="blocker",
                        dedup_key=f"mileage-gap:{prev['seq']}-{seg['seq']}",
                        detail={
                            "uncovered_interval_m": [round(prev_end_m, 4), round(start_m, 4)],
                            "gap_m": round(delta, 4),
                            "between_seq": [prev["seq"], seg["seq"]],
                            "reason": "相邻片段里程不连续，区间内无影像覆盖（可能为缺片或轮径跳变）",
                        },
                        related_segment_ids=[prev["id"], seg["id"]],
                        activation_seq=seg["seq"],
                    )
                )
                first_blocker_seen = True
            elif delta < -tolerance_m:
                findings.append(
                    Finding(
                        kind="overlap",
                        severity="blocker",
                        dedup_key=f"overlap:{prev['seq']}-{seg['seq']}",
                        detail={
                            "overlap_interval_m": [round(start_m, 4), round(prev_end_m, 4)],
                            "overlap_m": round(-delta, 4),
                            "between_seq": [prev["seq"], seg["seq"]],
                            "reason": "相邻片段里程区间相互重叠，坐标冲突，需人工判定取舍",
                        },
                        related_segment_ids=[prev["id"], seg["id"]],
                        activation_seq=seg["seq"],
                    )
                )
                first_blocker_seen = True

        rows.append(
            PositionRow(
                segment_id=seg["id"],
                seq=seg["seq"],
                raw_start=seg["encoder_start"],
                raw_end=seg["encoder_end"],
                chainage_start_m=round(start_m, 6),
                chainage_end_m=round(end_m, 6),
                scale=scale,
                provisional=first_blocker_seen,
            )
        )
        prev = seg
        expected_seq = seg["seq"] + 1

    chainage_end = rows[-1].chainage_end_m if rows else None

    # 3) 与管线标称长度比对（警告级，不单独阻断，但留给人工判断）
    if nominal_length_m and chainage_end is not None:
        deviation = abs(chainage_end - nominal_length_m) / nominal_length_m
        if deviation > LENGTH_DEVIATION_LIMIT:
            findings.append(
                Finding(
                    kind="chainage_conflict",
                    severity="warning",
                    dedup_key="nominal-length",
                    detail={
                        "nominal_length_m": nominal_length_m,
                        "computed_length_m": round(chainage_end, 4),
                        "deviation_ratio": round(deviation, 4),
                        "reason": "推算总长与管线标称长度偏差超过 2%",
                    },
                )
            )

    return PositionResult(
        scale=scale,
        rows=rows,
        findings=findings,
        chainage_origin_raw=origin,
        chainage_end_m=chainage_end,
    )


def explain_point(
    raw_offset: float,
    segment: dict,
    position_row: dict,
    wheel_diameter_mm: float,
    encoder_ppr: int,
) -> dict:
    """生成单个缺陷点的里程换算过程说明。"""
    scale = compute_scale(wheel_diameter_mm, encoder_ppr)
    in_segment_pulses = raw_offset - segment["encoder_start"]
    in_segment_m = scale * in_segment_pulses
    chainage = position_row["chainage_start_m"] + in_segment_m
    return {
        "formula": "chainage_m = segment_start_m + (π · wheel_diameter_mm / (1000 · ppr)) · (raw_offset - encoder_start)",
        "wheel_diameter_mm": wheel_diameter_mm,
        "encoder_ppr": encoder_ppr,
        "scale_m_per_pulse": scale,
        "segment_chainage_start_m": position_row["chainage_start_m"],
        "raw_offset": raw_offset,
        "segment_encoder_start": segment["encoder_start"],
        "pulses_into_segment": in_segment_pulses,
        "meters_into_segment": round(in_segment_m, 6),
        "chainage_m": round(chainage, 6),
        "provisional": bool(position_row.get("provisional")),
    }
