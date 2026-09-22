"""角色、令牌与可见范围。

角色（与 reference/domain.json 对齐）：

* ``engineer``（项目工程师/运维人员）：可管理任务、校准、上传原始影像、
  处理复核、创建缺陷标记与发布报告，可查看原始影像。
* ``dispatcher``（调度员）：可查看任务与脱敏截图，不可见原始影像。
* ``supervisor``（监管人员）：可查看已发布版本与脱敏截图，不可见原始影像。
* ``readonly``（只读用户）：仅可查看已发布版本与脱敏截图。

脱敏截图（redacted snapshot）剥离了客户/地点敏感信息，发布后供外部角色
查看；原始影像（raw video）仅工程师可见。
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass

from .errors import AuthError, PermissionError

ENGINEER = "engineer"
DISPATCHER = "dispatcher"
SUPERVISOR = "supervisor"
READONLY = "readonly"

ROLES = (ENGINEER, DISPATCHER, SUPERVISOR, READONLY)

# 可查看原始分段文件（含下载地址/校验和等明细）
CAN_VIEW_RAW = frozenset({ENGINEER})
# 可查看脱敏截图
CAN_VIEW_SNAPSHOT = frozenset({ENGINEER, DISPATCHER, SUPERVISOR, READONLY})
# 写操作
CAN_MANAGE = frozenset({ENGINEER})
# 可执行发布
CAN_PUBLISH = frozenset({ENGINEER})


@dataclass(frozen=True)
class Principal:
    token: str
    role: str
    name: str

    def require(self, allowed: frozenset[str]) -> None:
        if self.role not in allowed:
            raise PermissionError(f"角色 {self.role} 无权执行该操作")

    @property
    def can_view_raw(self) -> bool:
        return self.role in CAN_VIEW_RAW


def _token() -> str:
    return secrets.token_urlsafe(18)


# 演示用内置令牌（无状态鉴权）。生产应替换为签发的 JWT/会话。
SEED_TOKENS: dict[str, tuple[str, str]] = {
    "tok-engineer": (ENGINEER, "项目工程师-甲"),
    "tok-dispatcher": (DISPATCHER, "调度员-乙"),
    "tok-supervisor": (SUPERVISOR, "监管人员-丙"),
    "tok-readonly": (READONLY, "只读用户-丁"),
}


def issue_token(role: str, name: str) -> str:  # pragma: no cover - 便捷工具
    if role not in ROLES:
        raise AuthError(f"未知角色: {role}")
    # 简化：返回的令牌即凭证；不落库，测试使用固定令牌。
    return f"tok-{role}-{_token()}"


def authenticate(authorization: str | None) -> Principal:
    """解析 ``Authorization: Bearer <token>``。"""
    if not authorization or not authorization.startswith("Bearer "):
        raise AuthError("缺少 Bearer 令牌")
    token = authorization[7:].strip()
    found = SEED_TOKENS.get(token)
    if found is None:
        raise AuthError("令牌无效")
    role, name = found
    return Principal(token=token, role=role, name=name)
