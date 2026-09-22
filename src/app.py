"""兼容基线入口：委托给 inspection 包。"""

from inspection.app import SERVICE_NAME, create_server  # noqa: F401

__all__ = ["SERVICE_NAME", "create_server"]
