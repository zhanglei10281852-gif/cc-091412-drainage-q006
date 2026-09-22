"""领域错误与错误码。"""

from __future__ import annotations


class AppError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, code: str | None = None, status: int | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status is not None:
            self.status = status

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message}


class NotFoundError(AppError):
    status = 404
    code = "not_found"


class ConflictError(AppError):
    status = 409
    code = "conflict"


class ValidationError(AppError):
    status = 422
    code = "validation_error"


class StateError(ConflictError):
    code = "invalid_state"


class ChecksumMismatchError(ConflictError):
    code = "checksum_mismatch"


class PermissionError(AppError):  # noqa: A001 - 领域语义明确
    status = 403
    code = "forbidden"


class AuthError(AppError):
    status = 401
    code = "unauthorized"
