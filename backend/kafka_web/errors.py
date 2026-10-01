"""Application error hierarchy; every error maps to an HTTP status and a stable code."""


class AppError(Exception):
    status: int = 500
    code: str = "internal_error"

    def __init__(self, message: str, *, code: str | None = None, field: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or type(self).code
        self.field = field

    def to_body(self) -> dict[str, str]:
        body = {"code": self.code, "message": self.message}
        if self.field is not None:
            body["field"] = self.field
        return body


class ValidationFailed(AppError):
    status = 422
    code = "validation_failed"


class Unauthorized(AppError):
    status = 401
    code = "authentication_failed"


class Forbidden(AppError):
    status = 403
    code = "forbidden"


class NotFound(AppError):
    status = 404
    code = "not_found"


class Conflict(AppError):
    status = 409
    code = "conflict"


class BrokerError(AppError):
    status = 502
    code = "broker_error"


class KafkaTimeout(AppError):
    status = 504
    code = "kafka_timeout"


class ConfigFileInvalid(AppError):
    status = 500
    code = "config_file_invalid"
