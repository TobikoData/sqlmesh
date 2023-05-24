import sys
import traceback
import typing as t

from fastapi import HTTPException
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.utils.date import now_timestamp


class ApiException(HTTPException):
    def __init__(
        self,
        message: str,
        origin: str,
        status_code: int = HTTP_422_UNPROCESSABLE_ENTITY,
        trigger: t.Optional[str] = None,
    ):
        super().__init__(status_code)
        error_type, error_value, error_traceback = sys.exc_info()

        self.message = message
        self.timestamp = now_timestamp()
        self.origin = origin
        self.trigger = trigger
        self.traceback = traceback.format_exc() if error_traceback else None
        self.type = str(error_type) if error_type else None
        self.description = str(error_value) if error_value else None
        self.stack = traceback.format_tb(error_traceback) if error_traceback else None

    def __str__(self) -> str:
        return f"Summary: {self.message}\n{self.description}\n{self.traceback}"

    def to_dict(self) -> t.Dict[str, t.Union[str, int, t.List[str]]]:
        output: t.Dict[str, t.Union[str, int, t.List[str], None]] = {
            "status": self.status_code,
            "timestamp": self.timestamp,
            "message": self.message,
            "origin": self.origin,
            "trigger": self.trigger,
            "type": self.type,
            "description": self.description,
            "traceback": self.traceback,
            "stack": self.stack,
        }

        return {k: v for k, v in output.items() if v is not None}
