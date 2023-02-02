"""https://html.spec.whatwg.org/multipage/server-sent-events.html"""
from __future__ import annotations

import dataclasses
import typing as t

from starlette.background import BackgroundTask
from starlette.responses import AsyncContentStream, ContentStream, StreamingResponse


@dataclasses.dataclass
class Event:
    data: str
    event: t.Optional[str] = ""
    id: t.Optional[str] = ""
    retry: t.Optional[int] = None
    separator: str = "\r\n"
    is_comment: bool = False

    def stream(self) -> t.Generator:
        if self.is_comment:
            for line in str(self.data).splitlines():
                yield f": {line}{self.separator}"
            return

        if self.event:
            yield f"event: {self.event}{self.separator}"

        for line in str(self.data).splitlines():
            yield f"data: {line}{self.separator}"

        if self.id is None:
            yield "id"
        elif self.id:
            yield f"id: {self.id}{self.separator}"

        if self.retry is not None:
            yield f"retry: {self.retry}{self.separator}"

        yield self.separator

    @classmethod
    def to_event(cls, chunk: str | bytes) -> Event:
        if isinstance(chunk, Event):
            return chunk
        elif isinstance(chunk, dict):
            return cls(**chunk)
        elif isinstance(chunk, bytes):
            return cls(data=chunk.decode("utf-8"))
        else:
            return cls(data=chunk)


class SSEResponse(StreamingResponse):
    def __init__(
        self,
        content: ContentStream,
        status_code: int = 200,
        headers: t.Optional[t.Dict[str, str]] = None,
        media_type: t.Optional[str] = "text/event-stream",
        background: t.Optional[BackgroundTask] = None,
    ) -> None:
        super().__init__(content, status_code, headers, media_type, background)
        self.body_iterator = self.wrap_body(self.body_iterator)

    async def wrap_body(self, body_iterator: AsyncContentStream) -> t.AsyncGenerator:
        async for chunk in body_iterator:
            for data in Event.to_event(chunk).stream():
                yield data
