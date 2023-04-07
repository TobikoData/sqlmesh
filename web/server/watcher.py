import asyncio
import typing as t

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from sqlmesh.core.context import Context
from web.server.api.endpoints.models import get_models
from web.server.settings import _get_loaded_context, get_settings
from web.server.sse import Event


def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = _get_loaded_context(settings.project_path, settings.config)

    observer = Observer()
    observer.schedule(HandleFileSystemEvent(queue, context), settings.project_path, recursive=True)
    observer.start()


class HandleFileSystemEvent(FileSystemEventHandler):
    def __init__(self, queue: asyncio.Queue, context: Context):
        self.queue = queue
        self.context = context

    def on_modified(self, event: t.Any) -> None:
        if event.is_directory:
            return None

        self.context.refresh()

        self.queue.put_nowait(Event(event="models", data=str(get_models(self.context).json())))

        print("Received on_modified event - %s." % event.src_path)

    def on_created(self, event: t.Any) -> None:
        if event.is_directory:
            return None

        self.context.refresh()

        self.queue.put_nowait(Event(event="models", data=str(get_models(self.context).json())))

        print("Received on_created event - %s." % event.src_path)

    def on_deleted(self, event: t.Any) -> None:
        if event.is_directory:
            return None

        self.context.refresh()

        self.queue.put_nowait(Event(event="models", data=str(get_models(self.context).json())))

        print("Received on_deleted event - %s." % event.src_path)
