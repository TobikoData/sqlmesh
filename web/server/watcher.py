import asyncio

from watchfiles import awatch

from web.server.settings import _get_loaded_context, get_settings


async def watch_project(queue: asyncio.Queue) -> None:
    settings = get_settings()
    context = _get_loaded_context(settings.project_path, settings.config)

    print("Watching project path: ", settings.project_path)

    async for changes in awatch(settings.project_path):
        print(changes, queue, context)

        #         self.context.refresh()

        #         self.queue.put_nowait(
        #             Event(event="models", data=json.dumps([model.dict() for model in get_models(self.context)])))
        #         self.queue.put_nowait(
        #             Event(event="lineage", data=json.dumps({name: list(models) for name, models in self.context.dag.graph.items()})))
