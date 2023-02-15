import asyncio

from fastapi import FastAPI

from web.server.api.endpoints import api_router
from web.server.console import ApiConsole

app = FastAPI()
api_console = ApiConsole()

app.include_router(api_router, prefix="/api")


@app.on_event("startup")
async def startup_event() -> None:
    async def dispatch() -> None:
        while True:
            item = await api_console.queue.get()
            for listener in app.state.console_listeners:
                await listener.put(item)
            api_console.queue.task_done()

    app.state.console_listeners = []
    app.state.dispatch_task = asyncio.create_task(dispatch())


@app.on_event("shutdown")
def shutdown_event() -> None:
    app.state.dispatch_task.cancel()


@app.get("/health")
def health() -> str:
    return "ok"
