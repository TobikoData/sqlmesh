import asyncio
import pathlib

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from web.server.api.endpoints import api_router
from web.server.console import ApiConsole

app = FastAPI()
api_console = ApiConsole()

app.include_router(api_router, prefix="/api")
WEB_DIRECTORY = pathlib.Path(__file__).parent.parent


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


@app.get("/")
def index() -> HTMLResponse:
    with open(WEB_DIRECTORY / "client/dist/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


app.mount(
    "/assets",
    StaticFiles(directory=WEB_DIRECTORY / "client/dist/assets", check_dir=False),
    name="assets",
)
app.mount(
    "/favicons",
    StaticFiles(directory=WEB_DIRECTORY / "client/dist/favicons", check_dir=False),
    name="favicons",
)
