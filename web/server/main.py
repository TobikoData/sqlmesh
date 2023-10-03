import asyncio
import mimetypes
import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from web.server.api.endpoints import api_router
from web.server.console import api_console
from web.server.exceptions import ApiException
from web.server.settings import get_context, get_settings
from web.server.watcher import watch_project

app = FastAPI()

app.include_router(api_router, prefix="/api")
WEB_DIRECTORY = pathlib.Path(__file__).parent.parent

# Starlette uses mimetypes.guess_type to determine a file response's content type. Since this method
# is not consistent across different computers, operating systems, etc., we enumerate some of the
# more common ones we use here.
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")


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
    app.state.watch_task = asyncio.create_task(watch_project(api_console.queue))


@app.on_event("shutdown")
async def shutdown_event() -> None:
    app.state.dispatch_task.cancel()
    app.state.watch_task.cancel()
    context = await get_context(settings=get_settings())
    context.close()


@app.exception_handler(ApiException)
async def handle_api_exception(_: Request, e: ApiException) -> JSONResponse:
    return JSONResponse(status_code=e.status_code, content=e.to_dict())


@app.exception_handler(Exception)
async def handle_uncaught_exeption(_: Request, e: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        content=ApiException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            message=str(e),
            origin="API -> main -> custom_exception_handler",
        ).to_dict(),
    )


@app.get("/health")
def health() -> str:
    return "ok"


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


@app.get("/{full_path:path}")
async def index(full_path: str = "") -> HTMLResponse:
    with open(WEB_DIRECTORY / "client/dist/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
