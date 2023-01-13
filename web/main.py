from os import getcwd

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from web.api.v1.endpoints import api_router

app = FastAPI()

path_index = getcwd() + "/web/app/prod/index.html"
path_assets = getcwd() + "/web/app/prod/assets"


@app.get("/")
def root():
    return FileResponse(path=path_index, status_code=200)


app.mount("/assets", StaticFiles(directory=path_assets), name="assets")

app.include_router(api_router, prefix="/api/v1")
