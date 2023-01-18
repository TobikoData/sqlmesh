from fastapi import FastAPI

from web.server.api.endpoints import router

app = FastAPI()

app.include_router(router, prefix="/api")
