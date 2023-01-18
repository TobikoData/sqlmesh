from fastapi import FastAPI

from web.server.api.endpoints import router
from web.server.api.v1.api import api_router

app = FastAPI()

app.include_router(router, prefix="/api")
app.include_router(api_router, prefix="/api/v1")
