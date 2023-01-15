from api.v1.endpoints import api_router
from fastapi import FastAPI

app = FastAPI()

app.include_router(api_router, prefix="/api/v1")
