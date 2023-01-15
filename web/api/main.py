from fastapi import FastAPI
from v1.endpoints import api_router

app = FastAPI()

app.include_router(api_router, prefix="/api/v1")
