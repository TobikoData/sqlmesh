from api.v1.resources import project
from fastapi import APIRouter

api_router = APIRouter()

api_router.include_router(project.router, prefix="/projects", tags=["projects"])
