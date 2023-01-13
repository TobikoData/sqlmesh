from fastapi import APIRouter

from web.api.v1.resources import project

api_router = APIRouter()

api_router.include_router(project.router, prefix="/projects", tags=["projects"])
