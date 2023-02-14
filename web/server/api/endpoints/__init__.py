from fastapi import APIRouter

from web.server.api.endpoints import directories, endpoints, files, plan

api_router = APIRouter()
api_router.include_router(endpoints.router)
api_router.include_router(
    files.router,
    prefix="/files",
)
api_router.include_router(directories.router, prefix="/directories")
api_router.include_router(plan.router, prefix="/plan")
