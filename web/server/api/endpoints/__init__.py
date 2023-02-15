from fastapi import APIRouter

from web.server.api.endpoints import commands, directories, events, files, plan

api_router = APIRouter()
api_router.include_router(commands.router)
api_router.include_router(
    files.router,
    prefix="/files",
)
api_router.include_router(directories.router, prefix="/directories")
api_router.include_router(plan.router, prefix="/plan")
api_router.include_router(events.router, prefix="/events")
