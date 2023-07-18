from fastapi import APIRouter

from web.server.api.endpoints import (
    commands,
    directories,
    environments,
    events,
    files,
    lineage,
    meta,
    models,
    plan,
    table_diff,
)

api_router = APIRouter()
api_router.include_router(commands.router, prefix="/commands")
api_router.include_router(
    files.router,
    prefix="/files",
)
api_router.include_router(directories.router, prefix="/directories")
api_router.include_router(plan.router, prefix="/plan")
api_router.include_router(environments.router, prefix="/environments")
api_router.include_router(events.router, prefix="/events")
api_router.include_router(lineage.router, prefix="/lineage")
api_router.include_router(models.router, prefix="/models")
api_router.include_router(meta.router, prefix="/meta")
api_router.include_router(table_diff.router, prefix="/table_diff")
