from fastapi import APIRouter
import vscode.server.api.lineage as lineage

api_router = APIRouter()
api_router.include_router(lineage.router, prefix="/lineage")