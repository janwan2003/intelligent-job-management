"""API routers — combined into a single router for inclusion in the app."""

from fastapi import APIRouter

from src.routers.health import router as health_router
from src.routers.images import router as images_router
from src.routers.jobs import router as jobs_router
from src.routers.nodes import router as nodes_router
from src.routers.profiling import router as profiling_router

router = APIRouter()
router.include_router(health_router)
router.include_router(nodes_router)
router.include_router(jobs_router)
router.include_router(images_router)
router.include_router(profiling_router)
