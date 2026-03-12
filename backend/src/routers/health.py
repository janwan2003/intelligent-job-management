"""Health check endpoints."""

from typing import Any

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def root() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "message": "Intelligent Job Management Platform API"}


@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Detailed health check."""
    return {"status": "healthy", "version": "0.1.0"}
