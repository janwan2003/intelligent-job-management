"""Health check endpoints."""

import logging
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import src.state as state

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
async def root() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "message": "Intelligent Job Management Platform API"}


@router.get("/health")
async def health_check() -> JSONResponse:
    """Detailed health check — pings DB and NATS."""
    checks: dict[str, Any] = {"version": "0.1.0"}
    healthy = True

    # Check database
    try:
        async with state.get_conn() as conn:
            await conn.execute("SELECT 1")
        checks["database"] = "ok"
    except Exception:
        checks["database"] = "unavailable"
        healthy = False

    # Check NATS
    if state.nc and state.nc.is_connected:
        checks["nats"] = "ok"
    else:
        checks["nats"] = "unavailable"
        healthy = False

    checks["status"] = "healthy" if healthy else "degraded"
    status_code = 200 if healthy else 503
    return JSONResponse(content=checks, status_code=status_code)
