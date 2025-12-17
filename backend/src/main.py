"""Main FastAPI application."""

import json
import logging
import os
import subprocess
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import nats  # type: ignore[import-not-found]
import psycopg  # type: ignore[import-not-found]
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from nats.js import JetStreamContext  # type: ignore[import-not-found]
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Database and NATS clients (global)
db_pool: psycopg.AsyncConnection[Any] | None = None
nc: Any = None
js: JetStreamContext | None = None


class JobCreate(BaseModel):
    """Job creation request."""

    image: str
    command: list[str]


class Job(BaseModel):
    """Job response model."""

    id: str
    image: str
    command: list[str]
    status: str
    created_at: datetime
    updated_at: datetime
    container_name: str | None = None
    exit_code: int | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    global db_pool, nc, js

    # Get configuration from environment
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ijm")
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")

    # Connect to database
    logger.info("Connecting to database: %s", database_url)
    db_pool = await psycopg.AsyncConnection.connect(database_url)

    # Create jobs table if not exists
    async with db_pool.cursor() as cur:
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                image TEXT NOT NULL,
                command JSONB NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                container_name TEXT,
                exit_code INT
            )
        """)
        await db_pool.commit()
    logger.info("Database initialized")

    # Connect to NATS
    logger.info("Connecting to NATS: %s", nats_url)
    nc = await nats.connect(nats_url)
    js = nc.jetstream()

    # Ensure JetStream stream exists
    try:
        await js.add_stream(name="JOBS", subjects=["jobs.>"])
        logger.info("NATS JetStream initialized")
    except Exception as e:
        logger.warning("NATS stream already exists or error: %s", e)

    yield

    # Cleanup
    if db_pool:
        await db_pool.close()
    if nc:
        await nc.close()


app = FastAPI(
    title="Intelligent Job Management Platform",
    description="API for Intelligent Job Management",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS configuration for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "message": "Intelligent Job Management Platform API"}


@app.get("/health")
async def health_check() -> dict[str, Any]:
    """Detailed health check."""
    return {"status": "healthy", "version": "0.1.0"}


@app.post("/images/upload")
async def upload_image(file: UploadFile) -> dict[str, Any]:
    """Upload a Docker image file (.tar or .tar.gz) and load it."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    # Validate file extension
    if not file.filename.endswith((".tar", ".tar.gz", ".tgz")):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only .tar, .tar.gz, or .tgz files are allowed",
        )

    # Save uploaded file to temporary location
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp_file:
        tmp_path = tmp_file.name
        content = await file.read()
        tmp_file.write(content)

    try:
        # Load image into Docker
        result = subprocess.run(
            ["docker", "load", "-i", tmp_path],
            capture_output=True,
            text=True,
            check=True,
        )

        # Parse output to get image name
        # Output format: "Loaded image: <image_name:tag>"
        output = result.stdout.strip()
        if "Loaded image:" in output:
            image_name = output.split("Loaded image:")[-1].strip()
        else:
            # Fallback parsing
            image_name = output.split()[-1] if output else "unknown"

        return {
            "status": "success",
            "image": image_name,
            "message": f"Successfully loaded image: {image_name}",
        }

    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load Docker image: {e.stderr}",
        ) from e
    finally:
        # Clean up temporary file
        Path(tmp_path).unlink(missing_ok=True)


@app.post("/jobs", response_model=Job, status_code=201)
async def create_job(job_request: JobCreate) -> Job:
    """Create a new job."""
    assert db_pool is not None, "Database not initialized"
    assert js is not None, "NATS not initialized"

    job_id = str(uuid4())
    now = datetime.now(UTC)

    # Insert job into database
    async with db_pool.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO jobs (id, image, command, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                job_request.image,
                json.dumps(job_request.command),
                "QUEUED",
                now,
                now,
            ),
        )
        await db_pool.commit()

    # Publish job submission event to NATS
    await js.publish(
        "jobs.submitted",
        json.dumps({"job_id": job_id}).encode(),
    )

    return Job(
        id=job_id,
        image=job_request.image,
        command=job_request.command,
        status="QUEUED",
        created_at=now,
        updated_at=now,
    )


@app.get("/jobs", response_model=list[Job])
async def list_jobs() -> list[Job]:
    """List all jobs ordered by creation time (newest first)."""
    assert db_pool is not None, "Database not initialized"

    async with db_pool.cursor() as cur:
        await cur.execute(
            """
            SELECT id, image, command, status, created_at, updated_at, container_name, exit_code
            FROM jobs
            ORDER BY created_at DESC
            """
        )
        rows = await cur.fetchall()

    jobs = []
    for row in rows:
        jobs.append(
            Job(
                id=row[0],
                image=row[1],
                command=row[2],  # JSONB is auto-deserialized by psycopg3
                status=row[3],
                created_at=row[4],
                updated_at=row[5],
                container_name=row[6],
                exit_code=row[7],
            )
        )

    return jobs


@app.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str) -> Job:
    """Get a specific job by ID."""
    assert db_pool is not None, "Database not initialized"

    async with db_pool.cursor() as cur:
        await cur.execute(
            """
            SELECT id, image, command, status, created_at, updated_at, container_name, exit_code
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return Job(
        id=row[0],
        image=row[1],
        command=row[2],  # JSONB is auto-deserialized by psycopg3
        status=row[3],
        created_at=row[4],
        updated_at=row[5],
        container_name=row[6],
        exit_code=row[7],
    )


@app.post("/jobs/{job_id}/stop", status_code=202)
async def stop_job(job_id: str) -> dict[str, str]:
    """Request job to be stopped."""
    assert db_pool is not None, "Database not initialized"
    assert js is not None, "NATS not initialized"

    logger.info("Received stop request for job: %s", job_id)

    # Verify job exists
    async with db_pool.cursor() as cur:
        await cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

    # Publish stop request to NATS
    ack = await js.publish(
        "jobs.stop_requested",
        json.dumps({"job_id": job_id}).encode(),
    )
    logger.debug("NATS ack - seq: %s, duplicate: %s", ack.seq, ack.duplicate)

    return {"status": "stop_requested", "job_id": job_id}


@app.post("/jobs/{job_id}/resume", status_code=202)
async def resume_job(job_id: str) -> dict[str, str]:
    """Request job to be resumed."""
    assert db_pool is not None, "Database not initialized"
    assert js is not None, "NATS not initialized"

    # Verify job exists
    async with db_pool.cursor() as cur:
        await cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

    # Publish resume request to NATS
    await js.publish(
        "jobs.resume_requested",
        json.dumps({"job_id": job_id}).encode(),
    )

    return {"status": "resume_requested", "job_id": job_id}


@app.delete("/jobs/{job_id}", status_code=204)
async def delete_job(job_id: str) -> None:
    """Delete a job."""
    assert db_pool is not None, "Database not initialized"

    async with db_pool.cursor() as cur:
        # Check if job exists
        await cur.execute("SELECT id FROM jobs WHERE id = %s", (job_id,))
        if not await cur.fetchone():
            raise HTTPException(status_code=404, detail="Job not found")

        # Delete the job
        await cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))
        await db_pool.commit()

    return None
