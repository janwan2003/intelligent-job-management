"""Docker image upload endpoint."""

import subprocess
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile

router = APIRouter()


@router.post("/images/upload")
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
