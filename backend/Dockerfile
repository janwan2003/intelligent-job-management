FROM python:3.13-slim

WORKDIR /app

# Install uv and Docker CLI
RUN pip install uv && \
    apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* get-docker.sh

# Copy dependency files
COPY backend/pyproject.toml .

# Install dependencies
RUN uv sync --no-dev

# Copy application code
COPY backend/src/ ./src/
COPY backend/schema.sql ./schema.sql
COPY shared/ ./shared/

EXPOSE 8000

CMD [".venv/bin/uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
