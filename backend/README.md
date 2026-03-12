# Backend

FastAPI backend with Python 3.13 and uv.

## Setup

```bash
uv sync
```

## Test

```bash
uv run pytest
uv run pytest --cov=src
```

## Tools

```bash
uv run ruff check .
uv run ruff format .
uv run mypy src
uv run deptry .
```
