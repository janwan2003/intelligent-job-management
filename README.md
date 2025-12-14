# Research project 4.3

## Prerequisites

- Node.js 23+
- [pnpm](https://pnpm.io/) - `curl -fsSL https://get.pnpm.io/install.sh | sh -`
- Docker & Docker Compose

## Setup

```bash
# Frontend
cd frontend
pnpm install

# Pre-commit hooks (REQUIRED for development)
uv tool install pre-commit
uv tool update-shell  # Add UV tools to PATH
pre-commit install
```

**Important**: After installing pre-commit, restart your terminal or run:
```bash
export PATH="$HOME/.local/bin:$PATH"
```

## Development

```bash
# Backend (http://localhost:8000)
cd backend
docker-compose up

# Frontend (http://localhost:5173)
cd frontend
pnpm dev
```

Backend: `http://localhost:8000`  
Frontend: `http://localhost:5173`

## Structure

```
backend/          # Python 3.12, FastAPI, Docker
frontend/         # React, TypeScript, Vite, pnpm
documentation/    # Project specs & diagrams
```

## Stack

**Backend**: Python 3.12, FastAPI, UV, Ruff, Mypy, Deptry  
**Frontend**: TypeScript, React, Vite, pnpm, TanStack Query, Tailwind  
**AWS**: *not defined yet*
