"""FastAPI application entry point.

Run with:
    uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000

Or from the project root:
    python -m uvicorn backend.app.main:app --reload
"""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.auth import require_auth
from backend.app.config import get_settings
from backend.app.database.models import init_all_databases
from backend.app.routers import auth, cyber, halfs, royka


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown."""
    init_all_databases()
    yield


settings = get_settings()

app = FastAPI(
    title="Excel Analyzer Pro — Web API",
    description=(
        "REST API for basketball statistics analysis.\n\n"
        "Sections: **Halfs** (База половин), **Royka** (Ройка), **Cyber** (Cybers Bases, Cyber LIVE)."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend and external apps to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Auth router (public — no token required for login)
app.include_router(auth.router, prefix="/api")

# Protected routers — all endpoints require a valid JWT token
app.include_router(
    halfs.router,
    prefix="/api",
    dependencies=[Depends(require_auth)],
)
app.include_router(
    royka.router,
    prefix="/api",
    dependencies=[Depends(require_auth)],
)
app.include_router(
    cyber.router,
    prefix="/api",
    dependencies=[Depends(require_auth)],
)


@app.get("/")
def root():
    return {
        "app": "Excel Analyzer Pro",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}
