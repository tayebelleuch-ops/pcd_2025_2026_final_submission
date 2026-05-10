"""FastAPI application entry point."""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.config import settings
from app.routers.chat import router as chat_router
from app.routers.chat import limiter
from app.repositories.postgres import close_pg_pool

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events securely.
    """
    logger.info(f"🚀 Starting up {settings.app_name}...")
    # Startup: We could eagerly initialize the DB pools here if we wanted, 
    # but they initialize safely on first use in our current setup.
    yield
    
    # Shutdown: Clean up resources
    logger.info("🛑 Shutting down gracefully...")
    await close_pg_pool()    

# Initialize the FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=settings.app_description,
    lifespan=lifespan
)

# Register the Rate Limiter state and error handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Configure CORS (Cross-Origin Resource Sharing)
# This allows your Vite/React frontend to talk to this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Attach the AI Chat Router
app.include_router(chat_router)

@app.get("/health", tags=["System"])
async def health_check():
    """Simple ping to verify the API is alive."""
    return {"status": "healthy", "version": settings.app_version}