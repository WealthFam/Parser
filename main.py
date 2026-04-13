import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import sys
import os
import logging

logger = logging.getLogger(__name__)

# Allow running from inside the 'parser' directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser.config import settings
from parser.db.database import init_db
from parser.core.scheduler import start_cleanup_job, stop_cleanup_job
from parser.api import ingestion, config, analytics, system, patterns, batch_ingestion

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Init DB, Start Scheduler
    logger.info("Starting Parser Service...")
    init_db()
    start_cleanup_job()  # [SUCCESS] Enable cleanup scheduler
    yield
    # Shutdown
    logger.info("Shutting down Parser Service...")
    stop_cleanup_job()  # [STOP] Stop scheduler gracefully

app = FastAPI(
    title="Financial Parser Microservice",
    description="A robust microservice for parsing financial transactions from SMS, Email, and Files.",
    version="1.1.0",
    lifespan=lifespan
)

# Middleware
origins = [o.strip() for o in settings.ALLOWED_ORIGINS.split(",")] if settings.ALLOWED_ORIGINS else []

cors_params = {
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}

if "*" in origins:
    cors_params["allow_origin_regex"] = ".*"
else:
    cors_params["allow_origins"] = origins

app.add_middleware(CORSMiddleware, **cors_params)


# Categorized Routers
app.include_router(system.router)
app.include_router(ingestion.router)
app.include_router(batch_ingestion.router)
app.include_router(config.router)
app.include_router(analytics.router)
app.include_router(patterns.router)

if __name__ == "__main__":
    uvicorn.run("parser.main:app", host="0.0.0.0", port=8001, reload=True)
