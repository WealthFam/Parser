"""
Parser Service Background Scheduler
Handles periodic cleanup tasks and maintenance operations.
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from parser.db.database import SessionLocal
from sqlalchemy import text
from datetime import timedelta
from parser.core import timezone
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

def cleanup_old_logs():
    """Delete request logs older than 24 hours to save disk space and improve performance."""
    try:
        with SessionLocal() as db:
            cutoff = timezone.utcnow() - timedelta(hours=24)
            
            # Use distinct parameters for safety
            result = db.execute(
                text("DELETE FROM request_logs WHERE created_at < :cutoff"),
                {"cutoff": cutoff.isoformat()}
            )
            db.commit()
            
            deleted_count = result.rowcount
            logger.info(f"[CLEANUP] Cleaned up {deleted_count} old logs (>24 hours)")
        
    except Exception as e:
        logger.error(f"[ERROR] Log cleanup failed: {e}")

def start_cleanup_job():
    """Start the background cleanup scheduler. Runs every hour."""
    try:
        # Add cleanup job - runs every 1 hour
        scheduler.add_job(
            cleanup_old_logs,
            'interval',
            hours=1,
            id='cleanup_logs',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("[SUCCESS] Cleanup scheduler started (runs every 1 hour)")
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to start cleanup scheduler: {e}")

def stop_cleanup_job():
    """Stop the cleanup scheduler gracefully."""
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("[STOP] Cleanup scheduler stopped")
    except Exception as e:
        logger.error(f"[ERROR] Failed to stop cleanup scheduler: {e}")
