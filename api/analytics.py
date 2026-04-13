from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
import datetime
from parser.core import timezone
from parser.db.database import get_db
from parser.db.models import RequestLog
from parser.core.auth import get_current_tenant

router = APIRouter(prefix="/v1", tags=["Analytics"])

@router.get("/stats")
def get_stats(db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    """
    Get ingestion performance analytics for the last 24 hours, scoped to tenant.
    """
    since = timezone.utcnow() - datetime.timedelta(hours=24)
    
    status_counts = db.query(
        RequestLog.status, func.count(RequestLog.id)
    ).filter(
        RequestLog.tenant_id == tenant_id,
        RequestLog.created_at >= since
    ).group_by(RequestLog.status).all()
    
    source_counts = db.query(
        RequestLog.source, func.count(RequestLog.id)
    ).filter(
        RequestLog.tenant_id == tenant_id,
        RequestLog.created_at >= since
    ).group_by(RequestLog.source).all()

    success_logs = db.query(RequestLog).filter(
        RequestLog.tenant_id == tenant_id,
        RequestLog.status == "success",
        RequestLog.created_at >= since
    ).all()
    
    parser_breakdown = {}
    for log in success_logs:
        try:
            payload = log.output_payload
            if not payload: continue
            
            # Handle both single item and result list formats
            items = []
            if "results" in payload:
                items = payload["results"]
            elif "metadata" in payload: # Single ParsedItem
                items = [payload]
                
            for r in items:
                p_used = r.get("metadata", {}).get("parser_used", "Unknown")
                parser_breakdown[p_used] = parser_breakdown.get(p_used, 0) + 1
        except: continue

    return {
        "window": "24h",
        "summary": {
            "total_processed": sum(c for s, c in status_counts),
            "status_breakdown": {s: c for s, c in status_counts},
            "source_breakdown": {s: c for s, c in source_counts}
        },
        "parser_performance": parser_breakdown,
        "server_time": timezone.to_iso(timezone.utcnow())
    }
