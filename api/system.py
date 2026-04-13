from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from parser.db.database import get_db
from parser.db.models import RequestLog
from parser.core.auth import get_current_tenant
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["System"])

@router.get("/health")
def health_check():
    return {"status": "ok", "service": "parser-engine"}

@router.get("/logs")
def list_logs(
    limit: int = 50, 
    offset: int = 0, 
    source: Optional[str] = None, 
    status: Optional[str] = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    """
    List ingestion logs with filtering and pagination, scoped to tenant.
    """
    query = db.query(RequestLog).filter(RequestLog.tenant_id == tenant_id)
    
    if source:
        query = query.filter(RequestLog.source == source)
    if status:
        query = query.filter(RequestLog.status == status)
        
    total = query.count()
    logs = query.order_by(RequestLog.created_at.desc()).offset(offset).limit(limit).all()
    
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "logs": logs
    }

@router.get("/logs/{log_id}")
def get_log(
    log_id: str, 
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    """
    Get detailed logs for a specific request, scoped to tenant.
    """
    log = db.query(RequestLog).filter(
        RequestLog.id == log_id,
        RequestLog.tenant_id == tenant_id
    ).first()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")
        
    return log
