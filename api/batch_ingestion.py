from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Dict, Optional

from parser.db.database import get_db
from parser.core.auth import get_current_tenant
from parser.core.batch_pipeline import BatchIngestionPipeline
from parser.schemas.transaction import IngestionResult
from pydantic import BaseModel

router = APIRouter(prefix="/v1/ingest/batch", tags=["Batch Ingestion"])

class BatchItem(BaseModel):
    id: str
    content: str
    subject: Optional[str] = None
    sender: Optional[str] = None
    received_at: Optional[str] = None

class BatchRequest(BaseModel):
    source: str
    items: List[BatchItem]

class BatchResponse(BaseModel):
    results: Dict[str, IngestionResult]

@router.post("/", response_model=BatchResponse)
def ingest_batch(
    payload: BatchRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    """
    Ingest a batch of items (e.g. 5-10 emails) securely via a dedicated module.
    """
    pipeline = BatchIngestionPipeline(db, tenant_id=tenant_id)
    
    # Format for pipeline
    items_dicts = []
    for it in payload.items:
        items_dicts.append({
            "id": it.id,
            "content": it.content,
            "subject": it.subject,
            "sender": it.sender,
            "received_at": it.received_at
        })
        
    res_map = pipeline.run_batch(items_dicts, payload.source)
    return BatchResponse(results=res_map)
