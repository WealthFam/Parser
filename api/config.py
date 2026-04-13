from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from pydantic import BaseModel
from parser.db.database import get_db
from parser.db.models import AIConfig, FileParsingConfig, PatternRule
from parser.core.auth import get_current_tenant
import re

router = APIRouter(prefix="/v1/config", tags=["Configuration"])

class AIConfigUpdate(BaseModel):
    provider: str = "gemini"
    api_key: Optional[str] = None
    model_name: str = "gemini-1.5-flash"
    is_enabled: bool = True

@router.get("/ai")
def get_ai_config(db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    config = db.query(AIConfig).filter(AIConfig.tenant_id == tenant_id).first()
    if not config:
        return {"status": "not_configured"}
    
    masked_key = "****" + config.api_key_enc[-4:] if config.api_key_enc and len(config.api_key_enc) > 4 else "****"
    
    return {
        "provider": config.provider,
        "model_name": config.model_name,
        "is_enabled": config.is_enabled,
        "api_key_masked": masked_key
    }

@router.post("/ai")
def update_ai_config(payload: AIConfigUpdate, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    config = db.query(AIConfig).filter(AIConfig.tenant_id == tenant_id).first()
    if not config:
        config = AIConfig(tenant_id=tenant_id)
        db.add(config)
    
    config.provider = payload.provider
    config.model_name = payload.model_name
    config.is_enabled = payload.is_enabled
    
    if payload.api_key:
        config.api_key_enc = payload.api_key
    
    db.commit()
    return {"status": "success", "message": "AI Config Updated"}

@router.post("/mapping")
def save_file_mapping(
    payload: dict,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    
    fingerprint = payload.get("fingerprint")
    if not fingerprint:
        raise HTTPException(status_code=400, detail="Fingerprint required")

    config = db.query(FileParsingConfig).filter(
        FileParsingConfig.fingerprint == fingerprint,
        FileParsingConfig.tenant_id == tenant_id
    ).first()
    if not config:
        config = FileParsingConfig(fingerprint=fingerprint, tenant_id=tenant_id)
        db.add(config)
    
    config.format = payload.get("format", "EXCEL")
    config.header_row_index = payload.get("header_row_index", 0)
    config.columns_json = payload.get("mapping", {})
    
    db.commit()
    return {"status": "success", "message": "Mapping saved"}

class PatternRuleCreate(BaseModel):
    source: str
    regex_pattern: str
    mapping: Dict[str, Any]

@router.post("/patterns")
def create_pattern_rule(payload: PatternRuleCreate, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    
    
    try:
        re.compile(payload.regex_pattern)
    except re.error:
        raise HTTPException(status_code=400, detail="Invalid Regex Pattern")

    rule = PatternRule(
        tenant_id=tenant_id,
        source=payload.source,
        regex_pattern=payload.regex_pattern,
        mapping_json=payload.mapping
    )
    db.add(rule)
    db.commit()
    return {"status": "success", "id": rule.id}

class AliasCreate(BaseModel):
    pattern: str
    alias: str

@router.get("/aliases")
def get_aliases(db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    from parser.db.models import MerchantAlias
    return db.query(MerchantAlias).filter(MerchantAlias.tenant_id == tenant_id).order_by(MerchantAlias.created_at.desc()).all()

@router.post("/aliases")
def create_alias(payload: AliasCreate, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    from parser.db.models import MerchantAlias
    # Upsert logic (scoped to tenant)
    existing = db.query(MerchantAlias).filter(
        MerchantAlias.tenant_id == tenant_id,
        MerchantAlias.pattern == payload.pattern
    ).first()
    if existing:
        existing.alias = payload.alias
    else:
        new_alias = MerchantAlias(tenant_id=tenant_id, pattern=payload.pattern, alias=payload.alias)
        db.add(new_alias)
    db.commit()
    return {"status": "success"}

@router.put("/aliases/{alias_id}")
def update_alias(
    alias_id: str, 
    payload: AliasCreate, 
    db: Session = Depends(get_db), 
    tenant_id: str = Depends(get_current_tenant)
):
    from parser.db.models import MerchantAlias
    alias = db.query(MerchantAlias).filter(
        MerchantAlias.id == alias_id,
        MerchantAlias.tenant_id == tenant_id
    ).first()
    
    if not alias:
        raise HTTPException(status_code=404, detail="Alias not found")
        
    alias.pattern = payload.pattern
    alias.alias = payload.alias
    db.commit()
    return {"status": "success"}

@router.delete("/aliases/{alias_id}")
def delete_alias(alias_id: str, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    from parser.db.models import MerchantAlias
    db.query(MerchantAlias).filter(
        MerchantAlias.id == alias_id,
        MerchantAlias.tenant_id == tenant_id
    ).delete()
    db.commit()
    return {"status": "success"}
