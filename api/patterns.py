"""Pattern management API for parser service."""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime
import re

from parser.db.database import get_db
from parser.db.models import PatternRule
from sqlalchemy.orm import Session
from parser.core.auth import get_current_tenant

router = APIRouter(prefix="/v1/patterns", tags=["patterns"])


# Schemas
class PatternBase(BaseModel):
    bank_name: str = Field(..., min_length=1, max_length=50)
    regex_pattern: str = Field(..., min_length=1)
    field_mapping: Dict[str, Any] = Field(default_factory=dict) # Allow int (group index) or str (static value)
    date_format: Optional[str] = None
    confidence: Optional[Any] = None
    
    @validator('regex_pattern')
    def validate_regex(cls, v):
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        return v


class PatternCreate(PatternBase):
    pass


class PatternUpdate(BaseModel):
    regex_pattern: Optional[str] = None
    field_mapping: Optional[Dict[str, Any]] = None
    date_format: Optional[str] = None
    confidence: Optional[Any] = None
    
    @validator('regex_pattern')
    def validate_regex(cls, v):
        if v is not None:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")
        return v


class PatternResponse(PatternBase):
    id: str
    is_ai_generated: bool
    created_at: datetime
    date_format: Optional[str] = None
    
    class Config:
        from_attributes = True


class PatternTest(BaseModel):
    regex_pattern: str
    field_mapping: Dict[str, Any]
    test_text: str


class PatternTestResult(BaseModel):
    matches: bool
    extracted: Dict[str, str] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)


# Endpoints
@router.get("")
async def list_patterns(
    bank: Optional[str] = Query(None),
    is_ai_generated: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    """List all saved parser patterns with optional filters."""
    query = db.query(PatternRule).filter(PatternRule.is_active == True, PatternRule.tenant_id == tenant_id)
    
    if bank:
        query = query.filter(PatternRule.source.ilike(f"%{bank}%"))
    if is_ai_generated is not None:
        query = query.filter(PatternRule.is_ai_generated == is_ai_generated)
    if search:
        query = query.filter(PatternRule.regex_pattern.ilike(f"%{search}%"))
    
    # Get total count
    total = query.count()
    
    # Get paginated results
    patterns = query.order_by(PatternRule.created_at.desc()).offset(skip).limit(limit).all()
    
    # Convert to response format (adapt field names)
    pattern_dicts = []
    for p in patterns:
        pattern_dicts.append({
            "id": p.id,
            "bank_name": p.source,  # Map source to bank_name
            "regex_pattern": p.regex_pattern,
            "field_mapping": p.mapping_json,  # Map mapping_json to field_mapping
            "date_format": p.date_format,
            "confidence": p.confidence,
            "is_ai_generated": p.is_ai_generated,
            "created_at": p.created_at
        })
    
    return {
        "total": total,
        "patterns": pattern_dicts,
        "limit": limit,
        "skip": skip
    }


@router.get("/{pattern_id}")
async def get_pattern(pattern_id: str, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    """Get a single pattern by ID."""
    pattern = db.query(PatternRule).filter(PatternRule.id == pattern_id, PatternRule.tenant_id == tenant_id).first()
    
    if not pattern:
        raise HTTPException(404, "Pattern not found")
    
    return {
        "id": pattern.id,
        "bank_name": pattern.source,
        "regex_pattern": pattern.regex_pattern,
        "field_mapping": pattern.mapping_json,
        "date_format": pattern.date_format,
        "confidence": pattern.confidence,
        "is_ai_generated": pattern.is_ai_generated,
        "created_at": pattern.created_at
    }


@router.post("", status_code=201)
async def create_pattern(pattern: PatternCreate, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    """Create a new pattern manually."""
    new_pattern = PatternRule(
        tenant_id=tenant_id,
        source=pattern.bank_name,  # Store bank_name as source
        regex_pattern=pattern.regex_pattern,
        mapping_json=pattern.field_mapping,  # Store field_mapping as mapping_json
        date_format=pattern.date_format,
        confidence=pattern.confidence,
        is_ai_generated=False,
        is_active=True
    )
    
    db.add(new_pattern)
    db.commit()
    db.refresh(new_pattern)
    
    return {
        "id": new_pattern.id,
        "bank_name": new_pattern.source,
        "regex_pattern": new_pattern.regex_pattern,
        "field_mapping": new_pattern.mapping_json,
        "date_format": new_pattern.date_format,
        "confidence": new_pattern.confidence,
        "is_ai_generated": new_pattern.is_ai_generated,
        "created_at": new_pattern.created_at
    }


@router.put("/{pattern_id}")
async def update_pattern(
    pattern_id: str, 
    update: PatternUpdate,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    """Update an existing pattern."""
    pattern = db.query(PatternRule).filter(PatternRule.id == pattern_id, PatternRule.tenant_id == tenant_id).first()
    
    if not pattern:
        raise HTTPException(404, "Pattern not found")
    
    # Update fields
    if update.regex_pattern is not None:
        pattern.regex_pattern = update.regex_pattern
    if update.field_mapping is not None:
        pattern.mapping_json = update.field_mapping
    if update.date_format is not None:
        pattern.date_format = update.date_format
    if update.confidence is not None:
        pattern.confidence = update.confidence
    
    if update.regex_pattern is None and update.field_mapping is None and update.date_format is None and update.confidence is None:
        raise HTTPException(400, "No updates provided")
    
    db.commit()
    db.refresh(pattern)
    
    return {
        "id": pattern.id,
        "bank_name": pattern.source,
        "regex_pattern": pattern.regex_pattern,
        "field_mapping": pattern.mapping_json,
        "date_format": pattern.date_format,
        "confidence": pattern.confidence,
        "is_ai_generated": pattern.is_ai_generated,
        "created_at": pattern.created_at
    }


@router.delete("/{pattern_id}")
async def delete_pattern(pattern_id: str, db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    """Delete a pattern."""
    pattern = db.query(PatternRule).filter(PatternRule.id == pattern_id, PatternRule.tenant_id == tenant_id).first()
    
    if not pattern:
        raise HTTPException(404, "Pattern not found")
    
    # Soft delete by setting is_active to False
    pattern.is_active = False
    db.commit()
    
    return {"status": "deleted", "id": pattern_id}


@router.post("/test")
async def test_pattern(test: PatternTest):
    """Test a regex pattern against sample text."""
    try:
        regex = re.compile(test.regex_pattern, re.IGNORECASE)
        match = regex.search(test.test_text)
        
        if match:
            extracted = {}
            for field_name, value in test.field_mapping.items():
                # 1. Try int index
                if isinstance(value, int):
                    try:
                        extracted[field_name] = match.group(value)
                    except IndexError:
                        extracted[field_name] = None
                # 2. Try string (named group or static)
                elif isinstance(value, str):
                    if value.isdigit():
                        try:
                            extracted[field_name] = match.group(int(value))
                        except IndexError:
                            extracted[field_name] = None
                    else:
                        # Try named group, fallback to static value
                        try:
                            extracted[field_name] = match.group(value)
                        except (IndexError, KeyError):
                            extracted[field_name] = value
                else:
                    extracted[field_name] = None
            
            return PatternTestResult(
                matches=True,
                extracted=extracted,
                errors=[]
            )
        else:
            return PatternTestResult(
                matches=False,
                extracted={},
                errors=["Pattern does not match the test text"]
            )
    
    except re.error as e:
        return PatternTestResult(
            matches=False,
            extracted={},
            errors=[f"Invalid regex: {str(e)}"]
        )
    except Exception as e:
        return PatternTestResult(
            matches=False,
            extracted={},
            errors=[f"Error: {str(e)}"]
        )


@router.get("/banks/list")
async def get_banks(db: Session = Depends(get_db), tenant_id: str = Depends(get_current_tenant)):
    """Get list of unique bank names from patterns."""
    banks = db.query(PatternRule.source)\
        .filter(PatternRule.tenant_id == tenant_id)\
        .filter(PatternRule.source.isnot(None))\
        .filter(PatternRule.is_active == True)\
        .distinct()\
        .order_by(PatternRule.source)\
        .all()
    
    return {"banks": [b[0] for b in banks]}
