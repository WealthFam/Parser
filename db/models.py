from sqlalchemy import Column, String, Integer, DateTime, Boolean, JSON
from parser.db.database import Base
import uuid
from parser.core import timezone
from parser.core.timezone import UTCDateTime

def generate_uuid():
    return str(uuid.uuid4())

class RequestLog(Base):
    __tablename__ = "request_logs"

    id = Column(String, primary_key=True, default=generate_uuid)
    tenant_id = Column(String, index=True, nullable=False)
    created_at = Column(UTCDateTime, default=timezone.utcnow)
    source = Column(String, nullable=False) # SMS, EMAIL, FILE
    input_hash = Column(String, index=True) # For idempotency
    input_payload = Column(JSON, nullable=True)
    output_payload = Column(JSON, nullable=True)
    status = Column(String) # success, duplicate, failed
    parser_steps = Column(JSON, nullable=True) # Trace of execution

class FileParsingConfig(Base):
    __tablename__ = "file_parsing_configs"

    fingerprint = Column(String, primary_key=True) # e.g. "HDFCBANK-1234"
    tenant_id = Column(String, index=True, nullable=False)
    format = Column(String, default="EXCEL")
    header_row_index = Column(Integer, default=0)
    columns_json = Column(JSON, nullable=False) # {"date": "Transaction Date", ...}
    created_at = Column(UTCDateTime, default=timezone.utcnow)
    updated_at = Column(UTCDateTime, default=timezone.utcnow, onupdate=timezone.utcnow)

class AIConfig(Base):
    __tablename__ = "ai_configs"

    id = Column(String, primary_key=True, default="default")
    tenant_id = Column(String, index=True, nullable=False)
    provider = Column(String, default="gemini")
    api_key_enc = Column(String, nullable=True) # Encrypted or just stored if internal
    model_name = Column(String, default="gemini-1.5-flash")
    is_enabled = Column(Boolean, default=True)
    prompts_json = Column(JSON, nullable=True)

class PatternRule(Base):
    __tablename__ = "pattern_rules"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    tenant_id = Column(String, index=True, nullable=False)
    source = Column(String, nullable=False)  # SMS, EMAIL, or bank name
    regex_pattern = Column(String, nullable=False)
    mapping_json = Column(JSON, nullable=False)
    date_format = Column(String, nullable=True) # e.g. "%d-%b-%y"
    is_active = Column(Boolean, default=True)
    is_ai_generated = Column(Boolean, default=False)
    confidence = Column(JSON, nullable=True)  # AI's confidence in this pattern
    created_at = Column(UTCDateTime, default=timezone.utcnow)

class MerchantAlias(Base):
    __tablename__ = "merchant_aliases"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    tenant_id = Column(String, index=True, nullable=False)
    pattern = Column(String, nullable=False, unique=True) # The raw string to match (e.g. "BUNDL TECHNOLOGIES")
    alias = Column(String, nullable=False) # The clean name (e.g. "Swiggy")
    created_at = Column(UTCDateTime, default=timezone.utcnow)

class AICallCache(Base):
    __tablename__ = "ai_call_cache"

    id = Column(String, primary_key=True, default=generate_uuid)
    tenant_id = Column(String, index=True, nullable=False)
    content_hash = Column(String, index=True, nullable=False)
    source = Column(String, nullable=False)
    response_json = Column(JSON, nullable=False)
    created_at = Column(UTCDateTime, default=timezone.utcnow)
