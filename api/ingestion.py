from fastapi import APIRouter, Depends, HTTPException, Header, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel
import json
import hashlib

from parser.db.database import get_db
from parser.core.auth import get_current_tenant
from parser.core.pipeline import IngestionPipeline
from parser.schemas.transaction import IngestionResult, ParsedItem, TransactionMeta
from parser.parsers.bank.hdfc import HdfcSmsParser, HdfcEmailParser
from parser.parsers.bank.icici import IciciSmsParser, IciciEmailParser
from parser.parsers.bank.sbi import SbiSmsParser, SbiEmailParser
from parser.parsers.bank.axis import AxisSmsParser, AxisEmailParser
from parser.parsers.bank.kotak import KotakSmsParser, KotakEmailParser
from parser.parsers.bank.indusind import IndusIndSmsParser, IndusIndEmailParser
from parser.parsers.bank.yesbank import YesBankSmsParser, YesBankEmailParser
from parser.parsers.bank.pnb import PnbSmsParser, PnbEmailParser
from parser.parsers.bank.bob import BobSmsParser, BobEmailParser
from parser.parsers.bank.canara import CanaraSmsParser, CanaraEmailParser
from parser.parsers.bank.unionbank import UnionBankSmsParser, UnionBankEmailParser
from parser.parsers.bank.idfc import IdfcSmsParser, IdfcEmailParser
from parser.parsers.bank.rbl import RblSmsParser, RblEmailParser
from parser.parsers.bank.federal import FederalBankSmsParser, FederalBankEmailParser
from parser.parsers.bank.idbi import IdbiSmsParser, IdbiEmailParser
from parser.parsers.bank.indianbank import IndianBankSmsParser, IndianBankEmailParser
from parser.parsers.bank.ausfb import AuSfbSmsParser, AuSfbEmailParser
from parser.parsers.bank.bandhan import BandhanSmsParser, BandhanEmailParser
from parser.parsers.bank.centralbank import CentralBankSmsParser, CentralBankEmailParser
from parser.parsers.bank.boi import BoiSmsParser, BoiEmailParser
from parser.parsers.bank.slice import SliceSmsParser
from parser.parsers.bank.government_schemes import EpfoSmsParser, PpfSmsParser, NpsSmsParser, EpfoEmailParser, PpfEmailParser, NpsEmailParser
from parser.parsers.bank.generic import GenericSmsParser
from parser.parsers.registry import ParserRegistry
from parser.parsers.file.universal_parser import UniversalParser
from parser.parsers.cas.cas_parser import CasParser
from parser.db.models import FileParsingConfig

# Register SMS Parsers
ParserRegistry.register_sms(HdfcSmsParser())
ParserRegistry.register_sms(IciciSmsParser())
ParserRegistry.register_sms(SbiSmsParser())
ParserRegistry.register_sms(AxisSmsParser())
ParserRegistry.register_sms(KotakSmsParser())
ParserRegistry.register_sms(IndusIndSmsParser())
ParserRegistry.register_sms(YesBankSmsParser())
ParserRegistry.register_sms(PnbSmsParser())
ParserRegistry.register_sms(BobSmsParser())
ParserRegistry.register_sms(CanaraSmsParser())
ParserRegistry.register_sms(UnionBankSmsParser())
ParserRegistry.register_sms(IdfcSmsParser())
ParserRegistry.register_sms(RblSmsParser())
ParserRegistry.register_sms(FederalBankSmsParser())
ParserRegistry.register_sms(IdbiSmsParser())
ParserRegistry.register_sms(IndianBankSmsParser())
ParserRegistry.register_sms(AuSfbSmsParser())
ParserRegistry.register_sms(BandhanSmsParser())
ParserRegistry.register_sms(CentralBankSmsParser())
ParserRegistry.register_sms(BoiSmsParser())
ParserRegistry.register_sms(SliceSmsParser())
# Government Schemes
ParserRegistry.register_sms(EpfoSmsParser())
ParserRegistry.register_sms(PpfSmsParser())
ParserRegistry.register_sms(NpsSmsParser())
# Generic fallback
ParserRegistry.register_sms(GenericSmsParser())

# Register Email Parsers
ParserRegistry.register_email(HdfcEmailParser())
ParserRegistry.register_email(IciciEmailParser())
ParserRegistry.register_email(SbiEmailParser())
ParserRegistry.register_email(AxisEmailParser())
ParserRegistry.register_email(KotakEmailParser())
ParserRegistry.register_email(IndusIndEmailParser())
ParserRegistry.register_email(YesBankEmailParser())
ParserRegistry.register_email(PnbEmailParser())
ParserRegistry.register_email(BobEmailParser())
ParserRegistry.register_email(CanaraEmailParser())
ParserRegistry.register_email(UnionBankEmailParser())
ParserRegistry.register_email(IdfcEmailParser())
ParserRegistry.register_email(RblEmailParser())
ParserRegistry.register_email(FederalBankEmailParser())
ParserRegistry.register_email(IdbiEmailParser())
ParserRegistry.register_email(IndianBankEmailParser())
ParserRegistry.register_email(AuSfbEmailParser())
ParserRegistry.register_email(BandhanEmailParser())
ParserRegistry.register_email(CentralBankEmailParser())
ParserRegistry.register_email(BoiEmailParser())
# Government Schemes
ParserRegistry.register_email(EpfoEmailParser())
ParserRegistry.register_email(PpfEmailParser())
ParserRegistry.register_email(NpsEmailParser())

router = APIRouter(prefix="/v1/ingest", tags=["Ingestion"])

class SmsIngestRequest(BaseModel):
    sender: str
    body: str
    received_at: Optional[str] = None

class EmailIngestRequest(BaseModel):
    subject: str
    body_text: str
    sender: str
    received_at: Optional[str] = None

@router.post("/sms", response_model=IngestionResult)
def ingest_sms(
    payload: SmsIngestRequest,
    x_api_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    pipeline = IngestionPipeline(db, tenant_id=tenant_id)
    result = pipeline.run(payload.body, "SMS", sender=payload.sender, date_hint=payload.received_at)
    return result

@router.post("/email", response_model=IngestionResult)
def ingest_email(
    payload: EmailIngestRequest,
    x_api_key: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    pipeline = IngestionPipeline(db, tenant_id=tenant_id)
    result = pipeline.run(payload.body_text, "EMAIL", sender=payload.sender, subject=payload.subject, date_hint=payload.received_at)
    return result

@router.post("/file", response_model=IngestionResult)
async def ingest_file(
    file: UploadFile = File(...),
    account_fingerprint: Optional[str] = Form(None),
    mapping_override: Optional[str] = Form(None), 
    header_row_index: Optional[int] = Form(None),
    password: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    
    
    content = await file.read()
    file_hash = hashlib.sha256(content).hexdigest()
    filename = file.filename
    from parser.db.models import RequestLog
    
    mapping = {}
    header_idx = header_row_index or 0
    
    if account_fingerprint:
        saved = db.query(FileParsingConfig).filter(
            FileParsingConfig.fingerprint == account_fingerprint,
            FileParsingConfig.tenant_id == tenant_id
        ).first()
        if saved:
            mapping = saved.columns_json
            if header_row_index is None:
                header_idx = saved.header_row_index
            
    if mapping_override:
        try:
             mapping = json.loads(mapping_override)
        except: pass

    if not mapping:
        try:
            analysis = UniversalParser.analyze(content, filename)
            db.add(RequestLog(
                tenant_id=tenant_id,
                input_hash=file_hash, 
                source="FILE", 
                input_payload={"filename": filename, "op": "analyze"},
                status="success",
                output_payload={"status": "analysis_required", "analysis": analysis}
            ))
            db.commit()
            return IngestionResult(status="analysis_required", results=[], logs=["No mapping found. Analysis: " + json.dumps(analysis, default=str)])
        except Exception as e:
            db.add(RequestLog(
                tenant_id=tenant_id,
                input_hash=file_hash, 
                source="FILE", 
                status="failed",
                input_payload={"filename": filename, "op": "analyze"},
                output_payload={"error": str(e)}
            ))
            db.commit()
            return IngestionResult(status="failed", results=[], logs=[str(e)])

    try:
        raw_txns, skipped_logs = UniversalParser.parse(content, filename, mapping, header_idx, password=password)
        pipeline = IngestionPipeline(db, tenant_id=tenant_id)
        results = []
        
        for t_dict in raw_txns:
            t = pipeline._convert_to_schema_txn(t_dict)
            item = ParsedItem(
                status="extracted",
                transaction=t,
                metadata=TransactionMeta(
                    confidence=1.0, 
                    parser_used="UniversalParser", 
                    source_original="FILE",
                    units=t_dict.get("units"),
                    nav=t_dict.get("nav")
                )
            )
            results.append(item)

        # Log once for the entire file
        output = IngestionResult(
            status="success" if results else "failed", 
            results=results, 
            logs=skipped_logs
        )
        
        db.add(RequestLog(
            tenant_id=tenant_id,
            input_hash=file_hash,
            source="FILE",
            status=output.status,
            input_payload={"filename": filename, "op": "parse"},
            output_payload=output.model_dump(mode='json')
        ))
        
        db.commit()
        return output
    except Exception as e:
        db.add(RequestLog(
            tenant_id=tenant_id,
            input_hash=file_hash,
            source="FILE",
            status="failed",
            input_payload={"filename": filename, "op": "parse"},
            output_payload={"error": str(e)}
        ))
        db.commit()
        return IngestionResult(status="failed", results=[], logs=[str(e)])

@router.post("/cas", response_model=IngestionResult)
async def ingest_cas(
    file: UploadFile = File(...),
    password: Optional[str] = Form(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(get_current_tenant)
):
    content = await file.read()
    file_hash = hashlib.sha256(content).hexdigest()
    from parser.db.models import RequestLog
    
    try:
        data = CasParser.parse(content, password)
        pipeline = IngestionPipeline(db, tenant_id=tenant_id)
        results = []
        
        for t_dict in data:
            t = pipeline._convert_to_schema_txn(t_dict)
            item = ParsedItem(
                status="extracted",
                transaction=t,
                metadata=TransactionMeta(
                    confidence=1.0, 
                    parser_used="CasParser", 
                    source_original="CAS",
                    units=t_dict.get("units"),
                    nav=t_dict.get("nav"),
                    amfi=t_dict.get("amfi"),
                    isin=t_dict.get("isin"),
                    is_synthesized=t_dict.get("is_synthesized", False)
                )
            )
            results.append(item)
            
        output = IngestionResult(
            status="success" if results else "failed",
            results=results,
            logs=[]
        )
        
        db.add(RequestLog(
            tenant_id=tenant_id,
            input_hash=file_hash,
            source="CAS",
            status=output.status,
            input_payload={"filename": file.filename},
            output_payload=output.model_dump(mode='json')
        ))
        
        db.commit()
        return output
    except Exception as e:
        db.add(RequestLog(
            tenant_id=tenant_id,
            input_hash=file_hash,
            source="CAS",
            status="failed",
            output_payload={"error": str(e)}
        ))
        db.commit()
        # Still return 400 for errors like wrong password in CAS
        raise HTTPException(status_code=400, detail=f"CAS Parse Failed: {str(e)}")
