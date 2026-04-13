from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from enum import Enum

class TransactionType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"

class AccountInfo(BaseModel):
    mask: Optional[str] = None
    provider: Optional[str] = None # Bank Name

class MerchantInfo(BaseModel):
    raw: Optional[str] = None
    cleaned: Optional[str] = None
    category_hint: Optional[str] = None 

class Transaction(BaseModel):
    amount: Decimal
    type: TransactionType
    date: datetime
    currency: str = "INR"
    account: AccountInfo
    merchant: MerchantInfo
    description: Optional[str] = None
    ref_id: Optional[str] = None
    balance: Optional[Decimal] = None
    credit_limit: Optional[Decimal] = None
    category: Optional[str] = None
    raw_message: Optional[str] = None
    recipient: Optional[str] = None
    confidence: float = 0.0
    
class TransactionMeta(BaseModel):
    confidence: float
    parser_used: str
    source_original: str
    units: Optional[float] = None
    nav: Optional[float] = None
    amfi: Optional[str] = None
    isin: Optional[str] = None
    is_synthesized: bool = False

class ParsedItem(BaseModel):
    status: str # extracted, partial, ignored
    transaction: Optional[Transaction] = None
    metadata: TransactionMeta

class IngestionResult(BaseModel):
    status: str
    results: List[ParsedItem]
    logs: Optional[List[str]] = []
