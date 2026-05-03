from parser.core import timezone
from datetime import datetime
from decimal import Decimal
from rapidfuzz import fuzz
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
from pydantic import BaseModel, model_validator

class ParsedTransaction(BaseModel):
    """Parsed transaction from SMS/Email - matches backend structure"""
    amount: Decimal
    date: datetime
    description: str
    type: str  # DEBIT or CREDIT
    account_mask: Optional[str] = None
    recipient: Optional[str] = None
    merchant: Optional[str] = None
    category: Optional[str] = None
    ref_id: Optional[str] = None
    balance: Optional[Decimal] = None
    credit_limit: Optional[Decimal] = None
    raw_message: str
    source: str = "SMS"  # SMS, EMAIL, etc.
    is_ai_parsed: bool = False
    confidence: float = 1.0

    @model_validator(mode='after')
    def generate_fallback_ref(self) -> 'ParsedTransaction':
        if not self.ref_id:
            date_str = self.date.strftime("%Y%m%d%H%M%S")
            mask = self.account_mask or "XXXX"
            amt_str = f"{self.amount:.2f}"
            self.ref_id = f"GEN-{date_str}-{mask}-{amt_str}"
        return self

class TransactionPattern(BaseModel):
    """Definition for a single regex pattern with its metadata."""
    regex: Any # re.Pattern
    confidence: float
    txn_type: str # DEBIT, CREDIT
    # Mapping of regex group names or indices to ParsedTransaction fields
    # e.g. {"amount": 1, "date": 2, "recipient": 4, "ref_id": 6}
    field_map: Dict[str, int]
    source: str = "SMS"

class BaseParser(ABC):
    name: str = "Generic"

    @abstractmethod
    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        """Legacy single-parse method."""
        pass

    def get_patterns(self) -> List[TransactionPattern]:
        """Return list of patterns supported by this parser."""
        return []

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        """Evaluate all patterns and return all matches with their confidence."""
        results = []
        clean_content = " ".join(content.split())
        
        for p in self.get_patterns():
            match = p.regex.search(clean_content)
            if match:
                try:
                    # Extract fields based on field_map
                    fields = {}
                    for field_name, group_idx in p.field_map.items():
                        val = match.group(group_idx)
                        if val:
                            fields[field_name] = val.strip()

                    # Convert types
                    amount = Decimal(fields.get("amount", "0").replace(",", ""))
                    
                    # Date parsing logic could be parser-specific, but we'll provide a helper
                    date_val = self._parse_date(fields.get("date"), date_hint)
                    
                    # Create ParsedTransaction
                    results.append(ParsedTransaction(
                        amount=amount,
                        date=date_val,
                        description=f"{self.name}: {fields.get('recipient', 'Unknown')}",
                        type=p.txn_type,
                        account_mask=self._parse_mask(fields.get("mask")),
                        recipient=fields.get("recipient"),
                        merchant=fields.get("merchant") or fields.get("recipient"),
                        ref_id=fields.get("ref_id"),
                        balance=Decimal(fields.get("balance").replace(",", "")) if fields.get("balance") else None,
                        raw_message=content,
                        source=p.source,
                        confidence=p.confidence
                    ))
                except Exception as e:
                    # Log error? skip this match
                    continue
        
        return sorted(results, key=lambda x: x.confidence, reverse=True)

    def _parse_date(self, date_str: Optional[str], hint: Optional[datetime]) -> datetime:
        if not date_str:
            return hint or timezone.utcnow()
        
        # Standardize separators
        clean_date = date_str.replace("/", "-").replace(".", "-").replace(",", " ").strip()
        
        # 1. Try ISO-like with time (YYYY-MM-DD HH:MM:SS or variations)
        # Handle "2026-02-01:19:44:53" or "01-02-2026 19:44"
        try:
            # Replace first colon after date or a space with a standard separator if needed
            # We use dateutil-like heuristic if we had it, but sticking to standard formats
            dt_formats = [
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
                "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M",
                "%d-%m-%y %H:%M:%S", "%d-%m-%y %H:%M",
                "%d-%b-%y %H:%M:%S", "%d-%b-%y %H:%M",
            ]
            
            # Pre-processing for "2026-02-01:19:44:53"
            norm_date = clean_date
            if ":" in clean_date and " " not in clean_date:
                # If there's a colon but no space, the first colon might be the date/time separator
                parts = clean_date.split("-")
                if len(parts) == 3: # Likely starts with YYYY-MM-DD or DD-MM-YYYY
                    if ":" in parts[2]: # separator is in the 3rd part?
                         # e.g. "2026-02-01:19:44:53"
                         sub_parts = parts[2].split(":", 1)
                         norm_date = "-".join(parts[:2]) + "-" + sub_parts[0] + " " + sub_parts[1]
            
            for fmt in dt_formats:
                try:
                    dt = datetime.strptime(norm_date, fmt)
                    if dt.year < 100: dt = dt.replace(year=dt.year + 2000)
                    return dt
                except: continue
        except:
            pass

        # 2. Try Date-only formats
        formats = [
            "%d-%m-%Y", "%d-%m-%y",
            "%d-%b-%Y", "%d-%b-%y",
            "%Y-%m-%d",
            "%d%b%y", "%d%b%Y",
            "%d-%B-%Y"
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(clean_date, fmt)
                if dt.year < 100:
                    dt = dt.replace(year=dt.year + 2000)
                
                # If hint has time, preserve it? 
                # Actually, usually hinted time is "received_at", 
                # but if the SMS had a date, we want that date.
                # If the hint is also from today, we might want the hint's time.
                if hint and dt.date() == hint.date():
                    return dt.replace(hour=hint.hour, minute=hint.minute, second=hint.second)
                
                return dt
            except:
                continue
        
        return hint or timezone.utcnow()

    def _parse_mask(self, mask_str: Optional[str]) -> Optional[str]:
        if not mask_str:
            return None
        # Extract last 4 digits if possible
        digits = "".join(filter(str.isdigit, mask_str))
        if len(digits) >= 4:
            return digits[-4:]
        return digits or mask_str

class BaseSmsParser(BaseParser, ABC):
    @abstractmethod
    def can_handle(self, sender: str, message: str) -> bool:
        """Determine if this parser can handle the given SMS"""
        pass

class BaseEmailParser(BaseParser, ABC):
    @abstractmethod
    def can_handle(self, subject: str, body: str) -> bool:
        """Determine if this parser can handle the given Email"""
        pass
