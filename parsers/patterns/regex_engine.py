from parser.core import timezone
from typing import Optional, List
from rapidfuzz import fuzz
from sqlalchemy.orm import Session
import re
from datetime import datetime
from decimal import Decimal
from parser.db.models import PatternRule
from parser.schemas.transaction import Transaction, TransactionType, AccountInfo, MerchantInfo

class PatternParser:
    def __init__(self, db: Session, source: str, tenant_id: str):
        self.db = db
        self.source = source
        self.tenant_id = tenant_id
        self.rules = self._load_rules()

    def _load_rules(self) -> List[PatternRule]:
        return self.db.query(PatternRule).filter(
            PatternRule.tenant_id == self.tenant_id,
            PatternRule.source == self.source,
            PatternRule.is_active == True
        ).all()

    def parse(self, content: str) -> Optional[Transaction]:
        for rule in self.rules:
            try:
                match = re.search(rule.regex_pattern, content, re.IGNORECASE)
                if not match: continue

                # Mapping Logic
                # mapping_json expected format:
                # { "amount": 1, "date": 2, "merchant": 3, "account": 4, "type": "DEBIT" } 
                # OR named groups support if regex uses (?P<name>...)
                
                mapping = rule.mapping_json or {}
                groups = match.groups()
                group_dict = match.groupdict()

                def get_val(key):
                    # 1. Try named group
                    if key in group_dict: return group_dict[key]
                    # 2. Try index from mapping
                    idx = mapping.get(key)
                    if isinstance(idx, int) and 0 <= idx - 1 < len(groups):
                        return groups[idx - 1]
                    # 3. Try literal value in mapping (if not int)
                    if isinstance(idx, str) and not idx.isdigit():
                        return idx
                    return None

                # Extract
                amount_str = get_val("amount")
                date_str = get_val("date")
                merchant_str = get_val("merchant")
                account_str = get_val("account")
                type_str = get_val("type") or "DEBIT" # Default to DEBIT if not found

                if not amount_str: continue

                # Normalize
                amount = self._clean_amount(str(amount_str))
                
                txn_date = timezone.utcnow()
                if date_str:
                    txn_date = self._parse_date(str(date_str), getattr(rule, 'date_format', None)) or timezone.utcnow()

                return Transaction(
                    amount=amount,
                    type=TransactionType(type_str.upper()),
                    date=txn_date,
                    account=AccountInfo(mask=account_str, provider="Pattern Match"),
                    merchant=MerchantInfo(raw=merchant_str, cleaned=merchant_str),
                    description=merchant_str,
                    recipient=merchant_str,
                    raw_message=content,
                    confidence=float(rule.confidence) if rule.confidence else 0.95
                )

            except Exception as e:
                print(f"Pattern Rule {rule.id} failed: {e}")
                continue
        
        return None

    def _clean_amount(self, val: str) -> Decimal:
        clean = re.sub(r'[^\d.]', '', val.replace(",", ""))
        try: return Decimal(clean)
        except: return Decimal(0)

    def _parse_date(self, val: str, custom_fmt: Optional[str] = None) -> Optional[datetime]:
        if custom_fmt:
            try:
                return datetime.strptime(val, custom_fmt)
            except:
                pass

        formats = ["%d-%m-%y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d%b%y", "%d%b%Y", "%d-%b-%y", "%d-%b-%Y"]
        for fmt in formats:
            try: return datetime.strptime(val, fmt)
            except: pass
        return None
