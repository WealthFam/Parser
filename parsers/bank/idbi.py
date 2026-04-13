from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class IdbiSmsParser(BaseSmsParser):
    """
    Parser for IDBI Bank SMS Alerts.
    """
    name = "IDBI"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Mini Statement
            TransactionPattern(
                regex=re.compile(r"(?i)(Dr|Cr)\.?\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d/-]+)", re.IGNORECASE),
                confidence=0.8, # Looser since it's mini stmt
                txn_type="DEBIT", # Default, will be adjusted in parse_with_confidence
                field_map={"type": 1, "amount": 2, "date": 3}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(debited|credited).*?A/c\s*([\d*xX]*\d+)\s*on\s*([\d/-]+).*?(?:Avl\s*Bal.*?INR\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "type": 2, "mask": 3, "date": 4, "balance": 5}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if tx.type and ("cr" in tx.type.lower() or "credited" in tx.type.lower()):
                tx.type = "CREDIT"
            else:
                tx.type = "DEBIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "idbi" in sender.lower() or "idbi" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            date_str = date_str.replace("/", "-") if date_str else None
            if len(date_str) == 8: # DD-MM-YY
                fmt = "%d-%m-%y"
            elif len(date_str) == 10: # DD-MM-YYYY
                fmt = "%d-%m-%Y"
            else:
                fmt = "%d-%m-%Y"
            txn_date = datetime.strptime(date_str, fmt) if date_str else timezone.utcnow()
        except:
            txn_date = timezone.utcnow()

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient) if recipient and recipient != "Unknown" else None,
            account_mask=account_mask,
            date=txn_date,
            type=type_str.replace("CREDITED", "CREDIT").replace("DEBITED", "DEBIT"),
            raw_message=raw,
            ref_id=ref_id,
            balance=balance,
            source="SMS"
        )

class IdbiEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "idbibank" in (sender or "").lower() or "idbi" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return IdbiSmsParser().parse(body)
