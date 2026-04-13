from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class IndusIndSmsParser(BaseSmsParser):
    """
    Parser for IndusInd Bank SMS Alerts.
    """
    name = "IndusInd"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit
            TransactionPattern(
                regex=re.compile(r"(?i)credited\s*your\s*account\s*\(No\.\s*([\d*xX]+)\)\s*with\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*for\s*(.*?)\.\s*(?:The\s*current\s*balance.*?is\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "recipient": 3, "balance": 4}
            ),
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*debited\s*from\s*your\s*a/c\s*([\d*xX]+)\s*on\s*([\d-]+).*?(?:Avl\s*Bal.*?INR\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "balance": 4}
            ),
            # Card Alert
            TransactionPattern(
                regex=re.compile(r"(?i)Transaction\s*Alert\s*on\s*your\s*card\s*([\d*xX]+).*?INR\s*([\d,]+\.?\d*)\s*at\s*(.*?)\s*on\s*([\d-]+)", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"mask": 1, "amount": 2, "recipient": 3, "date": 4}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "indus" in sender.lower() or "indus" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, balance=None, date_hint=None):
        txn_date = date_hint or timezone.utcnow()
        if date_str:
            for fmt in ["%d-%m-%y", "%d-%m-%Y"]:
                try:
                    txn_date = datetime.strptime(date_str, fmt)
                    break
                except: continue

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient) if recipient != "Unknown" else None,
            account_mask=account_mask,
            date=txn_date,
            type=type_str,
            raw_message=raw,
            balance=balance,
            source="SMS"
        )

class IndusIndEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "indusind" in (sender or "").lower() or "indusind" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        # Implementation similar to SMS but with cleaner content
        return IndusIndSmsParser().parse(body)
