from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class FederalBankSmsParser(BaseSmsParser):
    """
    Parser for Federal Bank SMS Alerts.
    """
    name = "Federal"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit Alt
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*is\s*credited\s*to\s*your\s*account\s*on\s*([\d-]+).*?(?:Available\s*balance:?\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"amount": 1, "date": 2, "balance": 3}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:debited\s*with|credited\s*to).*?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?(?:Available\s*balance:?\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "date": 2, "balance": 3}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if "credited" in content.lower():
                tx.type = "CREDIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "federal" in sender.lower() or "federal" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            date_str = date_str.replace("/", "-") if date_str else None
            txn_date = self._parse_date(date_str) if date_str else (date_hint or timezone.utcnow())
        except:
            txn_date = timezone.utcnow()

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient) if recipient and recipient != "Unknown" else None,
            account_mask=account_mask,
            date=txn_date,
            type=type_str,
            raw_message=raw,
            ref_id=ref_id,
            balance=balance,
            source="SMS"
        )

class FederalBankEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "federalbank" in (sender or "").lower() or "federal bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return FederalBankSmsParser().parse(body)
