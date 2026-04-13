from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class AuSfbSmsParser(BaseSmsParser):
    """
    Parser for AU Small Finance Bank SMS Alerts.
    """
    name = "AU SFB"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # UPI
            TransactionPattern(
                regex=re.compile(r"(?i)UPI\s*Txn\s*of\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*(?:to|from)\s*(.*?)\s*on\s*([\d-]+).*?A/c\s*([\d*xX]*\d+).*?(?:Ref\s*No[:\.\s]*(\d+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "recipient": 2, "date": 3, "mask": 4, "ref_id": 5}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*(debited|credited)\s*(?:from|to)\s*A/c\s*([\d*xX]*\d+)\s*on\s*([\d-]+).*?(?:for|from)\s*(.*?)\.\s*(?:Avail\s*Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "type": 2, "mask": 3, "date": 4, "recipient": 5, "balance": 6}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if "credited" in content.lower() or " from " in content.lower():
                tx.type = "CREDIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "aubank" in sender.lower() or "aubank" in message.lower() or "au sfb" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            txn_date = datetime.strptime(date_str, "%d-%m-%y") if date_str else timezone.utcnow()
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

class AuSfbEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "aubank" in (sender or "").lower() or "au small finance" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return AuSfbSmsParser().parse(body)
