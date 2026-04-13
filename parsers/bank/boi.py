import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class BoiSmsParser(BaseSmsParser):
    """
    Parser for Bank of India (BoI) SMS Alerts.
    """
    name = "BoI"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*([xX]*\d+)\s*credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?(?:by|from)\s*(.*?)(?:\(IMPS\s*Ref\s*No[:\.\s]*(\w+)\))?.*?(?:Available\s*bal[:\.\s]*(?:INR|Rs\.?)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5, "balance": 6}
            ),
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*is\s*debited\s*from\s*A/c\s*(?:.*?|x*|X*)(\d+)\s*for\s*(.*?)(?:\.\s*Call|$)", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "boind" in sender.lower() or "bankofindia" in sender.lower() or "bank of india" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class BoiEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "boi" in (sender or "").lower() or "bank of india" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return BoiSmsParser().parse(body)
