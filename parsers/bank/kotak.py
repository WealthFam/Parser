import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class KotakSmsParser(BaseSmsParser):
    """
    Parser for Kotak Bank SMS Alerts.
    """
    name = "Kotak"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*debited\s*from\s*A/c\s*(?:.*?|x*|X*)(\d+)\s*on\s*([\d-]+)\s*to\s*(.*?)(?:\.\s*Ref[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Spent
            TransactionPattern(
                regex=re.compile(r"(?i)Spent\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*Kotak\s*.*?(?:Card|A/c)\s*([xX]*\d+)\s*at\s*(.*?)\s*on\s*([\d-]+)(?:.*?[Rr]ef[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "kotak" in sender.lower() or "kotak" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class KotakEmailParser(BaseEmailParser):
    """
    Parser for Kotak Bank Email Alerts.
    """
    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return "kotak" in combined and any(k in combined for k in ["transaction", "spent", "debited", "alert", "upi"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        # Implementation similar to BaseEmailParser but using Kotak logic if needed specifically
        return KotakSmsParser().parse(content, date_hint)
