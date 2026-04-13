import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class AxisSmsParser(BaseSmsParser):
    """
    Parser for Axis Bank SMS Alerts.
    """
    name = "Axis"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Spent
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*spent\s*on\s*Axis\s*Bank\s*.*?(?:Card|A/c)\s*([xX]*\d+)\s*at\s*(.*?)\s*on\s*([\d-]+)(?:.*?[Rr]ef[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*([xX]*\d+)\s*is\s*debited\s*for\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+)\.\s*Info:\s*(.*?)(?:.*?[Rr]ef[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "axis" in sender.lower() or "axis" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class AxisEmailParser(BaseEmailParser):
    """
    Parser for Axis Bank Email Alerts.
    """
    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return "axis" in combined and any(k in combined for k in ["transaction", "spent", "debited", "alert", "upi"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        return AxisSmsParser().parse(content, date_hint)
