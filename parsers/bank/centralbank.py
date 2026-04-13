import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class CentralBankSmsParser(BaseSmsParser):
    """
    Parser for Central Bank of India SMS Alerts.
    """
    name = "CentralBank"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*(?:.*?|x*|X*)(\d+)\s*is\s*credited\s*by\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*(?:Total\s*Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "balance": 3}
            ),
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*(?:deducted|debited)\s*from\s*(?:your)?\s*(\w+)?\s*(.*?)(?:\.\s*Avl\s*Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "recipient": 3, "balance": 4}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "central" in sender.lower() or "cbobnk" in sender.lower() or "central bank" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class CentralBankEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "centralbank" in (sender or "").lower() or "central bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return CentralBankSmsParser().parse(body)
