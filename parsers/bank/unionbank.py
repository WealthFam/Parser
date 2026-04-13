from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class UnionBankSmsParser(BaseSmsParser):
    """
    Parser for Union Bank of India SMS Alerts.
    """
    name = "UnionBank"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # UPI
            TransactionPattern(
                regex=re.compile(r"(?i)UPI\s*(Debit|Credit)\s*(?:Rs\.?|INR)?\s*([\d,]+\.?\d*)\s*(?:from|to)\s*A/c\s*([\d*xX]*\d+)\s*(?:to|from)\s*VPA\s*(.*?)\.\s*(?:Ref[:\.\s]*(\w+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"type": 1, "amount": 2, "mask": 3, "recipient": 4, "ref_id": 5}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)?\s*([\d,]+\.?\d*)\s*(Debit|Credit)\s*(?:Rs\.?|INR)?\s*.*?(?:from|to|in)\s*a/c\s*([\d*xX]*\d+).*?(?:on|at)\s*([\d/-]+).*?(?:Ref[:\.\s]*(\w+))?.*?(?:Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "type": 2, "mask": 3, "date": 4, "ref_id": 5, "balance": 6}
            ),
            # Date-less Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(Debit|Credit)\s*(?:Rs\.?|INR)?\s*([\d,]+\.?\d*)\s*from\s*a/c\s*([\d*xX]*\d+).*?(?:Ref[:\.\s]*(\w+))?.*?(?:Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.85,
                txn_type="DEBIT",
                field_map={"type": 1, "amount": 2, "mask": 3, "ref_id": 4, "balance": 5}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if tx.type and "credit" in tx.type.lower():
                tx.type = "CREDIT"
            else:
                tx.type = "DEBIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "union" in sender.lower() or "unionbank" in sender.lower() or "union" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            # Handle both / and -
            date_str = date_str.replace("/", "-") if date_str else None
            txn_date = datetime.strptime(date_str, "%d-%m-%Y") if date_str else timezone.utcnow()
        except:
            txn_date = timezone.utcnow()

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient) if recipient and recipient != "Unknown" else None,
            account_mask=account_mask,
            date=txn_date,
            type=type_str.replace("CREDIT", "CREDIT").replace("DEBIT", "DEBIT"),
            raw_message=raw,
            ref_id=ref_id,
            balance=balance,
            source="SMS"
        )

class UnionBankEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "unionbankofindia" in (sender or "").lower() or "union bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return UnionBankSmsParser().parse(body)
