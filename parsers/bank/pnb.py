from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class PnbSmsParser(BaseSmsParser):
    """
    Parser for Punjab National Bank (PNB) SMS Alerts.
    """
    name = "PNB"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # UPI
            TransactionPattern(
                regex=re.compile(r"(?i)A/C\s*([\d*xX]+)\s*(?:debited|credited)\s*by\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*via\s*UPI\s*(?:to|from)\s*(.*?)\s*on\s*([\d-]+).*?(?:Ref\s*No\.?\s*(\w+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT", # Type logic in field_map or post-process? Let's use two patterns for clarity if type varies.
                field_map={"mask": 1, "amount": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)A/C\s*([\d*xX]+)\s*has\s*been\s*debited\s*by\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?(?:Ref\s*No\.?\s*(\w+))?.*?(?:Avl\s*Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "ref_id": 4, "balance": 5}
            ),
            # Credit
            TransactionPattern(
                regex=re.compile(r"(?i)A/C\s*([\d*xX]+)\s*has\s*been\s*credited\s*by\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?(?:Ref\s*No\.?\s*(\w+))?.*?(?:Avl\s*Bal.*?Rs\.?\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "ref_id": 4, "balance": 5}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        # Type adjustment for UPI if needed
        for tx in results:
            if "credited" in content.lower():
                tx.type = "CREDIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "pnb" in sender.lower() or "punjab" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            txn_date = datetime.strptime(date_str, "%d-%m-%Y")
        except:
            txn_date = timezone.utcnow()

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient) if recipient != "Unknown" else None,
            account_mask=account_mask,
            date=txn_date,
            type=type_str,
            raw_message=raw,
            ref_id=ref_id,
            balance=balance,
            source="SMS"
        )

class PnbEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "pnb" in (sender or "").lower() or "punjab national bank" in (sender or "").lower() or "pnb" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return PnbSmsParser().parse(body)
