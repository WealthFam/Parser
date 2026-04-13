from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class IndianBankSmsParser(BaseSmsParser):
    """
    Parser for Indian Bank SMS Alerts.
    """
    name = "IndianBank"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit Alt
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*([\d*xX]*\d+)\s*credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?(?:by|from)\s*(.*?)(?:\(IMPS\s*Ref\s*No[:\.\s]*(\w+)\))?.*?(?:Available\s*bal[:\.\s]*(?:INR|Rs\.?)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5, "balance": 6}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*is\s*(debited|credited).*?A/c\s*([\d*xX]*\d+).*?(?:for|by)\s*(.*?)\.\s*(?:Call|Avl)?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "type": 2, "mask": 3, "recipient": 4}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if tx.type and "credited" in tx.type.lower():
                tx.type = "CREDIT"
            else:
                tx.type = "DEBIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "indianbank" in sender.lower() or "indian bank" in message.lower() or "indian" in sender.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id=None, balance=None):
        try:
            txn_date = self._parse_date(date_str) if date_str else timezone.utcnow()
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

class IndianBankEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "indianbank" in (sender or "").lower() or "indian bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return IndianBankSmsParser().parse(body)
