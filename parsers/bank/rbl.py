from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class RblSmsParser(BaseSmsParser):
    """
    Parser for RBL Bank SMS Alerts.
    """
    name = "RBL"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Credit Alt (Ref is NEFT/...)
            TransactionPattern(
                regex=re.compile(r"(?i)account\s*([\d*xX]*\d+)\s*is\s*credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+).*?ref\s*(.*?)\.\s*(?:Your\s*available\s*balance\s*is\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "ref_id": 4, "balance": 5}
            ),
            # Generic
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(debited|credited|spent).*?account\s*([\d*xX]*\d+)\s*on\s*([\d-]+).*?(?:ref|Ref)[:\.\s]*(\w+/\w+/\w+|\w+)?", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "type": 2, "mask": 3, "date": 4, "ref_id": 5}
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        for tx in results:
            if tx.type and "credited" in tx.type.lower():
                tx.type = "CREDIT"
            elif tx.type and ("debited" in tx.type.lower() or "spent" in tx.type.lower()):
                tx.type = "DEBIT"
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "rbl" in sender.lower() or "rbl" in message.lower()

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

class RblEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "rblbank" in (sender or "").lower() or "rbl bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return RblSmsParser().parse(body)
