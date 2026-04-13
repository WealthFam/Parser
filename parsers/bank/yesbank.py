from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class YesBankSmsParser(BaseSmsParser):
    """
    Parser for Yes Bank SMS Alerts.
    """
    name = "YesBank"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # UPI
            TransactionPattern(
                regex=re.compile(r"(?i)UPI/(?:P2M|P2A)/([^/]+)/([^/]+)/.*?/(?:YES\s*BANK\s*LIMITED\s*YBS|YES\s*BANK\s*LIMITED|YBS)?\s*([\d,]+\.?\d*)", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"ref_id": 1, "recipient": 2, "amount": 3}
            ),
            # POS
            TransactionPattern(
                regex=re.compile(r"(?i)POS/(.*?)/(?:.*?/)?(\d{6})/.*?/\d+\s*([\d,]+\.?\d*)", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"recipient": 1, "date": 2, "amount": 3}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "yes" in sender.lower() or "yesbank" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, type_str, raw, ref_id=None, date_str=None, date_hint=None):
        txn_date = date_hint or timezone.utcnow()
        if date_str:
            # Try some common formats
            for fmt in ["%d%m%y", "%m%d%y"]:
                try:
                    txn_date = datetime.strptime(date_str, fmt)
                    break
                except: continue

        return ParsedTransaction(
            amount=amount,
            recipient=RecipientParser.extract(recipient),
            account_mask=None,
            date=txn_date,
            type=type_str,
            raw_message=raw,
            ref_id=ref_id,
            source="SMS"
        )

class YesBankEmailParser(BaseEmailParser):
    def can_handle(self, subject: str, body: str, sender: Optional[str] = None) -> bool:
        return "yesbank" in (sender or "").lower() or "yes bank" in subject.lower()

    def parse(self, subject: str, body: str, sender: Optional[str] = None) -> Optional[ParsedTransaction]:
        return YesBankSmsParser().parse(body)
