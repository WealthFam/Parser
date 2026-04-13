from parser.core import timezone
import re
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
from parser.parsers.base_compat import BaseSmsParser, ParsedTransaction, TransactionPattern

class SliceSmsParser(BaseSmsParser):
    """
    Parser for Slice SMS Alerts.
    """
    name = "Slice"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Debit: Rs. 349 sent from a/c xx3764 ...
            TransactionPattern(
                regex=re.compile(r"Rs\.?\s*(?P<amount>[\d,.]+)\s*sent\s+from\s+a\/c\s+xx(?P<account>\d+)\s+on\s+(?P<date>[\d-]+-[A-Za-z]+-\d+)\s+to\s+(?P<recipient>.+?)\s+\(UPI\s+Ref:\s*(?P<ref_id>\d+)\)", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Credit: Rs. 6,000 received in slice A/c xx3764 ...
            TransactionPattern(
                regex=re.compile(r"Rs\.?\s*(?P<amount>[\d,.]+)\s*received\s+in\s+slice\s+A\/c\s+xx(?P<account>\d+)\s+on\s+(?P<date>[\d-]+-[A-Za-z]+-\d+)\s+from\s+(?P<recipient>.+?)\s+via\s+UPI\s+\(Ref\s+ID:\s*(?P<ref_id>\d+)\)(?:.*Avl\.\s*Bal\.\s*Rs\.?\s*(?P<balance>[\d,.]+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5, "balance": 6}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "slice" in sender.lower() or "- slice" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        for pattern in self.get_patterns():
            match = pattern.regex.search(content)
            if match:
                amount_str = match.group("amount").replace(",", "")
                date_str = match.group("date")
                
                txn_date = self._parse_date(date_str) or date_hint or timezone.utcnow()
                
                # Check for balance group presence
                balance = None
                try:
                    if match.group("balance"):
                        balance = Decimal(match.group("balance").replace(",", ""))
                except IndexError:
                    pass

                return ParsedTransaction(
                    amount=Decimal(amount_str),
                    date=txn_date,
                    description=f"Slice: {match.group('recipient')}",
                    type=pattern.txn_type,
                    account_mask=match.group("account"),
                    recipient=match.group("recipient"),
                    ref_id=match.group("ref_id"),
                    balance=balance,
                    raw_message=content,
                    source="SMS",
                    confidence=1.0
                )
        return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        formats = ["%d-%b-%y", "%d-%b-%Y", "%d-%m-%y", "%d-%m-%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
