import re
from typing import Optional, List
from datetime import datetime
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class EpfoSmsParser(BaseSmsParser):
    """
    Parser for EPFO (Employees' Provident Fund Organization) SMS Alerts.
    Sender: EPFOHO, EPFINDIA
    """
    name = "EPFO"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # EPF Credit (Contribution)
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Your\s+EPF\s+A/c|PF\s+Account)\s*(?:No\.?)?\s*([\d/]+)\s*(?:has\s+been\s+)?credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:on|dated)\s*([\d\-/]+).*?(?:for|towards)\s*(.*?)(?:\.|$)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4}
            ),
            # EPF Interest Credit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:EPF|PF)\s*A/c\s*([\d/]+)\s*credited\s*with\s*interest\s*(?:of\s+)?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:for|FY)\s*([\d\-/]+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3}
            ),
            # EPF Withdrawal/Claim
            TransactionPattern(
                regex=re.compile(r"(?i)(?:EPF|PF)\s*(?:withdrawal|claim)\s*(?:of\s+)?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*from\s*A/c\s*([\d/]+)\s*(?:has\s+been\s+)?(?:credited|processed)\s*on\s*([\d\-/]+)", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        sender_lower = sender.lower()
        message_lower = message.lower()
        return any(k in sender_lower for k in ["epfo", "epf"]) or ("epf" in message_lower or "pf account" in message_lower)

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class PpfSmsParser(BaseSmsParser):
    """
    Parser for PPF (Public Provident Fund) SMS Alerts.
    Typically sent by the bank hosting the PPF account.
    """
    name = "PPF"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # PPF Deposit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:PPF|Public\s+Provident\s+Fund)\s*A/c\s*([xX]*\d+)\s*(?:has\s+been\s+)?credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d\-/]+)(?:.*?Deposit)?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3}
            ),
            # PPF Interest Credit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:PPF|Public\s+Provident\s+Fund)\s*A/c\s*([xX]*\d+)\s*credited\s*with\s*(?:interest\s+of\s+)?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:for|on)\s*([\d\-/]+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3}
            ),
            # PPF Maturity/Withdrawal
            TransactionPattern(
                regex=re.compile(r"(?i)(?:PPF|Public\s+Provident\s+Fund)\s*(?:maturity|withdrawal)\s*(?:of\s+)?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*from\s*A/c\s*([xX]*\d+)\s*(?:credited|processed)\s*on\s*([\d\-/]+)", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        combined = (sender + " " + message).lower()
        return "ppf" in combined or "public provident fund" in combined

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

class NpsSmsParser(BaseSmsParser):
    """
    Parser for NPS (National Pension System) SMS Alerts.
    Sender: NPSTRU, CRA-NSDL
    """
    name = "NPS"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # NPS Contribution
            TransactionPattern(
                regex=re.compile(r"(?i)(?:NPS|PRAN)\s*(?:A/c|Account)?\s*([\d]+)\s*(?:has\s+been\s+)?credited\s*with\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*([\d-/]+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3}
            ),
            # NPS Withdrawal
            TransactionPattern(
                regex=re.compile(r"(?i)(?:NPS|PRAN)\s*(?:withdrawal|redemption)\s*(?:of\s+)?(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*from\s*(?:PRAN|A/c)\s*([\d]+)\s*(?:credited|processed)\s*on\s*([\d-/]+)", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        combined = (sender + " " + message).lower()
        return any(k in combined for k in ["nps", "pran", "npstru", "cra-nsdl", "national pension"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

# Email parsers for government schemes typically have similar patterns
class EpfoEmailParser(BaseEmailParser):
    """Parser for EPFO Email Alerts."""
    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return any(k in combined for k in ["epfo", "epf account", "provident fund"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        return EpfoSmsParser().parse(content, date_hint)

class PpfEmailParser(BaseEmailParser):
    """Parser for PPF Email Alerts."""
    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return "ppf" in combined or "public provident fund" in combined

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        return PpfSmsParser().parse(content, date_hint)

class NpsEmailParser(BaseEmailParser):
    """Parser for NPS Email Alerts."""
    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return any(k in combined for k in ["nps", "pran", "national pension"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        return NpsSmsParser().parse(content, date_hint)
