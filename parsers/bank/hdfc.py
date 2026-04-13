import re
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern

class HdfcSmsParser(BaseSmsParser):
    """
    Parser for HDFC Bank SMS Alerts.
    """
    name = "HDFC"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*debited\s*from\s*a/c\s*([xX]*\d+)\s*on\s*([\d/:-]+)\s*to\s*(.*?)\.\s*(?:Ref[:\.\s]+(\w+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Spent
            TransactionPattern(
                regex=re.compile(r"(?i)Spent\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*.*?(?:card|A/c)\s*([xX]*\d+)\s*at\s*(.*?)\s*on\s*([\d/:-]+)(?:.*?Ref[:\.\s]*(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Sent
            TransactionPattern(
                regex=re.compile(r"(?i)Sent\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*From\s*HDFC\s*Bank\s*A/C\s*(?:.*?|x*|\*|X*)(\d+)\s*To\s*(.*?)\s*On\s*([\d/:-]+)(?:.*?Ref[:\.\s]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Credit (With Ref/UPI) - Prioritized
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*credited\s*to\s*HDFC\s*Bank\s*A/c\s*(?:.*?|x*|\*|X*)(\d+)\s*on\s*([\d/:-]+)\s*from\s*(?:VPA\s+)?(.*?)\s*\(?(?:UPI|Ref)[:\.\s]*(\w+)\)?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Credit (No Ref) - Fallback
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*credited\s*to\s*HDFC\s*Bank\s*A/c\s*(?:.*?|x*|\*|X*)(\d+)\s*on\s*([\d/:-]+)\s*from\s*(?:VPA\s+)?(.*)", re.IGNORECASE),
                confidence=0.9,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4}
            ),
            # Salary/Deposit (UPDATE format with balance)
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Update!?\s*)?(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*deposited\s*in\s*HDFC\s*Bank\s*A/c\s*(?:[xX]*|\*)(\d+)\s*on\s*([\d-]+[A-Z]{3}-\d+)(?:.*?for\s*(.*?)\.)?(?:.*?Avl bal[:\s]*(?:Rs\.?|INR)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "balance": 5}
            ),
            # ATM Withdrawal
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*withdrawn\s*from\s*(?:ATM|Cash)\s*.*?(?:A/c|Card)\s*(?:.*?|x*|\*|X*)(\d+)\s*on\s*([\d/:-]+)(?:.*?Ref[:\.\s]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "ref_id": 4}
            ),
            # IMPS/NEFT/RTGS
            TransactionPattern(
                regex=re.compile(r"(?i)(?:IMPS|NEFT|RTGS)\s*(?:of|for)?\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*(?:debited|from)\s*HDFC\s*Bank\s*A/c\s*(?:.*?|x*|\*|X*)(\d+)\s*(?:to|towards)\s*(.*?)\s*on\s*([\d/:-]+).*?(?:Ref|UTR)[:\.\s]+(\w+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Funds Transfer (IB/SS format with balance)
            TransactionPattern(
                regex=re.compile(r"(?i)(?:UPDATE:\s*)?(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*debited\s*from\s*HDFC\s*Bank\s*(?:A/C\s*)?(?:[xX]*|\*)(\d+)\s*on\s*([\d-]+[A-Z]{3}-\d+)(?:.*?DR-[xX]*(\d+))?(?:.*?Avl bal:(?:Rs\.?|INR)\s*([\d,]+\.?\d*))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "ref_id": 4, "balance": 5}
            ),
            # Received (Common in UPI credits)
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*Received\s*in\s*A/c\s*(?:.*?|x*|\*|X*)(\d+)\s*on\s*([\d/:-]+)\s*from\s*(.*?)(?:(?:\s+?|\().*?Ref[:\.\s]*(\w+)(?:\))?)?$", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            )
        ]

    # Confidence Adjustment (Simplified implementation)
    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        # Post-process to find balance/limit and adjust confidence if all fields are present
        for tx in results:
            tx.balance = self._find_balance(content)
            tx.credit_limit = self._find_limit(content)
            if tx.ref_id and tx.amount and tx.date and tx.recipient:
                tx.confidence = max(tx.confidence, 0.95)
        return results

    def can_handle(self, sender: str, message: str) -> bool:
        return "hdfc" in sender.lower() or "hdfc" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    BAL_PATTERN = re.compile(r"(?i)(?:Avbl\s*Bal|Bal|Balance)[:\.\s-]+(?:Rs\.?|INR)\s*([\d,]+\.?\d*)", re.IGNORECASE)
    LIMIT_PATTERN = re.compile(r"(?i)(?:Credit\s*Limit|Limit)[:\.\s-]+(?:Rs\.?|INR)\s*([\d,]+\.?\d*)", re.IGNORECASE)

    def _find_balance(self, content: str) -> Optional[Decimal]:
        match = self.BAL_PATTERN.search(content)
        if match: return Decimal(match.group(1).replace(",", ""))
        return None

    def _find_limit(self, content: str) -> Optional[Decimal]:
        match = self.LIMIT_PATTERN.search(content)
        if match: return Decimal(match.group(1).replace(",", ""))
        return None

class HdfcEmailParser(BaseEmailParser):
    """
    Parser for HDFC Bank Email Alerts.
    """
    name = "HDFC"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Debit Card (made a transaction)
            TransactionPattern(
                regex=re.compile(r"(?i)made\s*a\s*transaction\s*of\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*your\s*HDFC\s*Bank\s*.*?(?:Card)\s*(?:.*?|x*|X*)(\d+)\s*at\s*(.*?)\s*on\s*([\d-]+)(?:.*?Ref[:\.\s]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5},
                source="EMAIL"
            ),
            # Spent
            TransactionPattern(
                regex=re.compile(r"(?i)spent\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*.*?card\s*(?:.*?|x*|X*)(\d+)\s*at\s*(.*?)\s*(?:on|Date)\s*([\d/-]+)(?:.*?Ref[:\.\s]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5},
                source="EMAIL"
            ),
            # Account Debit
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*(?:.*?|x*|X*)(\d+)\s*has\s*been\s*debited\s*for\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*on\s*([\d-]+)\s*towards\s*(.*?)(?:\.\s*Ref[:\s]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5},
                source="EMAIL"
            ),
            # UPI Debit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*has\s*been\s*debited\s*from\s*account\s*(\d+)\s*to\s*(.*?)\s*on\s*([\d-]+)(?:.*?\b(?:Ref|Reference)\s*(?:No|ID|Number)?(?:[\s:\.-]|\bis\b)+([a-zA-Z0-9]+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5},
                source="EMAIL"
            ),
            # Generic UPI
            TransactionPattern(
                regex=re.compile(r"(?i)UPI\s*txn.*?([\d,]+\.?\d*)\s*debited\s*from\s*A/c\s*(?:.*?|x*|X*)(\d+)\s*to\s*(.*?)\s*on\s*([\d-]+)(?:.*?\b(?:Ref|Reference)\s*(?:No|ID|Number)?(?:[\s:\.-]|\bis\b)+([a-zA-Z0-9]+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5},
                source="EMAIL"
            )
        ]

    def parse_with_confidence(self, content: str, date_hint: Optional[datetime] = None) -> List[ParsedTransaction]:
        results = super().parse_with_confidence(content, date_hint)
        # Handle secondary Ref ID extraction if not caught by pattern
        for tx in results:
            if not tx.ref_id:
                ref_match = self.REF_PATTERN.search(content)
                if ref_match: tx.ref_id = ref_match.group(1).strip()
            
            # Robust Ref ID check for UPI (12 digits)
            if not tx.ref_id:
                digits_match = re.search(r"(\d{12})", content)
                if digits_match: tx.ref_id = digits_match.group(1)

            tx.balance = self._find_balance(content)
            tx.credit_limit = self._find_limit(content)
        return results

    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        if "you have done a upi txn" in combined: return True
        if "hdfc" not in combined: return False
        keywords = ["transaction", "debited", "spent", "txn", "upi", "vpa", "rs"]
        return any(k in combined for k in keywords)

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    # More flexible pattern for Reference/UTR
    REF_PATTERN = re.compile(
        r"(?i)\b(?:Ref|UTR|TXN#|Ref\s*No|Reference\s*ID|reference\s*number|utr\s*no|Ref\s*ID)(?:[\s:\.-]|\bis\b)+([a-zA-Z0-9]{3,})", 
        re.IGNORECASE
    )

    BAL_PATTERN = re.compile(r"(?i)\b(?:Avbl\s*Bal|Bal|Balance)[:\.\s-]+(?:Rs\.?|INR)\s*([\d,]+\.?\d*)", re.IGNORECASE)
    LIMIT_PATTERN = re.compile(r"(?i)\b(?:Credit\s*Limit|Limit)[:\.\s-]+(?:Rs\.?|INR)\s*([\d,]+\.?\d*)", re.IGNORECASE)

    def _find_balance(self, content: str) -> Optional[Decimal]:
        match = self.BAL_PATTERN.search(content)
        if match: return Decimal(match.group(1).replace(",", ""))
        return None

    def _find_limit(self, content: str) -> Optional[Decimal]:
        match = self.LIMIT_PATTERN.search(content)
        if match: return Decimal(match.group(1).replace(",", ""))
        return None
