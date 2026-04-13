from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class SbiSmsParser(BaseSmsParser):
    """
    Parser for SBI Bank SMS Alerts.
    """
    name = "SBI"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # Debit Pattern with Ref ID
            TransactionPattern(
                regex=re.compile(
                    r"(?i)(?:Txn\s*of|INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:on|debited\s*from)\s*.*?A/c\s*(?:.*?|x*|X*)(\d+)\s*at\s*(.*?)\s*on\s*(\d{2}[A-Z]{3,}\d{2,4}|\d{2}[-/]\d{2}[-/]\d{2,4}).*?[Rr]ef[:\.\s-]+(\w+)",
                    re.IGNORECASE
                ),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # Debit Pattern without Ref ID
            TransactionPattern(
                regex=re.compile(
                    r"(?i)(?:Txn\s*of|INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:on|debited\s*from)\s*.*?A/c\s*(?:.*?|x*|X*)(\d+)\s*at\s*(.*?)\s*on\s*(\d{2}[A-Z]{3,}\d{2,4}|\d{2}[-/]\d{2}[-/]\d{2,4})",
                    re.IGNORECASE
                ),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4}
            ),
            # Credit Pattern (TD Closure etc)
            TransactionPattern(
                regex=re.compile(
                    r"(?i)A/C\s*(?:.*?|x*|X*)(\d+)\s*Credited\.\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*(\d{2}/\d{2}/\d{2,4})\s*on\s*account\s*of\s*(.*?)\.-",
                    re.IGNORECASE
                ),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4}
            ),
            # ATM Withdrawal
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*withdrawn\s*(?:from\s*ATM|at\s*ATM|Cash\s*withdrawal).*?(?:A/c|Card)\s*(?:.*?|x*|X*)(\d+)\s*on\s*(\d{2}[A-Z]{3,}\d{2,4}|\d{2}[-/]\d{2}[-/]\d{2,4})(?:.*?Ref[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "ref_id": 4}
            ),
            # IMPS/NEFT/RTGS
            TransactionPattern(
                regex=re.compile(r"(?i)(?:IMPS|NEFT|RTGS)\s*(?:of)?\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:debited|from)\s*A/c\s*(?:.*?|x*|X*)(\d+)\s*to\s*(.*?)\s*on\s*(\d{2}[A-Z]{3,}\d{2,4}|\d{2}[-/]\d{2}[-/]\d{2,4}).*?(?:Ref|UTR)[:\.\s-]+(\w+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # UPI/Generic Credit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*credited\s*to\s*SBI\s*A/c\s*(?:.*?|x*|X*)(\d+)\s*on\s*(\d{2}[A-Z]{3,}\d{2,4}|\d{2}[-/]\d{2}[-/]\d{2,4})\s*(?:from\s*(.*?))?(?:\s*Ref[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "sbi" in sender.lower() or "sbi" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id, source, date_hint=None):
        try:
            # Handle formats like 13Jan26 or 13-01-26 or 23/05/24
            date_str = date_str.replace("/", "-")
            formats = ["%d%b%y", "%d%b%Y", "%d-%m-%y", "%d-%m-%Y"]
            txn_date = None
            for fmt in formats:
                try:
                    txn_date = datetime.strptime(date_str, fmt)
                    break
                except: continue
            if not txn_date: txn_date = date_hint or timezone.utcnow()
        except:
            txn_date = timezone.utcnow()
            
        clean_recipient = RecipientParser.extract(recipient)
        return ParsedTransaction(
            amount=amount,
            date=txn_date,
            description=f"SBI: {clean_recipient or recipient}",
            type=type_str,
            account_mask=account_mask,
            recipient=clean_recipient,
            ref_id=ref_id,
            raw_message=raw,
            source=source
        )

class SbiEmailParser(BaseEmailParser):
    """
    Parser for SBI Bank Email Alerts.
    """
    REF_PATTERN = re.compile(r"(?i)\b(?:Ref|UTR|TXN#|Ref\s*No)(?:[\s:\.-]|\bis\b)+([a-zA-Z0-9]{3,})")

    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return "sbi" in combined and any(k in combined for k in ["transaction", "spent", "debited", "alert", "upi"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        clean_content = " ".join(content.split())
        
        if "sbi" in clean_content.lower():
            amt_match = re.search(r"(?i)(?:INR|Rs\.?)\s*([\d\.]+(?:,\d{3})*)", clean_content)
            if amt_match:
                amt_str = amt_match.group(1).replace(",", "")
                date_match = re.search(r"(\d{2}[A-Za-z]{3}\d{2,4}|\d{2}-\d{2}-\d{2,4})", clean_content)
                if date_match:
                    merchant_match = re.search(r"(?i)(?:at|to|towards|for|on)\s+([A-Z0-9\s*]{3,30}?)(?:\s+on|\s+at|\.|\s+from|\s+using)", clean_content)
                    merchant = merchant_match.group(1).strip() if merchant_match else "Unknown Merchant"
                    mask_match = re.search(r"(?i)(?:A/c|card|XX)\s*(\d{4,})", clean_content)
                    mask = mask_match.group(1) if mask_match else "XXXX"
                    
                    ref_match = self.REF_PATTERN.search(clean_content)
                    ref_id = ref_match.group(1).strip() if ref_match else None
                    
                    return self._create_txn(Decimal(amt_str), merchant, mask, date_match.group(1), "DEBIT", content, ref_id, date_hint)

        return None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id, date_hint=None):
        try:
            formats = ["%d%b%y", "%d%b%Y", "%d-%m-%y", "%d-%m-%Y"]
            txn_date = None
            for fmt in formats:
                try:
                    txn_date = datetime.strptime(date_str, fmt)
                    break
                except: continue
            if not txn_date: txn_date = date_hint or timezone.utcnow()
        except:
            txn_date = timezone.utcnow()
            
        clean_recipient = RecipientParser.extract(recipient)
        return ParsedTransaction(
            amount=amount,
            date=txn_date,
            description=f"SBI: {clean_recipient or recipient}",
            type=type_str,
            account_mask=account_mask,
            recipient=clean_recipient,
            ref_id=ref_id,
            raw_message=raw,
            source="EMAIL"
        )
