from parser.core import timezone
import re
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from parser.parsers.base_compat import BaseSmsParser, BaseEmailParser, ParsedTransaction, TransactionPattern
from parser.parsers.utils.recipient_parser import RecipientParser

class IciciSmsParser(BaseSmsParser):
    """
    Parser for ICICI Bank SMS Alerts.
    """
    name = "ICICI"

    def get_patterns(self) -> List[TransactionPattern]:
        return [
            # High specificity: Spent with Ref ID
            # Example: "INR 869.00 spent using ICICI Bank Card XX0004 on 23-Sep-24 on IND*Amazon. Ref No 12345. Avl Limit: INR 2,39,131.00"
            TransactionPattern(
                regex=re.compile(
                    r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*spent\s*(?:using|on)\s*ICICI\s*Bank\s*.*?(?:Card|A/c)\s*([xx*]*\d+)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})\s*(?:on|at)\s*(.*?)\.?\s*(?:Ref[:\.\s-]+|Ref\s*No\s+)(\w+)",
                    re.IGNORECASE
                ),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Standard specificity: Spent (user-provided and legacy formats)
            TransactionPattern(
                regex=re.compile(
                    r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*spent\s*(?:using|on)\s*ICICI\s*Bank\s*.*?(?:Card|A/c)\s*([xX]*\d+)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})\s*(?:on|at)\s*(.*?)\.\s*(?:Avl|Ref|$)",
                    re.IGNORECASE
                ),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4}
            ),
            # Debit Format
            TransactionPattern(
                regex=re.compile(
                    r"(?i)A/c\s*([xX]*\d+)\s*is\s*debited\s*for\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})\.\s*Info:\s*(.*?)(?:\.\s*(?:Ref[:\.\s-]+|Ref\s*No\s+)(\w+))?",
                    re.IGNORECASE
                ),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # ATM Withdrawal
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*withdrawn\s*from\s*(?:ATM|Cash|ATM\s*withdrawal).*?(?:A/c|Card)\s*([xX]*\d+)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})(?:.*?Ref[:\.\s-]+(\w+))?", re.IGNORECASE),
                confidence=0.9,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "ref_id": 4}
            ),
            # IMPS/NEFT
            TransactionPattern(
                regex=re.compile(r"(?i)(?:IMPS|NEFT|RTGS)\s*(?:of)?\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*(?:debited|from)\s*A/c\s*([xX]*\d+)\s*to\s*(.*?)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4}).*?(?:Ref|UTR)[:\.\s-]+(\w+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="DEBIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4, "ref_id": 5}
            ),
            # UPI Credit
            TransactionPattern(
                regex=re.compile(r"(?i)(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*credited\s*to\s*ICICI\s*Bank\s*A/c\s*([xX]*\d+)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})\s*from\s*(.*?)\s*\.\s*UPI\s*Ref\s*(?:No)?\s*(\w+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Account Credit
            TransactionPattern(
                regex=re.compile(r"(?i)A/c\s*([xX]*\d+)\s*is\s*credited\s*for\s*(?:INR|Rs\.?)\s*([\d,]+\.?\d*)\s*on\s*(\d{2}-[a-z]{3}-\d{2,4})(?:.*?[Ii]nfo:\s*(.*?))?\.(?:\s*Ref[:\s]+(\w+))?", re.IGNORECASE),
                confidence=0.95,
                txn_type="CREDIT",
                field_map={"mask": 1, "amount": 2, "date": 3, "recipient": 4, "ref_id": 5}
            ),
            # Bill Pay / CC Payment Received
            TransactionPattern(
                regex=re.compile(r"(?i)Payment\s*of\s*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)\s*has\s*been\s*received\s*on\s*your\s*ICICI\s*Bank\s*Credit\s*Card\s*([xX*]*\d+)\s*through\s*(.*?)\s*on\s*([\d/A-Z-]+)", re.IGNORECASE),
                confidence=1.0,
                txn_type="CREDIT",
                field_map={"amount": 1, "mask": 2, "recipient": 3, "date": 4}
            )
        ]

    def can_handle(self, sender: str, message: str) -> bool:
        return "icici" in sender.lower() or "icici" in message.lower()

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        matches = self.parse_with_confidence(content, date_hint)
        return matches[0] if matches else None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id, source, date_hint=None):
        try:
            # ICICI often uses 23-Sep-24
            txn_date = datetime.strptime(date_str, "%d-%b-%y")
        except:
            try:
                txn_date = datetime.strptime(date_str, "%d-%b-%Y")
            except:
                txn_date = date_hint or timezone.utcnow()
            
        clean_recipient = RecipientParser.extract(recipient)
        return ParsedTransaction(
            amount=amount,
            date=txn_date,
            description=f"ICICI: {clean_recipient or recipient}",
            type=type_str,
            account_mask=account_mask,
            recipient=clean_recipient,
            ref_id=ref_id,
            raw_message=raw,
            source=source
        )

class IciciEmailParser(BaseEmailParser):
    """
    Parser for ICICI Bank Email Alerts.
    """
    REF_PATTERN = re.compile(r"(?i)\b(?:Ref|UTR|TXN#|Ref\s*No)(?:[\s:\.-]|\bis\b)+([a-zA-Z0-9]{3,})")

    def can_handle(self, subject: str, body: str) -> bool:
        combined = (subject + " " + body).lower()
        return "icici" in combined and any(k in combined for k in ["transaction", "spent", "debited", "alert", "upi"])

    def parse(self, content: str, date_hint: Optional[datetime] = None) -> Optional[ParsedTransaction]:
        clean_content = " ".join(content.split())
        
        # Use common patterns or loose fallback
        # ICICI emails are quite varied, so we leverage the loose matcher with ICICI specific keywords
        if "icici" in clean_content.lower():
            amt_match = re.search(r"(?i)(?:INR|Rs\.?)\s*([\d\.]+(?:,\d{3})*)", clean_content)
            if amt_match:
                amt_str = amt_match.group(1).replace(",", "")
                # Flexible Date Support: 13-Jan-24, 13/01/2024, 13-01-2024
                date_match = re.search(r"(\d{2}[-/ ](?:[a-zA-Z]{3,9}|\d{2})[-/ ]\d{2,4})", clean_content)
                if date_match:
                    date_val = date_match.group(1)
                    merchant_match = re.search(r"(?i)(?:at|to|towards|for|on)\s+([A-Z0-9\s*]{3,30}?)(?:\s+on|\s+at|\.|\s+from|\s+using)", clean_content)
                    merchant = merchant_match.group(1).strip() if merchant_match else "Unknown Merchant"
                    # Improved Mask: A/c, card, Card, XX, xxxx ending in 1234
                    mask_match = re.search(r"(?i)(?:A/c|Account|card|Card|XX|xx|ending in)\s*.*?(?:x*|X*|\*)*(\d{4,})", clean_content)
                    mask = mask_match.group(1) if mask_match else "XXXX"
                    
                    ref_match = self.REF_PATTERN.search(clean_content)
                    ref_id = ref_match.group(1).strip() if ref_match else None
                    
                    return self._create_txn(Decimal(amt_str), merchant, mask, date_val, "DEBIT", content, ref_id, date_hint)

        return None

    def _create_txn(self, amount, recipient, account_mask, date_str, type_str, raw, ref_id, date_hint=None):
        try:
            formats = ["%d-%b-%y", "%d-%b-%Y", "%d-%m-%y", "%d-%m-%Y", "%d/%m/%y", "%d/%m/%Y", "%d %b %y", "%d %B %Y"]
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
            description=f"ICICI: {clean_recipient or recipient}",
            type=type_str,
            account_mask=account_mask,
            recipient=clean_recipient,
            ref_id=ref_id,
            raw_message=raw,
            source="EMAIL"
        )
