from typing import List, Dict, Any
from ..utils import parse_decimal, parse_date
import re

def parse_amzn_pay_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    ICICI Bank (Amazon Pay) Statement Parser logic.
    Uses regex text extraction because tables are borderless.
    """
    txns = []
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
            
        # Single-line regex: Date SerNo Desc Rewards Amount
        # Example: 14/03/2026 13048181548 AMAZON PAY... IN 57 1,900.00
        pattern = r'(\d{2}/\d{2}/\d{4})\s+(\d{11,})\s+(.*?)\s+(\d+)\s+([\d,]+\.\d{2}(?:\s*CR)?)'
        matches = re.finditer(pattern, text)
        
        for m in matches:
            date_str = m.group(1)
            ref_id = m.group(2)
            description = m.group(3).strip()
            amt_str = m.group(5)
            
            txn_date = parse_date(date_str)
            if not txn_date:
                continue
            
            is_credit = "CR" in amt_str.upper()
            amount = parse_decimal(amt_str)
            txn_type = "CREDIT" if is_credit else "DEBIT"
            
            if amount > 0:
                txns.append({
                    "date": txn_date,
                    "description": description,
                    "ref_id": ref_id,
                    "amount": amount,
                    "type": txn_type,
                    "account_mask": account_mask,
                    "source": "ICICI"
                })
    return txns
