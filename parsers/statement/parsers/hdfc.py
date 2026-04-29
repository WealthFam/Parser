from typing import List, Dict, Any
from ..utils import parse_decimal, parse_date

def parse_hdfc_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    HDFC Bank Statement Parser logic.
    """
    txns = []
    for page in pdf.pages:
        table = page.extract_table()
        if not table:
            continue
            
        for row in table:
            # Row format: Date, Narration, Chq/Ref, Value Date, Withdrawal, Deposit, Balance
            if not row or len(row) < 5:
                continue
            
            # Filter headers
            if "Date" in str(row[0]) or "Narration" in str(row[1]):
                continue
            
            txn_date = parse_date(row[0])
            if not txn_date:
                continue
            
            description = str(row[1])
            ref_id = str(row[2])
            
            withdrawal = parse_decimal(row[4])
            deposit = parse_decimal(row[5])
            
            amount = deposit if deposit > 0 else withdrawal
            txn_type = "CREDIT" if deposit > 0 else "DEBIT"
            
            if amount > 0:
                txns.append({
                    "date": txn_date,
                    "description": description,
                    "ref_id": ref_id,
                    "amount": amount,
                    "type": txn_type,
                    "account_mask": account_mask,
                    "source": "HDFC"
                })
    return txns
