import re
from datetime import datetime
from typing import List, Dict, Any

def parse_swiggy_hdfc_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    Swiggy HDFC Credit Card Statement Parser logic.
    """
    txns = []
    
    # Regex to match:
    # 22/03/2026| 15:34 PYU*Swiggy FoodBangalore C 630.00 l
    # 01/04/2026| 12:24 BPPY CC PAYMENT... + C 13,482.00 l
    txn_pattern = re.compile(r"(\d{2}/\d{2}/\d{4})\|\s*\d{2}:\d{2}\s+(.*?)\s+(\+?\s*[A-Za-z₹]?\s*[\d,]+\.\d{2})")

    for page in pdf.pages:
        # The Swiggy HDFC statement formats transactions inside tables, but the text is often
        # rendered as a single string per row. To accurately capture the order and avoid
        # layout issues, we iterate through table rows and extract the text.
        tables = page.extract_tables()
        for table in tables:
            for row in table:
                if not row or not row[0]: continue
                cell_text = str(row[0]).replace("\n", " ")
                
                match = txn_pattern.search(cell_text)
                if match:
                    date_str = match.group(1)
                    desc = match.group(2).strip()
                    amt_str = match.group(3)
                    
                    is_credit = "+" in amt_str
                    
                    # Clean amount
                    amt_clean = re.sub(r'[^\d.]', '', amt_str)
                    try:
                        amount = float(amt_clean)
                    except ValueError:
                        continue
                        
                    try:
                        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                    except ValueError:
                        continue
                        
                    txns.append({
                        "date": date_obj.strftime("%Y-%m-%d"),
                        "description": desc,
                        "ref_id": None, # Ref ID is usually within description but we can leave it
                        "amount": amount,
                        "type": "CREDIT" if is_credit else "DEBIT",
                        "account_mask": account_mask,
                        "source": "SWIGGY_HDFC"
                    })
    
    return txns
