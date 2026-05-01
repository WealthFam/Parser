import re
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_icici_sapphiro_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    Parser for ICICI Sapphiro Credit Card statements.
    Format: DD/MM/YYYY [RefID] [Description] [Points] [Amount] [CR]
    """
    transactions = []
    full_text = ""
    
    # Character reconstruction for robustness
    for page in pdf.pages:
        chars = page.chars
        chars.sort(key=lambda x: (x['top'], x['x0']))
        
        current_line = ""
        last_top = -1
        last_x1 = -1
        for c in chars:
            if last_top == -1 or abs(c['top'] - last_top) < 2:
                if last_x1 != -1 and c['x0'] - last_x1 > 2:
                    current_line += " "
                current_line += c['text']
            else:
                if current_line.strip():
                    full_text += current_line + "\n"
                current_line = c['text']
            last_top = c['top']
            last_x1 = c['x1']
        full_text += current_line + "\n"
    
    lines = full_text.split("\n")
    
    # Account mask (Credit cards have XXXX6008)
    account_match = re.search(r'(\d{4})X{4,}(\d{4})', full_text, re.I)
    detected_mask = account_match.group(2) if account_match else account_mask
    
    # If mask is still UNKNOWN, try searching for the card number format in reconstructed text
    if detected_mask == "UNKNOWN" or len(detected_mask) != 4:
         card_search = re.search(r'Card Number\s*[:\-\s]*\s*(?:\d{4}-?){2}(\d{4})-?(\d{4})', full_text, re.I)
         if card_search:
             detected_mask = card_search.group(2)
    
    # Regex for transaction
    # 24/03/2026 13111781502 PTMBHARTI AIRTEL LIM NOIDA IN 1 100.00
    # 01/04/2026 13149318954 BBPS Payment received 0 15,821.23 CR
    txn_regex = re.compile(r'^(\d{2}/\d{2}/\d{4})\s+(\d{10,})\s+(.*?)\s+([\d,]+\.\d{2})(\s+CR)?$')
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        match = txn_regex.match(line)
        if match:
            date_str = match.group(1)
            ref_id = match.group(2)
            raw_desc = match.group(3)
            amount_str = match.group(4).replace(",", "")
            is_credit = bool(match.group(5))
            
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y")
            except:
                dt = datetime.now()
            
            transactions.append({
                "date": dt.isoformat(),
                "description": raw_desc,
                "ref_id": ref_id,
                "amount": float(amount_str),
                "type": "CREDIT" if is_credit else "DEBIT",
                "account_mask": detected_mask
            })
            
    return transactions
