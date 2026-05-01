import re
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_icici_savings_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    Parser for ICICI Bank Savings Account statements.
    Format: DD-MM-YYYY ... [Withdrawal] [Deposit] [Balance]
    """
    transactions = []
    full_text = ""
    
    # Matches: DD-MM-YYYY Particulars... [Withdrawal/Deposit] [Balance]
    txn_regex = re.compile(r'^(\d{2}-\d{2}-\d{4})\s+(.*?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})$')
    
    pending_description = ""
    
    # Character-based reconstruction with position tracking
    for page in pdf.pages:
        chars = page.chars
        chars.sort(key=lambda x: (x['top'], x['x0']))
        
        current_line_data = [] # List of (char, x0, x1)
        last_top = -1
        
        def process_line(line_chars):
            nonlocal full_text, pending_description, transactions
            if not line_chars: return
            text = ""
            last_x1 = -1
            for c, x0, x1 in line_chars:
                if last_x1 != -1 and x0 - last_x1 > 2:
                    text += " "
                text += c
                last_x1 = x1
            
            clean_text = text.strip()
            full_text += clean_text + "\n"
            
            # Check for transaction line
            match = txn_regex.match(clean_text)
            if match:
                date_str = match.group(1)
                amount1_str = match.group(3).replace(",", "")
                amount2_str = match.group(4).replace(",", "")
                
                # Combine pending description with any text on the current line (Group 2)
                final_desc = (pending_description + " " + match.group(2)).strip()
                pending_description = "" # Reset
                
                # Find the x-position of amount1
                full_line_text_no_spaces = "".join([c for c, _, _ in line_chars])
                amount1_raw = match.group(3)
                start_idx = full_line_text_no_spaces.find(amount1_raw.replace(" ", ""))
                
                if start_idx != -1:
                    mid_x = line_chars[start_idx + len(amount1_raw)//2][1]
                    is_deposit = abs(mid_x - 389) < 25
                    
                    try:
                        dt = datetime.strptime(date_str, "%d-%m-%Y")
                    except:
                        dt = datetime.now()
                    
                    transactions.append({
                        "date": dt.isoformat(),
                        "description": final_desc,
                        "amount": float(amount1_str),
                        "type": "CREDIT" if is_deposit else "DEBIT",
                        "balance": float(amount2_str),
                        "account_mask": "UNKNOWN" # Will fill later
                    })
            else:
                # If not a transaction line, it might be a description part
                if clean_text and not any(h in clean_text for h in ["DATE", "MODE", "PARTICULARS", "BALANCE"]):
                    if "Sincerely" not in clean_text and "Page" not in clean_text:
                        pending_description += " " + clean_text

        for c in chars:
            if last_top == -1 or abs(c['top'] - last_top) < 2:
                current_line_data.append((c['text'], c['x0'], c['x1']))
            else:
                process_line(current_line_data)
                current_line_data = [(c['text'], c['x0'], c['x1'])]
            last_top = c['top']
        process_line(current_line_data)
    
    # Extract account mask from full_text
    account_match = re.search(r'ACCOUNT NUMBER\s+(?:XXXXX+)?(\d+)', full_text, re.I)
    detected_mask = account_match.group(1)[-4:] if account_match else account_mask
    
    # Update transactions with detected mask
    for txn in transactions:
        txn["account_mask"] = detected_mask
        
    return transactions
