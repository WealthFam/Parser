import re
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_slice_statement(pdf, account_mask: str) -> List[Dict[str, Any]]:
    """
    Parser for Slice Small Finance Bank statements.
    Format: DD Mon 'YY Description [RefID] ₹Amount ₹Balance
    May span multiple lines.
    """
    transactions = []
    full_text = ""
    for page in pdf.pages:
        full_text += page.extract_text() + "\n"
    
    lines = full_text.split("\n")
    
    # Regex to find the start of a transaction line
    # Matches: 30 Dec '25 UPI Credit... 8042536416489924 ₹443.00 ₹29,645.60
    txn_header_regex = re.compile(r'^(\d{2} [A-Z][a-z]{2} \'\d{2})\s+(.*?)\s+(\d{10,})\s+₹([\d,]+\.\d{2})\s+₹([\d,]+\.\d{2})')
    
    # Robust Account mask detection
    account_match = re.search(r'(?:A/c|Account|Savings A/c|No)[:\s]*[X\*\s-]*(\d{4,})', full_text, re.I)
    if not account_match:
        account_match = re.search(r'[X\*]{4,}\s*(\d{4})', full_text, re.I)
        
    detected_mask = account_match.group(1)[-4:] if account_match else account_mask
    if detected_mask == "UNKNOWN" and account_mask != "UNKNOWN":
        detected_mask = account_mask
    
    current_txn = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        header_match = txn_header_regex.match(line)
        if header_match:
            # If we were already building a transaction, save it
            if current_txn:
                transactions.append(current_txn)
            
            date_str = header_match.group(1)
            raw_desc = header_match.group(2)
            ref_id = header_match.group(3)
            amount_str = header_match.group(4).replace(",", "")
            balance_str = header_match.group(5).replace(",", "")
            
            # Parse Date: 30 Dec '25
            try:
                # Replace '25 with 2025
                date_clean = date_str.replace("'", "20")
                dt = datetime.strptime(date_clean, "%d %b %Y")
            except:
                dt = datetime.now() # Fallback
            
            # Determine Credit/Debit from description or signs (Slice uses separate types)
            # In our schema, we need absolute amount and type.
            # Usually Interest Cr. is credit. UPI Credit is credit.
            # Payments are usually debit.
            is_credit = "CREDIT" in raw_desc.upper() or "INTEREST CR" in raw_desc.upper()
            
            current_txn = {
                "date": dt.isoformat(),
                "description": raw_desc,
                "ref_id": ref_id,
                "amount": float(amount_str),
                "type": "CREDIT" if is_credit else "DEBIT",
                "balance": float(balance_str),
                "account_mask": detected_mask
            }
        elif current_txn:
            # This line might be a continuation of the description
            # Skip footer/header noise
            if "Need help?" in line or "slice small finance bank" in line or "Generated on" in line:
                continue
            if re.match(r'^\d+/\d+$', line): # Page numbers
                continue
                
            current_txn["description"] += " " + line
            
    # Add the last one
    if current_txn:
        transactions.append(current_txn)
        
    logger.info(f"Slice Parser extracted {len(transactions)} transactions.")
    return transactions
