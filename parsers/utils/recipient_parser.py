import re
from typing import Optional

class RecipientParser:
    """
    Dedicated logic for extracting merchant, recipient, or source names 
    from complex bank transaction descriptions.
    """

    @staticmethod
    def extract(description: str, source_type: str = "GENERIC") -> Optional[str]:
        """
        Extract recipient/merchant name from transaction description.
        Informed by Indian bank patterns: UPI, IMPS, NEFT, Salary, Fund Transfers.
        
        source_type: 'SMS', 'EMAIL', or 'FILE' (Excel/CSV)
        """
        if not description:
            return None
        
        desc = description.strip()
        desc_upper = desc.upper()
        
        # Helper to clean a string from common junk
        def clean_name(name: str) -> str:
            if not name: return ""
            # Remove VPA artifacts
            name = re.sub(r'^(VPA|TO VPA|FROM|TO)[-/ ]+', '', name, flags=re.IGNORECASE)
            # Remove trailing numbers/IDs (e.g. -116522, -1341) but KEEP short ones like "Store 01"
            if re.search(r'[- ]\d{5,}$', name): # Only remove if it's a long number (likely ID)
                name = re.sub(r'[- ]\d+$', '', name)
            
            # Remove titles
            name = re.sub(r'^(MR|MS|MRS|DR|PROF)\.?\s+', '', name, flags=re.IGNORECASE)
            # Remove VPA suffixes like @OKAXIS, @YBL, @ICICI etc
            name = re.sub(r'@[A-Z0-9.\-_]{3,}', '', name, flags=re.IGNORECASE)
            return name.strip()

        # Helper to check if a string looks like a random ID or masked field
        def is_junk_id(s: str) -> bool:
            # Masked with X's (e.g. XXXXXXXXXXXX1341)
            if re.search(r'X{3,}', s, re.IGNORECASE): return True
            # Purely numeric and long (e.g. 116522638546)
            s_num_only = re.sub(r'[^0-9]', '', s)
            if s_num_only.isdigit() and len(s_num_only) > 6: return True
            # Check for alphanumeric noise (e.g. IBKL0001370)
            if re.match(r'^[A-Z]{4}\d{7}$', s.upper()): return True # IFSC/ID pattern
            # Too short to be a name
            if len(s.strip()) < 3: return True
            # Common bank boilerplate words
            if s.upper() in {'DR', 'CR', 'TO', 'BY', 'FROM', 'IB', 'SS', 'UPI', 'IMPS', 'TRANSFER', 'FUNDS'}: return True
            return False

        # --- EXCEL/CSV SPECIFIC LOGIC ---
        if source_type == "FILE":
             # 1. SALARY with numeric prefix
             salary_match = re.search(r'(?:SALARY|PAYROLL).*', desc, re.IGNORECASE)
             if salary_match:
                 return salary_match.group(0).strip()[:100]

             # 2. UPI-style (Common in Indian Statements)
             # Catch patterns like: UPI-NAME-ID or UPI-ID-NAME
             if "UPI" in desc_upper:
                 parts = re.split(r'[-/]', desc)
                 # Look for a part that is purely alphabetic or mostly alphabetic
                 for p in parts:
                     p_clean = p.strip()
                     if p_clean.upper() in {"UPI", "IMPS", "NEFT", "RTGS", "TO", "BY"}: continue
                     if not is_junk_id(p_clean):
                         return clean_name(p_clean)[:100]

             # 3. IMPS-style
             if "IMPS" in desc_upper:
                 parts = re.split(r'[-/]', desc)
                 for p in parts:
                     p_clean = p.strip()
                     if p_clean.upper() in {"IMPS", "UPI"}: continue
                     if not is_junk_id(p_clean):
                         return clean_name(p_clean)[:100]

             # 4. General "TO NAME" or "BY NAME" (Statement style)
             transfer_match = re.search(r'(?:TO|BY|TRANSFER TO|TRANSFER FROM|PAYMENT TO)\s+([^0-9-/]{3,}[^0-9-/]*)', desc, re.IGNORECASE)
             if transfer_match:
                 return clean_name(transfer_match.group(1))[:100]

             # 5. Fallback for mixed "FUNDS TRANSFER"
             if "FUNDS TRANSFER" in desc_upper:
                 # Extract words surrounding "TRANSFER"
                 words = desc.split()
                 meaningful = [w for w in words if not is_junk_id(w) and w.upper() not in {"FUNDS", "TRANSFER"}]
                 if meaningful:
                     return " ".join(meaningful[:3])[:100]
        # --------------------------------

        # 1. Standard Prefix Patterns (UPI-NAME-ID etc)
        for prefix in ['UPI', 'IMPS', 'NEFT', 'RTGS']:
            if desc_upper.startswith(prefix):
                 parts = re.split(r'[-/]', desc)
                 # Skip prefix and check following parts
                 for i in range(1, len(parts)):
                     p = parts[i].strip()
                     if not is_junk_id(p):
                         return clean_name(p)[:100]

        # 2. SALARY with numeric prefix (e.g. 5200073603852SALARY FOR THE MONTH DEC)
        salary_match = re.search(r'\d{5,}(SALARY.*)', desc, re.IGNORECASE)
        if salary_match:
            return salary_match.group(1).strip()[:100]

        # 3. INTERNET BANKING / FUND TRANSFER (e.g. IB SS FUNDS TRANSFER DR-55000008469767)
        if 'FUNDS TRANSFER' in desc_upper:
            # Take words before DR/CR or IDs
            words = desc.split()
            meaningful = []
            for w in words:
                w_up = w.upper()
                if w_up in {'IB', 'SS', 'DR', 'CR', 'TO', 'TRANSFER', 'FUNDS'} or re.search(r'\d', w):
                    continue
                meaningful.append(w)
            if meaningful:
                return " ".join(meaningful[:3])[:100]

        # 4. Standard POS/ATM/CARD patterns
        card_match = re.search(r'(?:POS|ATM|WDL|CARD|PURCHASE|SHOPPING|ECOM)(?:\s+|-|/)([^ 0-9/-][^0-9/-]*)', desc, re.IGNORECASE)
        if card_match:
            res = clean_name(card_match.group(1))
            if len(res) > 2: return res[:100]

        # 5. Fallback word-based cleaning
        words = desc.split()
        skip_words = {
            'UPI', 'IMPS', 'NEFT', 'RTGS', 'POS', 'ATM', 'WDL', 'CASH', 'TRANSFER',
            'FUND', 'FUNDS', 'PAY', 'PAYMENT', 'TO', 'BY', 'FROM', 'THE', 'DEBIT',
            'CREDIT', 'PURCHASE', 'SALE', 'ONLINE', 'ECOM', 'CARD', 'NET', 'BANK',
            'IB', 'SS', 'DR', 'CR', 'CHEQ', 'VPA' 
        }
        filtered = [w for w in words if w.upper() not in skip_words and not re.search(r'\d', w)]
        
        if filtered:
            return clean_name(" ".join(filtered[:3]))[:100]
            
        return None
