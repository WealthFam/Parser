import os
import logging
import tempfile
import re
from typing import List, Dict, Any, Optional
import pdfplumber

from .parsers.hdfc import parse_hdfc_statement
from .parsers.swiggy_hdfc import parse_swiggy_hdfc_statement
from .parsers.amzn_pay import parse_amzn_pay_statement
from .parsers.icici_savings import parse_icici_savings_statement
from .parsers.icici_sapphiro import parse_icici_sapphiro_statement
from .parsers.slice import parse_slice_statement
from .parsers.generic import parse_generic_statement

logger = logging.getLogger(__name__)

class BankStatementParser:
    """
    Base class for Bank Statement PDF Parsing.
    """
    
    @staticmethod
    def parse(file_bytes: bytes, password: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Generic entry point for parsing bank statements.
        Initial implementation uses heuristics to detect bank type.
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(file_bytes)
            temp_path = f.name
            
        transactions = []
        try:
            with pdfplumber.open(temp_path, password=password) as pdf:
                # 1. Detect Bank Type (Heuristics)
                first_page_text = pdf.pages[0].extract_text() or ""
                
                # 1. Broad detection for any account/card label variations
                account_match = re.search(r'(?:Account|A/c|Card|Savings A/c|Credit Card|Number|No|#)[:\s]*[X\*\s-]*(\d{4,})', first_page_text, re.I)
                
                # 2. Look for any sequence of masking characters followed by digits (e.g. XXXXXXXX8597 or XXXX 8597)
                if not account_match:
                    account_match = re.search(r'[X\*]{4,}\s*(\d{4})', first_page_text, re.I)
                
                # 3. Look for typical card format (e.g. 4315-XXXX-XXXX-7119)
                if not account_match:
                    account_match = re.search(r'\d{4}[-\s]?[X\*]{4}[-\s]?[X\*]{4}[-\s]?(\d{4})', first_page_text, re.I)
                
                # 4. Slice specific account detection (e.g. jupiteraxis-FDRL...-909880903615-Sent)
                if not account_match:
                    slice_acc_match = re.search(r'jupiteraxis-FDRL\w+-(\d+)-Sent', first_page_text, re.I)
                    if slice_acc_match:
                        account_match = slice_acc_match
                        
                # 5. Last resort: Any 10+ digit number that looks like an account number
                if not account_match:
                    account_match = re.search(r'\b\d{10,}\b', first_page_text)

                account_mask = account_match.group(1) if account_match else "UNKNOWN"
                
                # Normalize to last 4 digits if long number was found
                if account_mask != "UNKNOWN" and len(account_mask) > 4:
                    account_mask = account_mask[-4:]
                
                if account_mask == "UNKNOWN":
                    logger.warning(f"Account detection failed. First page text snippet: {first_page_text[:500]}")
                
                if "SWIGGY HDFC" in first_page_text.upper():
                    transactions = parse_swiggy_hdfc_statement(pdf, account_mask)
                elif "HDFC BANK" in first_page_text.upper():
                    transactions = parse_hdfc_statement(pdf, account_mask)
                elif "ICICI BANK" in first_page_text.upper():
                    # Sub-detect: Credit Card vs Savings
                    if "SAPPHIRO" in first_page_text.upper() or re.search(r'\d{4}X{4,}\d{4}', first_page_text):
                        transactions = parse_icici_sapphiro_statement(pdf, account_mask)
                    else:
                        transactions = parse_icici_savings_statement(pdf, account_mask)
                elif "AMAZON PAY" in first_page_text.upper():
                    transactions = parse_amzn_pay_statement(pdf, account_mask)
                elif "SLICE" in first_page_text.upper() or "SLICEIT.COM" in first_page_text.upper():
                    transactions = parse_slice_statement(pdf, account_mask)
                else:
                    # Fallback to generic table extraction
                    transactions = parse_generic_statement(pdf, account_mask)
                    
        except Exception as e:
            logger.error(f"Failed to parse bank statement: {e}")
            raise e
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
        return transactions
