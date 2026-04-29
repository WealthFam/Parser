import os
import logging
import tempfile
import re
from typing import List, Dict, Any, Optional
import pdfplumber

from .parsers.hdfc import parse_hdfc_statement
from .parsers.swiggy_hdfc import parse_swiggy_hdfc_statement
from .parsers.amzn_pay import parse_amzn_pay_statement
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
                
                # Heuristic: Find account number (e.g. "Account No : 50100...")
                account_match = re.search(r'(?:Account No|A/c No|Account Number)[:\s]+(\d+)', first_page_text, re.I)
                account_mask = account_match.group(1) if account_match else "UNKNOWN"
                
                if "SWIGGY HDFC" in first_page_text.upper():
                    transactions = parse_swiggy_hdfc_statement(pdf, account_mask)
                elif "HDFC BANK" in first_page_text.upper():
                    transactions = parse_hdfc_statement(pdf, account_mask)
                elif "ICICI BANK" in first_page_text.upper() or "AMAZON PAY" in first_page_text.upper():
                    transactions = parse_amzn_pay_statement(pdf, account_mask)
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
