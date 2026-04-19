import os
import logging
import tempfile
import re
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Optional

from parser.core import timezone

logger = logging.getLogger(__name__)


def _parse_decimal(val: Any) -> Decimal:
    """Robustly convert a string or potentially comma-separated value to Decimal."""
    if val is None:
        return Decimal("0.00")
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    try:
        # Remove commas, whitespace, and currency symbols
        clean_val = str(val).replace(",", "").replace(" ", "").replace("₹", "").strip()
        return Decimal(clean_val or "0.00")
    except:
        return Decimal("0.00")


def _parse_date(val: Any) -> Optional[datetime]:
    """Robustly parse date strings or objects, bypassing locale-dependent strptime."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val

    s = str(val).strip()
    if not s:
        return None

    # Mapping for MMM month names to MM integers
    months = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
    }

    # Try DD-MMM-YYYY or DD MMM YYYY or DD-MMM-YY
    match = re.search(r"(\d{1,2})[-/\s]([A-Za-z]{3})[-/\s](\d{2,4})", s)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).upper()
        year = int(match.group(3))
        if year < 100:
            year += 2000  # Handle YY
        month = months.get(month_str[:3])
        if month:
            try:
                return datetime(year, month, day)
            except:
                pass

    # Try YYYY-MM-DD
    match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except:
            pass

    # Try DD-MM-YYYY
    match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
    if match:
        try:
            return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
        except:
            pass

    return None


def _safe_to_dict(obj: Any) -> Any:
    """Deeply convert Pydantic/library objects to dictionaries for serialization."""
    if obj is None:
        return None
    # Core types to preserve
    if isinstance(obj, (int, float, str, bool, Decimal, datetime, date)):
        return obj

    if isinstance(obj, list):
        return [_safe_to_dict(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _safe_to_dict(v) for k, v in obj.items()}

    # Pydantic or library objects
    for method in ["model_dump", "dict", "to_dict"]:
        if hasattr(obj, method):
            try:
                return _safe_to_dict(getattr(obj, method)())
            except:
                continue

    if hasattr(obj, "__dict__"):
        return _safe_to_dict(vars(obj))
    return obj


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Safely get a key from a dictionary or an attribute from an object."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


class CasParser:
    """
    Wrapper around casparser library to parse Mutual Fund CAS PDFs.
    """


    @staticmethod
    def parse(file_bytes: bytes, password: str) -> List[Dict[str, Any]]:
        flattened_transactions = []
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(file_bytes)
            temp_path = f.name
            
        # 0. Version-agnostic monkey-patch for CDSL/NSDL routing
        # Some versions of casparser (like 0.8.1) have FileType.CDSL but don't route it
        try:
            import casparser.process
            from casparser.enums import FileType
            from casparser.process.nsdl_statement import process_nsdl_text
            
            # Use a wrapper to check if the library already fixed it
            original_proc = casparser.process.process_cas_text
            
            def safe_patched_process(text, file_type=FileType.UNKNOWN):
                # If it's CDSL and not routed, we handle it. 
                # If the library handles it in the future, it won't reach process_detailed_text 
                # with a CDSL type and error out there, so this wrapper stays safe.
                if file_type == FileType.CDSL:
                    try:
                        return process_nsdl_text(text)
                    except Exception as e:
                        logger.debug(f"Patched CDSL routing failed, falling back: {e}")
                return original_proc(text, file_type)
                
            casparser.process.process_cas_text = safe_patched_process
        except Exception as e:
            pass

        try:
            data = None
            # 2. Main Parsing logic with broad strategy fallback
            strategies = [
                {"output": "dict", "force_pdfminer": True}, # Priority 1: More accurate for modern CAMS
                {"output": "dict"},                         # Priority 2: Standard fast extraction
                {}                                          # Priority 3: Fallback
            ]
            
            for strategy in strategies:
                try:
                    data = casparser.read_cas_pdf(temp_path, password, **strategy)
                    if data: 
                        break
                except Exception as e:
                    logger.debug(f"CAS Strategy {strategy} failed: {e}")
                    continue
            
            if not data:
                raise ValueError("CAS Parsing failed. Ensure the password is correct and the PDF is a valid Consolidated Account Statement.")

            # Force deep conversion to ensure we only have primitive types/dicts
            data = _safe_to_dict(data)
            
            # Extract common metadata for synthesis logic
            period = data.get("statement_period") or {}
            to_date_raw = period.get("to") or period.get("to_")
            from_date_raw = period.get("from") or period.get("from_")
            
            val_date = _parse_date(to_date_raw) or _parse_date(from_date_raw)
            val_date_str = val_date.strftime("%Y-%m-%d") if val_date else timezone.utcnow().strftime("%Y-%m-%d")

            # 3. Handle Depository Statements (NSDL/CDSL) which use 'accounts' instead of 'folios'
            if "accounts" in data and not data.get("folios"):
                logger.debug("Normalizing depository account structure...")
                folios = []
                # val_date_str defined above
                for account in data.get("accounts", []):
                    # NSDL/CDSL can have transactions at the account level or the MF level
                    account_txns = account.get("transactions", [])
                    mf_list = account.get("mutual_funds", [])
                    if not mf_list: continue
                    
                    folio_id = account.get("client_id") or account.get("dp_id") or "Demat-Folio"
                    schemes = []
                    
                    for mf in mf_list:
                        isin = mf.get("isin")
                        amfi = mf.get("amfi")
                        
                        # Try to find transactions for THIS specific fund in the account
                        fund_txns = mf.get("transactions", [])
                        
                        # If fund doesn't have local txns, check if the account-level txns belong to this fund
                        if not fund_txns and account_txns:
                            fund_txns = [t for t in account_txns if t.get("isin") == isin or t.get("amfi") == amfi]
                        
                        if not fund_txns:
                            # Fallback to balance snapshot only if no transactions found
                            fund_txns = [{
                                "date": val_date_str,
                                "description": "Balance from depository statement",
                                "amount": _parse_decimal(mf.get("value") or 0),
                                "units": _parse_decimal(mf.get("balance") or 0),
                                "nav": _parse_decimal(mf.get("nav") or 0),
                                "type": "PURCHASE",
                                "is_synthesized": True
                            }]
                            
                        schemes.append({
                            "scheme": mf.get("name") or "Unknown Scheme",
                            "isin": isin,
                            "amfi": amfi,
                            "transactions": fund_txns,
                            "close": float(mf.get("balance") or 0),
                            "valuation": float(mf.get("value") or 0),
                            "nav": float(mf.get("nav") or 0)
                        })
                    folios.append({"folio": folio_id, "schemes": schemes})
                data["folios"] = folios

            # 4. Process into WealthFam internal transaction schema
            folios = _safe_get(data, "folios", [])
            
            for f_idx, f_item in enumerate(folios):
                folio = _safe_to_dict(f_item)
                f_num = _safe_get(folio, "folio") or _safe_get(folio, "folio_no") or "Unknown"
                schemes = _safe_get(folio, "schemes", [])
                
                for s_idx, s_item in enumerate(schemes):
                    scheme = _safe_to_dict(s_item)
                    s_name = _safe_get(scheme, "scheme", "Unknown Scheme")
                    transactions = _safe_get(scheme, "transactions", [])
                    

                    # If no transactions but there is a balance, synthesize a virtual transaction
                    if not transactions:
                        # Extract metrics from scheme or valuation object
                        close_bal = _safe_get(scheme, "close") or _safe_get(scheme, "balance") or 0
                        valuation_obj = _safe_get(scheme, "valuation") or {}
                        
                        val_amt = Decimal("0.00")
                        val_nav = Decimal("0.00")
                        val_date_obj = None
                        
                        if isinstance(valuation_obj, dict):
                            val_amt = _parse_decimal(_safe_get(valuation_obj, "value"))
                            val_nav = _parse_decimal(_safe_get(valuation_obj, "nav"))
                            # Use valuation date if available, else statement date
                            val_date_obj = _parse_date(_safe_get(valuation_obj, "date"))
                        else:
                            val_amt = _parse_decimal(valuation_obj)
                            
                        # Ensure units is a decimal
                        units = _parse_decimal(close_bal)
                        
                        if units > 0 or val_amt > 0:
                            # Final date determination
                            final_val_date = val_date_obj or val_date # val_date is from statement_period
                            final_date_str = final_val_date.strftime("%Y-%m-%d") if final_val_date else val_date_str
                            
                            transactions = [{
                                "date": final_date_str,
                                "description": "Balance from CAS (History not available)",
                                "amount": val_amt,
                                "units": units,
                                "nav": val_nav or _parse_decimal(_safe_get(scheme, "nav")),
                                "type": "PURCHASE",
                                "is_synthesized": True
                            }]
                    
                    for t_item in transactions:
                        txn = _safe_to_dict(t_item)
                        raw_date = _safe_get(txn, "date")
                        if not raw_date: continue
                        
                        t_date = _parse_date(raw_date)
                        if not t_date: continue

                        desc = _safe_get(txn, "description", "")
                        # Filter noise
                        if any(x in desc for x in ["Stamp Duty", "STT", "Tax"]): continue

                        t_raw = str(_safe_get(txn, "type", "")).upper()
                        desc_upper = desc.upper()
                        
                        # Use robust decimal parsing
                        amt = _parse_decimal(_safe_get(txn, "amount", 0))
                        units_val = _parse_decimal(_safe_get(txn, "units", 0))
                        nav_val = _parse_decimal(_safe_get(txn, "nav", 0))
                        
                        # Categorization logic (Keep in sync with MutualFundService)
                        withdrawal_keywords = ["SELL", "CREDIT", "REDEMP", "PAYOUT", "OUT", "SWITCH-OUT", "STP-OUT"]
                        
                        t_type = "BUY"
                        if any(x in t_raw for x in withdrawal_keywords) or any(x in desc_upper for x in ["REDEMPTION", "SWITCH-OUT", "STP-OUT"]) or amt < 0:
                            t_type = "SELL"
                        
                        flattened_transactions.append({
                            "date": t_date,
                            "type": t_type,
                            "amount": abs(amt),
                            "units": abs(units_val),
                            "nav": nav_val,
                            "scheme_name": s_name,
                            "folio_number": f_num,
                            "amfi": _safe_get(scheme, "amfi"),
                            "isin": _safe_get(scheme, "isin"),
                            "description": desc,
                            "raw_message": f"{s_name} | {desc}",
                            "external_id": str(_safe_get(txn, "external_id") or _safe_get(txn, "ref_id") or ""),
                            "is_synthesized": bool(_safe_get(txn, "is_synthesized", False))
                        })
            
            # Success
        except Exception as e:
            raise e
        finally:
            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except: pass
                
        return flattened_transactions
