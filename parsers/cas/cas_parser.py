from typing import List, Dict, Any
import logging
import tempfile
import os
from datetime import datetime, date
from decimal import Decimal
from parser.core import timezone

logger = logging.getLogger(__name__)

class CasParser:
    """
    Wrapper around casparser library to parse Mutual Fund CAS PDFs.
    """
    
    @staticmethod
    def parse(file_bytes: bytes, password: str) -> List[Dict[str, Any]]:
        flattened_transactions = []
        
        def safe_to_dict(obj):
            if obj is None: return None
            # Core types to preserve
            if isinstance(obj, (int, float, str, bool, Decimal, datetime, date)): return obj
            
            if isinstance(obj, list): return [safe_to_dict(i) for i in obj]
            if isinstance(obj, dict): return {k: safe_to_dict(v) for k, v in obj.items()}
            
            # Pydantic or library objects
            for method in ["model_dump", "dict", "to_dict"]:
                if hasattr(obj, method):
                    try:
                        return safe_to_dict(getattr(obj, method)())
                    except: continue
            
            if hasattr(obj, "__dict__"):
                return safe_to_dict(vars(obj))
            return obj

        def safe_get(obj, key, default=None):
            if obj is None: return default
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

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
                {"output": "dict"}, 
                {"output": "dict", "force_pdfminer": True}, 
                {}
            ]
            
            for strategy in strategies:
                try:
                    data = casparser.read_cas_pdf(temp_path, password, **strategy)
                    if data: 
                        break
                except Exception as e:
                    continue
            
            if not data:
                raise ValueError("CAS Parsing failed. Ensure the password is correct and the PDF is a valid Consolidated Account Statement.")

            # Force deep conversion to ensure we only have primitive types/dicts
            data = safe_to_dict(data)

            # 3. Handle Depository Statements (NSDL/CDSL) which use 'accounts' instead of 'folios'
            if "accounts" in data and not data.get("folios"):
                logger.debug("Normalizing depository account structure...")
                folios = []
                # Statement period for virtual transaction dates
                period = data.get("statement_period", {})
                val_date_str = period.get("to") or period.get("from") or timezone.utcnow().strftime("%Y-%m-%d")
                
                for acc in data.get("accounts", []):
                    mf_list = acc.get("mutual_funds", [])
                    if not mf_list: continue
                    
                    folio_id = acc.get("client_id") or acc.get("dp_id") or "Demat-Folio"
                    schemes = []
                    for mf in mf_list:
                        schemes.append({
                            "scheme": mf.get("name") or "Unknown Scheme",
                            "isin": mf.get("isin"),
                            "amfi": mf.get("amfi"),
                            "transactions": [{
                                "date": val_date_str,
                                "description": "Current balance from depository statement",
                                "amount": float(mf.get("value") or 0),
                                "units": float(mf.get("balance") or 0),
                                "nav": float(mf.get("nav") or 0),
                                "type": "PURCHASE" # Default to purchase for net-new entry
                            }]
                        })
                    folios.append({"folio": folio_id, "schemes": schemes})
                data["folios"] = folios

            # 4. Process into WealthFam internal transaction schema
            folios = safe_get(data, "folios", [])
            
            for f_idx, f_item in enumerate(folios):
                folio = safe_to_dict(f_item)
                f_num = safe_get(folio, "folio") or safe_get(folio, "folio_no") or "Unknown"
                schemes = safe_get(folio, "schemes", [])
                
                for s_idx, s_item in enumerate(schemes):
                    scheme = safe_to_dict(s_item)
                    s_name = safe_get(scheme, "scheme", "Unknown Scheme")
                    transactions = safe_get(scheme, "transactions", [])
                    

                    # If no transactions but there is a balance, synthesize a virtual transaction
                    if not transactions:
                        units = safe_get(scheme, "close") or safe_get(scheme, "balance") or 0
                        # Valuation can be a float or a dict {'value': ..., 'cost': ...}
                        valuation_obj = safe_get(scheme, "valuation") or 0
                        valuation = 0.0
                        if isinstance(valuation_obj, dict):
                            valuation = float(safe_get(valuation_obj, "value") or 0)
                        else:
                            valuation = float(valuation_obj)
                            
                        # Ensure units is a float
                        try: units = float(units)
                        except: units = 0.0
                        
                        if units > 0 or valuation > 0:
                            # Statement period for virtual transaction dates
                            # Statement period for virtual transaction dates
                            period = data.get("statement_period", {})
                            val_date_str = period.get("to") or period.get("from") or timezone.utcnow().strftime("%Y-%m-%d")
                            
                            transactions = [{
                                "date": val_date_str,
                                "description": "Balance from CAS (History not available)",
                                "amount": valuation,
                                "units": units,
                                "nav": float(safe_get(scheme, "nav") or 0),
                                "type": "PURCHASE",
                                "is_synthesized": True
                            }]
                    
                    for t_item in transactions:
                        txn = safe_to_dict(t_item)
                        raw_date = safe_get(txn, "date")
                        if not raw_date: continue
                        
                        t_date = None
                        try:
                            if isinstance(raw_date, (datetime, date)): 
                                t_date = raw_date
                            else:
                                # Flexible string parsing for various CAS formats
                                date_str = str(raw_date).strip()
                                for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %b %Y"):
                                    try:
                                        t_date = datetime.strptime(date_str, fmt)
                                        break
                                    except: continue
                        except: continue

                        if not t_date: continue

                        desc = safe_get(txn, "description", "")
                        # Filter noise
                        if any(x in desc for x in ["Stamp Duty", "STT", "Tax"]): continue

                        t_raw = str(safe_get(txn, "type", "")).upper()
                        amt = float(safe_get(txn, "amount", 0) or 0)
                        
                        t_type = "BUY"
                        if any(x in t_raw for x in ["REDEMPTION", "SWITCH OUT"]) or amt < 0:
                            t_type = "SELL"
                        
                        flattened_transactions.append({
                            "date": t_date,
                            "type": t_type,
                            "amount": abs(amt),
                            "units": abs(float(safe_get(txn, "units", 0) or 0)),
                            "nav": float(safe_get(txn, "nav", 0) or 0),
                            "scheme_name": s_name,
                            "folio_number": f_num,
                            "amfi": safe_get(scheme, "amfi"),
                            "isin": safe_get(scheme, "isin"),
                            "description": desc,
                            "raw_message": f"{s_name} | {desc}",
                            "external_id": str(safe_get(txn, "external_id") or safe_get(txn, "ref_id") or ""),
                            "is_synthesized": bool(safe_get(txn, "is_synthesized", False))
                        })
            
            # Success
        finally:
            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except: pass
                
        return flattened_transactions
