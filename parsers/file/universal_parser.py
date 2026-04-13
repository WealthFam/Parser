import pandas as pd
import io
from typing import List, Dict, Optional, Any
from datetime import datetime
from parser.parsers.utils.recipient_parser import RecipientParser

class UniversalParser:
    @staticmethod
    def analyze(file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Analyze file to detect header row and return preview.
        """
        try:
            # Read first 30 rows raw (no header) to scan
            if filename.lower().endswith('.csv'):
                # Read with 'header=None' to get raw rows
                df_raw = pd.read_csv(io.BytesIO(file_content), header=None, nrows=30)
            elif filename.lower().endswith(('.xls', '.xlsx')):
                df_raw = pd.read_excel(io.BytesIO(file_content), header=None, nrows=30)
            else:
                raise ValueError("Unsupported file format")
            
            # Heuristic: Count intersections with keywords
            keywords = {'date', 'txn', 'transaction', 'valuedate', 'description', 
                        'desc', 'particulars', 'narration', 'remark', 'amount', 
                        'debit', 'credit', 'dr', 'cr', 'balance', 'bal', 'limit', 'ref'}
            
            best_idx = 0
            max_score = 0
            detected_headers = []
            
            for idx, row in df_raw.iterrows():
                # Convert row values to string and lower
                row_str = [str(val).lower().strip() for val in row.values if pd.notna(val)]
                score = len(set(row_str).intersection(keywords))
                
                # Bonus for 'date' and 'amount' as they are critical
                if any('date' in s for s in row_str): score += 1
                if any('amount' in s or 'debit' in s for s in row_str): score += 1
                
                if score > max_score:
                    max_score = score
                    best_idx = idx
                    # Original values as headers
                    detected_headers = [str(val).strip() for val in row.values if pd.notna(val)]

            # Fallback: if score is too low, might be index 0
            if max_score < 1:
                best_idx = 0
                detected_headers = [str(val).strip() for val in df_raw.iloc[0].values if pd.notna(val)]

            # Get Preview Data (next 3 rows) using the detected header
            # Re-read or just slice if we had enough data? 
            # Better to re-read sample properly with 'header' set
            if filename.lower().endswith('.csv'):
                df_preview = pd.read_csv(io.BytesIO(file_content), header=best_idx, nrows=5)
            else:
                df_preview = pd.read_excel(io.BytesIO(file_content), header=best_idx, nrows=5)
            
            # Sanitize preview for JSON
            preview_rows = []
            for _, split_row in df_preview.iterrows():
                # specific to json serialization (handle NaNs, timestamps)
                clean_row = {}
                for col in df_preview.columns:
                    val = split_row[col]
                    if pd.isna(val): val = ""
                    elif isinstance(val, (datetime, pd.Timestamp)): val = str(val)
                    clean_row[str(col)] = val
                preview_rows.append(clean_row)

            return {
                "header_row_index": int(best_idx),
                "headers": detected_headers,
                "preview": preview_rows
            }

        except Exception as e:
            raise ValueError(f"Analysis failed: {str(e)}")

    @staticmethod
    def parse(file_content: bytes, filename: str, mapping: Dict[str, str], header_row_index: int = 0, password: Optional[str] = None) -> List[dict]:
        """
        Parse CSV/Excel content using pandas.
        """
        try:
            # Detect format
            if filename.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(io.BytesIO(file_content), header=header_row_index, on_bad_lines='skip')
                except pd.errors.ParserError:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8-sig', header=header_row_index, on_bad_lines='skip')
            elif filename.lower().endswith(('.xls', '.xlsx')):
                df = pd.read_excel(io.BytesIO(file_content), header=header_row_index)
            else:
                raise ValueError("Unsupported file format")

            # Remove rows where ALL columns are NaN
            df.dropna(how='all', inplace=True)
            
            # Normalize Headers (strip whitespace)
            df.columns = df.columns.astype(str).str.strip()
            
            parsed_rows = []
            skipped_rows = []  # Track skipped rows for debugging
            
            # Helper to safely get value
            def get_val(row, col):
                if col not in row: return None # Handle case mapping/header mismatch
                if pd.isna(row.get(col)): return None
                return row.get(col)
            
            for idx, row in df.iterrows():
                try:
                    # --- JUNK FILTER ---
                     # Skip rows that are separators (e.g. ******* or --------)
                    row_vals = [str(v) for v in row.values if pd.notna(v)]
                    row_text = "".join(row_vals)
                    if not row_text.strip(): 
                        skipped_rows.append(f"Row {idx+1}: Empty row")
                        continue
                    
                    # If predominantly symbolic
                    if all(c in {'*', '-', '=', '_', ' '} for c in row_text):
                        skipped_rows.append(f"Row {idx+1}: Separator row")
                        continue
                    # -------------------

                    # 1. Date
                    date_col = mapping.get('date')
                    raw_date = get_val(row, date_col)
                    if not raw_date:
                        skipped_rows.append(f"Row {idx+1}: Missing date column '{date_col}'")
                        continue
                    
                    date_obj = UniversalParser._parse_date(raw_date)
                    if not date_obj:
                        skipped_rows.append(f"Row {idx+1}: Could not parse date '{raw_date}'")
                        continue

                    # 2. Description
                    desc_col = mapping.get('description')
                    desc = str(get_val(row, desc_col) or "No Description")

                    # 3. Amount
                    amount = 0.0
                    txn_type = "DEBIT"

                    if 'amount' in mapping:
                        amt_col = mapping['amount']
                        raw_amt = get_val(row, amt_col)
                        amount = UniversalParser._parse_amount(raw_amt)
                        if amount < 0:
                             txn_type = "DEBIT"
                        else:
                             txn_type = "CREDIT"
                    elif 'debit' in mapping and 'credit' in mapping:
                        raw_debit = get_val(row, mapping['debit'])
                        raw_credit = get_val(row, mapping['credit'])
                        
                        debit = abs(UniversalParser._parse_amount(raw_debit))
                        credit = abs(UniversalParser._parse_amount(raw_credit))
                        
                        if debit > 0:
                            amount = -debit
                            txn_type = "DEBIT"
                        elif credit > 0:
                            amount = credit
                            txn_type = "CREDIT"
                        else:
                            # Both columns are 0 or empty - skip this row
                            skipped_rows.append(f"Row {idx+1}: Both debit and credit are zero")
                            continue
                    
                    # Skip if amount is still 0 in single-column mode (likely parsing failed)
                    if 'amount' in mapping and amount == 0:
                        skipped_rows.append(f"Row {idx+1}: Amount is zero or failed to parse")
                        continue
                    
                    #  4. Extract Recipient from description
                    recipient = RecipientParser.extract(desc, source_type="FILE")
                    
                    # 5. Reference (External ID)
                    external_id = None
                    ref_col = mapping.get('reference') or mapping.get('ref')
                    if ref_col:
                        raw_ref = get_val(row, ref_col)
                        if raw_ref and str(raw_ref).strip():
                             external_id = str(raw_ref).strip()
                             # Normalize: remove leading zeros if numeric
                             if external_id.isdigit():
                                 external_id = external_id.lstrip('0')
                    
                    # 6. Balance & Credit Limit
                    balance_val = None
                    bal_col = mapping.get('balance') or mapping.get('bal')
                    if bal_col:
                        balance_val = UniversalParser._parse_amount(get_val(row, bal_col))
                    
                    limit_val = None
                    limit_col = mapping.get('credit_limit') or mapping.get('limit')
                    if limit_col:
                        limit_val = UniversalParser._parse_amount(get_val(row, limit_col))
                    
                    # Store as simple dict for JSON response
                    parsed_rows.append({
                        "date": date_obj.isoformat(),
                        "description": desc,
                        "recipient": recipient,
                        "amount": amount,
                        "type": txn_type,
                        "ref_id": external_id, # Return as ref_id to match Parser contract
                        "balance": balance_val,
                        "credit_limit": limit_val,
                        "raw_message": " | ".join([f"{k}: {v}" for k, v in row.to_dict().items() if pd.notna(v)]),
                        "original_row": {str(k): str(v) for k, v in row.to_dict().items()} # Serialize
                    })

                except Exception as e:
                    skipped_rows.append(f"Row {idx+1}: Exception - {str(e)}")
                    continue
            
            return parsed_rows, skipped_rows


        except Exception as e:
            raise ValueError(f"Failed to parse file: {str(e)}")

    @staticmethod
    def _parse_date(val: Any) -> Optional[datetime]:
        if pd.isna(val): return None
        
        # If already datetime (Excel often parses it)
        if isinstance(val, (datetime, pd.Timestamp)):
            return val
            
        date_str = str(val).strip()
        
        # Priority formats (Day-first common in India)
        formats = [
            "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d %b %Y",
            "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"
        ]
        
        # Try exact formats first
        for fmt in formats:
            try:
                # Handle cases with time (e.g. 13-01-2026 14:30)
                if ' ' in date_str and ':' in date_str:
                    return datetime.strptime(date_str, f"{fmt} %H:%M:%S")
                return datetime.strptime(date_str, fmt)
            except ValueError:
                try:
                    if ' ' in date_str and ':' in date_str:
                        return datetime.strptime(date_str, f"{fmt} %H:%M")
                except ValueError:
                    continue
                    
        # Try pandas to_datetime as fallback with dayfirst=True
        try:
             dt = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
             if pd.isna(dt): return None
             return dt.to_pydatetime()
        except:
             return None

    @staticmethod
    def _parse_amount(val) -> float:
        """
        Parse any format of amount:
        - Handles commas (1,000.50)
        - Handle parentheses: (100) -> -100
        - Handle Dr/Cr suffixes: 100 Dr -> -100, 100 Cr -> 100
        - Handle trailing negative: 100.00- -> -100
        - Handle currency symbols
        """
        if val is None or (isinstance(val, str) and not val.strip()):
            return 0.0
        
        if isinstance(val, (int, float)): return float(val)
        
        amt_str = str(val).strip()
        if not amt_str: return 0.0
        
        # Track if negative
        is_negative = False
        
        # Handle parentheses: (100) -> -100
        if amt_str.startswith('(') and amt_str.endswith(')'):
            is_negative = True
            amt_str = amt_str[1:-1].strip()
            
        # Handle Dr/Cr suffixes
        amt_lower = amt_str.lower()
        if 'dr' in amt_lower:
            is_negative = True
            amt_str = amt_str.lower().replace('dr', '').strip()
        elif 'cr' in amt_lower:
            is_negative = False
            amt_str = amt_str.lower().replace('cr', '').strip()
        
        # Handle trailing negative sign (e.g. "100-" or "100.00-")
        if amt_str.endswith('-'):
            is_negative = True
            amt_str = amt_str[:-1].strip()
        
        # Handle leading negative sign
        if amt_str.startswith('-'):
            is_negative = True
            amt_str = amt_str[1:].strip()
        
        # Remove commas, spaces, and currency symbols
        # Keep only digits and decimal point
        amt_str = amt_str.replace(',', '').replace(' ', '')
        clean = ''.join(c for c in amt_str if c.isdigit() or c == '.')
        
        if not clean: return 0.0
        
        try:
            val_float = float(clean)
            return -val_float if is_negative else val_float
        except ValueError:
            return 0.0
    
