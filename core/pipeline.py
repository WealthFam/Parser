import logging
import hashlib
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Optional, Any, List

from sqlalchemy.orm import Session
from rapidfuzz import fuzz

from parser.core import timezone
from parser.core.classifier import FinancialClassifier
from parser.core.normalizer import MerchantNormalizer
from parser.core.validator import TransactionValidator
from parser.core.guesser import CategoryGuesser
from parser.db.models import RequestLog, PatternRule, AICallCache
from parser.schemas.transaction import Transaction, IngestionResult, ParsedItem, AccountInfo, MerchantInfo, TransactionType, TransactionMeta
from parser.parsers.registry import ParserRegistry
from parser.parsers.patterns.regex_engine import PatternParser
from parser.parsers.ai.gemini_parser import GeminiParser

class AIGuardrailBlocked(Exception):
    """Exception raised when content is blocked by AI Guardrail heuristics."""
    pass

logger = logging.getLogger(__name__)

def get_decimal(val):
    """Utility to safely convert string/float/int to Decimal by cleaning non-numeric characters."""
    try:
        # Remove commas/currency symbols but keep dot and minus
        clean_val = "".join(c for c in str(val) if c.isdigit() or c in ".-") if val is not None else "0"
        return Decimal(clean_val or "0")
    except:
        return Decimal("0")

def get_digits(s):
    """Extract digits from a string and return the last 4."""
    return "".join(filter(str.isdigit, str(s or "")))[-4:]

class IngestionPipeline:

    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    def process_cas_data(self, data: List[dict]) -> List[ParsedItem]:
        """
        Process raw CAS dictionary data into a list of WealthFam ParsedItems.
        Adheres to WealthFam service patterns by ensuring Decimal precision and robust schema mapping.
        """
        results = []
        for t_dict in data:
            try:
                # 1. Convert to internal transaction schema (handles dates and categorization)
                t = self._convert_to_schema_txn(t_dict)
                
                # 2. Extract and sanitize metadata
                item = ParsedItem(
                    status="extracted",
                    transaction=t,
                    metadata=TransactionMeta(
                        confidence=1.0,
                        parser_used="CasParser",
                        source_original="CAS",
                        units=get_decimal(t_dict.get("units") or t_dict.get("balance")),
                        nav=get_decimal(t_dict.get("nav")),
                        amfi=str(t_dict.get("amfi") or ""),
                        isin=str(t_dict.get("isin") or ""),
                        is_synthesized=bool(t_dict.get("is_synthesized", False))
                    )
                )
                results.append(item)
            except Exception as e:
                logger.error(f"Failed to process CAS item: {e}")
                continue
        return results

    def _convert_to_schema_txn(self, pt: Any, date_hint: Optional[Any] = None) -> Transaction:
        """Helper to convert backend-style ParsedTransaction or dict to microservice Transaction"""
        
        
        # If it's already a Transaction object (from AI or Pattern parser)
        if isinstance(pt, Transaction):
            return pt
            
        # If it's a dict or dict-like (from UniversalParser or legacy CAS logs)
        if isinstance(pt, dict) or hasattr(pt, 'amount') or hasattr(pt, 'date'):
            def safe_pt_get(key, default=None):
                if isinstance(pt, dict):
                    return pt.get(key, default)
                return getattr(pt, key, default)


            raw_type = str(safe_pt_get("type") or "DEBIT").upper()
            txn_type = TransactionType.DEBIT
            if raw_type in ["CREDIT", "SELL", "REDEMPTION"]:
                txn_type = TransactionType.CREDIT
            elif raw_type in ["DEBIT", "BUY", "INVESTMENT"]:
                txn_type = TransactionType.DEBIT

            # Merchant mapping
            m_raw = safe_pt_get("description") or safe_pt_get("scheme_name") or "Unknown"
            m_clean = safe_pt_get("recipient") or safe_pt_get("scheme_name") or m_raw

            # Handle date parsing
            pt_date = pt.get("date") if isinstance(pt, dict) else getattr(pt, "date", None)
            
            # Use hint as context if we only have a date string but no year, 
            # or if the parser failed to return a valid date.
            final_date = None
            if isinstance(pt_date, str):
                try:
                    final_date = datetime.fromisoformat(pt_date)
                except: pass
            elif isinstance(pt_date, (datetime, date)):
                # Synchronize to datetime if it's just a date
                if isinstance(pt_date, datetime):
                    final_date = pt_date
                else:
                    final_date = datetime.combine(pt_date, datetime.min.time())
            
            # Fallback to date_hint if available, otherwise now
            if not final_date:
                if date_hint:
                    if isinstance(date_hint, datetime):
                        final_date = date_hint
                    else:
                        try: final_date = datetime.fromisoformat(date_hint)
                        except: final_date = timezone.utcnow()
                else:
                    final_date = timezone.utcnow()

            return Transaction(
                amount=get_decimal(safe_pt_get("amount")),
                type=txn_type,
                date=final_date,
                account=AccountInfo(mask=safe_pt_get("account_mask") or safe_pt_get("folio_number") or safe_pt_get("external_id")),
                merchant=MerchantInfo(raw=m_raw, cleaned=m_clean),
                description=safe_pt_get("description") or safe_pt_get("scheme_name"),
                ref_id=safe_pt_get("external_id") or safe_pt_get("ref_id") or safe_pt_get("folio_number"),
                balance=Decimal(str(safe_pt_get("balance"))) if safe_pt_get("balance") else None,
                category=safe_pt_get("category") or ("Mutual Fund" if safe_pt_get("scheme_name") else None),
                recipient=safe_pt_get("recipient") or safe_pt_get("scheme_name"),
                raw_message=safe_pt_get("raw_message") or (
                    getattr(safe_pt_get("original_row"), "description", "Imported") if safe_pt_get("original_row") and not isinstance(safe_pt_get("original_row"), dict) 
                    else (safe_pt_get("original_row") or {}).get("description", "Imported") if isinstance(safe_pt_get("original_row"), dict) 
                    else "Imported"
                ),
                confidence=float(safe_pt_get("confidence") or 0.0)
            )
            
        # If it's a backend ParsedTransaction
        return Transaction(
            amount=pt.amount,
            type=TransactionType.DEBIT if pt.type == "DEBIT" else TransactionType.CREDIT,
            date=pt.date,
            account=AccountInfo(mask=pt.account_mask),
            merchant=MerchantInfo(raw=pt.recipient or pt.description, cleaned=pt.recipient or pt.description),
            description=pt.description,
            ref_id=pt.ref_id,
            balance=pt.balance,
            category=pt.category,
            recipient=pt.recipient,
            raw_message=pt.raw_message,
            confidence=pt.confidence
        )

    def _save_ai_pattern(self, source: str, regex: str, mapping: dict, confidence: float):
        """Save high-confidence AI suggested patterns to the database."""
        try:
            # Check if this pattern already exists to avoid duplicates
            existing = self.db.query(PatternRule).filter(
                PatternRule.tenant_id == self.tenant_id,
                PatternRule.source == source,
                PatternRule.regex_pattern == regex
            ).first()
            
            if not existing:
                new_rule = PatternRule(
                    tenant_id=self.tenant_id,
                    source=source,
                    regex_pattern=regex,
                    mapping_json=mapping,
                    is_ai_generated=True,
                    confidence=confidence,
                    is_active=True
                )
                self.db.add(new_rule)
                self.db.commit()
        except Exception as e:
            logger.error(f"Error saving AI pattern: {e}")
            self.db.rollback()

    def run(self, content: str, source: str, sender: Optional[str] = None, subject: Optional[str] = None, date_hint: Optional[str] = None) -> IngestionResult:
        # 1. Idempotency Check
        input_hash = hashlib.sha256(f"{self.tenant_id}:{source}:{content}".encode()).hexdigest()
        
        # Check last 5 mins
        cutoff = timezone.utcnow() - timedelta(minutes=5)
        existing = self.db.query(RequestLog).filter(
            RequestLog.tenant_id == self.tenant_id,
            RequestLog.input_hash == input_hash,
            RequestLog.created_at >= cutoff
        ).first()

        if existing:
            return IngestionResult(status="duplicate_submission", results=[], logs=["Duplicate submission detected"])

        # Create Log Entry
        log = RequestLog(
            tenant_id=self.tenant_id,
            input_hash=input_hash, 
            source=source, 
            input_payload={"content": content, "sender": sender, "subject": subject, "date_hint": date_hint}, 
            status="processing"
        )
        self.db.add(log)
        self.db.commit()

        logs = []

        # 2. Classification
        if not FinancialClassifier.is_financial(content, source):
            log.status = "ignored"
            self.db.commit()
            return IngestionResult(status="ignored", results=[], logs=["Classified as non-financial"])

        # 3. Extraction Chain
        potential_matches: list[Any] = [] # List of ParsedTransaction
        parsed_txn = None
        
        # A. Static Bank Parsers
        parsers = ParserRegistry.get_sms_parsers() if source == "SMS" else ParserRegistry.get_email_parsers()
        for p in parsers:
            can_handle = False
            try:
                if source == "SMS":
                    can_handle = p.can_handle(sender or "", content)
                else:
                    can_handle = p.can_handle(subject or "", content)
            except Exception as e:
                logs.append(f"can_handle failed for {type(p).__name__}: {str(e)}")
                
            if can_handle:
                try:
                    # Try confidence-based parsing first
                    if hasattr(p, 'parse_with_confidence'):
                        matches = p.parse_with_confidence(content, date_hint=date_hint)
                        if matches:
                            potential_matches.extend(matches)
                    
                    # Fallback to legacy parse if no matches yet (for parsers not yet refactored)
                    if not potential_matches:
                        if hasattr(p, 'parse') and 'date_hint' in p.parse.__code__.co_varnames:
                            pt = p.parse(content, date_hint=date_hint)
                        else:
                            pt = p.parse(content)
                        if pt:
                            # Assign a default confidence if missing
                            if not hasattr(pt, 'confidence') or pt.confidence is None:
                                pt.confidence = 0.8
                            potential_matches.append(pt)
                except Exception as e:
                    logs.append(f"Parser {type(p).__name__} failed: {str(e)}")

        # B. User Patterns (as a fallback or secondary engine)
        if not potential_matches or max(m.confidence for m in potential_matches) < 0.9:
            try:
                p_parser = PatternParser(self.db, source, tenant_id=self.tenant_id)
                pt = p_parser.parse(content)
                if pt:
                    if not hasattr(pt, 'confidence') or pt.confidence is None:
                        pt.confidence = 0.9  # Learned patterns are actually quite high confidence
                    potential_matches.append(pt)
            except Exception as e:
                logs.append(f"Pattern Parser failed: {str(e)}")

        # C. AI Fallback (Trigger if no high-confidence match)
        best_regex_match = None
        if potential_matches:
            potential_matches.sort(key=lambda x: x.confidence, reverse=True)
            best_regex_match = potential_matches[0]

        if not best_regex_match or best_regex_match.confidence < 0.9:
            try:
                logs.append(f"Low confidence ({best_regex_match.confidence if best_regex_match else 0}) - Triggering AI Fallback")
                
                ai_data = None
                
                # Check permanent AI Cache first
                cached = self.db.query(AICallCache).filter(
                    AICallCache.tenant_id == self.tenant_id,
                    AICallCache.content_hash == input_hash
                ).first()
                
                if cached:
                    logs.append("Reusing cached AI response")
                    ai_data = cached.response_json
                else:
                    # --- AI GUARDRAIL PRE-CHECK ---
                    from parser.core.ai_filter import AIGuardrail
                    if not AIGuardrail.should_allow_ai_parsing(content, source):
                        logs.append("Blocked by AI Guardrail: Content does not meet strict transaction heuristics.")
                        if best_regex_match:
                            parsed_txn = self._convert_to_schema_txn(best_regex_match, date_hint=date_hint)
                            parser_used = "Best Regex (AI Guardrail Blocked)"
                        else:
                            parsed_txn = None
                            parser_used = "None (Blocked)"
                        # Skip the entire AI section by raising an exception
                        raise AIGuardrailBlocked("Blocked by AIGuardrail to protect Gemini quota")
                    
                    ai_parser = GeminiParser(self.db, tenant_id=self.tenant_id)
                    ai_data = ai_parser.parse_with_pattern(content, source, date_hint=date_hint)
                
                if ai_data and ai_data.get("transaction"):
                    tx_data = ai_data["transaction"]
                    

                    raw_type = str(tx_data.get("type") or "DEBIT").upper()
                    try:
                        txn_type = TransactionType(raw_type)
                    except:
                        txn_type = TransactionType.DEBIT

                    # Create Transaction object
                    pt = Transaction(
                        amount=get_decimal(tx_data.get("amount")),
                        type=txn_type,
                        date=datetime.fromisoformat(tx_data["date"]) if tx_data.get("date") else timezone.utcnow(),
                        account=AccountInfo(mask=tx_data.get("account_mask"), provider=source),
                        merchant=MerchantInfo(raw=tx_data.get("merchant"), cleaned=tx_data.get("merchant")),
                        description=tx_data.get("description", content[:50]),
                        recipient=tx_data.get("merchant"),
                        raw_message=content,
                        confidence=float(tx_data.get("confidence", 0.9))
                    )

                    ai_confidence = pt.confidence
                    
                    # --- AI RESPONSE CACHE ---
                    # Save a permanent copy of the AI response to avoid re-parsing
                    try:
                        new_cache = AICallCache(
                            tenant_id=self.tenant_id,
                            content_hash=input_hash,
                            source=source,
                            response_json=ai_data
                        )
                        self.db.add(new_cache)
                        self.db.commit()
                        logs.append("AI response saved to cache")
                    except Exception as e:
                        logger.error(f"Error saving AI cache: {e}")
                        self.db.rollback()

                    # Logic to save the new pattern if it's very high confidence
                    suggested_regex = ai_data.get("suggested_regex")
                    field_mapping = ai_data.get("field_mapping")
                    
                    if suggested_regex and field_mapping and ai_confidence >= 0.95:
                        self._save_ai_pattern(source, suggested_regex, field_mapping, ai_confidence)
                        logs.append(f"AI suggested new pattern for {source} - Saved for future use")

                    # Compare AI results with best regex match
                    if not best_regex_match or ai_confidence >= best_regex_match.confidence:
                        parsed_txn = self._convert_to_schema_txn(pt, date_hint=date_hint)
                        parser_used = "Gemini AI"
                        logs.append("Extracted via AI (High Confidence)")
                    else:
                        parsed_txn = self._convert_to_schema_txn(best_regex_match, date_hint=date_hint)
                        parser_used = "Best Regex"
                else:
                    if ai_data and ai_data.get("error"):
                        logs.append(f"AI Error: {ai_data['error']}")

                    if best_regex_match:
                        parsed_txn = self._convert_to_schema_txn(best_regex_match, date_hint=date_hint)
                        parser_used = "Best Regex (AI Failed)"
            except AIGuardrailBlocked:
                pass
            except Exception as e:
                logger.error(f"AI Parser failed: {str(e)}")
                logs.append(f"AI Parser failed: {str(e)}")
                if best_regex_match:
                    parsed_txn = self._convert_to_schema_txn(best_regex_match, date_hint=date_hint)
                    parser_used = "Best Regex (AI Error)"
        else:
            parsed_txn = self._convert_to_schema_txn(best_regex_match, date_hint=date_hint)
            parser_used = f"{best_regex_match.source if hasattr(best_regex_match, 'source') else 'Regex'} ({best_regex_match.confidence})"


        # 4. Normalization & Validation
        if parsed_txn:
             
             
             # Normalize Merchant
             if parsed_txn.merchant:
                 # Use the recipient (if extracted) as the seed for normalization/aliasing
                 # Otherwise fallback to raw description
                 name_seed = parsed_txn.recipient or parsed_txn.merchant.raw
                 parsed_txn.merchant.cleaned = MerchantNormalizer.normalize(name_seed, db=self.db, tenant_id=self.tenant_id)
                 
                 # Update description if it was raw/missing
                 if not parsed_txn.description or parsed_txn.description == parsed_txn.merchant.raw:
                     parsed_txn.description = parsed_txn.merchant.cleaned
             
             # Enrich Time
             TransactionValidator.enrich_time(parsed_txn)
             
             # Validate
             warnings = TransactionValidator.validate(parsed_txn, content)
             if warnings:
                 logs.extend(warnings)

             # 5. Category Hint
             if not parsed_txn.category:
                parsed_txn.category = CategoryGuesser.guess(parsed_txn.merchant.cleaned, parsed_txn.description)

             # 6. Cross-Source Deduplication (New Robust Feature)
             # Check if this EXACT transaction details appeared from another source recently
             # We check logs in the last 15 minutes for similar records
             duplicate_window = timezone.utcnow() - timedelta(minutes=15)
             
             # Search previously successful extractions
             # Note: output_payload is stored as JSON in DuckDB
             # Using a slightly fuzzy match: same amount, mask, and merchant
             # Due to DuckDB JSON limitations, we fetch and filter in Python for robustness
             recent_successes = self.db.query(RequestLog).filter(
                 RequestLog.tenant_id == self.tenant_id,
                 RequestLog.status == "success",
                 RequestLog.created_at >= duplicate_window,
                 RequestLog.input_hash != input_hash # Not ourselves
             ).all()

             
             is_cross_duplicate = False
             for rs in recent_successes:
                 try:
                     payload = rs.output_payload or {}
                     
                     # Extract list of transactions from payload
                     prev_txns = []
                     if "results" in payload:
                         for item in payload["results"]:
                             if item.get("transaction"):
                                 prev_txns.append(item["transaction"])
                     elif "transaction" in payload:
                         prev_txns.append(payload["transaction"])
                     
                     for prev_data in prev_txns:
                         # Comparison criteria
                         same_amt = Decimal(str(prev_data["amount"])) == parsed_txn.amount
                         
                         # Check Reference ID if both have it
                         prev_ref = str(prev_data.get("ref_id") or "").strip().lstrip('0')
                         curr_ref = str(parsed_txn.ref_id or "").strip().lstrip('0')
                         same_ref = prev_ref == curr_ref if (prev_ref and curr_ref) else False
                         
                         if same_ref: # High confidence match
                              is_cross_duplicate = True
                              logs.append(f"Matching Ref ID detected from {rs.id}")
                              break

                         # Robust mask check: compare last 4 digits
                         prev_mask = get_digits(prev_data.get("account", {}).get("mask"))
                         curr_mask = get_digits(parsed_txn.account.mask if parsed_txn.account else "")
                         same_mask = prev_mask == curr_mask if (prev_mask and curr_mask) else False
                         
                         # Fuzzy merchant match
                         m_prev = (prev_data.get("merchant", {}).get("cleaned") or prev_data.get("description") or "")
                         m_curr = parsed_txn.merchant.cleaned or parsed_txn.description or ""
                         same_merchant = fuzz.partial_ratio(m_prev, m_curr) > 90
                         
                         # Same Type (Debit vs Credit)
                         same_type = prev_data.get("type") == parsed_txn.type
                         
                         if same_amt and same_mask and same_merchant and same_type:
                             is_cross_duplicate = True
                             logs.append(f"Cross-source duplicate detected from {rs.id}")
                             break
                     
                     if is_cross_duplicate:
                         break
                 except: continue

             if is_cross_duplicate:
                 log.status = "success"
                 item = ParsedItem(
                    status="cross_source_duplicate",
                    transaction=parsed_txn,
                    metadata={"confidence": 1.0, "parser_used": "Deduplicator", "source_original": source}
                 )
                 log.output_payload = item.model_dump(mode='json')
                 self.db.commit()
                 return IngestionResult(status="success", results=[item], logs=logs)

             item = ParsedItem(
                status="extracted",
                transaction=parsed_txn,
                metadata={"confidence": 0.5 if parser_used == "User Patterns" else 1.0, 
                          "parser_used": parser_used, 
                          "source_original": source}
            )
            
             # Update Log
             log.status = "success"
             log.output_payload = item.model_dump(mode='json')
             self.db.commit()
            
             return IngestionResult(status="success", results=[item], logs=logs)

        # Failed
        log.status = "failed"
        self.db.commit()
        return IngestionResult(status="failed", results=[], logs=logs + ["No parser matched"])
