from parser.core import timezone
from google import genai
from google.genai import types, errors
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
import json
from parser.db.models import AIConfig
from parser.schemas.transaction import Transaction, TransactionType, AccountInfo, MerchantInfo
from datetime import datetime
from decimal import Decimal
import logging

import time

logger = logging.getLogger(__name__)

# Module-level circuit breaker state
_last_quota_error = 0
_last_ai_call = 0
_COOLDOWN_SECONDS = 60
_RATE_LIMIT_DELAY = 4.1

class GeminiParser:
    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.config = self._get_config()

    def _get_config(self) -> Optional[AIConfig]:
        return self.db.query(AIConfig).filter(AIConfig.tenant_id == self.tenant_id).first()

    def parse(self, content: str, source: str, date_hint: Optional[Any] = None) -> Optional[Transaction]:
        global _last_quota_error
        if time.time() - _last_quota_error < _COOLDOWN_SECONDS:
            return None

        if not self.config:
            return None
            
        if not self.config.is_enabled:
            return None
            
        if not self.config.api_key_enc:
            return None

        # New google-genai client
        client = genai.Client(api_key=self.config.api_key_enc)
        
        global _last_ai_call
        elapsed = time.time() - _last_ai_call
        if elapsed < _RATE_LIMIT_DELAY:
            time.sleep(_RATE_LIMIT_DELAY - elapsed)
        _last_ai_call = time.time()

        config = types.GenerateContentConfig(
            temperature=0.1,
            top_p=1,
            top_k=32,
            max_output_tokens=1024,
            response_mime_type="application/json",
        )

        model_id = self.config.model_name or "gemini-1.5-flash"

        # Determine reference date
        ref_date = timezone.utcnow()
        if date_hint and isinstance(date_hint, datetime):
            ref_date = date_hint
        elif date_hint and isinstance(date_hint, str):
             try:
                 ref_date = datetime.fromisoformat(date_hint)
             except: pass
             
        ref_date_str = ref_date.strftime('%Y-%m-%d')

        # Standard Prompt
        prompt = rf"""
        You are a precise financial parser. Extract transaction details from this {source} message.
        Return ONLY valid JSON.
        
        Input: "{content}"
        
        Required JSON Structure:
        {{
            "amount": float,
            "type": "DEBIT" or "CREDIT",
            "date": "YYYY-MM-DD",
            "currency": "INR" (default),
            "account_mask": "1234" (last 4 digits or null),
            "bank_name": "HDFC" (or null),
            "merchant": "Amazon" (clean name),
            "description": "raw description",
            "ref_id": "transaction reference/UTR number or null",
            "confidence": float (0.0 to 1.0, based on how certain you are of the extraction),
            "suggested_regex": "a Python regex to match this EXACT message format",
            "field_mapping": {{
                "amount": index of group,
                "date": index of group,
                "merchant": index of group,
                "account": index of group,
                "type": "DEBIT" or "CREDIT"
            }}
        }}
        
        Rules:
        1. If date is missing/relative (e.g. 'today', 'yesterday'), calculate it based on reference date: {ref_date_str}.
        2. ALWAYS return date in ISO format (YYYY-MM-DD). Convert '28-03-24' to '2024-03-28'.
        3. For 'merchant', extract the actual entity name (e.g. 'Uber', 'Zomato').
        4. If amount, type or date is missing, set confidence to 0.5 or lower.
        5. The `suggested_regex` should be generic enough to match similar messages (e.g. replace 123.45 with [\d,\.]+) but specific to this bank/type.
        6. In `field_mapping`, use 1-based indexing for the capture groups in your `suggested_regex`.
        7. If unable to extract strictly, return null.
        """

        try:
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=config
            )
            raw_text = response.text.strip()

            # More robust JSON cleaning
            cleaned_text = raw_text
            if "```json" in cleaned_text:
                cleaned_text = cleaned_text.split("```json")[1].split("```")[0].strip()
            elif "```" in cleaned_text:
                cleaned_text = cleaned_text.split("```")[1].split("```")[0].strip()

            try:
                data = json.loads(cleaned_text)
            except json.JSONDecodeError:
                # Fallback: escape single backslashes in regex/strings
                import re
                escaped_text = re.sub(r'(?<!\\)\\(?!["\\/bfnrtu])', r'\\\\', cleaned_text)
                data = json.loads(escaped_text)
            if not data: return None
            
            # Robust Date Parsing
            extracted_date = data.get("date")
            final_date = timezone.utcnow()
            if extracted_date:
                try:
                    if "-" in extracted_date and len(extracted_date) == 10:
                        final_date = datetime.strptime(extracted_date, "%Y-%m-%d")
                    else:
                        from dateutil import parser as date_parser
                        final_date = date_parser.parse(extracted_date)
                except:
                    final_date = timezone.utcnow()

            # Map to Schema
            return Transaction(
                amount=Decimal(str(data.get("amount", 0))),
                type=TransactionType(data.get("type", "DEBIT").upper()),
                date=final_date,
                currency=data.get("currency", "INR"),
                ref_id=data.get("ref_id"),
                account=AccountInfo(
                    mask=get_digits(data.get("account_mask")), 
                    provider=data.get("bank_name")
                ),
                merchant=MerchantInfo(
                    raw=data.get("description") or data.get("merchant") or "Unknown", 
                    cleaned=data.get("merchant") or "Unknown"
                ),
                description=data.get("description") or content,
                recipient=data.get("merchant") or "Unknown",
                raw_message=content,
                confidence=float(data.get("confidence", 0.9))
            )

        except errors.ClientError as e:
            status_code = getattr(e, 'status_code', None) or getattr(e, 'code', None)
            if status_code == 429:
                _last_quota_error = time.time()
                logger.warning(f"AI Quota Exhausted (429). Cooldown active.")
            else:
                logger.error(f"AI Client Error ({status_code}): {e}")
            return None
        except Exception as e:
            logger.error(f"AI Parse Error: {e}")
            return None

    def parse_with_pattern(self, content: str, source: str, date_hint: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """Extended parse that returns both transaction and suggested pattern."""
        global _last_quota_error
        if time.time() - _last_quota_error < _COOLDOWN_SECONDS:
            return {"error": "quota_exhausted", "message": "Circuit breaker active due to recent 429"}

        if not self.config:
            return {"error": "AI Config not found"}
            
        if not self.config.is_enabled:
            return {"error": "AI is disabled in settings"}
            
        if not self.config.api_key_enc:
             return {"error": "API Key missing"}

        client = genai.Client(api_key=self.config.api_key_enc)
        
        global _last_ai_call
        elapsed = time.time() - _last_ai_call
        if elapsed < _RATE_LIMIT_DELAY:
            time.sleep(_RATE_LIMIT_DELAY - elapsed)
        _last_ai_call = time.time()

        config = types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json",
        )
        model_id = self.config.model_name or "gemini-1.5-flash"

        ref_date = timezone.utcnow()
        if date_hint and isinstance(date_hint, datetime):
            ref_date = date_hint
        elif date_hint and isinstance(date_hint, str):
             try: ref_date = datetime.fromisoformat(date_hint)
             except: pass
        ref_date_str = ref_date.strftime('%Y-%m-%d')

        prompt = rf"""
        You are a precise financial parser. Extract transaction details AND generate a reusable regex for this {source} message.
        Return ONLY valid JSON.
        
        Input: "{content}"
        Reference Date: {ref_date_str}
        
        Required JSON Structure:
        {{
            "transaction": {{
                "amount": float,
                "type": "DEBIT" or "CREDIT",
                "date": "YYYY-MM-DD",
                "account_mask": "1234" (last 4 digits or null),
                "bank_name": "HDFC" (or null),
                "merchant": "Amazon",
                "description": "raw",
                "ref_id": "utr",
                "confidence": float
            }},
            "suggested_regex": "Python regex with capture groups",
            "field_mapping": {{
                "amount": group_idx,
                "date": group_idx,
                "merchant": group_idx,
                "account": group_idx,
                "type": "DEBIT" or "CREDIT"
            }}
        }}

        Rules:
        1. If date is missing/relative (e.g. 'today', 'yesterday'), calculate it based on reference date: {ref_date_str}.
        2. ALWAYS return date in ISO format (YYYY-MM-DD).

        Rules for Regex:
        1. Use [\d,\.]+ for amounts.
        2. Use (.*?) for merchants.
        3. Use [\d\-\/]+ for dates.
        4. Ensure it's robust enough for similar messages from this bank.
        """

        try:
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=config
            )
            raw_text = response.text.strip()
            
            # More robust JSON cleaning
            cleaned_text = raw_text
            if "```json" in cleaned_text:
                cleaned_text = cleaned_text.split("```json")[1].split("```")[0].strip()
            elif "```" in cleaned_text:
                cleaned_text = cleaned_text.split("```")[1].split("```")[0].strip()
            
            # Handle potential raw bit-escape issues in AI-generated regex (e.g. \d -> \\d)
            # This is tricky with raw json.loads, but we try to decode it safely.
            try:
                data = json.loads(cleaned_text)
            except json.JSONDecodeError:
                # Fallback: if it's just invalid \ escapes in a regex, let's try to escape them
                import re
                escaped_text = re.sub(r'(?<!\\)\\(?!["\\/bfnrtu])', r'\\\\', cleaned_text)
                data = json.loads(escaped_text)
                
            return data
        except errors.ClientError as e:
            status_code = getattr(e, 'status_code', None) or getattr(e, 'code', None)
            if status_code == 429:
                _last_quota_error = time.time()
                msg = "Gemini API quota exceeded. Circuit breaker active."
                logger.warning(f"AI Pattern Gen Quota Error: {msg}")
                return {"error": "quota_exhausted", "message": msg}
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"AI Pattern Gen Error: {e}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

def get_digits(s):
    if not s: return None
    return "".join(filter(str.isdigit, str(s)))[-4:]
