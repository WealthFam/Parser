import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from parser.schemas.transaction import IngestionResult, ParsedItem, TransactionMeta, Transaction
from parser.parsers.patterns.regex_engine import PatternParser
from parser.parsers.ai.batch_gemini_parser import BatchGeminiParser
from parser.core.ai_filter import AIGuardrail

logger = logging.getLogger(__name__)

class BatchIngestionPipeline:
    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id

    def run_batch(self, items: List[Dict[str, str]], source: str) -> Dict[str, IngestionResult]:
        """
        Process a list of items. Format: [{"id": "xyz", "content": "body", "sender": "s", "subject": "sub"}]
        Returns dict: { "id": IngestionResult }
        """
        results_map = {}
        ai_queue = []

        # 1. First Pass: Regex and User Patterns (Fast Path)
        for item in items:
            content = item.get("content", "")
            item_id = item.get("id")
            
            p_parser = PatternParser(self.db, source, tenant_id=self.tenant_id)
            pt = p_parser.parse(content)
            
            if pt:
                parsed = ParsedItem(
                    status="extracted",
                    transaction=Transaction(
                        amount=pt.amount,
                        type=pt.type,
                        date=pt.date,
                        account={"mask": pt.account_mask},
                        merchant={"raw": pt.merchant},
                        raw_message=content
                    ),
                    metadata=TransactionMeta(confidence=pt.confidence, parser_used="PatternParser", source_original=source)
                )
                results_map[item_id] = IngestionResult(status="success", results=[parsed], logs=["Parsed via Regex"])
                continue
                
            if AIGuardrail.should_allow_ai_parsing(content, source):
                ai_queue.append(item)
            else:
                results_map[item_id] = IngestionResult(status="ignored", results=[], logs=["Guardrail dropped noise"])

        # 2. Second Pass: Batch AI
        if ai_queue:
            batch_ai_parser = BatchGeminiParser(self.db, self.tenant_id)
            
            chunk_size = 10
            for i in range(0, len(ai_queue), chunk_size):
                chunk = ai_queue[i:i+chunk_size]
                
                ai_response = batch_ai_parser.parse_batch(chunk, source)
                status = ai_response.get("status")
                
                if status == "success":
                    mapping = ai_response.get("results", {})
                    for req_item in chunk:
                        item_id = req_item["id"]
                        txn_dict = mapping.get(str(item_id))
                        if txn_dict:
                            try:
                                txn = Transaction(
                                    amount=txn_dict.get("amount", 0),
                                    type=txn_dict.get("type", "DEBIT"),
                                    date=txn_dict.get("date"),
                                    account={"mask": None},
                                    merchant={"raw": txn_dict.get("merchant")},
                                    description=txn_dict.get("description"),
                                    raw_message=req_item.get("content")
                                )
                                parsed_ai = ParsedItem(
                                    status="extracted",
                                    transaction=txn,
                                    metadata=TransactionMeta(confidence=0.85, parser_used="AI Batch", source_original=source)
                                )
                                results_map[item_id] = IngestionResult(status="success", results=[parsed_ai], logs=["Parsed via Batch AI"])
                            except Exception as e:
                                results_map[item_id] = IngestionResult(status="failed", results=[], logs=[f"AI Validation Error: {e}"])
                        else:
                            results_map[item_id] = IngestionResult(status="failed", results=[], logs=["AI returned no output for this item"])
                else:
                    err = ai_response.get('error', 'unknown error')
                    for req_item in chunk:
                        results_map[req_item["id"]] = IngestionResult(status="failed", results=[], logs=[f"AI Error: {err}"])

        return results_map
