import json
import logging
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from google import genai
from google.genai import types, errors
from parser.db.models import AIConfig

logger = logging.getLogger(__name__)

class BatchGeminiParser:
    def __init__(self, db: Session, tenant_id: str):
        self.db = db
        self.tenant_id = tenant_id
        self.config = self.db.query(AIConfig).filter(
            AIConfig.tenant_id == self.tenant_id,
            AIConfig.is_enabled == True
        ).first()

    def parse_batch(self, items: List[Dict[str, str]], source: str) -> Dict[str, Any]:
        """
        Parses multiple messages using Gemini in one shot.
        items format: [{"id": "1", "content": "Email body..."}, ...]
        """
        if not self.config or not self.config.api_key_enc:
            return {"error": "API Key missing or AI deactivated"}

        client = genai.Client(api_key=self.config.api_key_enc)
        
        system_instruction = (
            f"You are a precise financial parser processing multiple {source} messages.\n"
            "Analyze each message block delimited by '--- ITEM ID: <id> ---'.\n"
            "Extract any valid transaction details from each item.\n"
            "Return exactly one JSON array where each object has:\n"
            "- 'id': (string, exact ID provided)\n"
            "- 'transaction': {"
            " 'amount': (float), 'date': ('YYYY-MM-DDTHH:MM:SSZ', or null), "
            "'description': (text), 'type': ('DEBIT' or 'CREDIT'), "
            "'merchant': (text) } or null if no transaction found.\n"
            "Output MUST be valid JSON array."
        )

        config = types.GenerateContentConfig(
            temperature=0.0,
            response_mime_type="application/json",
            system_instruction=system_instruction
        )

        model_id = self.config.model_name or "gemini-1.5-flash"

        # Construct batch prompt
        prompt = ""
        for item in items:
            prompt += f"\n--- ITEM ID: {item.get('id')} ---\n{item.get('content')}\n"

        try:
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=config
            )
            raw_text = response.text.strip()
            
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:]
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3]

            parsed_array = json.loads(raw_text.strip())
            
            result_map = {}
            if isinstance(parsed_array, list):
                for element in parsed_array:
                    item_id = element.get("id")
                    txn = element.get("transaction")
                    if str(item_id):
                        result_map[str(item_id)] = txn

            return {"status": "success", "results": result_map}
            
        except Exception as e:
            logger.error(f"Batch AI Limit Error: {e}")
            return {"error": "batch_failed", "message": str(e)}
