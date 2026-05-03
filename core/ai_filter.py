import re
import logging

logger = logging.getLogger(__name__)

class AIGuardrail:
    """
    A strict filter to prevent non-financial, conversational, or promotional 
    text from consuming Gemini AI quotas.
    """

    # Extremely high confidence noise that should NEVER hit the AI
    STRICT_NOISE_KEYWORDS = [
        r"\border confirmed\b", r"\bshipped\b", r"\bout for delivery\b", 
        r"\barriving today\b", r"\btrack your order\b", r"\bdelivered\b",
        # Marketing & Subscriptions
        r"\bunsubscribe\b", r"\bview in browser\b", r"\bnewsletter\b",
        r"\bsale\b", r"\boffer valid\b", r"\bexclusive access\b", r"\bdiscount\b", r"\bcoupon\b",
        r"\bshop now\b", r"\blimited time\b", r"\bdeals inside\b",
        # Personal / Conversational
        r"\bhow have you been\b", r"\bcheckout my\b", r"\bmiss you\b", r"\bhope you're doing well\b",
        # Surveys & Feedback
        r"\bfeedback\b", r"\bsurvey\b", r"\brate your experience\b"
    ]

    # Required financial signals if a standard currency symbol is missing
    FINANCIAL_VERBS = [
        r"\bdebited\b", r"\bcredited\b", r"\bspent\b", r"\bpaid\b", 
        r"\breceived\b", r"\bwithdrawn\b", r"\btransfer\b"
    ]

    @classmethod
    def should_allow_ai_parsing(cls, content: str, source: str) -> bool:
        """
        Evaluate if the content is highly likely to contain a parseable transaction.
        Returns True if it should be sent to AI, False to drop it.
        """
        if not content:
            return False

        content_lower = content.lower()
        
        # 1. Fast Fail on Length
        if len(content_lower) < 15:
            logger.info("AIGuardrail: Dropped - Too short")
            return False
            
        # Emails are often very long and conversational
        if source == "EMAIL" and len(content_lower) > 800:
            # Only allow if it has a blazing obvious transaction table/receipt layout
            strong_markers = ["transaction details", "receipt", "order id", "utr number", "upi ref"]
            if not any(marker in content_lower for marker in strong_markers):
                logger.info("AIGuardrail: Dropped - Email too long without clear receipt markers")
                return False

        # 2. Fast Fail on Strict Noise
        for kw in cls.STRICT_NOISE_KEYWORDS:
            if re.search(kw, content_lower):
                logger.info(f"AIGuardrail: Dropped - Strict noise detected: '{kw}'")
                return False

        # 3. Contextual Monetary Validation
        # Does it have a standard currency pattern? (Rs. 500, INR 5,000.00, ₹ 50)
        has_currency_pattern = bool(re.search(r'(?i)(?:rs\.?|inr\.?|₹\.?|usd\.?|\$)\s*[\d,]+(?:\.\d+)?', content_lower))
        
        if has_currency_pattern:
            return True # Clear monetary amount detected, safe to let AI try
            
        # 4. Fallback: Strong Financial Context
        # If no explicit Rs/INR/₹ symbol, we need a strong verb AND a number
        has_number = bool(re.search(r'\d+', content_lower))
        has_financial_verb = any(re.search(verb, content_lower) for verb in cls.FINANCIAL_VERBS)
        
        if has_number and has_financial_verb:
            # We have a number and a verb like "debited", allow it.
            # Example: "Your AC X*123 is debited for 500 on 12-Oct" (Missing Rs symbol)
            return True
            
        logger.info("AIGuardrail: Dropped - No strict monetary pattern or strong financial context found")
        return False
