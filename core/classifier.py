import re

class FinancialClassifier:
    
    # Keywords that suggest a financial transaction
    POSITIVE_KEYWORDS = [
        r"\bdebited\b", r"\bcredited\b", r"\bspent\b", r"\bpaid\b", r"\bsent\b", 
        r"\breceived\b", r"\btxn\b", r"\btransaction\b", r"\bacct\b", r"\ba/c\b", 
        r"\bbank\b", r"\bupi\b", r"\bwithdraw\b", r"\bpurchase\b", r"\bbill\b", 
        r"\bpayment\b", r"\btransfer\b"
    ]

    # Keywords that suggest noise (OTP, Promos, Notifications)
    NEGATIVE_KEYWORDS = [
        r"otp", r"login", r"password", r"verification code", r"kyc update",
        r"lucky winner", r"loan offer", r"apply now", r"your statement is ready",
        r"pre-approved", r"congratulations", r"cashback points", r"exclusive offer",
        r"click here", r"know more", r"vouchers", r"reward points",
        # Social
        r"linkedin", r"facebook", r"instagram", r"twitter", r"youtube", r"reddit",
        r"follower", r"connection request", r"mentioned in", r"tagged in", r"commented on",
        # Career
        r"job alert", r"recruitment", r"interview", r"career", r"naukri", r"indeed",
        r"application status", r"resume", r"hiring",
        # Marketing
        r"newsletter", r"promotional", r"subscription", r"digest", r"discount", r"coupon",
        r"clearance", r"deals of the day", r"unsubscribed"
    ]

    # Weak signals that are common in spam but also in bank footers
    CLEANUP_KEYWORDS = [
        r"click here", r"know more", r"unsubscribe", r"mobile app", r"social media",
        r"weekly roundup", r"daily briefing", r"activity update"
    ]

    @staticmethod
    def is_financial(content: str, source: str = "SMS") -> bool:
        """
        Heuristic check: filters noise while preserving valid bank alerts.
        """
        content_lower = content.lower()
        
        # Bill/Statement notifications are never transactions we want to track
        # We look for a combination of 'statement' and 'due' to be safe
        if re.search(r"statement", content_lower) and re.search(r"total due|min\. due|minimum due|amount due|payment due", content_lower):
            return False

        # 1. High-Confidence Noise Check (Fast Fail)
        # OTPs and Login alerts are never transactions we want to track
        fast_fail_keywords = [
            r"otp", r"login", r"password", r"verification code", r"kyc update"
        ]
        for kw in fast_fail_keywords:
            if re.search(kw, content_lower):
                return False
        
        # 2. Score and Detect Noise
        score = 0
        noise_detected = False
        strong_signals = {r"debited", r"credited", r"rs.", r"inr", r"spent", r"paid", r"received"}
        has_strong_signal = False
        
        # Check Positive Keywords
        for kw in FinancialClassifier.POSITIVE_KEYWORDS:
            if re.search(kw, content_lower):
                score += 1
                if kw in strong_signals:
                    has_strong_signal = True
        
        # Currency bonus
        if "rs." in content_lower or "inr" in content_lower or "₹" in content_lower:
            score += 2
            has_strong_signal = True

        # 3. Aggressive Negative Check
        penalty = 0
        for kw in FinancialClassifier.NEGATIVE_KEYWORDS + FinancialClassifier.CLEANUP_KEYWORDS:
            if re.search(kw, content_lower):
                noise_detected = True
                penalty += 1.0 # Increased penalty per noise keyword
        
        # 4. Strict Mode for Emails or Noise-Prone content
        # If noise is detected, we MUST see a clear monetary pattern to proceed
        if noise_detected or source == "EMAIL":
            # Strict Monetary Pattern: Currency + Number (e.g. Rs. 100, INR 5,000, ₹ 50)
            has_monetary_pattern = bool(re.search(r'(?i)(?:rs\.?|inr|₹)\s*[\d,]+', content_lower))
            
            if noise_detected and not has_monetary_pattern:
                return False # Instantly fail if noise exists but no clear money
            
            # If it's an email, even without explicit noise, we require a slightly higher bar
            if source == "EMAIL" and not has_monetary_pattern and not has_strong_signal:
                 return False

        # Threshold Logic:
        # - If it has a strong signal and isn't overwhelmed by noise, we keep it.
        if has_strong_signal:
            return (score - penalty) >= 0
        
        return (score - penalty) >= 1
