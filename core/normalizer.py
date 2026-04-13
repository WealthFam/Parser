from typing import Optional
import re
from rapidfuzz import process, fuzz

class MerchantNormalizer:
    
    # Simple regex based aliases
    ALIASES = {
        "Amazon": [r"AMZN", r"Amazon", r"AMAZON PAY", r"AMZ\*"],
        "Swiggy": [r"SWIGGY", r"BUNDL TECHNOLOGIES"],
        "Zomato": [r"ZOMATO"],
        "Uber": [r"UBER"],
        "Ola": [r"ANI TECHNOLOGIES", r"OLA"],
        "Starbucks": [r"TATA STARBUCKS"],
        "Netflix": [r"NETFLIX"],
        "Apple": [r"APPLE\.COM", r"ITUNES"],
        "Google": [r"GOOGLE", r"GOOGLE PLAY"],
        "Reliance Fresh": [r"RELIANCE FRESH", r"RELIANCE RETAIL"],
        "BigBasket": [r"BIGBASKET", r"SUPERMARKET GROCERY"],
        "Jio": [r"RELIANCE JIO", r"JIO"],
        "Airtel": [r"BHARTI AIRTEL", r"AIRTEL"],
    }

    @staticmethod
    def normalize(raw_merchant: str, db: Optional['Session'] = None, tenant_id: Optional[str] = None) -> str:
        if not raw_merchant: 
            return "Unknown"
        
        # 1. Immediate Cleanup
        # Remove common prefixes and noise
        clean = re.sub(r"^(UPI|POS|VPS|ATW|ATM|TXN|PAY)-?", "", raw_merchant, flags=re.IGNORECASE)
        # Remove common suffixes and IDs
        clean = re.sub(r"[-/ ]\d+$", "", clean) # Trailing numbers
        clean = re.sub(r"@[A-Z0-9.\-_]{3,}", "", clean, flags=re.IGNORECASE) # VPA suffix
        clean = clean.strip()
        
        if not clean: 
            return raw_merchant.title()

        # 2. Database Lookup (Priority)
        if db and tenant_id:
            from parser.db.models import MerchantAlias
            # Check for exact matches on raw or cleaned name in aliases, scoped by tenant
            alias_match = db.query(MerchantAlias).filter(
                MerchantAlias.tenant_id == tenant_id,
                (MerchantAlias.pattern.ilike(clean)) | (MerchantAlias.pattern.ilike(raw_merchant))
            ).first()
            if alias_match:
                return alias_match.alias

        # 3. Regex Alias Lookup (High Precision)
        for clean_name, patterns in MerchantNormalizer.ALIASES.items():
            for pattern in patterns:
                if re.search(pattern, clean, re.IGNORECASE):
                    return clean_name
        
        # 4. Fuzzy Matching (Recall Helper)
        # Match against keys of ALIASES
        choices = list(MerchantNormalizer.ALIASES.keys())
        if db and tenant_id:
            from parser.db.models import MerchantAlias
            db_aliases = db.query(MerchantAlias.alias).filter(
                MerchantAlias.tenant_id == tenant_id
            ).distinct().all()
            choices.extend([r[0] for r in db_aliases])

        result = process.extractOne(clean, choices, scorer=fuzz.WRatio)
        
        if result and result[1] > 85: # Threshold of 85%
            return result[0]
            
        return clean.title()
