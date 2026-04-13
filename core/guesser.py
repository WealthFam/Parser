from typing import Optional

class CategoryGuesser:
    # Basic keyword-based category hints
    # This is NOT the final categorization, just a hint for the ledger
    KEYWORDS = {
        "Food & Dining": ["swiggy", "zomato", "mcdonalds", "kfc", "dominos", "pizza", "burger", "restaurant", "cafe", "coffee", "starbucks"],
        "Groceries": ["blinkit", "zepto", "bigbasket", "instamart", "grocery", "supermarket", "dmart", "reliance fresh"],
        "Travel": ["uber", "ola", "rapido", "irctc", "rail", "flight", "air", "indigo", "vistara", "fuel", "petrol", "shell"],
        "Shopping": ["amazon", "flipkart", "myntra", "ajio", "decathlon", "uniqlo", "zara", "h&m"],
        "Utilities": ["bescom", "electricity", "water", "gas", "bill", "recharge", "jio", "airtel", "vi", "broadband"],
        "Entertainment": ["netflix", "prime", "hotstar", "bookmyshow", "pvr", "inox", "spotify", "youtube"],
        "Health": ["pharmacy", "apollo", "1mg", "netmeds", "hospital", "doctor", "clinic"],
        "Investment": ["zerodha", "groww", "upstox", "sip", "mutual fund", "ppf", "nps"]
    }

    @staticmethod
    def guess(merchant: str, description: str) -> Optional[str]:
        text = (f"{merchant} {description}").lower()
        
        for category, keywords in CategoryGuesser.KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    return category
        
        return None
