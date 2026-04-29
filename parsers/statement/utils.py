from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional

def parse_decimal(val: Any) -> Decimal:
    if val is None:
        return Decimal("0.00")
    if isinstance(val, Decimal):
        return val
    try:
        clean_val = str(val).replace(",", "").replace(" ", "").replace("₹", "").replace("Dr", "").replace("Cr", "").strip()
        if not clean_val:
            return Decimal("0.00")
        return Decimal(clean_val)
    except:
        return Decimal("0.00")

def parse_date(val: Any) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, (datetime, date)):
        return val
    
    s = str(val).strip()
    # Try common formats: DD/MM/YYYY, DD-MM-YYYY, DD MMM YYYY
    formats = ["%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%d/%m/%y", "%d-%m-%y"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    return None
