from fastapi import Header, HTTPException
from jose import jwt, JWTError
from typing import Optional
from parser.config import settings

def get_current_tenant(authorization: Optional[str] = Header(None)) -> str:
    """
    Decodes the JWT token from the Authorization header and returns the tenant_id (user_id).
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization Header")
    
    try:
        # Expected format "Bearer <token>"
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid Authentication Scheme")
            
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM]
        )
        
        # Prefer the 'tenant_id' claim (Family Circle) if available, 
        # otherwise fallback to 'sub' (User Identifier)
        tenant_id: str = payload.get("tenant_id") or payload.get("sub")
        
        if not tenant_id:
            raise HTTPException(status_code=401, detail="Invalid Token: Missing subject or tenant_id")
            
        return str(tenant_id)
    except (JWTError, ValueError, AttributeError):
        raise HTTPException(status_code=401, detail="Invalid or Expired Token")
