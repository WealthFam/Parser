from pydantic import BaseModel
from typing import Optional

class SmsIngestRequest(BaseModel):
    sender: str
    body: str
    received_at: Optional[str] = None

class EmailIngestRequest(BaseModel):
    subject: str
    body_text: str
    sender: str
    received_at: Optional[str] = None

class TestIngestRequest(BaseModel):
    content: str
    source: str # SMS or EMAIL
    sender: Optional[str] = None
    subject: Optional[str] = None
