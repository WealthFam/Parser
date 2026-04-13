from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import TypeDecorator, DateTime

def utcnow() -> datetime:
    """
    Returns a timezone-aware UTC datetime.
    """
    return datetime.now(timezone.utc)

def ensure_utc(dt: datetime) -> datetime:
    """
    Ensures a datetime object is timezone-aware and set to UTC.
    If naive, assumes it was intended as UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def to_iso(dt: datetime) -> str:
    """Returns ISO 8601 string with 'Z' suffix (e.g. 2026-02-27T19:51:00Z)"""
    return ensure_utc(dt).strftime('%Y-%m-%dT%H:%M:%SZ')

from datetime import date as datetime_date

class UTCDateTime(TypeDecorator):
    """
    SQLAlchemy TypeDecorator that ensures datetimes retrieved from the database
    are always timezone-aware (UTC). This prevents Pydantic from serializing
    naive datetimes without the 'Z' suffix.
    """
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc)
            elif isinstance(value, datetime_date):
                # Handle raw date objects by converting to midnight UTC datetime
                return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc)
        return value
