import duckdb
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from parser.config import settings

engine = create_engine(
    settings.DATABASE_URL,
    connect_args={
        "config": {
            "checkpoint_threshold": "10MB"
        }
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def init_db():
    """
    Initialize the database tables.
    DuckDB with SQLAlchemy requires explicit creation if not using migrations.
    """
    # 1. SQLAlchemy auto-create (Best effort for ORM models)
    from parser.db import models
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()