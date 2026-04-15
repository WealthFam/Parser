import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import ConfigDict, Field

class Settings(BaseSettings):
    PROJECT_NAME: str = "Ingestion Parser Microservice"
    PORT: int = 8001
    
    # Database
    # Use PARSER_DATABASE_URL if set, else fallback to DATABASE_URL (for generic configs)
    PARSER_DATABASE_URL: Optional[str] = None
    DATABASE_URL_ENV: Optional[str] = Field(None, alias="DATABASE_URL")

    @property
    def DATABASE_URL(self) -> str:
        url = self.PARSER_DATABASE_URL
        if not url:
            url = self.DATABASE_URL_ENV
        if not url:
            url = "duckdb:////data/ingestion_engine_parser.duckdb"

        # Robustness Check for Linux/Docker relative path discrepancy
        if url.startswith("duckdb:///data/") and os.path.exists("/data"):
             url = url.replace("duckdb:///data/", "duckdb:////data/")
             
        return url
    
    # Security
    SECRET_KEY: str = "CHANGE_THIS_TO_A_SECURE_SECRET_IN_PRODUCTION"
    ALGORITHM: str = "HS256"
    ALLOWED_ORIGINS: str = "*"
    
    model_config = ConfigDict(case_sensitive=True, env_file=".env", extra="ignore", populate_by_name=True)

settings = Settings()