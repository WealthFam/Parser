# Parser Microservice

A production-ready microservice for parsing financial transactions from various sources (SMS, Email, Files, CAS statements). This service leverages the perfected parsing logic from the main application, now encapsulated in a standalone, scalable microservice.

## ✨ Features

- **Multi-Source Parsing**: Support for SMS, Email, and File-based (CSV/Excel) transaction ingestion.
- **Bank-Specific Robustness**: Includes refined parsers for HDFC, SBI, ICICI, Axis, Kotak, and more.
- **AI-Powered Fallback**: Uses Google Gemini for complex or non-standard messages.
- **User-Trainable Patterns**: Dynamic regex-based parsing rules configurable via API.
- **Tenant Isolation**: Strict `tenant_id` mapping for all logs, patterns, and configurations.
- **Advanced Robustness**:
    - **Cross-Source Deduplication**: Automatically detects if an SMS and Email represent the same transaction within a 15-minute window.
    - **Fuzzy Merchant Normalization**: Uses Levenshtein distance (rapidfuzz) to group similar merchant names.
    - **Financial Classification**: Automatically filters out OTPs and promotional spam.
- **Monitoring & Analytics**: Built-in `/v1/stats` endpoint to track parser performance and status breakdowns.
- **Mutual Fund CAS Support**: PDF statement parsing with tenant-scoped processing.

## 🔐 Security & Architecture

This service is designed for multi-tenant environments with a focus on data privacy:

- **JWT Authentication**: Every request must include a Bearer JWT. The service decodes the `tenant_id` (or `sub`) claim to scope all database operations.
- **Strict Data Partitioning**: All database models (logs, pattern rules, merchant aliases, AI configs) are explicitly scoped with a `tenant_id` column and index.
- **Internal Service Auth**: The main backend communicates with the parser using short-lived, internal JWTs signed with a shared `SECRET_KEY`.
- **Stateless Processing**: While the parser maintains a configuration database (DuckDB), the ingestion logic is stateless and scoped per-request.

## 🛠 Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the service**:
   ```bash
   python main.py
   ```
   The service will start on **port 8001** and initialize a local DuckDB database.

## 📁 Repository Structure

- `parser/api`: Categorized FastAPI routers.
    - `system.py`: Health checks and diagnostic endpoints.
    - `ingestion.py`: SMS, Email, and File upload endpoints.
    - `config.py`: AI, Pattern, and File mapping configurations.
    - `analytics.py`: Usage stats, performance monitoring, and history logs.
- `parser/core`: The central ingestion pipeline, classifier, normalizer, and validator.
- `parser/parsers`:
    - `bank/`: Perfectioned static parsers for Indian banks (SMS & Email).
    - `ai/`: Gemini-powered LLM parsing logic.
    - `patterns/`: Dynamic regex engine for user-defined rules.
    - `universal.py`: Smart CSV/Excel parser with auto-header detection.
- `parser/db`: SQLAlchemy models and DuckDB configuration.
- `parser/tests`: Comprehensive integration and robustness test suite.

## 🧪 Testing

Run the integration tests to verify all parsers and the pipeline:
```bash
python parser/tests/test_integration.py
```
Or test robustness features:
```bash
python parser/tests/test_robustness.py
```

## 📖 API Documentation

Once the service is running, explore the interactive API documentation at:
**[http://localhost:8001/docs](http://localhost:8001/docs)**

---
*Maintained by the Antigravity Team*
