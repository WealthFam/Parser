# Production Dockerfile for WealthFam Parser
FROM python:3.11-slim

# 1. Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PARSER_DATABASE_URL="duckdb:////data/ingestion_engine_parser.duckdb"

# 2. Set working directory
WORKDIR /app

# 3. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 4. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Copy application code
COPY . .

# 6. Create data directory with appropriate permissions
RUN mkdir -p /data && chmod 777 /data

# 7. Expose port
EXPOSE 8001

# 8. Start the application
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8001"]
