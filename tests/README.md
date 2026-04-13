# Parser Microservice Tests

This directory contains integration tests for the Parser Microservice.

## Running Tests

Make sure the Parser Microservice is running on `http://localhost:8001`:

```bash
# Terminal 1 - Start the service
cd parser
python main.py

# Terminal 2 - Run tests
python tests/test_integration.py
```

## Test Coverage

All 8 tests validate:

1. **Health Check** - Service status endpoint
2. **HDFC SMS Parsing** - Amount, merchant normalization, category
3. **SBI SMS Parsing** - Complete parsing with normalization
4. **ICICI SMS Parsing** - Format variations and merchant cleanup
5. **Non-Financial Filtering** - Promotional message classification
6. **Idempotency** - Duplicate detection within time window
7. **Pattern Configuration** - User-trained regex rules
8. **File Upload** - CSV parsing with password support

## Expected Output

```
........
----------------------------------------------------------------------
Ran 8 tests in 37.371s

OK
```

## Verbose Mode

```bash
python tests/test_integration.py -v
```

This will show each test name as it runs.
