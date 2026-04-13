from parser.core import timezone
import unittest
import requests
import uuid
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
import json

BASE_URL = "http://localhost:8001"

class TestParserMicroservice(unittest.TestCase):

    def setUp(self):
        # Ensure service is up
        try:
            resp = requests.get(f"{BASE_URL}/v1/health")
            if resp.status_code != 200:
                self.fail("Parser service is not healthy")
        except requests.exceptions.ConnectionError:
            self.fail("Parser service is not running on port 8001")

    def test_01_health(self):
        resp = requests.get(f"{BASE_URL}/v1/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")

    def test_02_hdfc_sms(self):
        # Unique content to avoid idempotency issues during repeated testing
        unique_id = str(uuid.uuid4())[:8]
        payload = {
            "sender": "HDFCBK",
            "body": f"Rs.1234.00 debited from a/c XX1234 on 13-01-26 to VPA IND*AMZN Pay India. Ref {unique_id}. Not you? Call 1800...",
            "received_at": timezone.utcnow().isoformat()
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data.get('status'), 'success', f"Failed HDFC: {data.get('logs')}")
        
        result = data['results'][0]
        txn = result['transaction']
        
        self.assertEqual(float(txn['amount']), 1234.00)
        self.assertEqual(txn['type'], 'DEBIT')
        self.assertEqual(txn['account']['mask'], 'XX1234')
        # Check Normalization
        self.assertIn("Amazon", txn['merchant']['cleaned'])
        # Check Category Guess
        self.assertEqual(txn['category'], 'Shopping')

    def test_03_sbi_sms(self):
        unique_id = str(uuid.uuid4())[:8]
        payload = {
            "sender": "SBIINB",
            "body": f"Txn of Rs.500.00 on SBI A/c XX9999 at ZOMATO MEDIA on 13-01-26. Ref: {unique_id}",
            "received_at": timezone.utcnow().isoformat()
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data.get('status'), 'success', f"Failed SBI: {data.get('logs')}")
        
        txn = data['results'][0]['transaction']
        self.assertEqual(float(txn['amount']), 500.00)
        self.assertEqual(txn['account']['mask'], '9999')
        self.assertEqual(txn['merchant']['cleaned'], 'Zomato') # Normalizer check
        self.assertEqual(txn['category'], 'Food & Dining') # Guesser check

    def test_04_icici_sms(self):
        unique_id = str(uuid.uuid4())[:8]
        payload = {
            "sender": "ICICIB",
            "body": f"INR 2,000.00 spent using ICICI Bank Card XX8888 on 28-Jan-26 on UBER RIDES. Avl Limit: INR 50,000. Ref {unique_id}",
            "received_at": timezone.utcnow().isoformat()
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data.get('status'), 'success', f"Failed ICICI: {data.get('logs')}")
        
        txn = data['results'][0]['transaction']
        self.assertEqual(float(txn['amount']), 2000.00)
        self.assertEqual(txn['merchant']['cleaned'], 'Uber')
        self.assertEqual(txn['category'], 'Travel')

    def test_05_non_financial_ignore(self):
        payload = {
            "sender": "TM-JIO",
            "body": f"Your plan expires tomorrow. Recharge now. {uuid.uuid4()}",
            "received_at": timezone.utcnow().isoformat()
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data['status'], 'ignored')

    def test_06_idempotency(self):
        # Send same message twice
        unique_id = str(uuid.uuid4())
        payload = {
            "sender": "TESTBK",
            "body": f"Test Transaction Rs. 100 {unique_id}",
            "received_at": timezone.utcnow().isoformat()
        }
        # First call
        requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        # Second call
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
        
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data['status'], 'duplicate_submission')

    def test_07_pattern_config(self):
        # 1. create pattern
        pattern_payload = {
            "source": "SMS",
            "regex_pattern": r"Paid Rs (.*?) to (.*?) on",
            "mapping": {"amount": 1, "merchant": 2}
        }
        resp = requests.post(f"{BASE_URL}/v1/config/patterns", json=pattern_payload)
        self.assertEqual(resp.status_code, 200)
        
        # 2. Test it
        unique_id = str(uuid.uuid4())[:8]
        ingest_payload = {
             "sender": "CHAIWALA",
             "body": f"Paid Rs 50.00 to Local Chaiwala on 28-02-2026. Ref {unique_id}",
             "received_at": timezone.utcnow().isoformat()
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=ingest_payload)
        data = resp.json()
        
        # Should be picked up by pattern parser now (or maybe AI if pattern fails, but we want pattern)
        if data['status'] == 'success':
            txn = data['results'][0]['transaction']
            self.assertEqual(float(txn['amount']), 50.00)
            self.assertEqual(txn['merchant']['raw'], 'Local Chaiwala')
            # Check if metadata says Pattern
            self.assertIn("Pattern", data['results'][0]['metadata']['parser_used'])

    def test_08_file_ingest_password(self):
        # We don't have a real password protected excel for testing easily
        # But we can verify the parameter is accepted
        files = {'file': ('test.csv', b'date,amount,desc\n2026-01-28,100,Test')}
        data = {
            'mapping_override': '{"date": "date", "amount": "amount", "merchant": "desc"}',
            'password': 'secret_password'
        }
        resp = requests.post(f"{BASE_URL}/v1/ingest/file", files=files, data=data)
        self.assertEqual(resp.status_code, 200)
        # Should succeed because it's just a CSV and password shouldn't break it
        self.assertEqual(resp.json()['status'], 'success')

if __name__ == '__main__':
    unittest.main()
