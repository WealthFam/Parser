import requests
import uuid

BASE_URL = "http://localhost:8001"

def test_robustness():
    print("--- Testing Robustness Features ---")
    
    # 1. Fuzzy Normalization Test
    # "AMZ*PAY" is not in our exact regex, but should fuzzy match "Amazon"
    payload = {
        "sender": "AMZPAY",
        "body": f"Paid Rs 100 on SBI A/c XX1234 at AMZ*PAY IND on 13-01-26. Ref: {uuid.uuid4()}",
    }
    print("Testing Fuzzy Normalization for 'AMZ*PAY'...")
    resp = requests.post(f"{BASE_URL}/v1/ingest/sms", json=payload)
    data = resp.json()
    if data['status'] == 'success':
        merchant = data['results'][0]['transaction']['merchant']['cleaned']
        print(f"Result: {merchant}")
        if merchant == "Amazon":
            print("✅ Fuzzy Normalization PASS")
        else:
            print("❌ Fuzzy Normalization FAIL")
    
    # 2. Cross-Source Deduplication Test
    print("\nTesting Cross-Source Deduplication...")
    # Step A: Ingest via SMS
    msg_id = str(uuid.uuid4())[:8]
    sms_payload = {
        "sender": "HDFCBK",
        "body": f"Rs.500.00 debited from a/c XX1234 at STARBUCKS on 13-01-26. Ref {msg_id}",
    }
    requests.post(f"{BASE_URL}/v1/ingest/sms", json=sms_payload)
    
    # Step B: Ingest similar via Email (Subject matches content basically)
    email_payload = {
        "sender": "alerts@hdfc.com",
        "subject": "HDFC Bank Transaction Alert",
        "body_text": f"Dear Customer, Your HDFC A/c XX1234 has been debited for Rs.500.00 on 13-01-26 towards STARBUCKS COFFEE. Ref: {msg_id}"
    }
    resp = requests.post(f"{BASE_URL}/v1/ingest/email", json=email_payload)
    data = resp.json()
    if resp.status_code == 200 and data['status'] == 'success':
        status = data['results'][0]['status']
        print(f"Result Status: {status}")
        if status == "cross_source_duplicate":
            print("✅ Cross-Source Deduplication PASS")
        else:
            print(f"❌ Cross-Source Deduplication FAIL: status is {status}")
    else:
        print(f"❌ Cross-Source Deduplication ERROR: {data}")

    # 3. Stats Endpoint Test
    print("\nTesting Analytics Endpoint (/v1/stats)...")
    resp = requests.get(f"{BASE_URL}/v1/stats")
    if resp.status_code == 200:
        print("✅ Stats Endpoint PASS")
        import json
        print(json.dumps(resp.json(), indent=2))
    else:
        print("❌ Stats Endpoint FAIL")

if __name__ == "__main__":
    test_robustness()
