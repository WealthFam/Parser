import unittest
from decimal import Decimal
from parser.parsers.bank.hdfc import HdfcSmsParser
from parser.parsers.bank.icici import IciciSmsParser
from parser.parsers.bank.sbi import SbiSmsParser
from parser.parsers.bank.generic import GenericSmsParser

class TestCreditExtraction(unittest.TestCase):
    def test_hdfc_received(self):
        parser = HdfcSmsParser()
        msg = "Rs.500.00 Received in A/c XX1234 on 25/02/26 from JOHN DOE (UPI Ref 123456789012)"
        res = parser.parse(msg)
        self.assertIsNotNone(res)
        self.assertEqual(res.type, "CREDIT")
        self.assertEqual(res.amount, Decimal("500"))
        self.assertEqual(res.ref_id, "123456789012")

    def test_icici_credit(self):
        parser = IciciSmsParser()
        msg = "INR 2,000.00 credited to ICICI Bank A/c XX8888 on 25-Feb-26 from VPA test@upi. UPI Ref No 12345678"
        res = parser.parse(msg)
        self.assertIsNotNone(res)
        self.assertEqual(res.type, "CREDIT")
        self.assertEqual(res.amount, Decimal("2000"))

    def test_sbi_credit(self):
        parser = SbiSmsParser()
        msg = "INR 1,000.00 credited to SBI A/c XX9999 on 25-02-26 from SELF. Ref 987654321"
        res = parser.parse(msg)
        self.assertIsNotNone(res)
        self.assertEqual(res.type, "CREDIT")
        self.assertEqual(res.amount, Decimal("1000"))

    def test_generic_date_extraction(self):
        parser = GenericSmsParser()
        msg = "Rs 100 debited from a/c 1234 on 25-02-26 to Coffee Shop"
        res = parser.parse(msg)
        self.assertIsNotNone(res)
        self.assertEqual(res.date.day, 25)
        self.assertEqual(res.date.month, 2)
        self.assertEqual(res.date.year, 2026)

if __name__ == "__main__":
    unittest.main()
