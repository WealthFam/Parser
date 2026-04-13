import unittest
from datetime import datetime
from parser.parsers.base_compat import BaseParser

class MockParser(BaseParser):
    def parse(self, content, date_hint=None):
        return None

class TestDateTimeParsing(unittest.TestCase):
    def setUp(self):
        self.parser = MockParser()
        self.hint = datetime(2026, 2, 25, 12, 0, 0)

    def test_standard_date(self):
        dt = self.parser._parse_date("25-02-2026", self.hint)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 2)
        self.assertEqual(dt.day, 25)

    def test_date_with_time(self):
        dt = self.parser._parse_date("25-02-2026 14:30:45", self.hint)
        self.assertEqual(dt.hour, 14)
        self.assertEqual(dt.minute, 30)
        self.assertEqual(dt.second, 45)

    def test_iso_with_colon(self):
        dt = self.parser._parse_date("2026-02-25:15:45:00", self.hint)
        self.assertEqual(dt.hour, 15)
        self.assertEqual(dt.minute, 45)
        self.assertEqual(dt.year, 2026)

    def test_short_year(self):
        dt = self.parser._parse_date("25-02-26", self.hint)
        self.assertEqual(dt.year, 2026)

    def test_month_name(self):
        dt = self.parser._parse_date("25-Feb-2026", self.hint)
        self.assertEqual(dt.month, 2)

    def test_hint_preservation(self):
        # If only date is provided, and it matches hint date, keep hint's time
        dt = self.parser._parse_date("25-02-2026", self.hint)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.minute, 0)

    def test_different_date_uses_00_time(self):
        # If date is different from hint, we don't know the time, so default to 00:00
        dt = self.parser._parse_date("24-02-2026", self.hint)
        self.assertEqual(dt.day, 24)
        self.assertEqual(dt.hour, 0)
        self.assertEqual(dt.minute, 0)

if __name__ == "__main__":
    unittest.main()
