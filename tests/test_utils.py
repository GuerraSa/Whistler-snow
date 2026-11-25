import unittest
from datetime import date, datetime

from core.utils import card_date, parse_recent_date


class TestDates(unittest.TestCase):
    def test_recent_date(self):
        jan = datetime(2029, 1, 5)
        self.assertEqual(parse_recent_date("Dec 8", "%b %d", jan).date(), date(2028, 12, 8))
        self.assertEqual(parse_recent_date("Jan 3", "%b %d", jan).date(), date(2029, 1, 3))
        self.assertEqual(parse_recent_date("Dec 8", "%b %d", datetime(2028, 6, 1)).date(), date(2027, 12, 8))
        self.assertEqual(parse_recent_date("Feb 29", "%b %d", datetime(2028, 3, 1)).date(), date(2028, 2, 29))
        self.assertEqual(parse_recent_date("Nov 25 3:00pm", "%b %d %I:%M%p", datetime(2026, 11, 25, 16)),
                         datetime(2026, 11, 25, 15))

    def test_card_date(self):
        wed = date(2026, 11, 25)
        self.assertEqual(card_date("Tonight", wed), wed)
        self.assertEqual(card_date("Thursday", wed), date(2026, 11, 26))
        self.assertEqual(card_date("Tuesday", wed), date(2026, 12, 1))
        self.assertEqual(card_date("Wednesday", wed), date(2026, 12, 2))


if __name__ == "__main__":
    unittest.main()
