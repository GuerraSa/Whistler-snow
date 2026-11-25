import sys
import types
import unittest

# cred.py is git-ignored; the test needs no real token
sys.modules.setdefault("cred", types.SimpleNamespace(NOTION_TOKEN="test", GEMINI_API_KEY="test"))

from jobs import weather


class FakeClient:
    def __init__(self, rows):
        self.rows, self.payloads = rows, []

    def query_datasource(self, payload=None, strict=False):
        self.payloads.append(payload)
        return self.rows


class TestExistingForecasts(unittest.TestCase):
    def test_keys_match_what_upload_checks(self):
        # Shape of a real Notion row: date-only start, select name
        row = {"properties": {"Forecast Date": {"date": {"start": "2026-11-25", "end": None, "time_zone": None}},
                              "Time of Day": {"select": {"name": "AM"}}}}
        client = FakeClient([row])
        keys = weather.fetch_existing_forecasts(client, "1480m - 2026-11-25 09:00")
        self.assertEqual(keys, {("2026-11-25", "AM")})
        self.assertEqual(client.payloads[0]["filter"]["title"], {"equals": "1480m - 2026-11-25 09:00"})

    def test_empty_fields_do_not_crash(self):
        row = {"properties": {"Forecast Date": {"date": None}, "Time of Day": {"select": None}}}
        self.assertEqual(weather.fetch_existing_forecasts(FakeClient([row]), "t"), {(None, None)})


if __name__ == "__main__":
    unittest.main()
