import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

WHISTLER_TZ = ZoneInfo("America/Vancouver")


def safe_float(value, default=None):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default

def clean_notion_number(value_str):
    """Handles '10-15' ranges by averaging, handles NaNs."""
    if not value_str or value_str == "-" or "nan" in str(value_str).lower():
        return 0
    # Negative lookbehind to allow negative numbers
    numbers = re.findall(r'(?<!\d)-?\d+', str(value_str))
    if not numbers:
        return 0
    ints = list(map(int, numbers))
    return int(round(sum(ints) / len(ints)))

def parse_recent_date(text, fmt, now):
    """Parses a date shown without a year ('Dec 8') as the latest such date not after `now`.
    Raises ValueError if it does not parse."""
    dt = datetime.strptime(f"{text} {now.year}", f"{fmt} %Y")  # year in the input, so Feb 29 parses
    return dt.replace(year=now.year - 1) if dt.date() > now.date() else dt

def card_date(period, issued_date):
    """Date of a forecast card: 'Today'/'Tonight' is the issue day, a weekday name is the next such day after it."""
    if period.lower() in ("today", "tonight"):
        return issued_date
    weekday = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"].index(period[:3].lower())
    return issued_date + timedelta(days=(weekday - issued_date.weekday() - 1) % 7 + 1)
