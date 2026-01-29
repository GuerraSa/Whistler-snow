import re
from datetime import datetime, timedelta

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

def parse_whistler_date(date_str):
    """Parses 'December 8, 2025 3pm' to UTC string."""
    try:
        clean_str = date_str.strip().upper()
        # Parse local
        dt_local = datetime.strptime(clean_str, "%B %d, %Y %I%p")
        # Add 8 hours for UTC (approx, simplistic)
        dt_utc = dt_local + timedelta(hours=8)
        return dt_utc.isoformat() + "Z"
    except ValueError as e:
        print(f"Could not parse date: {e}")
        return None

def normalize_time_key(dt_obj):
    hour = dt_obj.strftime("%I").lstrip("0")
    rest = dt_obj.strftime(":%M%p")
    return (hour + rest).upper()