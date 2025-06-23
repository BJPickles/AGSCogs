import datetime
import random
import pytz

tz = pytz.timezone("Europe/London")

def seconds_until_next_scrape() -> int:
    """Return next scrape interval in seconds, with daytime/nighttime jitter."""
    now = datetime.datetime.now(tz)
    hour, minute = now.hour, now.minute
    # active hours 08:00–12:30 and 14:00–22:00
    active = ((hour >= 8 and (hour < 12 or (hour == 12 and minute <= 30)))
              or (hour >= 14 and hour < 22))
    if active:
        return 600 + random.randint(-60, 60)
    # off-hours
    return random.randint(900, 2700)

def now_in_windows(windows: list) -> bool:
    """
    Check if current London time falls within any user-defined windows.
    windows: list of [start_str, end_str], e.g. [["08:00","12:30"],["14:00","22:00"]]
    """
    now_time = datetime.datetime.now(tz).time()
    for start, end in windows:
        try:
            s = datetime.datetime.strptime(start, "%H:%M").time()
            e = datetime.datetime.strptime(end, "%H:%M").time()
            if s <= now_time <= e:
                return True
        except Exception:
            continue
    return False

def filter_listings(listings: list, settings: dict):
    """
    Filter raw listing dicts according to user settings.

    Returns (matched_listings, blocked_count).
    """
    # base blacklist terms
    blocked_terms = ["auction", "shared ownership", "25% share", "retirement"]
    if settings.get("blacklistleasehold"):
        blocked_terms.append("leasehold")
    # add custom blacklist terms
    for term in settings.get("customblacklist", []):
        blocked_terms.append(term.lower().strip())
    # prepare whitelist keywords
    whitelist = [kw.lower().strip() for kw in settings.get("keywords", []) if kw.strip()]
    matched = []
    blocked_count = 0

    for listing in listings:
        title = listing.get("title", "") or ""
        desc = listing.get("description", "") or ""
        text = f"{title} {desc}".lower()
        # blacklist filter
        if any(term in text for term in blocked_terms):
            blocked_count += 1
            continue
        # whitelist filter (if any keywords defined)
        if whitelist and not any(kw in text for kw in whitelist):
            continue
        # price filter
        maxp = settings.get("maxprice")
        if maxp is not None and listing.get("price", 0) > maxp:
            continue
        # bedrooms filter
        minb = settings.get("minbeds")
        if minb is not None and listing.get("beds", 0) < minb:
            continue
        matched.append(listing)

    return matched, blocked_count