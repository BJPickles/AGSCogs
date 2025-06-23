import datetime
import random
import pytz

tz = pytz.timezone("Europe/London")

def seconds_until_next_scrape() -> int:
    """
    Return next scrape interval in seconds, with daytime/nighttime jitter.
    Daytime (08:00–12:30, 14:00–22:00): ~600±60s
    Off-hours: random 900–2700s
    """
    now = datetime.datetime.now(tz)
    h, m = now.hour, now.minute
    active = ((h >= 8 and (h < 12 or (h == 12 and m <= 30))) or (h >= 14 and h < 22))
    if active:
        return 600 + random.randint(-60, 60)
    return random.randint(900, 2700)

def now_in_windows(windows: list) -> bool:
    """
    Given windows = [["HH:MM","HH:MM"], …], return True if current time
    (Europe/London) falls in any window.
    """
    # get a naive time so we don’t compare aware <-> naive
    now_time = datetime.datetime.now(tz).time().replace(tzinfo=None)
    for start, end in windows:
        try:
            s = datetime.datetime.strptime(start, "%H:%M").time()
            e = datetime.datetime.strptime(end,   "%H:%M").time()
            if s <= now_time <= e:
                return True
        except Exception:
            continue
    return False

def filter_listings(listings: list, settings: dict):
    """
    Apply blacklist, whitelist and numeric filters.
    Returns (matched_listings, blocked_count).
    """
    # base blacklist
    blocked_terms = ["auction", "shared ownership", "25% share", "retirement"]
    if settings.get("blacklistleasehold"):
        blocked_terms.append("leasehold")
    # custom blacklist
    for term in settings.get("customblacklist", []):
        blocked_terms.append(term.lower().strip())
    # whitelist keywords
    whitelist = [kw.lower().strip() for kw in settings.get("keywords", []) if kw.strip()]

    matched = []
    blocked_count = 0

    for listing in listings:
        title = listing.get("title", "") or ""
        desc = listing.get("description", "") or ""
        text = f"{title} {desc}".lower()
        # blacklist test
        if any(term in text for term in blocked_terms):
            blocked_count += 1
            continue
        # whitelist test (if any defined)
        if whitelist and not any(kw in text for kw in whitelist):
            continue
        # price
        maxp = settings.get("maxprice")
        if maxp is not None and listing.get("price", 0) > maxp:
            continue
        # bedrooms
        minb = settings.get("minbeds")
        if minb is not None and listing.get("beds", 0) < minb:
            continue

        matched.append(listing)

    return matched, blocked_count