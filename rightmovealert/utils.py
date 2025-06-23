import datetime
import random
import pytz

tz = pytz.timezone("Europe/London")

def seconds_until_next_scrape() -> int:
    now = datetime.datetime.now(tz)
    hour, minute = now.hour, now.minute
    active = ((hour >= 8 and (hour < 12 or (hour == 12 and minute <= 30))) or (hour >= 14 and hour < 22))
    if active:
        return 600 + random.randint(-60, 60)
    return random.randint(900, 2700)

def now_in_windows(windows):
    now = datetime.datetime.now(tz).time()
    for start, end in windows:
        try:
            s = datetime.datetime.strptime(start, "%H:%M").time()
            e = datetime.datetime.strptime(end, "%H:%M").time()
            if s <= now <= e:
                return True
        except:
            continue
    return False

def filter_listings(listings: list, settings: dict):
    blocked_terms = ["auction", "shared ownership", "25% share", "retirement"]
    if settings.get("blacklistleasehold"):
        blocked_terms.append("leasehold")
    blocked_count = 0
    matched = []
    for listing in listings:
        text = f"{listing.get('title','')} {listing.get('description','')}".lower()
        if any(term in text for term in blocked_terms):
            blocked_count += 1
            continue
        maxp = settings.get("maxprice")
        if maxp and listing.get("price",0) > maxp:
            continue
        minb = settings.get("minbeds")
        if minb and listing.get("beds",0) < minb:
            continue
        kw = settings.get("keyword")
        if kw and kw.lower() not in text:
            continue
        matched.append(listing)
    return matched, blocked_count