import datetime
import random
import pytz

tz = pytz.timezone("Europe/London")

def seconds_until_next_scrape() -> int:
    """
    (Not used in the fixed‐URL version but preserved.)
    Daytime (08–12:30, 14–22): ~600±60s; off-hours: 900–2700s
    """
    now = datetime.datetime.now(tz)
    h, m = now.hour, now.minute
    active = ((h >= 8 and (h < 12 or (h == 12 and m <= 30))) or (h >= 14 and h < 22))
    if active:
        return 600 + random.randint(-60, 60)
    return random.randint(900, 2700)

def now_in_windows(windows: list) -> bool:
    """(Not used here)"""
    now_time = datetime.datetime.now(tz).time().replace(tzinfo=None)
    for start, end in windows:
        try:
            s = datetime.datetime.strptime(start, "%H:%M").time()
            e = datetime.datetime.strptime(end,   "%H:%M").time()
            if s <= now_time <= e:
                return True
        except:
            pass
    return False

def filter_listings(listings: list, settings: dict):
    """
    Apply blacklist (default + custom), optional whitelist keywords,
    maxprice, minbeds.
    Returns (matched_listings, blocked_count).
    """
    blocked_terms = ["auction", "shared ownership", "25% share", "retirement"]
    if settings.get("blacklistleasehold"):
        blocked_terms.append("leasehold")
    for term in settings.get("customblacklist", []):
        blocked_terms.append(term.lower().strip())
    whitelist = [kw.lower().strip() for kw in settings.get("keywords", []) if kw.strip()]

    matched = []
    blocked = 0
    for L in listings:
        txt = f"{L['title']} {L.get('description','')}".lower()
        # blacklist
        if any(bt in txt for bt in blocked_terms):
            blocked += 1
            continue
        # whitelist
        if whitelist and not any(kw in txt for kw in whitelist):
            continue
        # maxprice
        mp = settings.get("maxprice")
        if mp is not None and L["price"] > mp:
            continue
        # minbeds
        mb = settings.get("minbeds")
        if mb is not None and L["beds"] < mb:
            continue
        matched.append(L)
    return matched, blocked