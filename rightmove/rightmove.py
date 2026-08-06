from __future__ import annotations

import asyncio
import copy
import datetime as dt
import hashlib
import json
from html import unescape as html_unescape
import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import discord
import requests
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from redbot.core import Config, commands

log = logging.getLogger("red.bjpickles.rightmove")

# Keep this identifier unchanged so the replacement cog opens the same Red Config.
CONFIG_IDENTIFIER = 1234567890
LONDON = ZoneInfo("Europe/London")
PROPERTY_CHANNEL_RE = re.compile(r"^prop-(\d+)", re.IGNORECASE)
TOPIC_MARKER_RE = re.compile(r"RIGHTMOVE_PROPERTY_ID=(\d+)", re.IGNORECASE)
OLD_CATEGORY_PREFIX = "RIGHTMOVE"
MAX_PRICE_HISTORY = 20
MAX_SEARCH_PAGES = 60
PAGE_SIZE = 24

DEFAULT_BANNED_PROPERTY_TYPES = [
    "studio",
    "land",
    "mobile home",
    "park home",
    "caravan",
    "garage",
    "garages",
    "parking",
    "flat",
    "maisonette",
    "plot",
]

DEFAULT_BANNED_TEXT = [
    "leasehold",
    "lease hold",
    "lease-hold",
    "sharedownership",
    "shared ownership",
    "shared-ownership",
    "over 50",
    "over50",
    "over-50",
    "over 50s",
    "over50s",
    "over-50s",
    "holiday home",
    "holiday-home",
    "holidayhome",
    "park home",
    "park-home",
    "parkhome",
    "mobile home",
    "mobile-home",
    "mobilehome",
    "caravan",
    "caravans",
    "not specified",
    "not-specified",
    "notspecified",
    "non-standard",
    "non standard",
]

# Scheme highlighting is intentionally narrow. It marks only homes sold with a
# legally retained percentage discount to market value: First Homes, LCHO/RSL,
# Discount Market Sale and equivalent Section 106 discounted-sale products. It does NOT
# mark shared ownership, shared equity, equity loans, Rent to Buy, ordinary
# first-time-buyer marketing or temporary builder incentives.
PERMANENT_DISCOUNT_RULES: Sequence[Tuple[str, re.Pattern[str]]] = (
    (
        "First Homes scheme",
        re.compile(r"\b(?:first\s+homes(?:\s+scheme)?|first\s+home\s+scheme)\b", re.IGNORECASE),
    ),
    (
        "Discount Market Sale",
        re.compile(
            r"\b(?:discount(?:ed)?\s+market\s+(?:sale|value)|"
            r"discount\s+open\s+market\s+value|"
            r"discounted\s+sale\s+(?:home|housing|property|scheme)|"
            r"domv)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "Low-cost ownership (LCHO)",
        re.compile(
            r"\b(?:lcho|low[\s-]+cost(?:\s+affordable)?\s+(?:home|housing|home\s+ownership|ownership))\b",
            re.IGNORECASE,
        ),
    ),
    (
        "Section 106 discounted sale",
        re.compile(
            r"\b(?:section\s*106|s106)\b.{0,120}\b(?:discount(?:ed)?\s+(?:market\s+)?sale|"
            r"discount(?:ed)?\s+(?:market\s+)?value|affordable\s+sale)\b|"
            r"\b(?:discount(?:ed)?\s+(?:market\s+)?sale|discount(?:ed)?\s+(?:market\s+)?value|"
            r"affordable\s+sale)\b.{0,120}\b(?:section\s*106|s106)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "Permanent resale discount",
        re.compile(
            r"\b(?:discount\s+(?:is\s+)?(?:retained|secured|protected)\s+(?:in\s+)?perpetuity|"
            r"discount\s+(?:will\s+be\s+|is\s+)?passed\s+on\s+to\s+(?:all\s+)?future\s+(?:buyers|purchasers)|"
            r"same\s+percentage\s+discount\s+(?:on|at)\s+(?:each\s+)?(?:future\s+)?resale|"
            r"resale\s+(?:price\s+)?(?:restriction|covenant)|"
            r"legal\s+(?:agreement|restriction|covenant).{0,100}future\s+(?:sale|resale)|"
            r"continue\s+to\s+be\s+sold\s+(?:at|for)\s+(?:the\s+same\s+)?(?:percentage\s+)?discount)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "Percentage of market value",
        re.compile(
            r"\b(?:sold|available|purchase|purchased|buy|bought|priced)\s+(?:at|for)\s+"
            r"(?:5\d|6\d|7\d|8\d|90)\s*%\s+of\s+(?:the\s+)?(?:full\s+|open\s+)?market\s+value\b|"
            r"\b(?:5\d|6\d|7\d|8\d|90)\s*%\s+of\s+(?:the\s+)?(?:full\s+|open\s+)?market\s+value\b",
            re.IGNORECASE,
        ),
    ),
)

# These products are specifically outside the user's target: outright ownership
# of the whole freehold at a discount that remains attached to the home.
DISQUALIFYING_ASSISTANCE_RE = re.compile(
    r"\b(?:shared\s+ownership|part\s*buy\s*[,/&-]?\s*part\s*rent|"
    r"shared\s+equity|equity\s+loan|help\s+to\s+buy\s+equity|"
    r"rent\s+to\s+buy|staircasing|buy(?:ing)?\s+(?:a|an|your)\s+share|"
    r"rent\s+on\s+(?:the\s+)?(?:remaining|unsold)\s+(?:share|equity)|"
    r"leasehold)\b",
    re.IGNORECASE,
)

# These phrases are not disqualifiers by themselves, but they are also not
# evidence of a permanent discount and therefore never create a star.
TEMPORARY_INCENTIVE_RE = re.compile(
    r"\b(?:deposit\s+contribution|mortgage\s+contribution|"
    r"stamp\s+duty\s+(?:paid|contribution)|cashback|"
    r"first[\s-]*time\s+buyer(?:s)?\s+(?:offer|incentive)|"
    r"key\s+worker(?:s)?\s+(?:offer|incentive)|"
    r"ideal\s+(?:first\s+home|for\s+first[\s-]*time\s+buyers?))\b",
    re.IGNORECASE,
)

DEFAULT_GUILD = {
    "settings": {
        "enabled": False,
        "profile_name": "Rightmove",
        "search_url": None,
        "anchor_channel_id": None,
        "log_channel_id": None,
        "scrape_hour": 7,
        "scrape_minute": 0,
        "retry_minutes": 30,
        "missing_confirmations": 3,
        "green_max": 220_000,
        "orange_max": 250_000,
        "include_locations": [],
        "exclude_locations": [],
        "banned_property_types": DEFAULT_BANNED_PROPERTY_TYPES,
        "banned_text": DEFAULT_BANNED_TEXT,
        # Rightmove sometimes groups permanent-discount LCHO/RSL homes under
        # its broad shared-ownership search flag. When enabled, the cog removes
        # that search-level flag but still rejects actual shared ownership from
        # each listing's own text and tenure details.
        "include_discounted_ownership_search": False,
        "scheme_highlight_enabled": False,
        "scheme_highlight_emoji": "⭐",
        "scheme_highlight_terms": [],
        "last_attempt_ts": None,
        "last_success_ts": None,
        # Keep this a scalar Config Value. Earlier versions registered this
        # path as None; changing it to a dict would make it a Config Group and
        # Red refuses to register a Group and Value under the same name.
        "last_summary": "",
        "consecutive_failures": 0,
        "legacy_migrated": False,
    },
    "properties": {},
    "ignored_properties": {},
}


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _utc_ts() -> int:
    return int(_utc_now().timestamp())


def _valid_timestamp(value: Any) -> Optional[int]:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None

    earliest = int(dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc).timestamp())
    latest = _utc_ts() + 86_400
    return timestamp if earliest <= timestamp <= latest else None


def _normalise_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalise_match_text(value: Any) -> str:
    text = _normalise_space(value).casefold()
    return re.sub(r"[^\w]+", " ", text).strip()


def _compact_match_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalise_space(value).casefold())


def _encode_summary(value: Dict[str, Any]) -> str:
    """Store scrape summaries as a scalar JSON value for Config compatibility."""
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return json.dumps({"error": "Summary could not be serialised."})


def _decode_summary(value: Any) -> Dict[str, Any]:
    """Read current JSON summaries and repair dictionaries from v1.1.1."""
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _matches_phrase(haystack: str, phrase: str) -> bool:
    normal_phrase = _normalise_match_text(phrase)
    if not normal_phrase:
        return False
    if normal_phrase in _normalise_match_text(haystack):
        return True
    compact_phrase = _compact_match_text(phrase)
    return bool(compact_phrase and compact_phrase in _compact_match_text(haystack))


def _safe_int(value: Any) -> Optional[int]:
    """Convert integer-like values without corrupting Discord snowflake IDs.

    Discord channel/message IDs are commonly 18-19 digits. Passing them through
    ``float`` loses precision, so exact integers and digit strings must be handled
    before any floating-point fallback used for values such as ``"220000.0"``.
    """
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None
        if re.fullmatch(r"[+-]?\d+", cleaned):
            try:
                return int(cleaned)
            except (ValueError, OverflowError):
                return None
        try:
            number = float(cleaned)
        except (TypeError, ValueError, OverflowError):
            return None
        return int(number) if math.isfinite(number) else None

    if isinstance(value, float):
        return int(value) if math.isfinite(value) else None

    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or isinstance(value, bool):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _format_price(value: Any) -> str:
    price = _safe_int(value)
    return f"£{price:,}" if price is not None else "Unknown"


def _format_discord_time(value: Any, *, fallback: str = "Unknown") -> str:
    timestamp = _valid_timestamp(value)
    if timestamp is None:
        return fallback
    return f"<t:{timestamp}:D> (<t:{timestamp}:R>)"


def _truncate(value: str, limit: int) -> str:
    value = value or ""
    return value if len(value) <= limit else value[: max(0, limit - 1)].rstrip() + "…"


def _chunk_lines(lines: Sequence[str], limit: int = 1800) -> List[str]:
    chunks: List[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _channel_property_id(channel: discord.TextChannel) -> Optional[str]:
    topic_match = TOPIC_MARKER_RE.search(channel.topic or "")
    if topic_match:
        return topic_match.group(1)
    name_match = PROPERTY_CHANNEL_RE.match(channel.name)
    return name_match.group(1) if name_match else None


def _canonical_url(
    url: str,
    *,
    index: Optional[int] = None,
    include_discounted_ownership: bool = False,
) -> str:
    parts = urlsplit(url.strip())
    # Preserve duplicate query keys used by some Rightmove filters. Only the
    # pagination index is replaced. Rightmove's broad shared-ownership flags
    # can also hide legitimate permanent-discount LCHO/RSL freehold homes, so
    # an opted-in profile removes those flags at request time and relies on the
    # cog's stricter listing-level filters to reject genuine part ownership.
    query: List[Tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_folded = key.casefold()
        if key_folded == "index":
            continue

        if include_discounted_ownership and key_folded == "dontshow":
            tokens = [token.strip() for token in value.split(",") if token.strip()]
            kept = [
                token
                for token in tokens
                if re.sub(r"[\s_-]+", "", token).casefold() != "sharedownership"
            ]
            if kept:
                query.append((key, ",".join(kept)))
            continue

        if include_discounted_ownership and key_folded == "partbuypartrent":
            # ``false`` is Rightmove's legacy "exclude part-buy/part-rent"
            # search flag. It is intentionally removed here because some LCHO
            # listings are misclassified into the same broad bucket.
            if value.strip().casefold() in {"", "0", "false", "no", "off"}:
                continue

        query.append((key, value))

    if index is not None:
        query.append(("index", str(index)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _validate_rightmove_url(url: str) -> Tuple[bool, str]:
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return False, "That is not a valid URL."

    host = (parts.hostname or "").casefold()
    if parts.scheme not in {"http", "https"}:
        return False, "The URL must begin with http:// or https://."
    if host != "rightmove.co.uk" and not host.endswith(".rightmove.co.uk"):
        return False, "The URL must be a Rightmove URL."
    if "/property-for-sale/" not in parts.path:
        return False, "Use a Rightmove property-for-sale search URL."
    return True, ""


def _tier_for_price(price: Any, settings: Dict[str, Any]) -> Tuple[str, discord.Color]:
    value = _safe_int(price)
    green = _safe_int(settings.get("green_max")) or 220_000
    orange = _safe_int(settings.get("orange_max")) or 250_000
    if value is None:
        return "⚪", discord.Color.light_grey()
    if value <= green:
        return "🟢", discord.Color.green()
    if value <= orange:
        return "🟠", discord.Color.orange()
    return "🔴", discord.Color.red()


def _highlight_emoji(settings: Dict[str, Any]) -> str:
    value = _normalise_space(settings.get("scheme_highlight_emoji")) or "⭐"
    # Discord channel names have a 100-character cap. Keep this marker compact
    # and remove separators that would make property IDs harder to recover.
    value = re.sub(r"[\s/#]+", "", value)
    return value[:8] or "⭐"


def _desired_channel_name(
    pid: str,
    price: Any,
    settings: Dict[str, Any],
    *,
    highlighted: bool = False,
) -> str:
    price_emoji, _ = _tier_for_price(price, settings)
    scheme_suffix = f"-{_highlight_emoji(settings)}" if highlighted else ""
    return f"prop-{pid}-{price_emoji}{scheme_suffix}"[:100]


def _property_topic(pid: str, url: Optional[str], profile_name: str) -> str:
    bits = [f"RIGHTMOVE_PROPERTY_ID={pid}", f"Profile={_normalise_space(profile_name) or 'Rightmove'}"]
    if url:
        bits.append(url)
    return _truncate(" | ".join(bits), 1024)


# ---------------------------------------------------------------------------
# Synchronous Rightmove client (always called through asyncio.to_thread)
# ---------------------------------------------------------------------------


@dataclass
class PropertyCard:
    property_id: str
    price: int
    address: str
    property_type: Optional[str]
    bedrooms: Optional[float]
    marketed_text: Optional[str]
    market_kind: Optional[str]
    market_ts: Optional[int]
    is_stc: bool
    url: str
    image_url: Optional[str]
    agent: Optional[str]
    agent_url: Optional[str]
    summary: str
    card_text: str
    fingerprint: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.property_id,
            "price": self.price,
            "address": self.address,
            "type": self.property_type,
            "number_bedrooms": self.bedrooms,
            "marketed_text": self.marketed_text,
            "market_kind": self.market_kind,
            "market_ts": self.market_ts,
            "is_stc": self.is_stc,
            "url": self.url,
            "image_url": self.image_url,
            "agent": self.agent,
            "agent_url": self.agent_url,
            "summary": self.summary,
            "card_text": self.card_text,
            "fingerprint": self.fingerprint,
        }


@dataclass
class ScrapeResult:
    properties: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    expected_count: Optional[int] = None
    raw_card_count: int = 0
    parsed_card_count: int = 0
    pages_fetched: int = 0
    complete: bool = False
    errors: List[str] = field(default_factory=list)


class RightmoveClient:
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )

    @classmethod
    def _session(cls) -> requests.Session:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
        session = requests.Session()
        session.headers.update({"User-Agent": cls.USER_AGENT, "Accept-Language": "en-GB,en;q=0.9"})
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @classmethod
    def _get(cls, session: requests.Session, url: str) -> Tuple[int, bytes]:
        response = session.get(url, timeout=(10, 25))
        return response.status_code, response.content

    @staticmethod
    def _expected_count(tree: html.HtmlElement) -> Optional[int]:
        candidates = tree.xpath("//span[@data-testid='search-header-result-count']//text()")
        if not candidates:
            candidates = tree.xpath("//*[contains(@class,'searchHeader-resultCount')]//text()")
        for value in candidates:
            match = re.search(r"([\d,]+)", value or "")
            if match:
                try:
                    return int(match.group(1).replace(",", ""))
                except ValueError:
                    continue
        return None

    @staticmethod
    def _explicit_zero_state(tree: html.HtmlElement) -> bool:
        if tree.xpath("//*[@data-testid='no-results' or @data-testid='zero-results']"):
            return True
        text = _normalise_match_text(" ".join(tree.xpath("//body//text()")))
        phrases = (
            "0 results",
            "no properties found",
            "we couldn t find any properties",
            "we couldn t find what you re looking for",
        )
        return any(phrase in text for phrase in phrases)

    @staticmethod
    def _parse_price(text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        # Take one monetary value rather than joining both ends of a range.
        match = re.search(r"£\s*([\d,]+)", text)
        if not match:
            match = re.search(r"\b([\d,]{4,})\b", text)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _normalise_image_url(value: Any, base: str = "https://www.rightmove.co.uk") -> Optional[str]:
        """Return a Discord-fetchable property image URL.

        Rightmove commonly wraps card images in an image-optimiser URL such as
        ``/_next/image?url=https%3A%2F%2Fmedia.rightmove...``. Unwrap the proxy
        before validating the actual property image.
        """
        text = _normalise_space(value)
        if not text:
            return None
        text = html_unescape(text).replace("\\u002F", "/").replace("\\/", "/")
        if text.startswith("data:"):
            return None
        if text.startswith("//"):
            text = "https:" + text

        candidate = urljoin(base, text)
        try:
            parts = urlsplit(candidate)
        except ValueError:
            return None

        query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
        wrapped = query_items.get("url") or query_items.get("image") or query_items.get("src")
        if wrapped and (
            parts.path.casefold().endswith("/_next/image")
            or "image" in parts.path.casefold()
            or "proxy" in parts.path.casefold()
        ):
            decoded = wrapped
            for _ in range(3):
                newer = html_unescape(unquote(decoded)).replace("\\u002F", "/").replace("\\/", "/")
                if newer == decoded:
                    break
                decoded = newer
            nested = RightmoveClient._normalise_image_url(decoded, base)
            if nested:
                return nested

        host = (parts.hostname or "").casefold()
        path = parts.path.casefold()
        if parts.scheme not in {"http", "https"}:
            return None
        if "media.rightmove.co.uk" not in host and not re.search(
            r"\.(?:jpe?g|png|webp)(?:$|\?)", candidate, re.IGNORECASE
        ):
            return None
        if any(
            marker in path
            for marker in (
                "property-location-marker",
                "/map/",
                "estate-agent-logo",
                "branch_rmchoice_logo",
                "clogo_",
            )
        ):
            return None
        return candidate

    @classmethod
    def _image_from_srcset(
        cls,
        value: Any,
        base: str = "https://www.rightmove.co.uk",
    ) -> Optional[str]:
        text = _normalise_space(value)
        if not text:
            return None
        candidates = [piece.strip().split()[0] for piece in text.split(",") if piece.strip()]
        for candidate in reversed(candidates):
            image_url = cls._normalise_image_url(candidate, base)
            if image_url:
                return image_url
        return None

    @classmethod
    def _image_from_value(
        cls,
        value: Any,
        base: str = "https://www.rightmove.co.uk",
        depth: int = 0,
    ) -> Optional[str]:
        if depth > 7:
            return None
        if isinstance(value, str):
            if "," in value and (" 1x" in value or " 2x" in value or "w," in value):
                srcset_image = cls._image_from_srcset(value, base)
                if srcset_image:
                    return srcset_image
            return cls._normalise_image_url(value, base)
        if isinstance(value, list):
            for nested in value:
                image_url = cls._image_from_value(nested, base, depth + 1)
                if image_url:
                    return image_url
            return None
        if not isinstance(value, dict):
            return None

        preferred_keys = (
            "mainImageSrc",
            "mainImageUrl",
            "mainImage",
            "primaryImage",
            "displayImage",
            "propertyImage",
            "imageUrl",
            "srcUrl",
            "src",
            "url",
        )
        for key in preferred_keys:
            if key in value:
                image_url = cls._image_from_value(value.get(key), base, depth + 1)
                if image_url:
                    return image_url
        for key, nested in value.items():
            key_folded = str(key).casefold()
            if any(word in key_folded for word in ("image", "photo", "picture", "media")):
                image_url = cls._image_from_value(nested, base, depth + 1)
                if image_url:
                    return image_url
        return None

    @classmethod
    def _image_from_card_html(
        cls,
        card: html.HtmlElement,
        base: str = "https://www.rightmove.co.uk",
    ) -> Optional[str]:
        srcset_values = card.xpath(
            ".//picture//source/@srcset | .//picture//source/@data-srcset | "
            ".//img/@srcset | .//img/@data-srcset"
        )
        for value in srcset_values:
            image_url = cls._image_from_srcset(value, base)
            if image_url:
                return image_url

        source_values = card.xpath(
            ".//img/@src | .//img/@data-src | .//img/@data-original | "
            ".//img/@data-lazy-src | .//source/@src | .//source/@data-src | "
            ".//*[@data-image]/@data-image | .//*[@data-background-image]/@data-background-image"
        )
        for value in source_values:
            image_url = cls._normalise_image_url(value, base)
            if image_url:
                return image_url

        for style in card.xpath(".//*[@style]/@style"):
            for raw_url in re.findall(r"url\((?:['\"])?([^)'\"]+)", str(style), re.IGNORECASE):
                image_url = cls._normalise_image_url(raw_url, base)
                if image_url:
                    return image_url

        # Last resort for current lazy-loaded/optimised card markup: inspect
        # attribute values for direct or percent-encoded Rightmove media links.
        for element in card.iterdescendants():
            for value in element.attrib.values():
                text = str(value)
                folded = text.casefold()
                if "media.rightmove" not in folded and "%2f%2fmedia.rightmove" not in folded:
                    continue
                image_url = cls._normalise_image_url(text, base)
                if image_url:
                    return image_url
                decoded = html_unescape(unquote(text))
                match = re.search(
                    r"https?://media\.rightmove\.co\.uk[^\s'\"<>]+",
                    decoded,
                    re.IGNORECASE,
                )
                if match:
                    image_url = cls._normalise_image_url(match.group(0).rstrip("),]"), base)
                    if image_url:
                        return image_url
        return None

    @classmethod
    def _image_from_detail_page(
        cls,
        tree: html.HtmlElement,
        content: bytes,
        base: str = "https://www.rightmove.co.uk",
    ) -> Optional[str]:
        meta_candidates = tree.xpath(
            "//meta[@property='og:image' or @property='og:image:url' or "
            "@name='twitter:image' or @name='twitter:image:src']/@content"
        )
        for value in meta_candidates:
            image_url = cls._normalise_image_url(value, base)
            if image_url:
                return image_url

        # Gallery data may be in JSON-LD, application/json, or __NEXT_DATA__.
        for script_text in tree.xpath(
            "//script[@type='application/ld+json' or @type='application/json' or @id='__NEXT_DATA__']/text()"
        ):
            try:
                payload = json.loads(script_text)
            except (json.JSONDecodeError, TypeError):
                continue
            image_url = cls._image_from_value(payload, base)
            if image_url:
                return image_url

        decoded = content.decode("utf-8", errors="ignore")
        decoded = html_unescape(decoded).replace("\\u002F", "/").replace("\\/", "/")

        for match in re.findall(
            r"https?://media\.rightmove\.co\.uk[^\"'<>\s]+",
            decoded,
            flags=re.IGNORECASE,
        ):
            image_url = cls._normalise_image_url(match.rstrip("),]"), base)
            if image_url:
                return image_url

        for match in re.findall(
            r"https?%3A%2F%2Fmedia\.rightmove\.co\.uk[^\"'<>\s&]+",
            decoded,
            flags=re.IGNORECASE,
        ):
            image_url = cls._normalise_image_url(unquote(match).rstrip("),]"), base)
            if image_url:
                return image_url

        for match in re.findall(
            r"(?:https?://www\.rightmove\.co\.uk)?/_next/image\?[^\"'<>\s]+",
            decoded,
            flags=re.IGNORECASE,
        ):
            image_url = cls._normalise_image_url(match, base)
            if image_url:
                return image_url
        return None

    @staticmethod
    def _parse_marketed_text(text: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
        cleaned = _normalise_space(text)
        if not cleaned:
            return None, None

        lower = cleaned.casefold()
        kind: Optional[str] = None
        if "reduced" in lower:
            kind = "reduced"
        elif "added" in lower:
            kind = "added"

        now_london = dt.datetime.now(LONDON)
        if "today" in lower:
            date_value = now_london.date()
        elif "yesterday" in lower:
            date_value = (now_london - dt.timedelta(days=1)).date()
        else:
            match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", lower)
            if not match:
                return kind, None
            day, month, year = map(int, match.groups())
            try:
                date_value = dt.date(year, month, day)
            except ValueError:
                return kind, None

        parsed = dt.datetime.combine(date_value, dt.time(hour=12), tzinfo=LONDON)
        return kind, int(parsed.timestamp())

    @staticmethod
    def _extract_json_model(tree: html.HtmlElement) -> Optional[Dict[str, Any]]:
        """Extract Rightmove's embedded ``window.jsonModel`` payload when present.

        The JSON payload is substantially more stable than presentation CSS classes
        and, importantly, includes promoted cards that can use different HTML markup.
        """
        decoder = json.JSONDecoder()
        for script in tree.xpath("//script/text()"):
            if not isinstance(script, str) or "window.jsonModel" not in script:
                continue
            marker = "window.jsonModel"
            marker_index = script.find(marker)
            equals_index = script.find("=", marker_index + len(marker))
            if equals_index < 0:
                continue
            candidate = script[equals_index + 1 :].lstrip()
            try:
                model, _ = decoder.raw_decode(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(model, dict):
                return model
        return None

    @staticmethod
    def _find_property_list(model: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find the most plausible property list inside a Rightmove JSON model."""
        candidates: List[List[Dict[str, Any]]] = []

        def walk(value: Any, depth: int = 0) -> None:
            if depth > 6:
                return
            if isinstance(value, dict):
                for key, nested in value.items():
                    if (
                        key in {"properties", "propertyResults", "results"}
                        and isinstance(nested, list)
                    ):
                        dict_items = [item for item in nested if isinstance(item, dict)]
                        if dict_items and any(
                            item.get("id") is not None
                            or item.get("propertyId") is not None
                            or item.get("propertyUrl")
                            for item in dict_items
                        ):
                            candidates.append(dict_items)
                    if isinstance(nested, (dict, list)):
                        walk(nested, depth + 1)
            elif isinstance(value, list):
                for nested in value:
                    if isinstance(nested, (dict, list)):
                        walk(nested, depth + 1)

        walk(model)
        return max(candidates, key=len) if candidates else []

    @staticmethod
    def _json_expected_count(model: Dict[str, Any]) -> Optional[int]:
        preferred_keys = ("resultCount", "totalResults", "totalResultCount")

        def walk(value: Any, depth: int = 0) -> Optional[int]:
            if depth > 6:
                return None
            if isinstance(value, dict):
                for key in preferred_keys:
                    candidate = _safe_int(value.get(key))
                    if candidate is not None and candidate >= 0:
                        return candidate
                for nested in value.values():
                    if isinstance(nested, (dict, list)):
                        found = walk(nested, depth + 1)
                        if found is not None:
                            return found
            elif isinstance(value, list):
                for nested in value:
                    if isinstance(nested, (dict, list)):
                        found = walk(nested, depth + 1)
                        if found is not None:
                            return found
            return None

        return walk(model)

    @staticmethod
    def _parse_iso_timestamp(value: Any) -> Optional[int]:
        text = _normalise_space(value)
        if not text:
            return None
        try:
            parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=LONDON)
        return int(parsed.timestamp())

    @classmethod
    def _parse_json_cards(
        cls,
        model: Dict[str, Any],
    ) -> Tuple[List[PropertyCard], int, bool, Optional[int]]:
        raw_properties = cls._find_property_list(model)
        expected = cls._json_expected_count(model)
        parsed: List[PropertyCard] = []
        parse_loss = False
        base = "https://www.rightmove.co.uk"

        for item in raw_properties:
            property_id_raw = item.get("id", item.get("propertyId"))
            property_id = str(property_id_raw).strip() if property_id_raw is not None else ""

            property_url_raw = item.get("propertyUrl") or item.get("url")
            property_url = urljoin(base, property_url_raw) if property_url_raw else None
            if not property_id and property_url:
                match = re.search(r"/properties/(\d+)", property_url)
                property_id = match.group(1) if match else ""
            if property_id and not property_url:
                property_url = f"{base}/properties/{property_id}"

            price_obj = item.get("price")
            price: Optional[int] = None
            if isinstance(price_obj, dict):
                price = _safe_int(price_obj.get("amount"))
                if price is None:
                    display_prices = price_obj.get("displayPrices")
                    if isinstance(display_prices, list):
                        for display in display_prices:
                            if not isinstance(display, dict):
                                continue
                            price = cls._parse_price(
                                display.get("displayPrice")
                                or display.get("displayPriceQualifier")
                            )
                            if price is not None:
                                break
                if price is None:
                    price = cls._parse_price(json.dumps(price_obj, ensure_ascii=False))
            else:
                price = _safe_int(price_obj) or cls._parse_price(str(price_obj or ""))

            address = _normalise_space(
                item.get("displayAddress")
                or item.get("address")
                or item.get("propertyAddress")
            )
            property_type = _normalise_space(
                item.get("propertySubType")
                or item.get("propertyTypeFullDescription")
                or item.get("propertyType")
            ) or None
            bedrooms = _safe_float(item.get("bedrooms", item.get("numberOfBedrooms")))

            marketed_text = _normalise_space(
                item.get("addedOrReduced")
                or item.get("marketedText")
                or item.get("firstVisibleDate")
            ) or None
            market_kind, market_ts = cls._parse_marketed_text(marketed_text)
            if market_ts is None:
                market_ts = cls._parse_iso_timestamp(
                    item.get("firstVisibleDate") or item.get("listingUpdateDate")
                )

            status_text = _normalise_match_text(
                " ".join(
                    str(item.get(key) or "")
                    for key in (
                        "displayStatus",
                        "displayStatusId",
                        "status",
                        "propertySubType",
                    )
                )
            )
            is_stc = any(
                phrase in status_text
                for phrase in ("sold stc", "sstc", "subject to contract", "under offer")
            )

            image_url: Optional[str] = None
            for image_key in (
                "propertyImages",
                "mainImage",
                "mainImageUrl",
                "mainImageSrc",
                "primaryImage",
                "displayImage",
                "propertyImage",
                "images",
                "photos",
                "media",
            ):
                if image_key not in item:
                    continue
                image_url = cls._image_from_value(item.get(image_key), base)
                if image_url:
                    break

            customer = item.get("customer") if isinstance(item.get("customer"), dict) else {}
            agent = _normalise_space(
                customer.get("branchDisplayName")
                or customer.get("companyName")
                or item.get("branchDisplayName")
            ) or None
            agent_url_raw = customer.get("branchDetailsUri") or item.get("branchDetailsUri")
            agent_url = urljoin(base, agent_url_raw) if agent_url_raw else None

            summary = _normalise_space(
                item.get("summary")
                or item.get("propertyDescription")
                or item.get("description")
            )

            # A listing ID, URL and price are enough to retain the property safely.
            # Some promoted cards omit the normal address element; give those a
            # stable fallback rather than treating the whole page as corrupt.
            if not property_id or price is None or not property_url:
                parse_loss = True
                continue
            if not address:
                address = f"Rightmove property {property_id}"

            card_text = _normalise_space(
                " ".join(
                    value
                    for value in (
                        address,
                        property_type or "",
                        marketed_text or "",
                        summary,
                        status_text,
                    )
                    if value
                )
            )
            canonical = {
                "id": property_id,
                "price": price,
                "address": address,
                "type": property_type,
                "bedrooms": bedrooms,
                "marketed_text": marketed_text,
                "is_stc": is_stc,
                "url": property_url,
                "image_url": image_url,
                "agent": agent,
                "summary": summary,
            }
            fingerprint = hashlib.sha256(
                json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()

            parsed.append(
                PropertyCard(
                    property_id=property_id,
                    price=price,
                    address=address,
                    property_type=property_type,
                    bedrooms=bedrooms,
                    marketed_text=marketed_text,
                    market_kind=market_kind,
                    market_ts=market_ts,
                    is_stc=is_stc,
                    url=property_url,
                    image_url=image_url,
                    agent=agent,
                    agent_url=agent_url,
                    summary=summary,
                    card_text=card_text,
                    fingerprint=fingerprint,
                )
            )

        return parsed, len(raw_properties), parse_loss, expected

    @staticmethod
    def _first_text(node: html.HtmlElement, xpaths: Sequence[str]) -> Optional[str]:
        for xpath in xpaths:
            values = node.xpath(xpath)
            for value in values:
                text = value if isinstance(value, str) else " ".join(value.xpath(".//text()"))
                cleaned = _normalise_space(text)
                if cleaned:
                    return cleaned
        return None

    @classmethod
    def _parse_page(cls, content: bytes) -> Tuple[List[PropertyCard], int, bool, Optional[int], bool]:
        try:
            tree = html.fromstring(content)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Could not parse Rightmove HTML: {exc}") from exc

        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        if not cards:
            cards = tree.xpath("//*[@data-testid and contains(@data-testid,'propertyCard')]")

        expected = cls._expected_count(tree)
        explicit_zero = cls._explicit_zero_state(tree)

        json_model = cls._extract_json_model(tree)
        if json_model is not None:
            json_cards, json_raw_count, json_parse_loss, json_expected = cls._parse_json_cards(json_model)
            if json_raw_count:
                return (
                    json_cards,
                    json_raw_count,
                    json_parse_loss,
                    json_expected if json_expected is not None else expected,
                    explicit_zero,
                )

        parsed: List[PropertyCard] = []
        parse_loss = False
        base = "https://www.rightmove.co.uk"

        for card in cards:
            card_text = _normalise_space(" ".join(card.xpath(".//text()")))

            price_raw = cls._first_text(
                card,
                (
                    ".//*[@data-testid='property-price']//text()",
                    ".//*[contains(@class,'PropertyPrice_price__')]//text()",
                ),
            )
            price = cls._parse_price(price_raw)

            address = cls._first_text(
                card,
                (
                    ".//*[@data-testid='property-address']//text()",
                    ".//*[contains(@class,'PropertyAddress_address')]//text()",
                    ".//address//text()",
                    ".//a[contains(@href,'/properties/')]//h2//text()",
                ),
            )

            property_type = cls._first_text(
                card,
                (
                    ".//*[contains(@class,'PropertyInformation_propertyType')]//text()",
                    ".//*[@data-testid='property-type']//text()",
                ),
            )

            bedrooms_raw = cls._first_text(
                card,
                (
                    ".//*[contains(@class,'PropertyInformation_bedroomsCount')]//text()",
                    ".//*[@data-testid='property-bedrooms']//text()",
                ),
            )
            bedrooms = _safe_float(bedrooms_raw)
            if bedrooms is None:
                bed_match = re.search(r"\b(\d+)\s+bed(?:room)?s?\b", card_text, re.IGNORECASE)
                bedrooms = float(bed_match.group(1)) if bed_match else None

            marketed_text = cls._first_text(
                card,
                (
                    ".//*[contains(@class,'MarketedBy_addedOrReduced')]//text()",
                    ".//*[@data-testid='property-added-or-reduced']//text()",
                    ".//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'added on')]/text()",
                    ".//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reduced on')]/text()",
                ),
            )
            market_kind, market_ts = cls._parse_marketed_text(marketed_text)

            hrefs = card.xpath(".//a[contains(@href,'/properties/')]/@href")
            property_url = urljoin(base, hrefs[0]) if hrefs else None
            property_id = None
            if property_url:
                id_match = re.search(r"/properties/(\d+)", property_url)
                property_id = id_match.group(1) if id_match else None

            image_url = cls._image_from_card_html(card, base)

            agent = cls._first_text(
                card,
                (
                    ".//*[contains(@class,'PropertyCard_propertyCardEstateAgent')]//img/@alt",
                    ".//*[@data-testid='estate-agent-logo']/@alt",
                ),
            )
            if agent:
                agent = re.sub(r"\s+Estate Agent Logo$", "", agent, flags=re.IGNORECASE).strip()

            agent_hrefs = card.xpath(
                ".//*[contains(@class,'PropertyCard_propertyCardEstateAgent')]//a/@href"
            )
            agent_url = urljoin(base, agent_hrefs[0]) if agent_hrefs else None

            summary = cls._first_text(
                card,
                (
                    ".//*[contains(@class,'PropertyCard_propertyCardDescription')]//text()",
                    ".//*[@data-testid='property-description']//text()",
                    ".//*[@data-testid='property-summary']//text()",
                ),
            ) or ""

            lower_card = card_text.casefold()
            is_stc = any(
                phrase in lower_card
                for phrase in ("sold stc", "sstc", "subject to contract", "under offer")
            )

            if not property_id or price is None or not property_url:
                parse_loss = True
                continue
            if not address:
                address = f"Rightmove property {property_id}"

            canonical = {
                "id": property_id,
                "price": price,
                "address": address,
                "type": property_type,
                "bedrooms": bedrooms,
                "marketed_text": marketed_text,
                "is_stc": is_stc,
                "url": property_url,
                "image_url": image_url,
                "agent": agent,
                "summary": summary,
            }
            fingerprint = hashlib.sha256(
                json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()

            parsed.append(
                PropertyCard(
                    property_id=property_id,
                    price=price,
                    address=address,
                    property_type=property_type,
                    bedrooms=bedrooms,
                    marketed_text=marketed_text,
                    market_kind=market_kind,
                    market_ts=market_ts,
                    is_stc=is_stc,
                    url=property_url,
                    image_url=image_url,
                    agent=agent,
                    agent_url=agent_url,
                    summary=summary,
                    card_text=card_text,
                    fingerprint=fingerprint,
                )
            )

        return parsed, len(cards), parse_loss, expected, explicit_zero

    @classmethod
    def scrape_search(
        cls,
        search_url: str,
        *,
        include_discounted_ownership: bool = False,
    ) -> ScrapeResult:
        result = ScrapeResult()
        session = cls._session()
        effective_url = _canonical_url(
            search_url,
            include_discounted_ownership=include_discounted_ownership,
        )
        seen_ids: set[str] = set()
        natural_end = False
        parse_loss_anywhere = False

        try:
            for page_number in range(MAX_SEARCH_PAGES):
                page_url = _canonical_url(effective_url, index=page_number * PAGE_SIZE)
                try:
                    status, content = cls._get(session, page_url)
                except requests.RequestException as exc:
                    result.errors.append(f"Page {page_number + 1} request failed: {exc}")
                    break

                if status != 200:
                    result.errors.append(f"Page {page_number + 1} returned HTTP {status}")
                    break

                try:
                    cards, raw_count, parse_loss, expected, explicit_zero = cls._parse_page(content)
                except Exception as exc:
                    result.errors.append(f"Page {page_number + 1} parse failed: {exc}")
                    break

                result.pages_fetched += 1
                result.raw_card_count += raw_count
                result.parsed_card_count += len(cards)
                parse_loss_anywhere = parse_loss_anywhere or parse_loss
                if page_number == 0:
                    result.expected_count = expected

                if page_number == 0 and raw_count == 0:
                    if explicit_zero and expected in (None, 0):
                        natural_end = True
                    else:
                        result.errors.append(
                            "The first page contained no recognisable property cards and no explicit zero-results state."
                        )
                    break

                new_on_page = 0
                for card in cards:
                    if card.property_id in seen_ids:
                        continue
                    seen_ids.add(card.property_id)
                    result.properties[card.property_id] = card.as_dict()
                    new_on_page += 1

                if raw_count < PAGE_SIZE:
                    natural_end = True
                    break

                if result.expected_count is not None and len(seen_ids) >= result.expected_count:
                    natural_end = True
                    break

                if raw_count > 0 and new_on_page == 0:
                    result.errors.append("Pagination repeated a page without producing any new property IDs.")
                    break
            else:
                result.errors.append(f"Stopped after the safety limit of {MAX_SEARCH_PAGES} pages.")
        finally:
            session.close()

        count_matches = (
            result.expected_count is None
            or result.expected_count == len(result.properties)
            or (result.expected_count == 0 and not result.properties)
        )

        result.complete = bool(
            natural_end
            and not result.errors
            and not parse_loss_anywhere
            and count_matches
            and result.raw_card_count == result.parsed_card_count
        )

        if natural_end and not count_matches:
            result.errors.append(
                f"Rightmove displayed {result.expected_count} result(s), but {len(result.properties)} unique property IDs were parsed."
            )
        if parse_loss_anywhere or result.raw_card_count != result.parsed_card_count:
            result.errors.append(
                f"Parsed {result.parsed_card_count} of {result.raw_card_count} property card(s); removals are disabled."
            )

        return result

    @classmethod
    def fetch_details(cls, property_url: str) -> Dict[str, str]:
        session = cls._session()
        try:
            status, content = cls._get(session, property_url)
            if status != 200 or not content:
                return {"description": "", "filter_text": "", "image_url": ""}
            tree = html.fromstring(content)

            description_parts = tree.xpath("//*[@data-testid='property-description']//text()")
            if not description_parts:
                description_parts = tree.xpath("//*[contains(@class,'PropertyDescription')]//text()")
            description = _normalise_space(" ".join(description_parts))

            feature_parts = tree.xpath("//*[@data-testid='key-features']//text()")
            if not feature_parts:
                feature_parts = tree.xpath("//*[contains(@class,'KeyFeatures')]//text()")

            detail_parts = tree.xpath(
                "//*[@data-testid='property-details' or @data-testid='tenure-information']//text()"
            )
            filter_text = _normalise_space(
                " ".join([description, " ".join(feature_parts), " ".join(detail_parts)])
            )

            if not description:
                metadata = tree.xpath("//meta[@name='description']/@content")
                description = _normalise_space(metadata[0]) if metadata else ""
                if description and not filter_text:
                    filter_text = description

            image_url = cls._image_from_detail_page(tree, content, property_url) or ""
            return {
                "description": description,
                "filter_text": filter_text,
                "image_url": image_url,
            }
        except (requests.RequestException, ValueError, TypeError):
            return {"description": "", "filter_text": "", "image_url": ""}
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------


class RightmoveCog(commands.Cog):
    """Reliable, multi-server Rightmove monitor with per-guild search profiles."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=CONFIG_IDENTIFIER,
            force_registration=True,
        )

        # Legacy global fields remain registered so the old cache can be adopted.
        self.config.register_global(
            properties={},
            settings={"cleanup_days": 7, "log_channel_id": None},
        )
        self.config.register_guild(**DEFAULT_GUILD)

        self._scheduler_task: Optional[asyncio.Task] = None
        self._guild_locks: Dict[int, asyncio.Lock] = {}
        self._active_runs: Dict[int, asyncio.Task] = {}

    async def cog_load(self) -> None:
        # Normalise data written by earlier versions before any command or the
        # scheduler reads it. In particular, Red cannot merge a stored mapping
        # into a registered default of ``None``.
        for guild in list(self.bot.guilds):
            await self._repair_guild_settings(guild)

        if self._scheduler_task is None or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(
                self._scheduler_loop(),
                name="rightmove-scheduler",
            )

    async def _repair_guild_settings(self, guild: discord.Guild) -> Dict[str, Any]:
        guild_config = self.config.guild(guild)
        try:
            raw = await guild_config.get_raw("settings", default={})
        except Exception:
            log.exception("Could not read raw Rightmove settings for guild %s; resetting settings only", guild.id)
            raw = {}

        if not isinstance(raw, dict):
            raw = {}

        settings = copy.deepcopy(DEFAULT_GUILD["settings"])
        for key, value in raw.items():
            if key in settings:
                settings[key] = value

        # v1.1.1 briefly stored this as a mapping even though older versions
        # registered the same path as a scalar. Convert any such value to JSON
        # before normal Config reads attempt to merge registered defaults.
        summary = settings.get("last_summary")
        if isinstance(summary, dict):
            settings["last_summary"] = _encode_summary(summary)
        elif not isinstance(summary, str):
            settings["last_summary"] = ""

        await guild_config.set_raw("settings", value=settings)
        return settings

    def cog_unload(self) -> None:
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
        for task in list(self._active_runs.values()):
            if not task.done():
                task.cancel()

    def _lock_for(self, guild_id: int) -> asyncio.Lock:
        lock = self._guild_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._guild_locks[guild_id] = lock
        return lock

    async def _scheduler_loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                now = dt.datetime.now(LONDON)
                for guild in list(self.bot.guilds):
                    try:
                        settings = await self.config.guild(guild).settings()
                        if not settings.get("enabled") or not settings.get("search_url"):
                            continue

                        scheduled = now.replace(
                            hour=int(settings.get("scrape_hour", 7)),
                            minute=int(settings.get("scrape_minute", 0)),
                            second=0,
                            microsecond=0,
                        )
                        if now < scheduled:
                            continue

                        last_success = _valid_timestamp(settings.get("last_success_ts"))
                        if last_success:
                            success_time = dt.datetime.fromtimestamp(last_success, LONDON)
                            # A manual/start run only satisfies today's schedule if it
                            # completed at or after the configured daily time.
                            if success_time >= scheduled:
                                continue

                        last_attempt = _valid_timestamp(settings.get("last_attempt_ts"))
                        retry_seconds = max(5, int(settings.get("retry_minutes", 30))) * 60
                        if last_attempt and _utc_ts() - last_attempt < retry_seconds:
                            continue

                        self._launch_background_run(guild, source="scheduled", force_refresh=False)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception("Rightmove scheduler failed while checking guild %s", guild.id)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Rightmove scheduler iteration failed")

            await asyncio.sleep(60)

    def _launch_background_run(
        self,
        guild: discord.Guild,
        *,
        source: str,
        force_refresh: bool,
    ) -> bool:
        existing = self._active_runs.get(guild.id)
        if existing and not existing.done():
            return False

        task = asyncio.create_task(
            self._run_scrape(guild, source=source, force_refresh=force_refresh),
            name=f"rightmove-{guild.id}-{source}",
        )
        self._active_runs[guild.id] = task

        def done_callback(done_task: asyncio.Task, guild_id: int = guild.id) -> None:
            if self._active_runs.get(guild_id) is done_task:
                self._active_runs.pop(guild_id, None)
            try:
                done_task.exception()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Unretrieved Rightmove task exception for guild %s", guild_id)

        task.add_done_callback(done_callback)
        return True

    async def _log(self, guild: discord.Guild, message: str) -> None:
        settings = await self.config.guild(guild).settings()
        channel_id = settings.get("log_channel_id")
        if not channel_id:
            return

        channel = guild.get_channel(int(channel_id))
        if not isinstance(channel, discord.TextChannel):
            return

        prefix = f"<t:{_utc_ts()}:F> (<t:{_utc_ts()}:R>) [Rightmove]"
        try:
            for chunk in _chunk_lines(message.splitlines() or [message]):
                await channel.send(f"{prefix} {chunk}"[:2000])
        except (discord.Forbidden, discord.HTTPException):
            log.exception("Could not send Rightmove log to guild %s", guild.id)

    async def _send_control_notice(self, guild: discord.Guild, message: str) -> None:
        settings = await self.config.guild(guild).settings()
        channel_id = settings.get("anchor_channel_id")
        channel = guild.get_channel(int(channel_id)) if channel_id else None
        if not isinstance(channel, discord.TextChannel):
            return
        try:
            await channel.send(message[:2000])
        except (discord.Forbidden, discord.HTTPException):
            await self._log(guild, f"Could not send control notice: {message}")

    async def _migrate_legacy_global(self, guild: discord.Guild) -> None:
        guild_config = self.config.guild(guild)
        settings = await guild_config.settings()
        if settings.get("legacy_migrated"):
            return

        guild_cache = await guild_config.properties()
        adopted = 0
        if not guild_cache:
            legacy_cache = await self.config.properties()
            for pid, raw in legacy_cache.items():
                if not isinstance(raw, dict):
                    continue
                channel_id = _safe_int(raw.get("channel_id"))
                channel = guild.get_channel(channel_id) if channel_id else None
                if not isinstance(channel, discord.TextChannel):
                    continue
                guild_cache[str(pid)] = self._normalise_cached_state(str(pid), raw)
                adopted += 1

            if adopted:
                await guild_config.properties.set(guild_cache)

        legacy_settings = await self.config.settings()
        legacy_log_id = _safe_int(legacy_settings.get("log_channel_id")) if isinstance(legacy_settings, dict) else None
        if legacy_log_id and not settings.get("log_channel_id"):
            if isinstance(guild.get_channel(legacy_log_id), discord.TextChannel):
                settings["log_channel_id"] = legacy_log_id

        settings["legacy_migrated"] = True
        await guild_config.settings.set(settings)
        if adopted:
            await self._log(guild, f"Adopted {adopted} property record(s) from the previous global cache.")

    @staticmethod
    def _normalise_cached_state(pid: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        current_price = _safe_int(raw.get("current_price"))
        if current_price is None:
            current_price = _safe_int(raw.get("price"))
        original_price = _safe_int(raw.get("original_price"))
        if original_price is None:
            original_price = current_price

        history = raw.get("price_history") if isinstance(raw.get("price_history"), list) else []
        clean_history = []
        for item in history[-MAX_PRICE_HISTORY:]:
            if not isinstance(item, dict):
                continue
            price = _safe_int(item.get("price"))
            timestamp = _valid_timestamp(item.get("detected_ts"))
            if price is not None and timestamp is not None:
                clean_history.append({"price": price, "detected_ts": timestamp})
        if not clean_history and current_price is not None:
            clean_history.append(
                {
                    "price": current_price,
                    "detected_ts": _valid_timestamp(raw.get("first_seen_ts")) or _utc_ts(),
                }
            )

        return {
            "id": str(pid),
            "channel_id": _safe_int(raw.get("channel_id")),
            "message_id": _safe_int(raw.get("message_id")),
            "address": _normalise_space(raw.get("address")),
            "type": _normalise_space(raw.get("type")) or None,
            "number_bedrooms": _safe_float(raw.get("number_bedrooms")),
            "url": raw.get("url"),
            "image_url": raw.get("image_url"),
            "agent": _normalise_space(raw.get("agent")) or None,
            "agent_url": raw.get("agent_url"),
            "description": _normalise_space(raw.get("description")),
            "filter_text": _normalise_space(raw.get("filter_text")) or _normalise_space(raw.get("description")),
            "current_price": current_price,
            "original_price": original_price,
            "previous_price": _safe_int(raw.get("previous_price")),
            "price_history": clean_history,
            "listed_ts": _valid_timestamp(raw.get("listed_ts")),
            "activity_ts": _valid_timestamp(raw.get("activity_ts") or raw.get("updated_ts")),
            "activity_text": _normalise_space(raw.get("activity_text")),
            "first_seen_ts": _valid_timestamp(raw.get("first_seen_ts")) or _utc_ts(),
            "last_seen_ts": _valid_timestamp(raw.get("last_seen_ts")),
            "last_changed_ts": _valid_timestamp(raw.get("last_changed_ts")),
            "is_stc": bool(raw.get("is_stc", False)),
            "scheme_highlighted": bool(raw.get("scheme_highlighted", False)),
            "scheme_matches": [
                _normalise_space(item)
                for item in (raw.get("scheme_matches") if isinstance(raw.get("scheme_matches"), list) else [])
                if _normalise_space(item)
            ][:12],
            "fingerprint": raw.get("fingerprint"),
            "missing_count": max(0, _safe_int(raw.get("missing_count")) or 0),
            "active": bool(raw.get("active", True)),
        }

    def _discover_channels(self, guild: discord.Guild) -> Dict[str, List[discord.TextChannel]]:
        found: Dict[str, List[discord.TextChannel]] = {}
        for channel in guild.text_channels:
            pid = _channel_property_id(channel)
            if pid:
                found.setdefault(pid, []).append(channel)
        return found

    async def _find_property_message(
        self,
        channel: discord.TextChannel,
        cached_message_id: Optional[int],
    ) -> Optional[discord.Message]:
        if cached_message_id:
            try:
                message = await channel.fetch_message(cached_message_id)
                if message.author.id == self.bot.user.id:
                    return message
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        try:
            async for message in channel.history(limit=25, oldest_first=True):
                if message.author.id == self.bot.user.id and message.embeds:
                    return message
        except (discord.Forbidden, discord.HTTPException):
            return None
        return None

    @staticmethod
    def _filter_reason(
        row: Dict[str, Any],
        settings: Dict[str, Any],
        details_filter_text: str = "",
    ) -> Optional[str]:
        address = _normalise_space(row.get("address"))
        property_type = _normalise_space(row.get("type"))
        card_text = _normalise_space(row.get("card_text"))
        summary = _normalise_space(row.get("summary"))
        combined = " ".join([address, property_type, card_text, summary, details_filter_text])

        include_locations = [
            _normalise_space(value)
            for value in settings.get("include_locations", [])
            if _normalise_space(value)
        ]
        if include_locations and not any(_matches_phrase(address, value) for value in include_locations):
            return "address did not match any required location"

        for value in settings.get("exclude_locations", []):
            if _matches_phrase(address, value):
                return f"excluded location: {value}"

        normal_type = _normalise_match_text(property_type)
        stripped_type = re.sub(r"^\d+\s+bed(?:room)?s?\s+", "", normal_type).strip()
        for banned_type in settings.get("banned_property_types", []):
            banned = _normalise_match_text(banned_type)
            if not banned:
                continue
            if normal_type == banned or stripped_type == banned:
                return f"banned property type: {banned_type}"
            if re.search(rf"(?:^|\s){re.escape(banned)}(?:$|\s)", stripped_type):
                return f"banned property type: {banned_type}"

        for phrase in settings.get("banned_text", []):
            if _matches_phrase(combined, phrase):
                return f"banned text: {phrase}"

        return None

    @staticmethod
    def _scheme_highlight_matches(
        row: Dict[str, Any],
        settings: Dict[str, Any],
        details_filter_text: str = "",
    ) -> List[str]:
        """Return reasons for marking a permanently discounted ownership home.

        A star means the whole home is being sold at a legally retained discount
        to market value. Generic affordability language, new-build incentives,
        first-time-buyer advertising and partial-equity products do not qualify.
        """
        if not bool(settings.get("scheme_highlight_enabled", False)):
            return []

        combined = _normalise_space(
            " ".join(
                [
                    _normalise_space(row.get("address")),
                    _normalise_space(row.get("type")),
                    _normalise_space(row.get("card_text")),
                    _normalise_space(row.get("summary")),
                    _normalise_space(details_filter_text),
                ]
            )
        )

        # Never star products where the buyer owns only a share, pays rent on
        # retained equity, uses an equity loan, or receives leasehold tenure.
        if DISQUALIFYING_ASSISTANCE_RE.search(combined):
            return []

        matches: List[str] = []
        for label, pattern in PERMANENT_DISCOUNT_RULES:
            if pattern.search(combined):
                matches.append(label)

        # Custom phrases can cover a council/developer's local terminology, but
        # they remain subject to the disqualifying-assistance check above.
        for phrase in settings.get("scheme_highlight_terms", []):
            phrase = _normalise_space(phrase)
            if phrase and _matches_phrase(combined, phrase):
                matches.append(f"Custom permanent-discount term: {phrase}")

        unique: List[str] = []
        seen: set[str] = set()
        for item in matches:
            key = item.casefold()
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique[:12]

    async def _details_for_candidate(
        self,
        row: Dict[str, Any],
        old_state: Optional[Dict[str, Any]],
        ignored_state: Optional[Dict[str, Any]],
        *,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        fingerprint = row.get("fingerprint")
        if not force_refresh:
            if old_state and old_state.get("fingerprint") == fingerprint and (
                old_state.get("description") or old_state.get("filter_text")
            ) and (row.get("image_url") or old_state.get("image_url")):
                return {
                    "description": old_state.get("description", ""),
                    "filter_text": old_state.get("filter_text") or old_state.get("description", ""),
                    "image_url": row.get("image_url") or old_state.get("image_url") or "",
                }
            if ignored_state and ignored_state.get("fingerprint") == fingerprint:
                return {
                    "description": ignored_state.get("description", ""),
                    "filter_text": ignored_state.get("filter_text", ""),
                    "image_url": row.get("image_url") or "",
                }

        return await asyncio.to_thread(RightmoveClient.fetch_details, row["url"])

    def _merge_property_state(
        self,
        pid: str,
        row: Dict[str, Any],
        old_raw: Optional[Dict[str, Any]],
        details: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], str, bool]:
        now = _utc_ts()
        old = self._normalise_cached_state(pid, old_raw or {})
        is_new = old_raw is None

        old_price = _safe_int(old.get("current_price"))
        new_price = _safe_int(row.get("price"))
        original_price = _safe_int(old.get("original_price"))
        if original_price is None:
            original_price = old_price if old_price is not None else new_price

        history = list(old.get("price_history") or [])
        if not history and new_price is not None:
            history.append({"price": new_price, "detected_ts": now})
        elif new_price is not None and old_price is not None and new_price != old_price:
            history.append({"price": new_price, "detected_ts": now})
        history = history[-MAX_PRICE_HISTORY:]

        listed_ts = _valid_timestamp(old.get("listed_ts"))
        market_kind = row.get("market_kind")
        market_ts = _valid_timestamp(row.get("market_ts"))
        # The previous cog invented timestamps for unparseable text. When its
        # legacy listed/updated values are identical and Rightmove currently says
        # "Reduced", do not preserve that false value as the original list date.
        if old_raw and market_kind == "reduced" and not old_raw.get("activity_ts"):
            legacy_listed = _valid_timestamp(old_raw.get("listed_ts"))
            legacy_updated = _valid_timestamp(old_raw.get("updated_ts"))
            if legacy_listed is not None and legacy_listed == legacy_updated:
                listed_ts = None
        if market_kind == "added" and market_ts is not None:
            listed_ts = min(filter(None, [listed_ts, market_ts]), default=market_ts)

        activity_text = _normalise_space(row.get("marketed_text")) or old.get("activity_text") or ""
        activity_ts = market_ts if market_ts is not None else _valid_timestamp(old.get("activity_ts"))

        old_stc = bool(old.get("is_stc"))
        new_stc = bool(row.get("is_stc"))
        fingerprint_changed = bool(
            old_raw is not None
            and (not old.get("fingerprint") or old.get("fingerprint") != row.get("fingerprint"))
        )
        new_image_url = (
            RightmoveClient._normalise_image_url(row.get("image_url"), row.get("url") or "https://www.rightmove.co.uk")
            or RightmoveClient._normalise_image_url(details.get("image_url"), row.get("url") or "https://www.rightmove.co.uk")
            or old.get("image_url")
        )
        image_changed = bool(old_raw is not None and new_image_url != old.get("image_url"))
        price_changed = old_price is not None and new_price is not None and old_price != new_price
        status_changed = old_stc != new_stc
        old_scheme_matches = [
            _normalise_space(item)
            for item in (old.get("scheme_matches") if isinstance(old.get("scheme_matches"), list) else [])
            if _normalise_space(item)
        ]
        new_scheme_matches = [
            _normalise_space(item)
            for item in (details.get("scheme_matches") if isinstance(details.get("scheme_matches"), list) else [])
            if _normalise_space(item)
        ]
        scheme_changed = old_scheme_matches != new_scheme_matches

        if is_new:
            event = "new"
        elif status_changed and new_stc:
            event = "stc"
        elif status_changed and not new_stc:
            event = "returned"
        elif price_changed and new_price < old_price:
            event = "price_reduced"
        elif price_changed and new_price > old_price:
            event = "price_increased"
        elif scheme_changed:
            event = "highlight_updated"
        elif fingerprint_changed or image_changed:
            event = "details_updated"
        else:
            event = "unchanged"

        changed = event != "unchanged"
        description = _normalise_space(details.get("description")) or old.get("description") or ""
        filter_text = _normalise_space(details.get("filter_text")) or old.get("filter_text") or description

        state = {
            "id": pid,
            "channel_id": old.get("channel_id"),
            "message_id": old.get("message_id"),
            "address": _normalise_space(row.get("address")),
            "type": _normalise_space(row.get("type")) or None,
            "number_bedrooms": _safe_float(row.get("number_bedrooms")),
            "url": row.get("url"),
            "image_url": new_image_url,
            "agent": _normalise_space(row.get("agent")) or None,
            "agent_url": row.get("agent_url"),
            "description": description,
            "filter_text": filter_text,
            "current_price": new_price,
            "original_price": original_price,
            "previous_price": old_price if price_changed else old.get("previous_price"),
            "price_history": history,
            "listed_ts": listed_ts,
            "activity_ts": activity_ts,
            "activity_text": activity_text,
            "first_seen_ts": _valid_timestamp(old.get("first_seen_ts")) or now,
            "last_seen_ts": now,
            "last_changed_ts": now if changed else _valid_timestamp(old.get("last_changed_ts")),
            "is_stc": new_stc,
            "scheme_highlighted": bool(new_scheme_matches),
            "scheme_matches": new_scheme_matches,
            "fingerprint": row.get("fingerprint"),
            "missing_count": 0,
            "active": True,
        }
        return state, event, changed

    async def _build_embed(
        self,
        state: Dict[str, Any],
        settings: Dict[str, Any],
        *,
        event: str,
    ) -> discord.Embed:
        labels = {
            "new": ("🆕", "New Listing"),
            "price_reduced": ("📉", "Price Reduced"),
            "price_increased": ("📈", "Price Increased"),
            "stc": ("💖", "Sold STC"),
            "returned": ("↩️", "Returned to Market"),
            "highlight_updated": ("⭐", "Scheme Highlight Updated"),
            "details_updated": ("✏️", "Listing Updated"),
            "refresh": ("🏠", "Property Listing"),
            "unchanged": ("🏠", "Property Listing"),
        }
        icon, label = labels.get(event, labels["refresh"])
        _, tier_color = _tier_for_price(state.get("current_price"), settings)
        color = discord.Color.magenta() if state.get("is_stc") else tier_color

        address = state.get("address") or f"Rightmove property {state.get('id')}"
        embed = discord.Embed(
            title=_truncate(f"{icon} {label} — {address}", 256),
            color=color,
        )
        if state.get("url"):
            embed.url = state["url"]

        if state.get("image_url"):
            embed.set_thumbnail(url=state["image_url"])

        current_price = _safe_int(state.get("current_price"))
        original_price = _safe_int(state.get("original_price"))
        embed.add_field(name="💷 Current Price", value=_format_price(current_price), inline=True)
        embed.add_field(name="🏷️ Original / First Recorded Price", value=_format_price(original_price), inline=True)

        if current_price is not None and original_price is not None and current_price != original_price:
            difference = current_price - original_price
            percentage = (difference / original_price * 100) if original_price else 0
            sign = "+" if difference > 0 else "−"
            change_value = f"{sign}£{abs(difference):,} ({percentage:+.1f}%)"
        else:
            change_value = "No recorded change"
        embed.add_field(name="📊 Overall Change", value=change_value, inline=True)

        bedrooms = _safe_float(state.get("number_bedrooms"))
        bedroom_value = str(int(bedrooms)) if bedrooms is not None and bedrooms.is_integer() else (
            str(bedrooms) if bedrooms is not None else "Unknown"
        )
        embed.add_field(name="🛏 Bedrooms", value=bedroom_value, inline=True)
        embed.add_field(name="🏠 Type", value=state.get("type") or "Unknown", inline=True)
        embed.add_field(name="📌 Status", value="Sold STC" if state.get("is_stc") else "Available", inline=True)

        scheme_matches = state.get("scheme_matches") if isinstance(state.get("scheme_matches"), list) else []
        if state.get("scheme_highlighted") and scheme_matches:
            marker = _highlight_emoji(settings)
            embed.add_field(
                name=f"{marker} Buyer / Affordable Scheme",
                value=_truncate("\n".join(f"• {item}" for item in scheme_matches), 1024),
                inline=False,
            )

        embed.add_field(
            name="📅 Listed",
            value=_format_discord_time(state.get("listed_ts"), fallback="Not supplied by Rightmove"),
            inline=True,
        )
        activity_text = state.get("activity_text") or "No dated activity supplied"
        activity_ts = _valid_timestamp(state.get("activity_ts"))
        listed_ts = _valid_timestamp(state.get("listed_ts"))
        duplicate_added_date = (
            "added" in activity_text.casefold()
            and activity_ts is not None
            and activity_ts == listed_ts
        )
        if not duplicate_added_date:
            activity_value = _truncate(activity_text, 700)
            if activity_ts:
                activity_value += f"\n{_format_discord_time(activity_ts)}"
            embed.add_field(name="🕒 Latest Rightmove Activity", value=activity_value, inline=True)
        embed.add_field(
            name="👁️ First Seen by Bot",
            value=_format_discord_time(state.get("first_seen_ts")),
            inline=True,
        )

        history = state.get("price_history") if isinstance(state.get("price_history"), list) else []
        if len(history) > 1:
            history_lines = []
            for item in history[-5:]:
                price = _format_price(item.get("price"))
                detected = _format_discord_time(item.get("detected_ts"), fallback="Unknown date")
                history_lines.append(f"{price} — {detected}")
            embed.add_field(name="📜 Recent Price History", value="\n".join(history_lines), inline=False)

        if state.get("agent"):
            agent_value = state["agent"]
            if state.get("agent_url"):
                agent_value = f"[{agent_value}]({state['agent_url']})"
            embed.add_field(name="🏢 Agent", value=_truncate(agent_value, 1024), inline=True)

        if state.get("url"):
            embed.add_field(
                name="🔗 Listing",
                value=f"[View on Rightmove]({state['url']})",
                inline=True,
            )

        description = _normalise_space(state.get("description"))
        if description:
            embed.add_field(name="📝 Description", value=_truncate(description, 1024), inline=False)

        embed.set_footer(
            text=f"Property ID {state.get('id')} • Checked {dt.datetime.now(LONDON):%d %b %Y %H:%M %Z}"
        )
        return embed

    async def _ensure_channel_and_message(
        self,
        guild: discord.Guild,
        anchor: discord.TextChannel,
        settings: Dict[str, Any],
        state: Dict[str, Any],
        event: str,
        discovered: Dict[str, List[discord.TextChannel]],
        *,
        force_refresh: bool,
    ) -> Tuple[Dict[str, Any], bool, bool]:
        pid = state["id"]
        channel: Optional[discord.TextChannel] = None
        cached_channel_id = _safe_int(state.get("channel_id"))
        cached_channel = guild.get_channel(cached_channel_id) if cached_channel_id else None
        if isinstance(cached_channel, discord.TextChannel) and _channel_property_id(cached_channel) == pid:
            channel = cached_channel
        elif discovered.get(pid):
            channel = sorted(discovered[pid], key=lambda item: item.id)[0]

        created_channel = False
        if channel is None:
            channel = await guild.create_text_channel(
                _desired_channel_name(
                    pid,
                    state.get("current_price"),
                    settings,
                    highlighted=bool(state.get("scheme_highlighted")),
                ),
                topic=_property_topic(pid, state.get("url"), settings.get("profile_name", "Rightmove")),
                overwrites=anchor.overwrites,
                reason=f"Rightmove property {pid}",
            )
            discovered.setdefault(pid, []).append(channel)
            created_channel = True

        state["channel_id"] = channel.id
        desired_name = _desired_channel_name(
            pid,
            state.get("current_price"),
            settings,
            highlighted=bool(state.get("scheme_highlighted")),
        )
        desired_topic = _property_topic(pid, state.get("url"), settings.get("profile_name", "Rightmove"))
        channel_needs_edit = channel.name != desired_name or channel.topic != desired_topic
        if channel_needs_edit:
            try:
                await channel.edit(name=desired_name, topic=desired_topic, reason="Rightmove listing metadata changed")
            except (discord.Forbidden, discord.HTTPException) as exc:
                await self._log(guild, f"Could not rename/update topic for {channel.mention}: {exc}")

        message = await self._find_property_message(channel, _safe_int(state.get("message_id")))
        should_write_embed = created_channel or message is None or event != "unchanged" or force_refresh
        wrote_embed = False
        if should_write_embed:
            embed_event = "new" if created_channel else ("refresh" if event == "unchanged" else event)
            embed = await self._build_embed(state, settings, event=embed_event)
            if message is None:
                message = await channel.send(embed=embed)
            else:
                await message.edit(embed=embed)
            state["message_id"] = message.id
            wrote_embed = True
        elif message is not None:
            state["message_id"] = message.id

        return state, created_channel, wrote_embed

    async def _reorder_channels(
        self,
        guild: discord.Guild,
        anchor: discord.TextChannel,
        cache: Dict[str, Dict[str, Any]],
    ) -> Tuple[int, List[str]]:
        """Move property channels out of legacy categories, then sort by price.

        Discord permits only one ``parent_id`` change per bulk request. Therefore
        category removal is deliberately performed one channel at a time. Once all
        movable channels are top-level, one position-only bulk request establishes
        the final order without touching any parent IDs.
        """
        active: List[Tuple[int, str, discord.TextChannel]] = []
        seen_channel_ids: set[int] = set()

        for pid, raw in cache.items():
            state = self._normalise_cached_state(pid, raw)
            if not state.get("active"):
                continue
            channel_id = _safe_int(state.get("channel_id"))
            channel = guild.get_channel(channel_id) if channel_id else None
            if not isinstance(channel, discord.TextChannel):
                continue
            if channel.id == anchor.id or channel.id in seen_channel_ids:
                continue
            seen_channel_ids.add(channel.id)
            price = _safe_int(state.get("current_price"))
            active.append((price if price is not None else 10**18, pid, channel))

        active.sort(key=lambda item: (item[0], int(item[1]) if item[1].isdigit() else item[1]))
        if not active:
            return 0, []

        errors: List[str] = []
        changed_channel_ids: set[int] = set()
        orderable: List[Tuple[int, str, discord.TextChannel]] = []

        # Phase 1: detach from old categories individually. Discord rejects a
        # bulk payload containing parent_id changes for multiple channels.
        for price, pid, channel in active:
            if channel.category_id is not None:
                try:
                    edited = await channel.edit(
                        category=None,
                        sync_permissions=False,
                        reason="Remove legacy Rightmove category",
                    )
                    if isinstance(edited, discord.TextChannel):
                        channel = edited
                    else:
                        refreshed = guild.get_channel(channel.id)
                        if isinstance(refreshed, discord.TextChannel):
                            channel = refreshed
                    changed_channel_ids.add(channel.id)
                    await asyncio.sleep(0.20)
                except asyncio.CancelledError:
                    raise
                except (discord.Forbidden, discord.HTTPException, TypeError) as exc:
                    errors.append(f"prop-{pid}: could not leave old category: {exc}")
                    continue
            orderable.append((price, pid, channel))

        if not orderable:
            return len(changed_channel_ids), errors

        property_channels = [item[2] for item in orderable]
        property_ids = {channel.id for channel in property_channels}
        anchor_bucket = anchor._sorting_bucket

        current_top_level = sorted(
            [
                channel
                for channel in guild.channels
                if getattr(channel, "_sorting_bucket", None) == anchor_bucket
                and getattr(channel, "category_id", None) is None
            ],
            key=lambda channel: (channel.position, channel.id),
        )

        try:
            anchor_index = next(
                index for index, channel in enumerate(current_top_level) if channel.id == anchor.id
            )
        except StopIteration:
            errors.append("The anchor channel is not present in the uncategorised text-channel list.")
            return len(changed_channel_ids), errors

        desired_property_ids = [channel.id for channel in property_channels]
        current_ids = [channel.id for channel in current_top_level]
        current_after_anchor = current_ids[
            anchor_index + 1 : anchor_index + 1 + len(desired_property_ids)
        ]
        already_contiguous = current_after_anchor == desired_property_ids

        if not already_contiguous:
            siblings_without_properties = [
                channel for channel in current_top_level if channel.id not in property_ids
            ]
            try:
                clean_anchor_index = next(
                    index
                    for index, channel in enumerate(siblings_without_properties)
                    if channel.id == anchor.id
                )
            except StopIteration:
                errors.append("The anchor channel disappeared while preparing the reorder.")
                return len(changed_channel_ids), errors

            desired = (
                siblings_without_properties[: clean_anchor_index + 1]
                + property_channels
                + siblings_without_properties[clean_anchor_index + 1 :]
            )

            # Position-only payload: no parent_id keys are included here.
            payload = [
                {"id": channel.id, "position": position}
                for position, channel in enumerate(desired)
            ]

            try:
                await anchor._state.http.bulk_channel_update(
                    guild.id,
                    payload,
                    reason="Sort Rightmove properties by price",
                )
                changed_channel_ids.update(property_ids)
            except asyncio.CancelledError:
                raise
            except (discord.Forbidden, discord.HTTPException, TypeError) as exc:
                # Conservative fallback. All participating channels are already
                # uncategorised, so relative moves can now be resolved safely.
                errors.append(f"Bulk position update failed; using sequential fallback: {exc}")
                previous: discord.TextChannel = anchor
                for _, pid, channel in orderable:
                    try:
                        latest_top_level = sorted(
                            [
                                item
                                for item in guild.text_channels
                                if item.category_id is None
                            ],
                            key=lambda item: (item.position, item.id),
                        )
                        correct = (
                            previous in latest_top_level
                            and channel in latest_top_level
                            and latest_top_level.index(channel)
                            == latest_top_level.index(previous) + 1
                        )
                        if not correct:
                            await channel.move(
                                after=previous,
                                reason="Sort Rightmove properties by price",
                            )
                            changed_channel_ids.add(channel.id)
                            await asyncio.sleep(0.20)
                        previous = channel
                    except asyncio.CancelledError:
                        raise
                    except (discord.Forbidden, discord.HTTPException, TypeError) as fallback_exc:
                        errors.append(f"prop-{pid}: could not be positioned: {fallback_exc}")

        return len(changed_channel_ids), errors

    async def _run_scrape(
        self,
        guild: discord.Guild,
        *,
        source: str,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        lock = self._lock_for(guild.id)
        if lock.locked() and source != "scheduled":
            return {"ok": False, "message": "A scrape is already running for this server."}

        # Direct setup/manual/adopt runs used to be invisible to ``rm stop``
        # because only background tasks were recorded in _active_runs. Track
        # the current task too, without replacing a task already registered by
        # _launch_background_run. This lets ``rm stop`` cancel any live scrape.
        current_task = asyncio.current_task()
        registered_here = False
        active = self._active_runs.get(guild.id)
        if current_task is not None and (active is None or active.done()):
            self._active_runs[guild.id] = current_task
            registered_here = True

        try:
            async with lock:
                guild_config = self.config.guild(guild)
                settings = await guild_config.settings()
                settings["last_attempt_ts"] = _utc_ts()
                await guild_config.settings.set(settings)

                try:
                    await self._migrate_legacy_global(guild)
                    settings = await guild_config.settings()
                    search_url = settings.get("search_url")
                    anchor_id = _safe_int(settings.get("anchor_channel_id"))
                    anchor = guild.get_channel(anchor_id) if anchor_id else None

                    if not search_url:
                        raise RuntimeError("No search URL is configured. Use `rm seturl <Rightmove URL>`. ")
                    valid, reason = _validate_rightmove_url(search_url)
                    if not valid:
                        raise RuntimeError(reason)
                    if not isinstance(anchor, discord.TextChannel):
                        raise RuntimeError("The configured anchor channel no longer exists. Use `rm setanchor`.")
                    if anchor.category is not None:
                        raise RuntimeError("The anchor channel must be uncategorised so property channels can sit below it.")

                    scrape = await asyncio.to_thread(
                        RightmoveClient.scrape_search,
                        search_url,
                        include_discounted_ownership=bool(
                            settings.get("include_discounted_ownership_search", False)
                        ),
                    )
                    if scrape.pages_fetched == 0:
                        raise RuntimeError("The Rightmove search could not be fetched: " + "; ".join(scrape.errors))

                    cache_raw = await guild_config.properties()
                    cache: Dict[str, Dict[str, Any]] = {
                        str(pid): self._normalise_cached_state(str(pid), raw)
                        for pid, raw in cache_raw.items()
                        if isinstance(raw, dict)
                    }
                    ignored = await guild_config.ignored_properties()
                    discovered = self._discover_channels(guild)

                    accepted: Dict[str, Tuple[Dict[str, Any], Dict[str, str]]] = {}
                    newly_ignored = 0
                    for pid, row in scrape.properties.items():
                        preliminary_reason = self._filter_reason(row, settings)
                        if preliminary_reason:
                            ignored[pid] = {
                                "fingerprint": row.get("fingerprint"),
                                "reason": preliminary_reason,
                                "description": "",
                                "filter_text": "",
                                "last_seen_ts": _utc_ts(),
                            }
                            newly_ignored += 1
                            continue

                        details = await self._details_for_candidate(
                            row,
                            cache.get(pid),
                            ignored.get(pid) if isinstance(ignored.get(pid), dict) else None,
                            force_refresh=force_refresh,
                        )
                        final_reason = self._filter_reason(row, settings, details.get("filter_text", ""))
                        if final_reason:
                            ignored[pid] = {
                                "fingerprint": row.get("fingerprint"),
                                "reason": final_reason,
                                "description": details.get("description", ""),
                                "filter_text": details.get("filter_text", ""),
                                "last_seen_ts": _utc_ts(),
                            }
                            newly_ignored += 1
                            continue

                        details["scheme_matches"] = self._scheme_highlight_matches(
                            row,
                            settings,
                            details.get("filter_text", ""),
                        )
                        ignored.pop(pid, None)
                        accepted[pid] = (row, details)

                    created = 0
                    updated = 0
                    recovered = 0
                    unchanged = 0
                    errors: List[str] = []

                    for pid, (row, details) in sorted(
                        accepted.items(),
                        key=lambda item: (_safe_int(item[1][0].get("price")) or 10**18, item[0]),
                    ):
                        old_raw = cache.get(pid)
                        state, event, changed = self._merge_property_state(pid, row, old_raw, details)
                        had_valid_channel = False
                        if old_raw:
                            old_channel_id = _safe_int(old_raw.get("channel_id"))
                            had_valid_channel = isinstance(guild.get_channel(old_channel_id), discord.TextChannel)

                        try:
                            state, made_channel, wrote_embed = await self._ensure_channel_and_message(
                                guild,
                                anchor,
                                settings,
                                state,
                                event,
                                discovered,
                                force_refresh=force_refresh,
                            )
                        except (discord.Forbidden, discord.HTTPException) as exc:
                            errors.append(f"prop-{pid}: {exc}")
                            continue

                        cache[pid] = state
                        # Persist after each property so Discord and Config cannot drift far apart.
                        await guild_config.properties.set(cache)

                        if made_channel:
                            created += 1
                        elif wrote_embed and (changed or force_refresh):
                            updated += 1
                        else:
                            unchanged += 1

                        if old_raw and not had_valid_channel and not made_channel:
                            recovered += 1

                    # Missing listings are only counted after a demonstrably complete scrape.
                    removed = 0
                    missing_marked = 0
                    if scrape.complete:
                        accepted_ids = set(accepted)
                        threshold = max(1, int(settings.get("missing_confirmations", 3)))
                        for pid in list(cache):
                            if pid in accepted_ids:
                                continue
                            state = self._normalise_cached_state(pid, cache[pid])
                            state["missing_count"] = int(state.get("missing_count", 0)) + 1
                            missing_marked += 1
                            if state["missing_count"] >= threshold:
                                channel_id = _safe_int(state.get("channel_id"))
                                channel = guild.get_channel(channel_id) if channel_id else None
                                if isinstance(channel, discord.TextChannel):
                                    try:
                                        await channel.delete(reason="Rightmove listing absent from complete scrapes")
                                    except (discord.Forbidden, discord.HTTPException) as exc:
                                        errors.append(f"Could not delete prop-{pid}: {exc}")
                                        cache[pid] = state
                                        continue
                                cache.pop(pid, None)
                                removed += 1
                            else:
                                cache[pid] = state
                            await guild_config.properties.set(cache)
                    else:
                        errors.append("Scrape was incomplete; no missing counters were changed and no channels were deleted.")

                    await guild_config.ignored_properties.set(ignored)
                    await guild_config.properties.set(cache)

                    moved, move_errors = await self._reorder_channels(guild, anchor, cache)
                    errors.extend(move_errors)

                    now = _utc_ts()
                    settings = await guild_config.settings()
                    if scrape.complete:
                        settings["last_success_ts"] = now
                        settings["consecutive_failures"] = 0
                    else:
                        # Keep last_success_ts unchanged so the scheduler retries later
                        # instead of treating a partial scrape as the day's success.
                        settings["consecutive_failures"] = int(settings.get("consecutive_failures", 0)) + 1
                    settings["last_summary"] = _encode_summary({
                        "timestamp": now,
                        "source": source,
                        "complete": scrape.complete,
                        "expected": scrape.expected_count,
                        "parsed": len(scrape.properties),
                        "accepted": len(accepted),
                        "created": created,
                        "updated": updated,
                        "unchanged": unchanged,
                        "removed": removed,
                        "moved": moved,
                        "ignored": newly_ignored,
                        "highlighted": sum(
                            1 for state in cache.values()
                            if isinstance(state, dict) and state.get("scheme_highlighted")
                        ),
                        "errors": errors[-10:],
                    })
                    await guild_config.settings.set(settings)

                    summary = (
                        f"Scrape complete for **{settings.get('profile_name', 'Rightmove')}**: "
                        f"{len(accepted)} accepted, {created} created, {updated} updated, "
                        f"{unchanged} unchanged, {removed} removed, {moved} moved, "
                        f"{sum(1 for state in cache.values() if isinstance(state, dict) and state.get('scheme_highlighted'))} highlighted. "
                        f"Rightmove parse complete: **{scrape.complete}**."
                    )
                    if scrape.errors:
                        summary += "\nScraper notes: " + " | ".join(scrape.errors[:5])
                    if errors:
                        summary += "\nOperational notes: " + " | ".join(errors[:5])
                    await self._log(guild, summary)
                    return {"ok": True, "message": summary}

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    settings = await guild_config.settings()
                    settings["consecutive_failures"] = int(settings.get("consecutive_failures", 0)) + 1
                    settings["last_summary"] = _encode_summary({
                        "timestamp": _utc_ts(),
                        "source": source,
                        "complete": False,
                        "error": repr(exc),
                    })
                    await guild_config.settings.set(settings)
                    await self._log(guild, f"Scrape failed: {exc!r}")
                    log.exception("Rightmove scrape failed in guild %s", guild.id)
                    return {"ok": False, "message": f"Scrape failed: {exc}"}
        finally:
            if registered_here and self._active_runs.get(guild.id) is current_task:
                self._active_runs.pop(guild.id, None)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    @commands.group(name="rm", invoke_without_command=True)
    @commands.guild_only()
    @commands.is_owner()
    async def rm(self, ctx: commands.Context) -> None:
        """Configure and run the per-server Rightmove monitor."""
        await ctx.send_help(ctx.command)

    @rm.command(name="setup")
    async def rm_setup(
        self,
        ctx: commands.Context,
        anchor: discord.TextChannel,
        *,
        search_url: str,
    ) -> None:
        """Set the anchor and a complete Rightmove search URL, then enable monitoring."""
        if anchor.category is not None:
            return await ctx.send("❌ The anchor channel must be outside all categories.")
        valid, reason = _validate_rightmove_url(search_url)
        if not valid:
            return await ctx.send(f"❌ {reason}")

        guild_config = self.config.guild(ctx.guild)
        settings = await guild_config.settings()
        settings["anchor_channel_id"] = anchor.id
        settings["search_url"] = _canonical_url(search_url, index=0)
        settings["enabled"] = True
        if settings.get("profile_name") == "Rightmove":
            settings["profile_name"] = ctx.guild.name
        await guild_config.settings.set(settings)
        await self._migrate_legacy_global(ctx.guild)

        await ctx.send(
            f"✅ Configured **{settings['profile_name']}** using {anchor.mention}. "
            "Monitoring is enabled; running the first reconciliation now."
        )
        result = await self._run_scrape(ctx.guild, source="setup", force_refresh=True)
        await ctx.send(("✅ " if result["ok"] else "❌ ") + result["message"][:1900])

    @rm.command(name="seturl")
    async def rm_seturl(self, ctx: commands.Context, *, search_url: str) -> None:
        """Set this server's Rightmove URL, including any drawn-area polygon."""
        valid, reason = _validate_rightmove_url(search_url)
        if not valid:
            return await ctx.send(f"❌ {reason}")
        await self.config.guild(ctx.guild).settings.set_raw(
            "search_url", value=_canonical_url(search_url, index=0)
        )
        await ctx.send("✅ This server's Rightmove search URL has been saved.")

    @rm.command(name="affordable", aliases=["discountedsearch"])
    async def rm_affordable(self, ctx: commands.Context, mode: str = "status") -> None:
        """Include permanent-discount LCHO/RSL homes hidden by broad Rightmove flags."""
        mode = (mode or "status").strip().casefold()
        settings = await self.config.guild(ctx.guild).settings()
        if mode in {"on", "enable", "enabled", "yes", "true"}:
            settings["include_discounted_ownership_search"] = True
            await self.config.guild(ctx.guild).settings.set(settings)
            return await ctx.send(
                "✅ Discounted-ownership search enabled for this server. Rightmove's broad "
                "shared-ownership/part-buy search flags will be ignored so LCHO/RSL permanent-"
                "discount homes can appear. Actual shared ownership, shared equity, rent-on-"
                "remainder and leasehold listings are still rejected from their own details."
            )
        if mode in {"off", "disable", "disabled", "no", "false"}:
            settings["include_discounted_ownership_search"] = False
            await self.config.guild(ctx.guild).settings.set(settings)
            return await ctx.send(
                "✅ Discounted-ownership search disabled for this server. The saved Rightmove "
                "search flags will be used exactly as supplied."
            )
        if mode not in {"status", "show", "list"}:
            return await ctx.send("❌ Use `.rm affordable on`, `.rm affordable off`, or `.rm affordable status`.")
        enabled = bool(settings.get("include_discounted_ownership_search", False))
        await ctx.send(
            "**Discounted-ownership search:** " + ("Enabled" if enabled else "Disabled") + "\n"
            + (
                "LCHO/RSL and other permanent-discount homes can pass Rightmove's broad search bucket; "
                "genuine shared ownership remains blocked by the cog."
                if enabled
                else "Rightmove's own shared-ownership/part-buy exclusions remain in force."
            )
        )

    @rm.command(name="setanchor")
    async def rm_setanchor(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        """Set the uncategorised channel immediately above the property block."""
        channel = channel or ctx.channel
        if channel.category is not None:
            return await ctx.send("❌ The anchor channel must be outside all categories.")
        await self.config.guild(ctx.guild).settings.set_raw("anchor_channel_id", value=channel.id)
        await ctx.send(f"✅ Property channels will be kept directly below {channel.mention}.")

    @rm.command(name="setlog")
    async def rm_setlog(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        """Set a log channel; omit the channel to disable Discord logs."""
        await self.config.guild(ctx.guild).settings.set_raw(
            "log_channel_id", value=channel.id if channel else None
        )
        await ctx.send(f"✅ Log channel set to {channel.mention}." if channel else "✅ Discord logging disabled.")

    @rm.command(name="setname")
    async def rm_setname(self, ctx: commands.Context, *, name: str) -> None:
        name = _truncate(_normalise_space(name), 80)
        if not name:
            return await ctx.send("❌ Supply a profile name, such as `Hampshire` or `Cumbria`.")
        await self.config.guild(ctx.guild).settings.set_raw("profile_name", value=name)
        await ctx.send(f"✅ This search profile is now called **{name}**.")

    @rm.command(name="settime")
    async def rm_settime(self, ctx: commands.Context, value: str) -> None:
        """Set the daily London-time scrape as HH:MM."""
        match = re.fullmatch(r"(\d{1,2}):(\d{2})", value.strip())
        if not match:
            return await ctx.send("❌ Use 24-hour time in `HH:MM` format, for example `07:00`.")
        hour, minute = map(int, match.groups())
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            return await ctx.send("❌ That is not a valid time.")
        guild_config = self.config.guild(ctx.guild)
        settings = await guild_config.settings()
        settings["scrape_hour"] = hour
        settings["scrape_minute"] = minute
        await guild_config.settings.set(settings)
        await ctx.send(f"✅ Daily scrape set to **{hour:02d}:{minute:02d} Europe/London**.")

    @rm.command(name="settiers")
    async def rm_settiers(self, ctx: commands.Context, green_max: int, orange_max: int) -> None:
        if green_max < 1 or orange_max <= green_max:
            return await ctx.send("❌ The orange maximum must be greater than the green maximum.")
        guild_config = self.config.guild(ctx.guild)
        settings = await guild_config.settings()
        settings["green_max"] = green_max
        settings["orange_max"] = orange_max
        await guild_config.settings.set(settings)
        await ctx.send(
            f"✅ Price tiers set: 🟢 up to £{green_max:,}; 🟠 up to £{orange_max:,}; 🔴 above that."
        )

    @rm.command(name="setmissing")
    async def rm_setmissing(self, ctx: commands.Context, confirmations: int) -> None:
        if not 1 <= confirmations <= 30:
            return await ctx.send("❌ Choose between 1 and 30 complete scrapes.")
        await self.config.guild(ctx.guild).settings.set_raw(
            "missing_confirmations", value=confirmations
        )
        await ctx.send(
            f"✅ A property must be absent from {confirmations} complete scrape(s) before its channel is removed."
        )

    @rm.command(name="start")
    async def rm_start(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
    ) -> None:
        """Enable persistent daily monitoring for this server."""
        guild_config = self.config.guild(ctx.guild)
        settings = await guild_config.settings()
        if channel:
            if channel.category is not None:
                return await ctx.send("❌ The anchor channel must be outside all categories.")
            settings["anchor_channel_id"] = channel.id
        if not settings.get("search_url"):
            return await ctx.send("❌ Set a search URL first with `rm seturl <URL>` or use `rm setup`.")
        anchor_id = _safe_int(settings.get("anchor_channel_id"))
        anchor = ctx.guild.get_channel(anchor_id) if anchor_id else None
        if not isinstance(anchor, discord.TextChannel) or anchor.category is not None:
            return await ctx.send("❌ Set a valid uncategorised anchor with `rm setanchor`.")
        settings["enabled"] = True
        await guild_config.settings.set(settings)
        await self._migrate_legacy_global(ctx.guild)
        launched = self._launch_background_run(ctx.guild, source="start", force_refresh=False)
        await ctx.send(
            "✅ Persistent daily monitoring enabled. "
            + ("An initial reconciliation has started." if launched else "A reconciliation is already running.")
        )

    @rm.command(name="stop")
    async def rm_stop(self, ctx: commands.Context) -> None:
        """Disable scheduled monitoring and cancel any scrape currently running."""
        await self.config.guild(ctx.guild).settings.set_raw("enabled", value=False)
        active = self._active_runs.get(ctx.guild.id)
        cancelled = bool(active and not active.done())
        if cancelled:
            active.cancel()
        await ctx.send(
            "✅ Scheduled monitoring disabled for this server."
            + (" The active scrape was cancelled." if cancelled else " No scrape was running.")
        )

    @rm.command(name="run", aliases=["test"])
    async def rm_run(self, ctx: commands.Context, option: Optional[str] = None) -> None:
        """Run a reconciliation now. Add `refresh` to rewrite all embeds."""
        force_refresh = (option or "").casefold() == "refresh"
        if option and not force_refresh:
            return await ctx.send("❌ The only optional argument is `refresh`.")
        if self._lock_for(ctx.guild.id).locked():
            return await ctx.send("❌ A scrape is already running for this server.")
        await ctx.send("🔄 Running a Rightmove reconciliation…")
        result = await self._run_scrape(ctx.guild, source="manual", force_refresh=force_refresh)
        await ctx.send(("✅ " if result["ok"] else "❌ ") + result["message"][:1900])

    @rm.command(name="status")
    async def rm_status(self, ctx: commands.Context) -> None:
        data = await self.config.guild(ctx.guild).all()
        settings = data["settings"]
        properties = data["properties"]
        active_task = self._active_runs.get(ctx.guild.id)
        running = bool(active_task and not active_task.done()) or self._lock_for(ctx.guild.id).locked()
        anchor_id = _safe_int(settings.get("anchor_channel_id"))
        log_id = _safe_int(settings.get("log_channel_id"))

        embed = discord.Embed(
            title=f"Rightmove — {settings.get('profile_name', 'Rightmove')}",
            color=discord.Color.green() if settings.get("enabled") else discord.Color.orange(),
        )
        embed.add_field(name="Enabled", value=str(bool(settings.get("enabled"))), inline=True)
        embed.add_field(name="Running now", value=str(running), inline=True)
        embed.add_field(name="Tracked properties", value=str(len(properties)), inline=True)
        embed.add_field(
            name="Daily time",
            value=f"{int(settings.get('scrape_hour', 7)):02d}:{int(settings.get('scrape_minute', 0)):02d} Europe/London",
            inline=True,
        )
        embed.add_field(name="Anchor", value=f"<#{anchor_id}>" if anchor_id else "Not set", inline=True)
        embed.add_field(name="Log", value=f"<#{log_id}>" if log_id else "Disabled", inline=True)
        embed.add_field(
            name="Deletion safety",
            value=f"{settings.get('missing_confirmations', 3)} complete missing scrapes",
            inline=True,
        )
        embed.add_field(
            name="Price tiers",
            value=(
                f"🟢 ≤ £{int(settings.get('green_max', 220000)):,}\n"
                f"🟠 ≤ £{int(settings.get('orange_max', 250000)):,}\n🔴 above"
            ),
            inline=True,
        )
        embed.add_field(
            name="Last success",
            value=_format_discord_time(settings.get("last_success_ts"), fallback="Never"),
            inline=True,
        )
        highlighted_count = sum(
            1 for state in properties.values()
            if isinstance(state, dict) and state.get("scheme_highlighted")
        )
        embed.add_field(
            name="Scheme highlighting",
            value=(
                f"{_highlight_emoji(settings)} Enabled • {highlighted_count} highlighted"
                if settings.get("scheme_highlight_enabled")
                else "Disabled"
            ),
            inline=True,
        )
        embed.add_field(
            name="Discounted ownership search",
            value=(
                "Enabled • broad Rightmove ownership flags bypassed"
                if settings.get("include_discounted_ownership_search")
                else "Disabled"
            ),
            inline=True,
        )
        include = settings.get("include_locations", [])
        exclude = settings.get("exclude_locations", [])
        embed.add_field(
            name="Location filters",
            value=f"Required: {', '.join(include) if include else 'None'}\nExcluded: {', '.join(exclude) if exclude else 'None'}",
            inline=False,
        )
        url = settings.get("search_url")
        embed.add_field(name="Search URL", value=_truncate(url or "Not set", 1024), inline=False)

        last_summary = _decode_summary(settings.get("last_summary"))
        if last_summary:
            embed.add_field(
                name="Last result",
                value=_truncate(f"```json\n{json.dumps(last_summary, indent=2)}\n```", 1024),
                inline=False,
            )
        await ctx.send(embed=embed)

    @rm.group(name="exclude", invoke_without_command=True)
    async def rm_exclude(self, ctx: commands.Context) -> None:
        """Manage address phrases that are excluded after the drawn-area search."""
        await ctx.send_help(ctx.command)

    @rm_exclude.command(name="add")
    async def rm_exclude_add(self, ctx: commands.Context, *, phrase: str) -> None:
        phrase = _normalise_space(phrase)
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("exclude_locations", []))
        if any(value.casefold() == phrase.casefold() for value in values):
            return await ctx.send("❌ That excluded location already exists.")
        values.append(phrase)
        settings["exclude_locations"] = values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ Listings whose address contains **{phrase}** will be excluded.")

    @rm_exclude.command(name="remove")
    async def rm_exclude_remove(self, ctx: commands.Context, *, phrase: str) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("exclude_locations", []))
        new_values = [value for value in values if value.casefold() != phrase.casefold()]
        if len(new_values) == len(values):
            return await ctx.send("❌ That excluded location was not found.")
        settings["exclude_locations"] = new_values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ Removed **{phrase}** from the excluded locations.")

    @rm_exclude.command(name="list")
    async def rm_exclude_list(self, ctx: commands.Context) -> None:
        values = (await self.config.guild(ctx.guild).settings()).get("exclude_locations", [])
        await ctx.send("**Excluded locations:**\n" + ("\n".join(f"• {v}" for v in values) if values else "None"))

    @rm_exclude.command(name="clear")
    async def rm_exclude_clear(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).settings.set_raw("exclude_locations", value=[])
        await ctx.send("✅ Cleared this server's excluded locations.")

    @rm.group(name="include", invoke_without_command=True)
    async def rm_include(self, ctx: commands.Context) -> None:
        """Manage optional required address phrases. Empty means trust the drawn area."""
        await ctx.send_help(ctx.command)

    @rm_include.command(name="add")
    async def rm_include_add(self, ctx: commands.Context, *, phrase: str) -> None:
        phrase = _normalise_space(phrase)
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("include_locations", []))
        if any(value.casefold() == phrase.casefold() for value in values):
            return await ctx.send("❌ That required location already exists.")
        values.append(phrase)
        settings["include_locations"] = values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ An address must match **{phrase}** or another required location.")

    @rm_include.command(name="remove")
    async def rm_include_remove(self, ctx: commands.Context, *, phrase: str) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("include_locations", []))
        new_values = [value for value in values if value.casefold() != phrase.casefold()]
        if len(new_values) == len(values):
            return await ctx.send("❌ That required location was not found.")
        settings["include_locations"] = new_values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ Removed **{phrase}** from required locations.")

    @rm_include.command(name="list")
    async def rm_include_list(self, ctx: commands.Context) -> None:
        values = (await self.config.guild(ctx.guild).settings()).get("include_locations", [])
        await ctx.send("**Required address locations:**\n" + ("\n".join(f"• {v}" for v in values) if values else "None"))

    @rm_include.command(name="clear")
    async def rm_include_clear(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).settings.set_raw("include_locations", value=[])
        await ctx.send("✅ Cleared required locations; the drawn Rightmove area is now trusted by itself.")

    @rm.group(name="highlight", invoke_without_command=True)
    async def rm_highlight(self, ctx: commands.Context) -> None:
        """Highlight permanent-discount outright-ownership homes."""
        await ctx.send_help(ctx.command)

    @rm_highlight.command(name="status", aliases=["list"])
    async def rm_highlight_status(self, ctx: commands.Context) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        enabled = bool(settings.get("scheme_highlight_enabled", False))
        emoji = _highlight_emoji(settings)
        custom = [
            _normalise_space(item)
            for item in settings.get("scheme_highlight_terms", [])
            if _normalise_space(item)
        ]
        lines = [
            f"**Scheme highlighting:** {'Enabled' if enabled else 'Disabled'}",
            f"**Channel marker:** {emoji}",
            "**Built-in detection:** First Homes; Low-Cost Home Ownership (LCHO/RSL); Discount/Discounted Market Sale; DOMV; Section 106 discounted sale; homes sold at a stated percentage of market value; and wording that legally retains the discount on every resale.",
            "**Never highlighted:** shared ownership, part-buy/part-rent, shared equity, equity loans, Rent to Buy, leasehold, generic first-time-buyer wording, or temporary builder incentives.",
            "**Custom phrases:**",
            *(f"• {item}" for item in custom),
        ]
        if not custom:
            lines.append("None")
        for chunk in _chunk_lines(lines):
            await ctx.send(chunk)

    @rm_highlight.command(name="on", aliases=["enable"])
    async def rm_highlight_on(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).settings.set_raw("scheme_highlight_enabled", value=True)
        await ctx.send(
            "✅ Permanent-discount highlighting enabled for this server. "
            "Only outright-ownership homes with a retained market-value discount will receive stars. "
            "Run `.rm run refresh` after setup to scan existing listings."
        )

    @rm_highlight.command(name="off", aliases=["disable"])
    async def rm_highlight_off(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).settings.set_raw("scheme_highlight_enabled", value=False)
        await ctx.send(
            "✅ Scheme highlighting disabled for this server. "
            "Run `.rm run refresh` to remove existing star markers."
        )

    @rm_highlight.command(name="emoji")
    async def rm_highlight_emoji(self, ctx: commands.Context, *, marker: str) -> None:
        marker = re.sub(r"[\s/#]+", "", _normalise_space(marker))[:8]
        if not marker:
            return await ctx.send("❌ Supply a short marker, for example `⭐`.")
        await self.config.guild(ctx.guild).settings.set_raw("scheme_highlight_emoji", value=marker)
        await ctx.send(
            f"✅ Scheme-highlighted channels will use **{marker}**. "
            "Run `.rm run refresh` to rename existing highlighted channels."
        )

    @rm_highlight.command(name="add")
    async def rm_highlight_add(self, ctx: commands.Context, *, phrase: str) -> None:
        phrase = _normalise_space(phrase)
        if not phrase:
            return await ctx.send("❌ Supply a phrase to highlight.")
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("scheme_highlight_terms", []))
        if any(_normalise_match_text(item) == _normalise_match_text(phrase) for item in values):
            return await ctx.send("❌ That custom highlight phrase already exists.")
        values.append(phrase)
        settings["scheme_highlight_terms"] = values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(
            f"✅ Added **{phrase}** as a custom scheme-highlight phrase. "
            "Run `.rm run refresh` to rescan existing listings."
        )

    @rm_highlight.command(name="remove")
    async def rm_highlight_remove(self, ctx: commands.Context, *, phrase: str) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get("scheme_highlight_terms", []))
        new_values = [
            item for item in values
            if _normalise_match_text(item) != _normalise_match_text(phrase)
        ]
        if len(new_values) == len(values):
            return await ctx.send("❌ That custom highlight phrase was not found.")
        settings["scheme_highlight_terms"] = new_values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(
            f"✅ Removed **{phrase}** from custom scheme highlighting. "
            "Run `.rm run refresh` to update existing listings."
        )

    @rm_highlight.command(name="clear")
    async def rm_highlight_clear(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).settings.set_raw("scheme_highlight_terms", value=[])
        await ctx.send(
            "✅ Cleared custom phrases; built-in scheme detection remains available. "
            "Run `.rm run refresh` to update existing listings."
        )

    @rm_highlight.command(name="reset")
    async def rm_highlight_reset(self, ctx: commands.Context) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        settings["scheme_highlight_enabled"] = True
        settings["scheme_highlight_emoji"] = "⭐"
        settings["scheme_highlight_terms"] = []
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(
            "✅ Permanent-discount highlighting reset to the strict built-in rules with the ⭐ marker. "
            "Run `.rm run refresh` to update existing listings."
        )

    @rm.group(name="filter", invoke_without_command=True)
    async def rm_filter(self, ctx: commands.Context) -> None:
        """Manage per-server property type and text filters."""
        await ctx.send_help(ctx.command)

    @rm_filter.command(name="list")
    async def rm_filter_list(self, ctx: commands.Context) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        types = settings.get("banned_property_types", [])
        text = settings.get("banned_text", [])
        lines = ["**Banned property types:**", *(f"• {value}" for value in types), "", "**Banned text:**", *(f"• {value}" for value in text)]
        for chunk in _chunk_lines(lines):
            await ctx.send(chunk)

    @rm_filter.command(name="typeadd")
    async def rm_filter_typeadd(self, ctx: commands.Context, *, value: str) -> None:
        await self._add_filter_value(ctx, "banned_property_types", value, "property type")

    @rm_filter.command(name="typeremove")
    async def rm_filter_typeremove(self, ctx: commands.Context, *, value: str) -> None:
        await self._remove_filter_value(ctx, "banned_property_types", value, "property type")

    @rm_filter.command(name="textadd")
    async def rm_filter_textadd(self, ctx: commands.Context, *, value: str) -> None:
        await self._add_filter_value(ctx, "banned_text", value, "text filter")

    @rm_filter.command(name="textremove")
    async def rm_filter_textremove(self, ctx: commands.Context, *, value: str) -> None:
        await self._remove_filter_value(ctx, "banned_text", value, "text filter")

    @rm_filter.command(name="reset")
    async def rm_filter_reset(self, ctx: commands.Context) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        settings["banned_property_types"] = list(DEFAULT_BANNED_PROPERTY_TYPES)
        settings["banned_text"] = list(DEFAULT_BANNED_TEXT)
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send("✅ Restored the original hardcoded property filters for this server.")

    async def _add_filter_value(
        self,
        ctx: commands.Context,
        key: str,
        value: str,
        label: str,
    ) -> None:
        value = _normalise_space(value)
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get(key, []))
        if any(item.casefold() == value.casefold() for item in values):
            return await ctx.send(f"❌ That {label} already exists.")
        values.append(value)
        settings[key] = values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ Added **{value}** as a banned {label}.")

    async def _remove_filter_value(
        self,
        ctx: commands.Context,
        key: str,
        value: str,
        label: str,
    ) -> None:
        settings = await self.config.guild(ctx.guild).settings()
        values = list(settings.get(key, []))
        new_values = [item for item in values if item.casefold() != value.casefold()]
        if len(values) == len(new_values):
            return await ctx.send(f"❌ That {label} was not found.")
        settings[key] = new_values
        await self.config.guild(ctx.guild).settings.set(settings)
        await ctx.send(f"✅ Removed **{value}** from banned {label}s.")

    @rm.command(name="purgeover")
    async def rm_purgeover(
        self,
        ctx: commands.Context,
        maximum_price: int,
        confirmation: Optional[str] = None,
    ) -> None:
        """Preview or purge this server's tracked properties priced above a limit.

        Run ``rm purgeover 200000`` for a preview, then append ``confirm``
        to delete the matching property channels and their guild cache records.
        Other servers and all profile settings are untouched.
        """
        if maximum_price < 1:
            return await ctx.send("❌ Supply a positive maximum price, for example `rm purgeover 200000`.")
        if self._lock_for(ctx.guild.id).locked():
            return await ctx.send("❌ Wait for the current scrape to finish or stop it first.")

        guild_config = self.config.guild(ctx.guild)
        settings = await guild_config.settings()
        if settings.get("enabled"):
            return await ctx.send("❌ Disable this server first with `rm stop`, then run the purge preview.")

        cache_raw = await guild_config.properties()
        candidates: List[Tuple[str, Dict[str, Any], int]] = []
        unknown_price = 0
        for pid, raw in cache_raw.items():
            if not isinstance(raw, dict):
                continue
            state = self._normalise_cached_state(str(pid), raw)
            price = _safe_int(state.get("current_price"))
            if price is None:
                unknown_price += 1
                continue
            if price > maximum_price:
                candidates.append((str(pid), state, price))

        candidates.sort(key=lambda item: (item[2], item[0]), reverse=True)
        keyword_ok = (confirmation or "").casefold() == "confirm"
        if not keyword_ok:
            if not candidates:
                return await ctx.send(
                    f"✅ No tracked properties are priced above £{maximum_price:,}. "
                    f"{unknown_price} record(s) have no usable cached price and were left untouched."
                )
            highest = candidates[0][2]
            lowest = candidates[-1][2]
            return await ctx.send(
                f"⚠️ **Purge preview for this server only**\n"
                f"Would remove **{len(candidates)}** tracked property record(s) priced above "
                f"**£{maximum_price:,}** (range £{lowest:,}–£{highest:,}).\n"
                f"Would leave **{len(cache_raw) - len(candidates)}** tracked record(s), including "
                f"{unknown_price} with no usable cached price.\n\n"
                f"Nothing has been deleted. To proceed, run:\n"
                f"`rm purgeover {maximum_price} confirm`"
            )

        if not candidates:
            return await ctx.send(f"✅ Nothing is priced above £{maximum_price:,}; no changes were made.")

        progress = await ctx.send(
            f"🧹 Purging {len(candidates)} property record(s) above £{maximum_price:,} "
            "from this server only…"
        )
        discovered = self._discover_channels(ctx.guild)
        cache: Dict[str, Dict[str, Any]] = dict(cache_raw)
        deleted_channels = 0
        removed_records = 0
        missing_channels = 0
        failures: List[str] = []

        for index, (pid, state, price) in enumerate(candidates, start=1):
            # Delete every channel positively identified with this property ID.
            # Prefer discovery by name/topic, while also checking the cached ID.
            channels: Dict[int, discord.TextChannel] = {
                channel.id: channel for channel in discovered.get(pid, [])
            }
            cached_id = _safe_int(state.get("channel_id"))
            cached_channel = ctx.guild.get_channel(cached_id) if cached_id else None
            if isinstance(cached_channel, discord.TextChannel) and _channel_property_id(cached_channel) == pid:
                channels[cached_channel.id] = cached_channel

            property_failed = False
            if not channels:
                missing_channels += 1
            else:
                for channel in channels.values():
                    try:
                        await channel.delete(
                            reason=f"Rightmove property price £{price:,} exceeds configured purge limit £{maximum_price:,}"
                        )
                        deleted_channels += 1
                    except discord.NotFound:
                        pass
                    except (discord.Forbidden, discord.HTTPException) as exc:
                        property_failed = True
                        failures.append(f"prop-{pid}: {exc}")

            # Never remove the cache entry if a positively matched channel could
            # not be deleted; this keeps the operation recoverable on retry.
            if not property_failed:
                cache.pop(pid, None)
                removed_records += 1

            if index % 10 == 0:
                await guild_config.properties.set(cache)
            if index % 25 == 0 or index == len(candidates):
                try:
                    await progress.edit(
                        content=(
                            f"🧹 Purging properties above £{maximum_price:,}: "
                            f"**{index}/{len(candidates)}** checked, "
                            f"**{removed_records}** records removed…"
                        )
                    )
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

        await guild_config.properties.set(cache)
        result = (
            f"✅ Northern-only purge complete: **{removed_records}** cache record(s) removed and "
            f"**{deleted_channels}** property channel(s) deleted. "
            f"**{len(cache)}** tracked record(s) remain at or below £{maximum_price:,}."
        )
        if missing_channels:
            result += f" {missing_channels} removed record(s) already had no channel."
        if unknown_price:
            result += f" {unknown_price} record(s) with unknown prices were left untouched."
        if failures:
            result += (
                f" **{len(failures)}** deletion(s) failed and remain cached; retry after checking permissions. "
                + " | ".join(failures[:3])
            )
        await self._log(ctx.guild, result)
        try:
            await progress.edit(content=result[:2000])
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            await ctx.send(result[:2000])

    @rm.command(name="cleanup")
    async def rm_cleanup(self, ctx: commands.Context) -> None:
        """Delete duplicate prop channels and empty legacy RIGHTMOVE categories."""
        if self._lock_for(ctx.guild.id).locked():
            return await ctx.send("❌ Wait for the current scrape to finish.")

        discovered = self._discover_channels(ctx.guild)
        cache = await self.config.guild(ctx.guild).properties()
        deleted_duplicates = 0
        kept_orphans: List[str] = []

        for pid, channels in discovered.items():
            if len(channels) <= 1:
                if pid not in cache:
                    kept_orphans.append(channels[0].mention)
                continue

            preferred_id = _safe_int(cache.get(pid, {}).get("channel_id")) if isinstance(cache.get(pid), dict) else None
            keeper = next((ch for ch in channels if ch.id == preferred_id), None) or sorted(channels, key=lambda ch: ch.id)[0]
            for channel in channels:
                if channel.id == keeper.id:
                    continue
                try:
                    await channel.delete(reason=f"Duplicate Rightmove property channel for {pid}")
                    deleted_duplicates += 1
                except (discord.Forbidden, discord.HTTPException):
                    pass

        deleted_categories = 0
        for category in list(ctx.guild.categories):
            if category.name.startswith(OLD_CATEGORY_PREFIX) and not category.channels:
                try:
                    await category.delete(reason="Empty legacy Rightmove category")
                    deleted_categories += 1
                except (discord.Forbidden, discord.HTTPException):
                    pass

        message = (
            f"✅ Deleted {deleted_duplicates} duplicate channel(s) and "
            f"{deleted_categories} empty legacy category/categories."
        )
        if kept_orphans:
            message += (
                "\nI did **not** automatically delete these uncached property channels: "
                + ", ".join(kept_orphans[:20])
            )
        await ctx.send(message[:2000])

    @rm.command(name="adopt")
    async def rm_adopt(self, ctx: commands.Context) -> None:
        """Run a refresh that adopts existing prop-ID channels into the guild cache."""
        if self._lock_for(ctx.guild.id).locked():
            return await ctx.send("❌ A scrape is already running.")
        result = await self._run_scrape(ctx.guild, source="adopt", force_refresh=True)
        await ctx.send(("✅ " if result["ok"] else "❌ ") + result["message"][:1900])
