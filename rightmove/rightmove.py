import re
import time
import datetime
import asyncio
import math
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from lxml import html
import numpy as np
import pandas as pd
import requests

import discord
from discord.ext import tasks
from discord.ext.commands import TextChannelConverter, BadArgument
from redbot.core import Config, commands

# ----------------------------
# Configuration / Thresholds
# ----------------------------
CATEGORY_PREFIX      = "RIGHTMOVE"
MAX_PER_CATEGORY     = 50
LONDON               = ZoneInfo("Europe/London")
SCRAPE_TIME          = dt_time(hour=7, minute=0, tzinfo=LONDON)

TARGET_PRICE         = 250_000
IDEAL_DELTA          =   3_000

# exact‐match banned property types (lowercase)
BANNED_PROPERTY_TYPES = {
    "studio",
    "land",
    "mobile home",
    "park home",
    "caravan",
    "garage",
    "garages",
    "parking",
}

# substring‐based banned descriptors (lowercase)
BANNED_TYPE_SUBSTRINGS = [
    "leasehold", "lease hold", "lease-hold",
    "sharedownership", "shared ownership", "shared-ownership",
    "over 50", "over50", "over-50", "over 50s", "over50s", "over-50s",
    "holiday home", "holiday-home", "holidayhome",
    "park home", "park-home", "parkhome",
    "mobile home", "mobile-home", "mobilehome",
    "caravan", "caravans",
    "not specified", "not-specified", "notspecified",
]


class RightmoveData:
    """Scrapes Rightmove search results and returns a DataFrame."""

    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._results = self._get_results(get_floorplans=get_floorplans)

    @staticmethod
    def _request(url: str):
        r = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0 Safari/537.36"
                )
            },
            timeout=10,
        )
        return r.status_code, r.content

    @property
    def get_results(self) -> pd.DataFrame:
        return self._results

    @property
    def results_count_display(self) -> int:
        tree = html.fromstring(self._first_page)
        nodes = tree.xpath("//span[contains(@class,'searchHeader-resultCount')]/text()")
        if not nodes:
            return 0
        try:
            return int(nodes[0].replace(",", ""))
        except ValueError:
            return 0

    @property
    def page_count(self) -> int:
        total = self.results_count_display
        pages = total // 24 + (1 if total % 24 else 0)
        return min(max(pages, 1), 42)

    def _parse_date(self, text: str) -> int:
        now = int(time.time())
        if not text:
            return now
        t = text.lower()
        if "today" in t:
            return now
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", t)
        if m:
            d, mo, y = map(int, m.groups())
            dt = datetime.datetime(y, mo, d, tzinfo=LONDON)
            return int(dt.timestamp())
        return now

    def _get_page(self, content: bytes, get_floorplans: bool) -> pd.DataFrame:
        tree  = html.fromstring(content)
        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        rows  = []
        base  = "https://www.rightmove.co.uk"

        for c in cards:
            # Price
            pr = c.xpath(
                ".//a[@data-testid='property-price']//div"
                "[contains(@class,'PropertyPrice_price__')]/text()"
            )
            price_raw = pr[0].strip() if pr else None

            # Address
            ad = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = ad[0].strip() if ad else None

            # Property type
            tp = c.xpath(
                ".//span[contains(@class,'PropertyInformation_propertyType')]/text()"
            )
            if not tp:
                tp = c.xpath(
                    ".//div[@data-testid='property-information']//span/text()"
                )
            ptype = tp[0].strip() if tp else None

            # Bedrooms
            bd = c.xpath(
                ".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()"
            )
            try:
                beds = float(bd[0]) if bd else None
            except ValueError:
                beds = None

            # Listed / Updated timestamps
            ld = c.xpath(
                ".//span[contains(@class,'MarketedBy_joinedText')]/text()"
            ) or [None]
            ud = c.xpath(
                ".//span[contains(@class,'MarketedBy_addedOrReduced')]/text()"
            ) or [None]
            listed_ts  = self._parse_date(ld[0])
            updated_ts = self._parse_date(ud[0])

            # STC?
            stc = bool(
                c.xpath(
                    ".//span[contains(text(),'STC')"
                    " or contains(text(),'Subject to contract')]"
                )
            )

            # URL & ID — TRY MULTIPLE XPATHS TO FIND IT
            href = c.xpath(".//a[@data-test='property-details']/@href")
            if not href:
                href = c.xpath(".//a[@data-testid='property-details-lozenge']/@href")
            if not href:
                href = c.xpath(".//a[contains(@href,'/properties/')]/@href")
            url = f"{base}{href[0]}" if href else None
            pid = None
            if url:
                m2 = re.search(r"/properties/(\d+)", url)
                pid = m2.group(1) if m2 else None

            # First image
            img_elems = c.xpath(".//img[@data-testid='property-img-1']") or []
            if img_elems:
                img_el = img_elems[0]
                srcset = img_el.get("srcset", "")
                if srcset:
                    candidates = [seg.strip().split(" ")[0] for seg in srcset.split(",")]
                    img_url = candidates[-1]
                else:
                    img_url = img_el.get("src")
            else:
                img_url = None
            if img_url and img_url.startswith("//"):
                img_url = "https:" + img_url

            # Agent
            an = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//img/@alt"
            )
            agent = an[0].replace(" Estate Agent Logo", "").strip() if an else None
            au = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//a/@href"
            )
            agent_url = f"{base}{au[0]}" if au else None

            rows.append({
                "id":              pid,
                "price":           price_raw,
                "address":         address,
                "type":            ptype,
                "number_bedrooms": beds,
                "listed_ts":       listed_ts,
                "updated_ts":      updated_ts,
                "is_stc":          stc,
                "url":             url,
                "image_url":       img_url,
                "agent":           agent,
                "agent_url":       agent_url,
            })

        # Build with explicit columns so 'type' never disappears
        columns = [
            "id", "price", "address", "type",
            "number_bedrooms", "listed_ts", "updated_ts",
            "is_stc", "url", "image_url", "agent", "agent_url",
        ]
        df = pd.DataFrame.from_records(rows, columns=columns)

        # Clean price & drop rows missing id/price/address
        df["price"] = (
            df["price"]
              .replace(r"\D+", "", regex=True)
              .replace("", np.nan)
              .astype(float)
        )
        #df = df.dropna(subset=["id", "price", "address"])
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_results(self, get_floorplans: bool) -> pd.DataFrame:
        df = self._get_page(self._first_page, get_floorplans)
        for p in range(1, self.page_count):
            u   = f"{self._url}&index={p*24}"
            sc, ct = self._request(u)
            if sc != 200:
                break
            tmp = self._get_page(ct, get_floorplans)
            df  = pd.concat([df, tmp], ignore_index=True)
        return df


class RightmoveCog(commands.Cog):
    """A cog that scrapes Rightmove daily at 07:00 London…"""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        self.config.register_global(properties={})
        self.scrape_loop     = None
        self.target_channel  = None
        self._rebuild_lock   = asyncio.Lock()
        self._last_test      = 0.0

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.group(name="rm", invoke_without_command=True)
    async def rm(self, ctx):
        """Rightmove commands: .rm start .rm stop .rm test"""
        await ctx.send_help(ctx.command)

    @rm.command(name="start")
    async def rm_start(self, ctx, channel: discord.TextChannel = None):
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Already scheduled.")
        self.target_channel = channel or ctx.channel
        self.scrape_loop = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()
        await ctx.send(
            f"✅ Scheduled daily scrape at 07:00 Europe/London in channel {self.target_channel.mention}."
        )

    @rm.command(name="stop")
    async def rm_stop(self, ctx):
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ No scrape scheduled.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape unscheduled.")

    @rm.command(name="test")
    async def rm_test(self, ctx, *args):
        override = False
        channel  = None
        for arg in args:
            if arg.lower() == "override":
                override = True
            else:
                try:
                    channel = await TextChannelConverter().convert(ctx, arg)
                except BadArgument:
                    continue

        self.target_channel = channel or self.target_channel or ctx.channel

        if self._rebuild_lock.locked() and not override:
            return await ctx.send(
                "❌ A rebuild is already in progress. Use `.rm test override` to force."
            )

        now = time.time()
        if (now - self._last_test) < 300 and not override:
            rem = int(300 - (now - self._last_test))
            return await ctx.send(
                f"❌ You must wait {rem}s before running `.rm test` again, or use override."
            )
        self._last_test = now

        await ctx.send("🔄 Running manual scrape…")
        async with self._rebuild_lock:
            await self.do_scrape(force_refresh=override)
        await ctx.send("✅ Manual scrape done.")

    async def do_scrape(self, force_refresh: bool = False):
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=250000&radius=0.0"
            "&locationIdentifier=USERDEFINEDAREA%5E%7B"
            "%22polylines%22%3A%22sh%7CtHhu%7BE%7D%7CDr_Nf%7B"
            "AnjZxvLz%7Df%40reAllgA%7Bab%40fg%60%40kyu%40s_"
            "Ncq_%40crl%40uvO%7Dc%7C%40jTozbAlvMadq%40fu%5Bas"
            "Zpmi%40%7BeMjgf%40jdEhpJt%7BZ_%60Jlpz%40%22%7D"
            "&tenureTypes=FREEHOLD&transactionType=BUY"
            "&mustHave=parking"
            "&dontShow=newHome%2Cretirement%2CsharedOwnership%2Cauction"
        )
        data = RightmoveData(url)
        # HTTP check
        if data._status_code != 200:
            return await self.target_channel.send(f"❌ HTTP {data._status_code}, aborting.")
        df = data.get_results

        # Bail out if nothing scraped
        if df.empty:
            return await self.target_channel.send(
                f"⚠️ Scrape returned {data.results_count_display} results "
                f"but DataFrame is empty. Check your XPaths or URL."
            )

        # Now safe to filter on 'type'
        df = df[df["type"].notna()]
        df = df[~df["type"].str.lower().apply(
            lambda t: any(sub in t for sub in BANNED_TYPE_SUBSTRINGS)
        )]
        df = df[~df["type"].str.lower().isin(BANNED_PROPERTY_TYPES)]

        # Load cache
        cache     = await self.config.properties()
        new_props = {r["id"]: r for _, r in df.iterrows()}
        old_ids   = set(cache)
        new_ids   = set(new_props)
        guild     = self.target_channel.guild

        # Find or create target category
        existing = [c for c in guild.categories if c.name.startswith(CATEGORY_PREFIX)]
        existing.sort(key=lambda c: int(c.name.split()[-1]) if c.name.split()[-1].isdigit() else 1)
        target_cat = None
        for cat in existing:
            cnt = sum(
                1 for ch in cat.channels
                if isinstance(ch, discord.TextChannel) and ch.name.startswith("prop-")
            )
            if cnt < MAX_PER_CATEGORY:
                target_cat = cat
                break
        if not target_cat:
            idx = int(existing[-1].name.split()[-1]) + 1 if existing else 1
            target_cat = await guild.create_category(f"{CATEGORY_PREFIX} {idx}")

        # New & updates
        for pid, r in new_props.items():
            is_new        = pid not in cache
            old           = cache.get(pid, {})
            price_changed = (not is_new) and (r["price"] != old.get("price"))
            stc_changed   = r["is_stc"] and not old.get("is_stc", False)

            if is_new:
                ch = await guild.create_text_channel(f"prop-{pid}", category=target_cat)
                cache[pid] = {
                    "channel_id":   ch.id,
                    "message_id":   None,
                    "price":        r["price"],
                    "listed_ts":    r["listed_ts"],
                    "updated_ts":   r["updated_ts"],
                    "is_stc":       r["is_stc"],
                    "active":       True,
                }
                await self._send_or_edit(ch, pid, r, event="new", cache=cache)
                continue

            ch = guild.get_channel(old["channel_id"])
            if not ch:
                continue

            if stc_changed:
                cache[pid]["is_stc"]     = True
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self._send_or_edit(ch, pid, r, event="stc", cache=cache)
                continue

            if price_changed:
                cache[pid]["price"]      = r["price"]
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self._send_or_edit(ch, pid, r, event="price_update", cache=cache)
                continue

        # Vanished
        for pid in old_ids - new_ids:
            old = cache[pid]
            if old.get("active", True):
                ch = guild.get_channel(old["channel_id"])
                if ch:
                    cache[pid]["active"] = False
                    await self._send_or_edit(ch, pid, None, event="vanished", cache=cache)

        # Persist cache modifications (new, updates, vanished)
        await self.config.properties.set(cache)

        # Force‐refresh
        if force_refresh:
            for pid, r in new_props.items():
                data2 = cache.get(pid, {})
                if not data2.get("active"):
                    continue
                ch = guild.get_channel(data2["channel_id"])
                if ch:
                    await self._send_or_edit(ch, pid, r, event="refresh", cache=cache)

        # Reorder
        await self._reorder_channels()

    async def _send_or_edit(
        self,
        ch: discord.TextChannel,
        pid: str,
        r,
        event: str,
        cache: dict = None,
    ):
        # Use provided in‐memory cache or fall back to stored config
        local_cache = cache if cache is not None else await self.config.properties()
        data  = local_cache[pid]
        emojis = {
            "new":           ("🆕", "New", None),
            "price_update":  ("🔄", "Price Updated", None),
            "stc":           ("💖", "[STC]", discord.Color.magenta()),
            "vanished":      ("❌", "Vanished", discord.Color.greyple()),
            "refresh":       ("",    "",      None),
        }
        emoji, pre, color = emojis[event]

        if r is not None:
            # Choose embed color
            price = r["price"]
            if color is None:
                if abs(price - TARGET_PRICE) <= IDEAL_DELTA:
                    color = discord.Color.blue()
                elif price <= 170_000:
                    color = discord.Color.green()
                elif price <= 220_000:
                    color = discord.Color.orange()
                else:
                    color = discord.Color.red()

            title = r["address"] if event == "refresh" else f"{emoji} {pre} — {r['address']}"
            desc = (
                f"Listed: <t:{r['listed_ts']}:F> (<t:{r['listed_ts']}:R>)\n"
                f"Updated: <t:{r['updated_ts']}:F> (<t:{r['updated_ts']}:R>)"
            )
            embed = discord.Embed(title=title, color=color, description=desc)
            if r["image_url"]:
                embed.set_image(url=r["image_url"])

            embed.add_field(name="💷 Price", value=f"£{int(price):,}", inline=True)
            beds = r.get("number_bedrooms")
            beds_str = (
                str(int(beds)) if isinstance(beds, (int, float)) and not math.isnan(beds) else "N/A"
            )
            embed.add_field(name="🛏 Bedrooms", value=beds_str, inline=True)
            embed.add_field(name="🏠 Type", value=r["type"], inline=True)
            if r["agent"] and r["agent_url"]:
                embed.add_field(
                    name="🔗 Agent",
                    value=f"[{r['agent']}]({r['agent_url']})",
                    inline=True,
                )
            if r["url"]:
                embed.add_field(
                    name="🔗 Listing",
                    value=f"[View on Rightmove]({r['url']})",
                    inline=False,
                )
        else:
            # vanished embed
            emoji2, pre2, color2 = emojis["vanished"]
            embed = discord.Embed(
                title=f"{emoji2} {pre2}",
                color=color2,
                description="This property has vanished from the search.",
            )

        msg_id = data.get("message_id")
        try:
            if msg_id:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(embed=embed)
            else:
                msg = await ch.send(embed=embed)
                data["message_id"] = msg.id
                # persist the new message_id
                await self.config.properties.set(local_cache)
        except (discord.NotFound, discord.HTTPException):
            msg = await ch.send(embed=embed)
            data["message_id"] = msg.id
            await self.config.properties.set(local_cache)

    async def _reorder_channels(self):
        guild = self.target_channel.guild
        cache = await self.config.properties()
        for cat in guild.categories:
            if not cat.name.startswith(CATEGORY_PREFIX):
                continue
            items = []
            for ch in cat.channels:
                if not isinstance(ch, discord.TextChannel):
                    continue
                if not ch.name.startswith("prop-"):
                    continue
                pid   = ch.name.split("-", 1)[1]
                prop  = cache.get(pid, {})
                price = prop["price"] if prop.get("active", False) else float("inf")
                items.append((price, ch.id))
            items.sort(key=lambda x: x[0])
            positions = [
                {"id": cid, "position": idx, "parent_id": cat.id}
                for idx, (_, cid) in enumerate(items)
            ]
            if positions:
                try:
                    await guild.edit_channel_positions(positions=positions)
                except:
                    pass