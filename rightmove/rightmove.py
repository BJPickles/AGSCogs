import re
import time
import datetime
import asyncio
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from lxml import html
import numpy as np
import pandas as pd
import requests

import discord
from discord.ext import tasks
from discord.ext.commands import BadArgument, TextChannelConverter
from redbot.core import Config, commands

# ----------------------------
# Configuration / Thresholds
# ----------------------------
CATEGORY_PREFIX = "RIGHTMOVE"        # e.g. RIGHTMOVE 1, RIGHTMOVE 2, ...
MAX_PER_CATEGORY = 50               # up to 50 prop- channels per category
LONDON      = ZoneInfo("Europe/London")
SCRAPE_TIME = dt_time(hour=7, minute=0, tzinfo=LONDON)

TARGET_PRICE = 250_000
IDEAL_DELTA  = 3_000

# comprehensive banned‐terms regex
BANNED_PATTERN = re.compile(
    r"\b(?:"
      r"lease[\s-]?hold"
    r"|shared[\s-]?ownership"
    r"|over[\s-]?50(?:s)?"
    r"|holiday[\s-]?home(?:s)?"
    r"|park[\s-]?home(?:s)?"
    r"|mobile[\s-]?home(?:s)?"
    r"|caravan(?:s)?"
    r"|garage(?:s)?"
    r"|land(?:s)?"
    r"|studio(?:s)?"
    r"|not[\s-]?specified"
    r")\b",
    re.IGNORECASE,
)

class RightmoveData:
    """Scrapes Rightmove search results and returns a DataFrame of properties."""
    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
        self._results = self._get_results(get_floorplans=get_floorplans)

    @staticmethod
    def _request(url: str):
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=10,
        )
        return r.status_code, r.content

    def _validate_url(self):
        """Basic sanity check on Rightmove search URL."""
        template = "{}://www.rightmove.co.uk/{}/find.html?"
        protos = ["http", "https"]
        kinds = ["property-to-rent", "property-for-sale", "new-homes-for-sale"]
        valid_prefixes = [template.format(p, k) for p in protos for k in kinds]
        if not any(self._url.startswith(pref) for pref in valid_prefixes) or self._status_code != 200:
            raise ValueError(f"Invalid Rightmove URL:\n{self._url}")

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
        tree = html.fromstring(content)
        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        rows = []
        base = "https://www.rightmove.co.uk"
        for c in cards:
            pr = c.xpath(
                ".//a[@data-testid='property-price']//div"
                "[contains(@class,'PropertyPrice_price__')]/text()"
            )
            price_raw = pr[0].strip() if pr else None

            ad = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = ad[0].strip() if ad else None

            tp = c.xpath(".//span[contains(@class,'PropertyInformation_propertyType')]/text()")
            ptype = tp[0].strip() if tp else None

            bd = c.xpath(".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()")
            try:
                beds = int(bd[0]) if bd else None
            except ValueError:
                beds = None

            ld = c.xpath(".//span[contains(@class,'MarketedBy_joinedText')]/text()") or [None]
            ud = c.xpath(".//span[contains(@class,'MarketedBy_addedOrReduced')]/text()") or [None]
            listed_ts  = self._parse_date(ld[0])
            updated_ts = self._parse_date(ud[0])

            stc = bool(c.xpath(
                ".//span[contains(text(),'STC') or contains(text(),'Subject to contract')]"
            ))

            href = c.xpath(".//a[@data-test='property-details']/@href")
            url = f"{base}{href[0]}" if href else None

            img_elems = c.xpath(".//img[@data-testid='property-img-1']") or []
            img_url = None
            if img_elems:
                tag = img_elems[0]
                srcset = tag.get("srcset", "")
                if srcset:
                    candidates = [seg.strip().split(" ")[0] for seg in srcset.split(",")]
                    img_url = candidates[-1]
                else:
                    img_url = tag.get("src")
            if img_url and img_url.startswith("//"):
                img_url = "https:" + img_url

            an = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//img/@alt"
            )
            agent = an[0].replace(" Estate Agent Logo", "").strip() if an else None
            au = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//a/@href"
            )
            agent_url = f"{base}{au[0]}" if au else None

            pid = None
            if url:
                m = re.search(r"/properties/(\d+)", url)
                pid = m.group(1) if m else None

            rows.append({
                "id": pid,
                "price": price_raw,
                "address": address,
                "type": ptype,
                "number_bedrooms": beds,
                "listed_ts": listed_ts,
                "updated_ts": updated_ts,
                "is_stc": stc,
                "url": url,
                "image_url": img_url,
                "agent": agent,
                "agent_url": agent_url,
            })

        df = pd.DataFrame(rows)
        df["price"] = (
            df["price"]
            .replace(r"\D+", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        df = df.dropna(subset=["id", "price", "address", "type"])
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_results(self, get_floorplans: bool) -> pd.DataFrame:
        df = self._get_page(self._first_page, get_floorplans)
        for p in range(1, self.page_count):
            u = f"{self._url}&index={p*24}"
            sc, content = self._request(u)
            if sc != 200:
                break
            tmp = self._get_page(content, get_floorplans)
            df = pd.concat([df, tmp], ignore_index=True)
        return df


class RightmoveCog(commands.Cog):
    """Rightmove scraper with reorder, rename, big images, listing link, cooldown & override."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        self.config.register_global(properties={})
        self.scrape_loop = None
        self.target_channel = None
        self._rebuild_lock = asyncio.Lock()
        self._last_test = 0.0

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.group(name="rm", invoke_without_command=True)
    async def rm(self, ctx):
        """Rightmove commands: .rm start | stop | test"""
        await ctx.send_help(ctx.command)

    @rm.command(name="start")
    async def rm_start(self, ctx, channel: discord.TextChannel = None):
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Already scheduled.")
        self.target_channel = channel or ctx.channel
        self.scrape_loop = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()
        await ctx.send("✅ Scheduled daily scrape at 07:00 Europe/London.")

    @rm.command(name="stop")
    async def rm_stop(self, ctx):
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ No scrape scheduled.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape unscheduled.")

    @rm.command(name="test")
    async def rm_test(self, ctx, *args):
        """
        Run a manual scrape immediately.
        Optional: .rm test [#channel] [override]
        """
        override = False
        channel = None
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
        if now - self._last_test < 300 and not override:
            return await ctx.send(
                f"❌ Please wait {int(300 - (now - self._last_test))}s "
                "or use `.rm test override`."
            )
        self._last_test = now

        await ctx.send("🔄 Running manual scrape…")
        async with self._rebuild_lock:
            await self.do_scrape(force_update=override)
        await ctx.send("✅ Manual scrape done.")

    async def do_scrape(self, force_update: bool = False):
        # Full, un-truncated Rightmove URL
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=175000&radius=0.0"
            "&locationIdentifier=USERDEFINEDAREA%5E%7B"
            "%22polylines%22%3A%22sh%7CtHhu%7BE%7D%7CDr_Nf%7B"
            "AnjZxvLz%7Df%40reAllgA%7Bab%40fg%60%40kyu%40s_"
            "Ncq_%40crl%40uvO%7Dc%7C%40jTozbAlvMadq%40fu%5Bas"
            "Zpmi%40%7BeMjgf%40jdEhpJt%7BZ_%60Jlpz%40%22%7D"
            "&tenureTypes=FREEHOLD&transactionType=BUY"
            "&displayLocationIdentifier=undefined"
            "&mustHave=parking"
            "&dontShow=newHome%2Cretirement%2CsharedOwnership%2Cauction"
            "&maxDaysSinceAdded=14"
        )
        df = RightmoveData(url).get_results
        df = df[~df["type"].str.contains(BANNED_PATTERN, na=False)]

        cache = await self.config.properties()
        new_props = {r["id"]: r for _, r in df.iterrows()}
        old_ids, new_ids = set(cache), set(new_props)
        guild = self.target_channel.guild

        # find/create category under CATEGORY_PREFIX
        cats = [c for c in guild.categories if c.name.startswith(CATEGORY_PREFIX)]
        cats.sort(key=lambda c: int(c.name.split()[-1]) if c.name.split()[-1].isdigit() else 1)
        target_cat = None
        for cat in cats:
            cnt = sum(
                1 for ch in cat.channels
                if isinstance(ch, discord.TextChannel) and ch.name.startswith("prop-")
            )
            if cnt < MAX_PER_CATEGORY:
                target_cat = cat
                break
        if not target_cat:
            idx = int(cats[-1].name.split()[-1]) + 1 if cats else 1
            target_cat = await guild.create_category(f"{CATEGORY_PREFIX} {idx}")

        # new listings, price changes, STC
        for pid, r in new_props.items():
            is_new = pid not in cache
            old = cache.get(pid, {})
            price_changed = (not is_new) and (r["price"] != old.get("price"))
            stc_changed = r["is_stc"] and not old.get("is_stc", False)

            # force_update => treat as price_changed
            if force_update and not is_new:
                price_changed = True

            if is_new:
                name = f"prop-{int(r['price'])}-{pid}"
                ch = await guild.create_text_channel(name, category=target_cat)
                cache[pid] = {
                    "channel_id": ch.id,
                    "price": r["price"],
                    "listed_ts": r["listed_ts"],
                    "updated_ts": r["updated_ts"],
                    "is_stc": r["is_stc"],
                    "active": True,
                }
                await self.post_embed(ch, r, "new")
                continue

            ch = guild.get_channel(old["channel_id"])
            if not ch:
                continue

            if stc_changed:
                cache[pid]["is_stc"] = True
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self.post_embed(ch, r, "stc")
                continue

            if price_changed:
                new_name = f"prop-{int(r['price'])}-{pid}"
                await ch.edit(name=new_name)
                cache[pid]["price"] = r["price"]
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self.post_embed(ch, r, "price_update")
                continue

        # vanished
        for pid in old_ids - new_ids:
            old = cache[pid]
            if old.get("active", False):
                ch = guild.get_channel(old["channel_id"])
                if ch:
                    cache[pid]["active"] = False
                    await self.post_embed(ch, None, "vanished")

        await self.config.properties.set(cache)
        await self._reorder_channels()

    async def _reorder_channels(self):
        guild = self.target_channel.guild
        cache = await self.config.properties()
        for cat in guild.categories:
            if not cat.name.startswith(CATEGORY_PREFIX):
                continue
            ordering = []
            for ch in cat.channels:
                if not isinstance(ch, discord.TextChannel) or not ch.name.startswith("prop-"):
                    continue
                pid = ch.name.rsplit("-", 1)[-1]
                prop = cache.get(pid, {})
                price = prop["price"] if prop.get("active", False) else float("inf")
                ordering.append((price, ch.id))
            ordering.sort(key=lambda x: x[0])
            positions = [
                {"id": cid, "position": idx, "parent_id": cat.id}
                for idx, (_, cid) in enumerate(ordering)
            ]
            if positions:
                try:
                    await guild.edit_channel_positions(positions=positions)
                except Exception:
                    pass

    async def post_embed(self, ch: discord.TextChannel, r, event: str):
        emojis = {
            "new": ("🆕", "New", None),
            "price_update": ("🔄", "Price Updated", None),
            "stc": ("💖", "[STC]", discord.Color.magenta()),
            "vanished": ("❌", "Vanished", discord.Color.greyple()),
        }
        emoji, pre, color = emojis[event]

        if r is not None:
            price = r["price"]
            if color is None:
                if abs(price - TARGET_PRICE) <= IDEAL_DELTA:
                    color = discord.Color.light_blue()
                elif price <= 170_000:
                    color = discord.Color.green()
                elif price <= 220_000:
                    color = discord.Color.orange()
                else:
                    color = discord.Color.red()

            title = f"{emoji} {pre} — {r['address']}"
            desc = (
                f"Listed: <t:{r['listed_ts']}:F> (<t:{r['listed_ts']}:R>)\n"
                f"Updated: <t:{r['updated_ts']}:F> (<t:{r['updated_ts']}:R>)"
            )
            embed = discord.Embed(title=title, color=color, description=desc)

            # big image
            if r.get("image_url"):
                embed.set_image(url=r["image_url"])

            # bedrooms as integer
            beds = r.get("number_bedrooms")
            beds_str = str(int(beds)) if beds is not None else "N/A"
            embed.add_field(name="🛏 Bedrooms", value=beds_str, inline=True)

            embed.add_field(name="💷 Price", value=f"£{int(price):,}", inline=True)
            embed.add_field(name="🏠 Type", value=r["type"], inline=True)

            # listing link
            if r.get("url"):
                embed.add_field(
                    name="🔗 Listing",
                    value=f"[View on Rightmove]({r['url']})",
                    inline=False
                )

            # agent
            if r.get("agent") and r.get("agent_url"):
                embed.add_field(
                    name="🏢 Agent",
                    value=f"[{r['agent']}]({r['agent_url']})",
                    inline=True
                )

            await ch.send(embed=embed)
        else:
            emoji, pre, color = emojis["vanished"]
            embed = discord.Embed(
                title=f"{emoji} {pre}",
                color=color,
                description="This property has vanished from the search."
            )
            await ch.send(embed=embed)