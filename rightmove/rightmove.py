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

# price tiers: (threshold, emoji, color)
TIER_THRESHOLDS = [
    (220_000, "🟢", discord.Color.green()),
    (250_000, "🟠", discord.Color.orange()),
]
TIER_FALLBACK = ("🔴", discord.Color.red())

# exact-match banned property types (lowercase)
BANNED_PROPERTY_TYPES = {
    "studio", "land", "mobile home", "park home",
    "caravan", "garage", "garages", "parking", "flat", "maisonette", "plot",
}

# substring-based banned descriptors (lowercase)
BANNED_TYPE_SUBSTRINGS = [
    "leasehold", "lease hold", "lease-hold",
    "sharedownership", "shared ownership", "shared-ownership",
    "over 50", "over50", "over-50", "over 50s", "over50s", "over-50s",
    "holiday home", "holiday-home", "holidayhome",
    "park home", "park-home", "parkhome",
    "mobile home", "mobile-home", "mobilehome",
    "caravan", "caravans",
    "not specified", "not-specified", "notspecified",
    "non-standard", "non standard",
]

def _get_tier_emoji(price: float) -> str:
    for threshold, emoji, _ in TIER_THRESHOLDS:
        if price <= threshold:
            return emoji
    return TIER_FALLBACK[0]

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
        nodes = tree.xpath("//span[@data-testid='search-header-result-count']/text()")
        if not nodes:
            return 0
        try:
            return int(nodes[0].replace(",", ""))
        except ValueError:
            return 0

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

            tp = c.xpath(
                ".//span[contains(@class,'PropertyInformation_propertyType')]/text()"
            ) or c.xpath(
                ".//div[@data-testid='property-information']//span/text()"
            )
            ptype = tp[0].strip() if tp else None

            bd = c.xpath(
                ".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()"
            )
            try:
                beds = float(bd[0]) if bd else None
            except ValueError:
                beds = None

            ld = c.xpath(
                ".//span[contains(@class,'MarketedBy_joinedText')]/text()"
            ) or [None]
            ud = c.xpath(
                ".//span[contains(@class,'MarketedBy_addedOrReduced')]/text()"
            ) or [None]
            listed_ts = self._parse_date(ld[0])
            updated_ts = self._parse_date(ud[0])

            stc = bool(
                c.xpath(
                    ".//span[contains(text(),'STC')"
                    " or contains(text(),'Subject to contract')]"
                )
            )

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

            img_el = c.xpath(".//img[@data-testid='property-img-1']") or []
            if img_el:
                srcset = img_el[0].get("srcset", "")
                if srcset:
                    candidates = [seg.strip().split(" ")[0] for seg in srcset.split(",")]
                    img_url = candidates[-1]
                else:
                    img_url = img_el[0].get("src")
            else:
                img_url = None
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

        columns = [
            "id", "price", "address", "type",
            "number_bedrooms", "listed_ts", "updated_ts",
            "is_stc", "url", "image_url", "agent", "agent_url",
        ]
        df = pd.DataFrame.from_records(rows, columns=columns)
        df["price"] = (
            df["price"]
              .replace(r"\D+", "", regex=True)
              .replace("", np.nan)
              .astype(float)
        )
        df = df.dropna(subset=["id", "price", "address"])
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_results(self, get_floorplans: bool) -> pd.DataFrame:
        df = self._get_page(self._first_page, get_floorplans)
        page = 1
        while True:
            u = f"{self._url}&index={page * 24}"
            sc, ct = self._request(u)
            if sc != 200:
                break
            tmp = self._get_page(ct, get_floorplans)
            if tmp.empty:
                break
            df = pd.concat([df, tmp], ignore_index=True)
            page += 1
        return df

class RightmoveCog(commands.Cog):
    """A cog that scrapes Rightmove daily and manages prop-<pid> channels."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        self.config.register_global(
            properties={},
            settings={"cleanup_days": 7, "log_channel_id": None},
        )
        self.scrape_loop    = None
        self.target_channel = None
        self._last_test     = 0.0
        self._halt          = False
        self._lock          = asyncio.Lock()

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    async def _log(self, message: str):
        settings = await self.config.settings()
        ch_id    = settings.get("log_channel_id")
        if ch_id:
            ch = self.bot.get_channel(ch_id)
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"[RightmoveCog] {message}")
                except:
                    pass

    @commands.is_owner()
    @commands.group(name="rm", invoke_without_command=True)
    async def rm(self, ctx):
        """Rightmove commands: start, stop, test, cleanup, setlog, setcleanup, abort"""
        await ctx.send_help(ctx.command)

    @rm.command(name="setlog")
    async def rm_setlog(self, ctx, channel: discord.TextChannel = None):
        cid = channel.id if channel else None
        await self.config.settings.set_raw("log_channel_id", value=cid)
        if channel:
            await ctx.send(f"✅ Log channel set to {channel.mention}")
        else:
            await ctx.send("✅ Log channel unset")

    @rm.command(name="setcleanup")
    async def rm_setcleanup(self, ctx, days: int):
        if days < 0:
            return await ctx.send("❌ Days must be non-negative.")
        await self.config.settings.set_raw("cleanup_days", value=days)
        await ctx.send(f"✅ Cleanup interval set to {days} day(s).")

    @rm.command(name="start")
    async def rm_start(self, ctx, channel: discord.TextChannel = None):
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Already scheduled.")
        self.target_channel = channel or ctx.channel
        self._halt          = False
        self.scrape_loop    = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()
        await ctx.send(f"✅ Scheduled daily scrape in {self.target_channel.mention}")
        await self._log(f"Scheduled scrape in {self.target_channel.mention}")

    @rm.command(name="stop")
    async def rm_stop(self, ctx):
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ No scrape scheduled.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape unscheduled.")
        await self._log("Scrape unscheduled")

    @rm.command(name="abort")
    async def rm_abort(self, ctx):
        self._halt = True
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()
        await ctx.send("🛑 Scrape aborted and halted.")
        await self._log("Scrape aborted and halted")

    @rm.command(name="cleanup")
    async def rm_cleanup(self, ctx):
        """Delete orphan channels (invalid/missing pids)."""
        await ctx.send("🔄 Running orphan cleanup…")
        count = await self._cleanup_orphans()
        await ctx.send(f"✅ Removed {count} orphan channel(s).")
        await self._log(f"Manual orphan cleanup removed {count} channel(s)")

    @rm.command(name="test")
    async def rm_test(self, ctx, *args):
        override = "override" in [a.lower() for a in args]
        channel  = None
        for arg in args:
            if arg.lower() == "override":
                continue
            try:
                channel = await TextChannelConverter().convert(ctx, arg)
            except BadArgument:
                pass
        self.target_channel = channel or self.target_channel or ctx.channel
        now = time.time()
        if self._lock.locked() and not override:
            return await ctx.send("❌ Rebuild in progress. Use override.")
        if (now - self._last_test) < 300 and not override:
            rem = int(300 - (now - self._last_test))
            return await ctx.send(f"❌ Wait {rem}s or use override.")
        self._last_test = now
        await ctx.send("🔄 Running manual scrape…")
        await self._log(f"Manual scrape by {ctx.author}")
        async with self._lock:
            await self.do_scrape(force_refresh=override)
        await ctx.send("✅ Manual scrape done.")
        await self._log("Manual scrape completed")

    async def _fetch_property_description(self, url: str) -> str:
        sc, content = await asyncio.to_thread(RightmoveData._request, url)
        if sc != 200 or not content:
            return ""
        tree = html.fromstring(content)
        nodes = tree.xpath("//div[@data-testid='property-description']//p/text()")
        desc = " ".join(n.strip() for n in nodes if n and n.strip())
        if not desc:
            meta = tree.xpath("//meta[@name='description']/@content")
            desc = meta[0].strip() if meta else ""
        return desc

    async def do_scrape(self, force_refresh: bool = False):
        if self._halt:
            await self._log("Scrape halted by flag")
            return

        # 1) SCRAPE
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=265000&radius=0.0"
            "&locationIdentifier=USERDEFINEDAREA%5E%7B"
            "%22polylines%22%3A%22sh%7CtHhu%7BE%7D%7CDr_Nf%7B"
            "AnjZxvLz%7Df%40reAllgA%7Bab%40fg%60%40kyu%40s_"
            "Ncq_%40crl%40uvO%7Dc%7C%40jTozbAlvMadq%40fu%5Bas"
            "Zpmi%40%7BeMjgf%40jdEhpJt%7BZ_%60Jlpz%40%22%7D"
            "&tenureTypes=FREEHOLD&transactionType=BUY"
            "&displayLocationIdentifier=undefined"
            "&mustHave=parking%2Cgarden"
            "&dontShow=newHome%2Cretirement%2CsharedOwnership%2Cauction"
            "&minBedrooms=3"
        )
        data = await asyncio.to_thread(RightmoveData, url)
        if data._status_code != 200:
            msg = f"❌ HTTP {data._status_code}, aborting."
            await self.target_channel.send(msg)
            await self._log(msg)
            return

        df = data.get_results
        if df.empty:
            msg = (
                f"⚠️ Scrape returned {data.results_count_display} results "
                "but DataFrame is empty."
            )
            await self.target_channel.send(msg)
            await self._log(msg)
            return

        # 2) FILTER
        df = df[df["type"].notna()]
        df = df[~df["type"].str.lower().apply(
            lambda t: any(sub in t for sub in BANNED_TYPE_SUBSTRINGS)
        )]
        df = df[~df["type"].str.lower().isin(BANNED_PROPERTY_TYPES)]

        # 3) LOAD CACHE & DIFF
        cache   = await self.config.properties()
        old_ids = set(cache.keys())
        rows    = list(df.to_dict("records"))
        new_props = {r["id"]: r for r in rows}
        new_ids   = set(new_props.keys())

        to_create = new_ids - old_ids
        to_update = new_ids & old_ids
        to_remove = old_ids - new_ids

        guild = self.target_channel.guild
        cats  = [c for c in guild.categories if c.name.startswith(CATEGORY_PREFIX)]
        cats.sort(key=lambda c: int(c.name.split()[-1]) if c.name.split()[-1].isdigit() else 0)

        # 4) CREATE NEW CHANNELS
        for pid in to_create:
            r = new_props[pid]
            for cat in cats:
                if len(cat.channels) < MAX_PER_CATEGORY:
                    target_cat = cat
                    break
            else:
                nums = [int(c.name.split()[-1]) for c in cats if c.name.split()[-1].isdigit()]
                idx = max(nums) + 1 if nums else 1
                target_cat = await guild.create_category(f"{CATEGORY_PREFIX} {idx}")
                cats.append(target_cat)
                await self._log(f"Created category {target_cat.name}")

            ch = await guild.create_text_channel(f"prop-{pid}", category=target_cat)
            embed, tier = await self._build_embed(r, event="new")
            msg = await ch.send(embed=embed)
            await ch.edit(name=f"prop-{pid} {tier}")
            cache[pid] = {
                "channel_id":  ch.id,
                "message_id":  msg.id,
                "price":       r["price"],
                "listed_ts":   r["listed_ts"],
                "updated_ts":  r["updated_ts"],
                "is_stc":      r["is_stc"],
                "active":      True,
            }
            await self._log(f"Created prop-{pid}")

        # 5) UPDATE EXISTING CHANNELS (with tier-change)
        for pid in to_update:
            r   = new_props[pid]
            old = cache[pid]

            old_price = old["price"]
            new_price = r["price"]
            old_tier  = _get_tier_emoji(old_price)
            new_tier  = _get_tier_emoji(new_price)

            # FORCE a rename/embed when override is True, or when price/tier changes
            price_changed = force_refresh or (new_price != old_price) or (new_tier != old_tier)
            stc_changed   = r["is_stc"] and not old["is_stc"]
            if not price_changed and not stc_changed:
                continue

            ch = guild.get_channel(old["channel_id"])
            if not ch:
                continue

            event = "stc" if stc_changed else "price_update"
            embed, _ = await self._build_embed(r, event=event)

            # rename the channel with the new tier emoji
            await ch.edit(name=f"prop-{pid} {new_tier}")

            # edit or resend the embed
            try:
                msg = await ch.fetch_message(old["message_id"])
                await msg.edit(embed=embed)
            except discord.NotFound:
                msg = await ch.send(embed=embed)

            # update our cache
            old.update({
                "price":       new_price,
                "updated_ts":  r["updated_ts"],
                "is_stc":      r["is_stc"],
            })
            await self._log(f"Updated prop-{pid} ({event})")

        # 6) DELETE VANISHED CHANNELS
        for pid in to_remove:
            old = cache[pid]
            ch  = guild.get_channel(old["channel_id"])
            if ch:
                try:
                    await ch.delete()
                    await self._log(f"Deleted vanished prop-{pid}")
                except:
                    pass
            cache.pop(pid, None)

        # 7) DELETE ORPHAN CHANNELS
        for cat in cats:
            for ch in list(cat.channels):
                if not isinstance(ch, discord.TextChannel):
                    continue
                m = re.match(r"^prop-(\d+)", ch.name)
                if not m or m.group(1) not in cache:
                    try:
                        await ch.delete()
                        await self._log(f"Deleted orphan channel {ch.name}")
                    except:
                        pass

        # 8) GLOBAL REBALANCE ACROSS CATEGORIES
        #    Collect every active prop-<pid> channel, sort by price,
        #    then slice into buckets of MAX_PER_CATEGORY and physically move.
        active = []
        for cat in cats:
            for ch in cat.channels:
                if not isinstance(ch, discord.TextChannel):
                    continue
                m = re.match(r"^prop-(\d+)", ch.name)
                if not m:
                    continue
                pid = m.group(1)
                prop = cache.get(pid)
                if not prop:
                    continue
                active.append((prop["price"], pid, ch))

        # Sort globally by price (ascending)
        active.sort(key=lambda x: x[0])

        # Ensure we have enough RIGHTMOVE N categories
        needed = math.ceil(len(active) / MAX_PER_CATEGORY)
        nums   = [int(c.name.split()[-1]) for c in cats if c.name.split()[-1].isdigit()]
        next_idx = max(nums) + 1 if nums else 1
        for _ in range(needed - len(cats)):
            new_cat = await guild.create_category(f"{CATEGORY_PREFIX} {next_idx}")
            next_idx += 1
            cats.append(new_cat)
            await self._log(f"Created category {new_cat.name}")

        # Move each channel into the correct bucket
        for idx, (_, pid, ch) in enumerate(active):
            target_cat = cats[idx // MAX_PER_CATEGORY]
            if ch.category_id != target_cat.id:
                try:
                    await ch.edit(category=target_cat)
                    await self._log(f"Moved prop-{pid} to {target_cat.name}")
                except:
                    pass

        # 9) REORDER WITHIN EACH CATEGORY
        for cat in cats:
            items = []
            for ch in cat.channels:
                if not isinstance(ch, discord.TextChannel):
                    continue
                m = re.match(r"prop-(\d+)", ch.name)
                if not m:
                    continue
                pid = m.group(1)
                prop = cache.get(pid)
                if not prop:
                    continue
                items.append((prop["price"], ch.id))
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

        # 10) PERSIST CACHE & LOG
        await self.config.properties.set(cache)
        await self._log("Scrape complete and cache persisted")

    async def _build_embed(self, r: dict, event: str):
        emojis = {
            "new":          ("🆕", "New",           None),
            "price_update": ("🔄", "Price Updated", None),
            "stc":          ("💖", "[STC]",         discord.Color.magenta()),
        }
        emoji, label, color = emojis[event]
        price = r["price"]
        tier_emoji = None
        for threshold, t_emoji, t_color in TIER_THRESHOLDS:
            if price <= threshold:
                tier_emoji = t_emoji
                if color is None:
                    color = t_color
                break
        if tier_emoji is None:
            tier_emoji, fallback_color = TIER_FALLBACK
            if color is None:
                color = fallback_color

        title = f"{emoji} {label} — {r['address']}"
        desc  = (
            f"Listed: <t:{r['listed_ts']}:F> (<t:{r['listed_ts']}:R>)\n"
            f"Updated: <t:{r['updated_ts']}:F> (<t:{r['updated_ts']}:R>)"
        )
        embed = discord.Embed(title=title, description=desc, color=color)
        if r.get("image_url"):
            embed.set_image(url=r["image_url"])
        embed.add_field(name="💷 Price", value=f"£{int(price):,}", inline=True)
        beds = r.get("number_bedrooms")
        beds_str = (
            str(int(beds)) if isinstance(beds, (int, float)) and not math.isnan(beds)
            else "N/A"
        )
        embed.add_field(name="🛏 Bedrooms", value=beds_str, inline=True)
        embed.add_field(name="🏠 Type", value=r["type"], inline=True)
        if r.get("agent") and r.get("agent_url"):
            embed.add_field(
                name="🔗 Agent",
                value=f"[{r['agent']}]({r['agent_url']})",
                inline=True,
            )
        if r.get("url"):
            embed.add_field(
                name="🔗 Listing",
                value=f"[View on Rightmove]({r['url']})",
                inline=False,
            )
            full_desc = await self._fetch_property_description(r["url"])
            if full_desc:
                if len(full_desc) > 1021:
                    full_desc = full_desc[:1021] + "..."
                embed.add_field(name="📝 Description", value=full_desc, inline=False)

        return embed, tier_emoji

    async def _cleanup_orphans(self) -> int:
        cache = await self.config.properties()
        guild = self.target_channel.guild
        deleted = 0
        cats = [c for c in guild.categories if c.name.startswith(CATEGORY_PREFIX)]
        for cat in cats:
            for ch in list(cat.channels):
                if not isinstance(ch, discord.TextChannel):
                    continue
                m = re.match(r"^prop-(\d+)", ch.name)
                if not m or m.group(1) not in cache:
                    try:
                        await ch.delete()
                        deleted += 1
                        await self._log(f"Deleted orphan channel {ch.name}")
                    except:
                        pass
        return deleted