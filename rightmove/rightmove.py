import re
import time
import datetime
import asyncio
import math
from datetime import time as dt_time, timedelta
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
        nodes = tree.xpath("//span[@data-testid='search-header-result-count']/text()")
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
            pr = c.xpath(
                ".//a[@data-testid='property-price']//div"
                "[contains(@class,'PropertyPrice_price__')]/text()"
            )
            price_raw = pr[0].strip() if pr else None

            ad = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = ad[0].strip() if ad else None

            tp = c.xpath(
                ".//span[contains(@class,'PropertyInformation_propertyType')]/text()"
            )
            if not tp:
                tp = c.xpath(
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
            listed_ts  = self._parse_date(ld[0])
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
    """A cog that scrapes Rightmove daily at 07:00 London…"""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        # properties: pid → {channel_id, message_id, price, listed_ts, updated_ts, is_stc, active, vanished_ts}
        # settings: cleanup_days, log_channel_id
        self.config.register_global(
            properties={},
            settings={
                "cleanup_days": 7,
                "log_channel_id": None,
            }
        )
        self.scrape_loop     = None
        self.target_channel  = None
        self._rebuild_lock   = asyncio.Lock()
        self._last_test      = 0.0
        self._halt           = False

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    async def _log(self, message: str):
        settings = await self.config.settings()
        ch_id = settings.get("log_channel_id")
        if ch_id:
            ch = self.bot.get_channel(ch_id)
            if ch and isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"[RightmoveCog] {message}")
                except:
                    pass

    @commands.is_owner()
    @commands.group(name="rm", invoke_without_command=True)
    async def rm(self, ctx):
        """Rightmove commands: .rm start .rm stop .rm test .rm cleanup .rm setlog .rm setcleanup .rm abort"""
        await ctx.send_help(ctx.command)

    @rm.command(name="setlog")
    async def rm_setlog(self, ctx, channel: discord.TextChannel = None):
        """Set or unset the log channel."""
        cid = channel.id if channel else None
        await self.config.settings.set_raw("log_channel_id", value=cid)
        if channel:
            await ctx.send(f"✅ Log channel set to {channel.mention}")
        else:
            await ctx.send("✅ Log channel unset")

    @rm.command(name="setcleanup")
    async def rm_setcleanup(self, ctx, days: int):
        """Set number of days after vanished to delete channels."""
        if days < 0:
            return await ctx.send("❌ Days must be non-negative.")
        await self.config.settings.set_raw("cleanup_days", value=days)
        await ctx.send(f"✅ Cleanup interval set to {days} day(s).")

    @rm.command(name="start")
    async def rm_start(self, ctx, channel: discord.TextChannel = None):
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Already scheduled.")
        self.target_channel = channel or ctx.channel
        self._halt = False
        self.scrape_loop = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()
        await ctx.send(
            f"✅ Scheduled daily scrape at 07:00 Europe/London in channel {self.target_channel.mention}."
        )
        await self._log(f"Scheduled daily scrape in {self.target_channel.mention}")

    @rm.command(name="stop")
    async def rm_stop(self, ctx):
        """Unschedule daily scrapes."""
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ No scrape scheduled.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape unscheduled.")
        await self._log("Scrape unscheduled")

    @rm.command(name="abort")
    async def rm_abort(self, ctx):
        """Abort current and future scrapes."""
        self._halt = True
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()
        await ctx.send("🛑 Scrape aborted and halted. Use `.rm start` to resume.")
        await self._log("Scrape aborted and halted")

    @rm.command(name="cleanup")
    async def rm_cleanup(self, ctx):
        """Manually run cleanup of vanished channels."""
        await ctx.send("🔄 Running manual cleanup…")
        count = await self._cleanup_old()
        await ctx.send(f"✅ Cleanup done, removed {count} channel(s).")
        await self._log(f"Manual cleanup removed {count} channel(s)")

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
        await self._log(f"Manual scrape triggered by {ctx.author} in {self.target_channel.mention}")
        async with self._rebuild_lock:
            await self.do_scrape(force_refresh=override)
        await ctx.send("✅ Manual scrape done.")
        await self._log("Manual scrape completed")

    async def do_scrape(self, force_refresh: bool = False):
        if self._halt:
            await self._log("Scrape aborted by halt flag")
            return

        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=170000&radius=0.0"
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
        if data._status_code != 200:
            msg = f"❌ HTTP {data._status_code}, aborting."
            await self.target_channel.send(msg)
            await self._log(msg)
            return

        df = data.get_results
        if df.empty:
            msg = (
                f"⚠️ Scrape returned {data.results_count_display} results "
                f"but DataFrame is empty. Check your XPaths or URL."
            )
            await self.target_channel.send(msg)
            await self._log(msg)
            return

        df = df[df["type"].notna()]
        df = df[~df["type"].str.lower().apply(
            lambda t: any(sub in t for sub in BANNED_TYPE_SUBSTRINGS)
        )]
        df = df[~df["type"].str.lower().isin(BANNED_PROPERTY_TYPES)]

        cache     = await self.config.properties()
        new_props = {r["id"]: r for _, r in df.iterrows()}
        old_ids   = set(cache)
        new_ids   = set(new_props)
        guild     = self.target_channel.guild

        existing_cats = [
            c for c in guild.categories if c.name.startswith(CATEGORY_PREFIX)
        ]
        existing_cats.sort(
            key=lambda c: int(c.name.split()[-1]) if c.name.split()[-1].isdigit() else 0
        )

        now_ts = int(time.time())

        # New & updates
        for pid, r in new_props.items():
            is_new        = pid not in cache
            old           = cache.get(pid, {})
            price_changed = (not is_new) and (r["price"] != old.get("price"))
            stc_changed   = r["is_stc"] and not old.get("is_stc", False)

            if is_new:
                for cat in existing_cats:
                    if len(cat.channels) < MAX_PER_CATEGORY:
                        target_cat = cat
                        break
                else:
                    nums = [
                        int(c.name.split()[-1])
                        for c in existing_cats
                        if c.name.split()[-1].isdigit()
                    ]
                    next_idx = max(nums) + 1 if nums else 1
                    target_cat = await guild.create_category(f"{CATEGORY_PREFIX} {next_idx}")
                    existing_cats.append(target_cat)
                    await self._log(f"Created category {target_cat.name}")

                ch = await guild.create_text_channel(f"prop-{pid}", category=target_cat)
                await self._log(f"Created channel {ch.name} in {target_cat.name}")
                cache[pid] = {
                    "channel_id":   ch.id,
                    "message_id":   None,
                    "price":        r["price"],
                    "listed_ts":    r["listed_ts"],
                    "updated_ts":   r["updated_ts"],
                    "is_stc":       r["is_stc"],
                    "active":       True,
                    "vanished_ts":  None,
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
                    cache[pid]["active"]     = False
                    cache[pid]["vanished_ts"]= now_ts
                    await self._send_or_edit(ch, pid, None, event="vanished", cache=cache)

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

        # Cleanup old vanished channels
        await self._cleanup_old()

        # Reorder
        await self._reorder_channels()
        await self._log("Scrape run completed and channels reordered")

    async def _send_or_edit(
        self,
        ch: discord.TextChannel,
        pid: str,
        r,
        event: str,
        cache: dict = None,
    ):
        local_cache = cache if cache is not None else await self.config.properties()
        data  = local_cache[pid]
        emojis = {
            "new":           ("🆕",    "New",            None),
            "price_update":  ("🔄",    "Price Updated",  None),
            "stc":           ("💖",    "[STC]",          discord.Color.magenta()),
            "vanished":      ("❌",    "Vanished",       discord.Color.greyple()),
            "refresh":       ("",      "",               None),
        }
        emoji, pre, color = emojis[event]

        if r is not None:
            price = r["price"]
            if color is None:
                if abs(price - TARGET_PRICE) <= IDEAL_DELTA:
                    color = discord.Color.blue()
                    prefix_emoji = "🟢"
                elif price <= 170_000:
                    color = discord.Color.green()
                    prefix_emoji = "🟢"
                elif price <= 220_000:
                    color = discord.Color.orange()
                    prefix_emoji = "🟠"
                else:
                    color = discord.Color.red()
                    prefix_emoji = "🔴"
            else:
                prefix_emoji = "🔴" if event == "vanished" else "🟢"

            # update channel name emoji
            base_name = ch.name.split(" ",1)[0]
            new_name = f"{base_name} {prefix_emoji}"
            try:
                await ch.edit(name=new_name)
            except:
                pass

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
            emoji2, pre2, color2 = emojis["vanished"]
            embed = discord.Embed(
                title=f"{emoji2} {pre2}",
                color=color2,
                description="This property has vanished from the search.",
            )
            # mark red and channel
            try:
                await ch.edit(name=f"{ch.name.split()[0]} 🔴")
            except:
                pass

        msg_id = data.get("message_id")
        try:
            if msg_id:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(embed=embed)
                await self._log(f"Edited embed in {ch.mention} for {pid}")
            else:
                msg = await ch.send(embed=embed)
                data["message_id"] = msg.id
                await self.config.properties.set(local_cache)
                await self._log(f"Sent new embed in {ch.mention} for {pid}")
        except (discord.NotFound, discord.HTTPException):
            msg = await ch.send(embed=embed)
            data["message_id"] = msg.id
            await self.config.properties.set(local_cache)
            await self._log(f"Sent embed (after error) in {ch.mention} for {pid}")

    async def _cleanup_old(self) -> int:
        """Delete channels for properties vanished > cleanup_days ago."""
        cache = await self.config.properties()
        settings = await self.config.settings()
        days = settings.get("cleanup_days", 7)
        threshold = time.time() - days * 86400
        to_delete = []
        for pid, data in list(cache.items()):
            if not data.get("active", True) and data.get("vanished_ts") and data["vanished_ts"] < threshold:
                ch = self.bot.get_channel(data["channel_id"])
                if ch:
                    try:
                        await ch.delete()
                        await self._log(f"Deleted channel {ch.name} for vanished {pid}")
                    except:
                        pass
                to_delete.append(pid)
        for pid in to_delete:
            cache.pop(pid, None)
        if to_delete:
            await self.config.properties.set(cache)
        return len(to_delete)

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