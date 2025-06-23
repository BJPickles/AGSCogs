import re
import time
import datetime
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from lxml import html
import numpy as np
import pandas as pd
import requests

import discord
from discord.ext import tasks
from redbot.core import Config, commands

# 07:00 Europe/London
LONDON      = ZoneInfo("Europe/London")
SCRAPE_TIME = dt_time(hour=7, minute=0, tzinfo=LONDON)

TARGET_PRICE = 250_000
IDEAL_DELTA  = 3_000

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
    """Scrapes Rightmove listings; extracts all needed fields."""
    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
        self._results = self._get_results(get_floorplans=get_floorplans)

    @staticmethod
    def _request(url: str):
        r = requests.get(
            url,
            headers={"User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0 Safari/537.36"
            },
            timeout=10,
        )
        return r.status_code, r.content

    def _validate_url(self):
        templ = "{}://www.rightmove.co.uk/{}/find.html?"
        protos = ["http","https"]
        types = ["property-to-rent","property-for-sale","new-homes-for-sale"]
        prefixes = [templ.format(p,t) for p in protos for t in types]
        if not any(self._url.startswith(p) for p in prefixes) or self._status_code != 200:
            raise ValueError(f"Invalid Rightmove URL:\n{self._url}")

    @property
    def url(self): return self._url

    @property
    def get_results(self): return self._results

    @property
    def results_count_display(self):
        tree = html.fromstring(self._first_page)
        txt = tree.xpath("//span[contains(@class,'searchHeader-resultCount')]/text()")
        if not txt:
            return 0
        try:
            return int(txt[0].replace(",",""))
        except:
            return 0

    @property
    def page_count(self):
        tot = self.results_count_display
        pages = tot//24 + (1 if tot%24 else 0)
        return min(max(pages,1),42)

    def _parse_date(self, text):
        now = int(time.time())
        if not text:
            return now
        lt = text.lower()
        if "today" in lt:
            return now
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", lt)
        if m:
            d,mo,y = map(int, m.groups())
            dt = datetime.datetime(y,mo,d, tzinfo=LONDON)
            return int(dt.timestamp())
        return now

    def _get_page(self, content: bytes, get_floorplans: bool = False):
        tree = html.fromstring(content)
        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        rows = []
        base = "https://www.rightmove.co.uk"
        for c in cards:
            p = c.xpath(
                ".//a[@data-testid='property-price']"
                "//div[contains(@class,'PropertyPrice_price__')]/text()"
            )
            price_raw = p[0].strip() if p else None

            ad = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = ad[0].strip() if ad else None

            tp = c.xpath(
                ".//span[contains(@class,'PropertyInformation_propertyType')]/text()"
            )
            ptype = tp[0].strip() if tp else None

            bd = c.xpath(
                ".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()"
            )
            try:
                beds = int(bd[0]) if bd else None
            except:
                beds = None

            ld = c.xpath(".//span[contains(@class,'MarketedBy_joinedText')]/text()") or [None]
            ud = c.xpath(".//span[contains(@class,'MarketedBy_addedOrReduced')]/text()") or [None]
            listed_txt = ld[0]
            updated_txt = ud[0]

            stc = bool(c.xpath(
                ".//span[contains(text(),'STC') or contains(text(),'Subject to contract')]"
            ))

            href = c.xpath(".//a[@data-test='property-details']/@href")
            url = f"{base}{href[0]}" if href else None

            img = c.xpath(".//img[@data-testid='property-img-1']/@src") or [None]
            img_url = img[0]

            an = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//img/@alt"
            )
            agent = an[0].replace(" Estate Agent Logo","").strip() if an else None
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
                "listed_date": listed_txt,
                "updated_date": updated_txt,
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
        df = df.dropna(subset=["id","price","address","type"])
        df.reset_index(drop=True, inplace=True)
        df["listed_ts"]  = df["listed_date"].apply(self._parse_date)
        df["updated_ts"] = df["updated_date"].apply(self._parse_date)
        return df

    def _get_results(self, get_floorplans: bool=False):
        df = self._get_page(self._first_page, get_floorplans)
        for p in range(1, self.page_count):
            u = f"{self.url}&index={p*24}"
            sc, ct = self._request(u)
            if sc != 200:
                break
            tmp = self._get_page(ct, get_floorplans)
            df = pd.concat([df, tmp], ignore_index=True)
        return df


class RightmoveCog(commands.Cog):
    """Daily Rightmove scraper, caching, multi-category channel allocation."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        self.config.register_global(properties={})
        self.scrape_loop = None
        self.target_channel = None

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.command(name="start-scrape")
    async def start_scrape(self, ctx, channel: discord.TextChannel=None):
        """
        Schedule daily scrape at 07:00 Europe/London.
        Use `!fetch-now` to run it manually.
        """
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape already scheduled.")
        self.target_channel = channel or ctx.channel

        self.scrape_loop = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()

        cat = self.target_channel.category.name if self.target_channel.category else "NONE"
        await ctx.send(f"✅ Scheduled scrape (07:00 London) → will create channels under categories named `{cat}` series.")

    @commands.is_owner()
    @commands.command(name="stop-scrape")
    async def stop_scrape(self, ctx):
        """Unschedule the daily scrape."""
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ No scrape scheduled.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape unscheduled.")

    @commands.is_owner()
    @commands.command(name="fetch-now")
    async def fetch_now(self, ctx):
        """Run a manual scrape immediately."""
        await ctx.send("🔄 Manual scrape…")
        await self.do_scrape()
        await ctx.send("✅ Manual scrape done.")

    async def do_scrape(self):
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=175000&radius=0.0"
            "&locationIdentifier=USERDEFINEDAREA%5E%7B…%22%7D"
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
        old_ids   = set(cache)
        new_ids   = set(new_props)

        guild    = self.target_channel.guild
        orig_cat = self.target_channel.category
        prefix   = orig_cat.name.rsplit(" ",1)[0] if orig_cat and " " in orig_cat.name else (orig_cat.name if orig_cat else "RIGHTMOVE")

        # find or create category with <50 prop channels
        def idx(c):
            parts=c.name.rsplit(" ",1)
            return int(parts[1]) if len(parts)==2 and parts[1].isdigit() else 1

        cats = [c for c in guild.categories if c.name.startswith(prefix)]
        cats.sort(key=idx)
        target_cat = None
        for c in cats:
            count = sum(isinstance(ch, discord.TextChannel) and ch.name.startswith("prop-") for ch in c.channels)
            if count < 50:
                target_cat = c
                break
        if not target_cat:
            n = idx(cats[-1]) + 1 if cats else 1
            target_cat = await guild.create_category(f"{prefix} {n}")

        # new & updates
        for pid, r in new_props.items():
            is_new = pid not in cache
            old    = cache.get(pid, {})
            price_changed = (not is_new) and (r["price"] != old.get("price"))
            stc_changed   = r["is_stc"] and not old.get("is_stc", False)

            if is_new:
                ch = await guild.create_text_channel(f"prop-{pid}", category=target_cat)
                cache[pid] = {
                    "channel_id": ch.id,
                    "price": r["price"],
                    "listed_ts": r["listed_ts"],
                    "updated_ts": r["updated_ts"],
                    "is_stc": r["is_stc"],
                    "active": True,
                }
                await self.post_embed(ch, r, event="new")
                continue

            ch = guild.get_channel(old["channel_id"])
            if not ch:
                continue

            if stc_changed:
                cache[pid]["is_stc"] = True
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self.post_embed(ch, r, event="stc")
                continue

            if price_changed:
                cache[pid]["price"] = r["price"]
                cache[pid]["updated_ts"] = r["updated_ts"]
                await self.post_embed(ch, r, event="price_update")
                continue

        # vanished
        for pid in old_ids - new_ids:
            old = cache[pid]
            if old.get("active", True):
                ch = guild.get_channel(old["channel_id"])
                if ch:
                    cache[pid]["active"] = False
                    await self.post_embed(ch, None, event="vanished")

        await self.config.properties.set(cache)

    async def post_embed(self, ch: discord.TextChannel, r, event: str):
        if event=="new":
            pre, emoji, color = "New", "🆕", None
        elif event=="price_update":
            pre, emoji, color = "Price Updated", "🔄", None
        elif event=="stc":
            pre, emoji, color = "[STC]", "💖", discord.Color.magenta()
        else:  # vanished
            pre, emoji, color = "Vanished", "❌", discord.Color.greyple()

        if r:
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
            embed.set_thumbnail(url=r["image_url"])
            embed.add_field(name="💷 Price", value=f"£{int(price):,}", inline=True)
            embed.add_field(name="🛏 Bedrooms", value=str(r["number_bedrooms"]), inline=True)
            embed.add_field(name="🏠 Type", value=r["type"], inline=True)
            if r["agent"] and r["agent_url"]:
                embed.add_field(name="🔗 Agent", value=f"[{r['agent']}]({r['agent_url']})", inline=True)
            await ch.send(embed=embed)
        else:
            title = f"❌ {emoji} {pre}"
            embed = discord.Embed(
                title=title,
                color=discord.Color.greyple(),
                description="This property has vanished from the search.",
            )
            await ch.send(embed=embed)