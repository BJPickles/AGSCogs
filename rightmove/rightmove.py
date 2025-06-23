# cogs/rightmove/rightmove.py

import datetime
import time
from datetime import time as dt_time
from zoneinfo import ZoneInfo

from lxml import html
import numpy as np
import pandas as pd
import requests

import discord
from discord.ext import tasks
from redbot.core import commands

# Schedule at 07:00 Europe/London daily
LONDON      = ZoneInfo("Europe/London")
SCRAPE_TIME = dt_time(hour=7, minute=0, tzinfo=LONDON)

TARGET_PRICE = 250_000
IDEAL_DELTA  = 3_000

BANNED_TERMS = [
    "leasehold", "lease hold", "shared ownership",
    "over 50", "over50", "holiday home", "park home"
]


class RightmoveData:
    """Your original RightmoveData—only _request and _get_page are updated for the
       new Next.js markup and to extract `type`."""
    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
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

    def _validate_url(self):
        real = "{}://www.rightmove.co.uk/{}/find.html?"
        protocols = ["http", "https"]
        types = ["property-to-rent", "property-for-sale", "new-homes-for-sale"]
        prefixes = [real.format(p, t) for p in protocols for t in types]
        if not any(self._url.startswith(p) for p in prefixes) or self._status_code != 200:
            raise ValueError(f"Invalid Rightmove URL:\n{self._url}")

    @property
    def url(self):
        return self._url

    @property
    def get_results(self):
        return self._results

    @property
    def results_count_display(self):
        tree = html.fromstring(self._first_page)
        txt = tree.xpath("//span[contains(@class,'searchHeader-resultCount')]/text()")
        if not txt:
            return 0
        try:
            return int(txt[0].replace(",", ""))
        except ValueError:
            return 0

    @property
    def page_count(self):
        total = self.results_count_display
        if total <= 24:
            return 1
        pages = total // 24 + (1 if total % 24 else 0)
        return min(pages, 42)

    def _get_page(self, content: bytes, get_floorplans: bool = False):
        tree = html.fromstring(content)
        # find all cards by data-testid
        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        rows = []
        base = "https://www.rightmove.co.uk"

        for c in cards:
            # price
            p = c.xpath(
                ".//a[@data-testid='property-price']"
                "//div[contains(@class,'PropertyPrice_price__')]/text()"
            )
            price_raw = p[0].strip() if p else None

            # address
            addr = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = addr[0].strip() if addr else None

            # type
            t = c.xpath(".//span[contains(@class,'PropertyInformation_propertyType')]/text()")
            prop_type = t[0].strip() if t else None

            # bedrooms
            b = c.xpath(
                ".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()"
            )
            try:
                beds = int(b[0]) if b else None
            except ValueError:
                beds = None

            # link
            href = c.xpath(".//a[@data-test='property-details']/@href")
            url = f"{base}{href[0]}" if href else None

            # agent & agent_url
            an = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]"
                "//img/@alt"
            )
            agent = (
                an[0].replace(" Estate Agent Logo", "").strip() if an else None
            )
            au = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]"
                "//a/@href"
            )
            agent_url = f"{base}{au[0]}" if au else None

            rows.append({
                "price": price_raw,
                "address": address,
                "type": prop_type,
                "number_bedrooms": beds,
                "url": url,
                "agent": agent,
                "agent_url": agent_url,
            })

        df = pd.DataFrame(rows)
        # clean & convert price, drop bad rows
        df["price"] = (
            df["price"]
            .replace(r"\D+", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        df = df.dropna(subset=["price", "address", "type"])
        df.reset_index(drop=True, inplace=True)
        return df

    def _get_results(self, get_floorplans: bool = False):
        df = self._get_page(self._first_page, get_floorplans)
        for p in range(1, self.page_count):
            page_url = f"{self.url}&index={p*24}"
            sc, content = self._request(page_url)
            if sc != 200:
                break
            tmp = self._get_page(content, get_floorplans)
            df = pd.concat([df, tmp], ignore_index=True)
        return df


class RightmoveCog(commands.Cog):
    """Redbot cog to scrape Rightmove once daily at 07:00 London time."""

    def __init__(self, bot):
        self.bot = bot
        self.scrape_loop = None
        self.target_channel = None

    def cog_unload(self):
        if self.scrape_loop and self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.command(name="start-scrape")
    async def start_scrape(self, ctx, channel: discord.TextChannel = None):
        """
        Start the daily scrape at 07:00 Europe/London.
        Optionally specify a channel; otherwise uses this one.
        """
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape already running.")
        self.target_channel = channel or ctx.channel
        # schedule daily
        self.scrape_loop = tasks.loop(time=SCRAPE_TIME)(self.do_scrape)
        self.scrape_loop.start()
        await ctx.send(f"✅ Started daily scraping at 07:00 London time. Posting to {self.target_channel.mention}")

    @commands.is_owner()
    @commands.command(name="stop-scrape")
    async def stop_scrape(self, ctx):
        """Stop the daily scrape."""
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape is not running.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scrape stopped.")

    async def do_scrape(self):
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=175000&radius=0.0"
            "&locationIdentifier=USERDEFINEDAREA%5E%7B%22polylines%22%3A%22"
            "sh%7CtHhu%7BE%7D%7CDr_Nf%7BAnjZxvLz%7Df%40reAllgA%7Bab%40fg%60"
            "%40kyu%40s_Ncq_%40crl%40uvO%7Dc%7C%40jTozbAlvMadq%40fu%5BasZpmi"
            "%40%7BeMjgf%40jdEhpJt%7BZ_%60Jlpz%40%22%7D"
            "&tenureTypes=FREEHOLD&transactionType=BUY"
            "&displayLocationIdentifier=undefined"
            "&mustHave=parking"
            "&dontShow=newHome%2Cretirement%2CsharedOwnership%2Cauction"
            "&maxDaysSinceAdded=14"
        )
        df = RightmoveData(url).get_results

        # filter out banned terms in `type`
        mask = False
        for term in BANNED_TERMS:
            mask |= df["type"].str.lower().str.contains(term, na=False)
        df = df[~mask].reset_index(drop=True)

        # now one embed per property
        for _, r in df.iterrows():
            price = r["price"]
            # color logic
            if abs(price - TARGET_PRICE) <= IDEAL_DELTA:
                color = discord.Color.from_rgb(173, 216, 230)  # light blue
            elif price <= 170_000:
                color = discord.Color.green()
            elif price <= 220_000:
                color = discord.Color.orange()
            else:
                color = discord.Color.red()

            # first send the plain URL for preview
            await self.target_channel.send(r["url"])

            # build the embed
            em = discord.Embed(
                title=r["address"],
                color=color,
                url=r["url"],
            )
            em.add_field(name="💷 Price", value=f"£{int(price):,}", inline=True)
            em.add_field(name="🛏 Bedrooms", value=str(r["number_bedrooms"]), inline=True)
            em.add_field(name="🏠 Type", value=r["type"], inline=True)
            if r["agent"] and r["agent_url"]:
                em.add_field(
                    name="🔗 Agent",
                    value=f"[{r['agent']}]({r['agent_url']})",
                    inline=True,
                )
            await self.target_channel.send(embed=em)

    @tasks.loop(time=SCRAPE_TIME)
    async def scrape_loop(self):
        # this is replaced by start_scrape; never used directly
        pass