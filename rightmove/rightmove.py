# cogs/rightmove/rightmove.py

import datetime
import time

import numpy as np
import pandas as pd
import requests
from lxml import html

import discord
from discord.ext import tasks
from redbot.core import commands


class RightmoveData:
    """Scrape structured property data from a Rightmove search URL."""
    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
        self._results = self._get_results(get_floorplans=get_floorplans)

    @staticmethod
    def _request(url: str):
        r = requests.get(url)
        return r.status_code, r.content

    def _validate_url(self):
        real = "{}://www.rightmove.co.uk/{}/find.html?"
        protocols = ["http", "https"]
        types = ["property-to-rent", "property-for-sale", "new-homes-for-sale"]
        prefixes = [real.format(p, t) for p in protocols for t in types]
        if not self._url.startswith(tuple(prefixes)) or self._status_code != 200:
            raise ValueError(f"Invalid Rightmove URL:\n{self._url}")

    @property
    def url(self):
        return self._url

    @property
    def get_results(self):
        return self._results

    @property
    def rent_or_sale(self):
        u = self.url
        if "/property-for-sale/" in u or "/new-homes-for-sale/" in u:
            return "sale"
        if "/property-to-rent/" in u:
            return "rent"
        if "/commercial-property-for-sale/" in u:
            return "sale-commercial"
        if "/commercial-property-to-let/" in u:
            return "rent-commercial"
        raise ValueError(f"Invalid Rightmove URL:\n{u}")

    @property
    def results_count_display(self):
        """Total listings shown on first page, or 0."""
        tree = html.fromstring(self._first_page)
        xp = "//span[contains(@class,'searchHeader-resultCount')]/text()"
        items = tree.xpath(xp)
        if not items:
            return 0
        try:
            return int(items[0].replace(",", ""))
        except ValueError:
            return 0

    @property
    def page_count(self):
        """Pages of 24 results, max 42."""
        total = self.results_count_display
        if total <= 24:
            return 1
        pages = total // 24 + (1 if total % 24 else 0)
        return min(pages, 42)

    def _get_page(self, content: bytes, get_floorplans: bool = False):
        """Scrape a single page by iterating each `.propertyCard`."""
        tree = html.fromstring(content)
        cards = tree.xpath("//div[contains(@class,'propertyCard')]")
        rows = []
        base = "https://www.rightmove.co.uk"
        for c in cards:
            # price
            price = c.xpath(".//*[contains(@class,'propertyCard-priceValue')]/text()")
            price = price[0].strip() if price else None
            # title / type
            title = c.xpath(".//h2[contains(@class,'propertyCard-title')]/text()")
            title = title[0].strip() if title else None
            # address
            parts = c.xpath(".//address[contains(@class,'propertyCard-address')]//span/text()")
            address = " ".join(p.strip() for p in parts) if parts else None
            # listing link
            href = c.xpath(".//a[contains(@class,'propertyCard-link')]/@href")
            url = base + href[0] if href else None
            # agent link
            ag = c.xpath(".//a[contains(@class,'propertyCard-branchLogo-link')]/@href")
            agent_url = base + ag[0] if ag else None

            # floorplan (optional)
            floorplan = np.nan
            if get_floorplans and url:
                sc, ct = self._request(url)
                if sc == 200:
                    t2 = html.fromstring(ct)
                    fp = t2.xpath("//*[@id='floorplanTabs']/div[2]/div[2]/img/@src")
                    floorplan = fp[0] if fp else np.nan

            row = {
                "price": price,
                "type": title,
                "address": address,
                "url": url,
                "agent_url": agent_url,
            }
            if get_floorplans:
                row["floorplan_url"] = floorplan
            rows.append(row)
        return pd.DataFrame(rows)

    def _get_results(self, get_floorplans: bool = False):
        df = self._get_page(self._first_page, get_floorplans)
        # pages 2..N
        for p in range(1, self.page_count):
            page_url = f"{self.url}&index={p*24}"
            sc, content = self._request(page_url)
            if sc != 200:
                break
            tmp = self._get_page(content, get_floorplans)
            df = pd.concat([df, tmp], ignore_index=True)
        return self._clean(df)

    @staticmethod
    def _clean(df: pd.DataFrame):
        # price → float
        df["price"] = df["price"].replace(r"\D+", "", regex=True).astype(float)
        # short postcode
        df["postcode"] = df["address"].str.extract(r"\b([A-Za-z][A-Za-z]?[0-9][0-9]?[A-Za-z]?)\b")[0]
        # full postcode
        df["full_postcode"] = df["address"].str.extract(
            r"([A-Za-z][A-Za-z]?[0-9][0-9]?[A-Za-z]?[0-9]?\s[0-9]?[A-Za-z][A-Za-z])"
        )[0]
        # bedrooms
        beds = df["type"].str.extract(r"\b(\d{1,2})\b")[0].fillna("")
        beds[beds.str.lower().str.contains("studio")] = "0"
        df["number_bedrooms"] = pd.to_numeric(beds, errors="coerce").fillna(0).astype(int)
        df["type"] = df["type"].str.strip()
        df["search_date"] = datetime.datetime.now()
        return df


class RightmoveCog(commands.Cog):
    """Scrapes Rightmove daily and posts new listings in an embed."""
    def __init__(self, bot):
        self.bot = bot
        self.target_channel: discord.TextChannel = None

    def cog_unload(self):
        if self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.command(name="start-scrape")
    async def start_scrape(self, ctx, channel: discord.TextChannel = None):
        """Start daily Rightmove scrape (owner only)."""
        if self.scrape_loop.is_running():
            return await ctx.send("❌ Already running.")
        self.target_channel = channel or ctx.channel
        await self.do_scrape()      # immediate run
        self.scrape_loop.start()    # then every 24h
        await ctx.send(f"✅ Scraping started. Posting to {self.target_channel.mention}")

    @commands.is_owner()
    @commands.command(name="stop-scrape")
    async def stop_scrape(self, ctx):
        """Stop the daily scrape."""
        if not self.scrape_loop.is_running():
            return await ctx.send("❌ Not running.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Scraping stopped.")

    async def do_scrape(self):
        # your 14-day URL
        url = (
            "https://www.rightmove.co.uk/property-for-sale/find.html?"
            "sortType=1&viewType=LIST&channel=BUY"
            "&maxPrice=275000&radius=0.0"
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
        ts = int(time.time())
        df = RightmoveData(url).get_results
        # drop any with missing price
        df = df[df["price"].notnull()]

        em = discord.Embed(
            title="📈 New Rightmove Listings (past 14 days)",
            description=f"Scraped at <t:{ts}:F> (<t:{ts}:R>)",
            color=discord.Color.blue(),
        )
        if df.empty:
            em.add_field(name="No new listings", value="None found in the past 14 days.")
        else:
            for _, r in df.iterrows():
                price_int = int(r["price"])
                em.add_field(
                    name=r["address"] or "No address",
                    value=(
                        f"💷 **£{price_int:,}**\n"
                        f"🛏 **{r['number_bedrooms']}** beds\n"
                        f"🏠 [View listing]({r['url']})\n"
                        f"🔗 [Agent page]({r['agent_url']})"
                    ),
                    inline=False,
                )
        await self.target_channel.send(embed=em)

    @tasks.loop(hours=24)
    async def scrape_loop(self):
        await self.do_scrape()