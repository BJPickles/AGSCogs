import datetime
import time

from lxml import html
import numpy as np
import pandas as pd
import requests

import discord
from discord.ext import tasks
from redbot.core import commands


class RightmoveData:
    """The `RightmoveData` webscraper collects structured data on properties
    returned by a search performed on www.rightmove.co.uk.  (Your original
    class, only _request and _get_page have been updated for the new markup.)"""

    def __init__(self, url: str, get_floorplans: bool = False):
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
        self._results = self._get_results(get_floorplans=get_floorplans)

    @staticmethod
    def _request(url: str):
        # Send a real browser User-Agent so Rightmove returns the full HTML
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
        """Total listings as displayed on the first page, or 0 if not found."""
        tree = html.fromstring(self._first_page)
        # Look for the header span that contains the count
        text = tree.xpath("//span[contains(@class,'searchHeader-resultCount')]/text()")
        if not text:
            return 0
        try:
            return int(text[0].replace(",", ""))
        except ValueError:
            return 0

    @property
    def page_count(self):
        """Number of result pages (24 per page, max 42)."""
        total = self.results_count_display
        if total <= 24:
            return 1
        pages = total // 24 + (1 if total % 24 else 0)
        return min(pages, 42)

    def _get_page(self, request_content: bytes, get_floorplans: bool = False):
        """Scrape data from a single page, using the new data-testids and classes."""
        tree = html.fromstring(request_content)

        # Each card now has a data-testid="propertyCard-<index>"
        cards = tree.xpath("//div[starts-with(@data-testid,'propertyCard-')]")
        rows = []
        base = "https://www.rightmove.co.uk"

        for c in cards:
            # 1) Price
            price = c.xpath(
                ".//a[@data-testid='property-price']//div[contains(@class,'PropertyPrice_price__')]/text()"
            )
            raw_price = price[0].strip() if price else None

            # 2) Address
            addr = c.xpath(".//*[@data-testid='property-address']//address/text()")
            address = addr[0].strip() if addr else None

            # 3) Bedrooms
            beds = c.xpath(
                ".//span[contains(@class,'PropertyInformation_bedroomsCount')]/text()"
            )
            try:
                number_bedrooms = int(beds[0]) if beds else None
            except ValueError:
                number_bedrooms = None

            # 4) Property details URL
            prop = c.xpath(".//a[@data-test='property-details']/@href")
            prop_url = f"{base}{prop[0]}" if prop else None

            # 5) Agent name & URL
            an = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//img/@alt"
            )
            agent = an[0].replace(" Estate Agent Logo", "").strip() if an else None
            au = c.xpath(
                ".//div[contains(@class,'PropertyCard_propertyCardEstateAgent')]//a/@href"
            )
            agent_url = f"{base}{au[0]}" if au else None

            rows.append({
                "price": raw_price,
                "address": address,
                "number_bedrooms": number_bedrooms,
                "url": prop_url,
                "agent": agent,
                "agent_url": agent_url,
            })

        df = pd.DataFrame(rows)
        return self._clean_results(df)

    def _get_results(self, get_floorplans: bool = False):
        """Build a DataFrame with all pages of results."""
        results = self._get_page(self._first_page, get_floorplans=get_floorplans)
        for p in range(1, self.page_count):
            page_url = f"{self.url}&index={p*24}"
            sc, content = self._request(page_url)
            if sc != 200:
                break
            temp_df = self._get_page(content, get_floorplans=get_floorplans)
            results = pd.concat([results, temp_df], ignore_index=True)
        return results

    @staticmethod
    def _clean_results(df: pd.DataFrame):
        # Convert price to numeric
        df["price"] = (
            df["price"]
            .replace(r"\D+", "", regex=True)
            .replace("", np.nan)
            .astype(float)
        )
        # Drop rows missing price or address
        df = df.dropna(subset=["price", "address"])
        df.reset_index(drop=True, inplace=True)
        return df


class RightmoveCog(commands.Cog):
    """Redbot cog to start/stop daily Rightmove scraping."""

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
        Start the daily Rightmove scrape.
        Optionally specify a channel; otherwise uses this one.
        """
        if self.scrape_loop and self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape already running.")
        self.target_channel = channel or ctx.channel

        # Do an immediate scrape:
        await self.do_scrape()
        # Then schedule every 24h:
        self.scrape_loop = tasks.loop(hours=24)(self.do_scrape)
        self.scrape_loop.start()

        await ctx.send(f"✅ Started scraping. Posting to {self.target_channel.mention}")

    @commands.is_owner()
    @commands.command(name="stop-scrape")
    async def stop_scrape(self, ctx):
        """Stop the daily Rightmove scrape."""
        if not self.scrape_loop or not self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape is not running.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Stopped scraping.")

    async def do_scrape(self):
        # ← your 14-day URL here:
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

        ts = int(time.time())
        df = RightmoveData(url).get_results

        embed = discord.Embed(
            title="📈 New Rightmove Listings (past 14 days)",
            description=f"Scraped at <t:{ts}:F> (<t:{ts}:R>)",
            color=discord.Color.blue(),
        )

        if df.empty:
            embed.add_field(name="No new listings", value="None found.")
        else:
            # One field per listing
            for _, row in df.iterrows():
                embed.add_field(
                    name=row["address"],
                    value=(
                        f"💷 **£{int(row['price']):,}**\n"
                        f"🛏 **{row['number_bedrooms']}** beds\n"
                        f"🏠 [View listing]({row['url']})\n"
                        f"🔗 [Agent page]({row['agent_url']})"
                    ),
                    inline=False,
                )

        await self.target_channel.send(embed=embed)