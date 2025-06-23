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

    def refresh_data(self, url: str = None, get_floorplans: bool = False):
        url = self.url if not url else url
        self._status_code, self._first_page = self._request(url)
        self._url = url
        self._validate_url()
        self._results = self._get_results(get_floorplans=get_floorplans)

    def _validate_url(self):
        real = "{}://www.rightmove.co.uk/{}/find.html?"
        protocols = ["http", "https"]
        types = ["property-to-rent", "property-for-sale", "new-homes-for-sale"]
        prefixes = [real.format(p, t) for p in protocols for t in types]
        if not self.url.startswith(tuple(prefixes)) or self._status_code != 200:
            raise ValueError(f"Invalid Rightmove URL:\n{self.url}")

    @property
    def url(self):
        return self._url

    @property
    def get_results(self):
        return self._results

    @property
    def rent_or_sale(self):
        if "/property-for-sale/" in self.url or "/new-homes-for-sale/" in self.url:
            return "sale"
        if "/property-to-rent/" in self.url:
            return "rent"
        if "/commercial-property-for-sale/" in self.url:
            return "sale-commercial"
        if "/commercial-property-to-let/" in self.url:
            return "rent-commercial"
        raise ValueError(f"Invalid Rightmove URL:\n{self.url}")

    @property
    def results_count_display(self):
        """Total listings as shown on the first page, or 0 if not found."""
        tree = html.fromstring(self._first_page)
        xp = "//span[@class='searchHeader-resultCount']/text()"
        items = tree.xpath(xp)
        if not items:
            return 0
        try:
            return int(items[0].replace(",", ""))
        except ValueError:
            return 0

    @property
    def page_count(self):
        """Number of pages (24 results per page, max 42)."""
        total = self.results_count_display
        # if no total or fewer than 25, just 1 page
        if total < 1 or total <= 24:
            return 1
        pages = total // 24 + (1 if total % 24 else 0)
        return min(pages, 42)

    def _get_page(self, content: bytes, get_floorplans: bool = False):
        tree = html.fromstring(content)
        # price XPaths differ for rent vs sale
        if "rent" in self.rent_or_sale:
            xp_price = "//span[@class='propertyCard-priceValue']/text()"
        else:
            xp_price = "//div[@class='propertyCard-priceValue']/text()"

        xp_title   = "//div[@class='propertyCard-details']//h2[@class='propertyCard-title']/text()"
        xp_address = "//address[@class='propertyCard-address']//span/text()"
        xp_link    = "//div[@class='propertyCard-details']//a[@class='propertyCard-link']/@href"
        xp_agent   = (
            "//div[@class='propertyCard-contactsItem']"
            "//a[@class='propertyCard-branchLogo-link']/@href"
        )

        prices    = tree.xpath(xp_price)
        titles    = tree.xpath(xp_title)
        addresses = tree.xpath(xp_address)
        base      = "https://www.rightmove.co.uk"
        links     = [f"{base}{u}" for u in tree.xpath(xp_link)]
        agents    = [f"{base}{u}" for u in tree.xpath(xp_agent)]

        floorplans = [] if get_floorplans else np.nan
        if get_floorplans:
            for u in links:
                sc, ct = self._request(u)
                if sc != 200:
                    floorplans.append(np.nan)
                    continue
                t2 = html.fromstring(ct)
                fp = t2.xpath("//*[@id='floorplanTabs']/div[2]/div[2]/img/@src")
                floorplans.append(fp[0] if fp else np.nan)

        data = [prices, titles, addresses, links, agents]
        if get_floorplans:
            data.append(floorplans)

        cols = ["price", "type", "address", "url", "agent_url"]
        if get_floorplans:
            cols.append("floorplan_url")

        df = pd.DataFrame(list(zip(*data)), columns=cols)
        return df[df["address"].notnull()]

    def _get_results(self, get_floorplans: bool = False):
        results = self._get_page(self._first_page, get_floorplans)
        # only iterate pages 2..page_count
        for p in range(1, self.page_count):
            p_url = f"{self.url}&index={p*24}"
            sc, content = self._request(p_url)
            if sc != 200:
                break
            temp = self._get_page(content, get_floorplans)
            results = pd.concat([results, temp], ignore_index=True)
        return self._clean_results(results)

    @staticmethod
    def _clean_results(results: pd.DataFrame):
        results["price"] = (
            results["price"].replace(r"\D+", "", regex=True).astype(float)
        )
        results["postcode"] = results["address"].str.extract(
            r"\b([A-Za-z][A-Za-z]?[0-9][0-9]?[A-Za-z]?)\b"
        )[0]
        results["full_postcode"] = results["address"].str.extract(
            r"([A-Za-z][A-Za-z]?[0-9][0-9]?[A-Za-z]?[0-9]?\s[0-9]?[A-Za-z][A-Za-z])"
        )[0]
        beds = results["type"].str.extract(r"\b(\d{1,2})\b")[0].fillna("")
        beds[beds.str.lower().str.contains("studio")] = "0"
        results["number_bedrooms"] = pd.to_numeric(beds, errors="coerce").fillna(0).astype(int)
        results["type"] = results["type"].str.strip()
        results["search_date"] = datetime.datetime.now()
        return results


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
        await self.do_scrape()      # immediate first run
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

        em = discord.Embed(
            title="📈 New Rightmove Listings (past 14 days)",
            description=f"Scraped at <t:{ts}:F> (<t:{ts}:R>)",
            color=discord.Color.blue(),
        )
        if df.empty:
            em.add_field(name="No new listings", value="None found in the past 14 days.")
        else:
            for _, r in df.iterrows():
                em.add_field(
                    name=r["address"],
                    value=(
                        f"💷 **£{int(r['price']):,}**\n"
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