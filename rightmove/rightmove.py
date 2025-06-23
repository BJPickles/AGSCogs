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
    """The `RightmoveData` webscraper collects structured data on properties
    returned by a search performed on www.rightmove.co.uk."""
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
        real_url = "{}://www.rightmove.co.uk/{}/find.html?"
        protocols = ["http", "https"]
        types = ["property-to-rent", "property-for-sale", "new-homes-for-sale"]
        valid_prefixes = [real_url.format(p, t) for p in protocols for t in types]
        if not self.url.startswith(tuple(valid_prefixes)) or self._status_code != 200:
            raise ValueError(f"Invalid rightmove search URL:\n\n\t{self.url}")

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
        elif "/property-to-rent/" in self.url:
            return "rent"
        elif "/commercial-property-for-sale/" in self.url:
            return "sale-commercial"
        elif "/commercial-property-to-let/" in self.url:
            return "rent-commercial"
        else:
            raise ValueError(f"Invalid rightmove URL:\n\n\t{self.url}")

    @property
    def results_count_display(self):
        tree = html.fromstring(self._first_page)
        xpath = "//span[@class='searchHeader-resultCount']/text()"
        return int(tree.xpath(xpath)[0].replace(",", ""))

    @property
    def page_count(self):
        pages = self.results_count_display // 24
        if self.results_count_display % 24:
            pages += 1
        return min(pages, 42)

    def _get_page(self, request_content: str, get_floorplans: bool = False):
        tree = html.fromstring(request_content)
        if "rent" in self.rent_or_sale:
            xp_prices = "//span[@class='propertyCard-priceValue']/text()"
        else:
            xp_prices = "//div[@class='propertyCard-priceValue']/text()"
        xp_titles = (
            "//div[@class='propertyCard-details']"
            "//a[@class='propertyCard-link']"
            "//h2[@class='propertyCard-title']/text()"
        )
        xp_addresses = "//address[@class='propertyCard-address']//span/text()"
        xp_weblinks = "//div[@class='propertyCard-details']//a[@class='propertyCard-link']/@href"
        xp_agent_urls = (
            "//div[@class='propertyCard-contactsItem']"
            "//div[@class='propertyCard-branchLogo']"
            "//a[@class='propertyCard-branchLogo-link']/@href"
        )

        prices = tree.xpath(xp_prices)
        titles = tree.xpath(xp_titles)
        addresses = tree.xpath(xp_addresses)
        base = "https://www.rightmove.co.uk"
        weblinks = [f"{base}{u}" for u in tree.xpath(xp_weblinks)]
        agent_urls = [f"{base}{u}" for u in tree.xpath(xp_agent_urls)]

        floorplan_urls = [] if get_floorplans else np.nan
        if get_floorplans:
            for link in weblinks:
                sc, cont = self._request(link)
                if sc != 200:
                    floorplan_urls.append(np.nan)
                    continue
                t2 = html.fromstring(cont)
                xp_fp = "//*[@id='floorplanTabs']/div[2]/div[2]/img/@src"
                fps = t2.xpath(xp_fp)
                floorplan_urls.append(fps[0] if fps else np.nan)

        data = [prices, titles, addresses, weblinks, agent_urls]
        if get_floorplans:
            data.append(floorplan_urls)
        df = pd.DataFrame(list(zip(*data)), columns=[
            "price", "type", "address", "url", "agent_url"
        ] + (["floorplan_url"] if get_floorplans else []))
        df = df[df["address"].notnull()]
        return df

    def _get_results(self, get_floorplans: bool = False):
        results = self._get_page(self._first_page, get_floorplans=get_floorplans)
        for p in range(1, self.page_count + 1):
            p_url = f"{self.url}&index={p * 24}"
            sc, content = self._request(p_url)
            if sc != 200:
                break
            temp = self._get_page(content, get_floorplans=get_floorplans)
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
    """Redbot cog to scrape Rightmove once daily for new listings."""
    def __init__(self, bot):
        self.bot = bot
        self.target_channel: discord.TextChannel = None

    def cog_unload(self):
        if self.scrape_loop.is_running():
            self.scrape_loop.cancel()

    @commands.is_owner()
    @commands.command(name="start-scrape")
    async def _start(self, ctx, channel: discord.TextChannel = None):
        """
        Start the daily Rightmove scrape.
        Optionally specify a channel; otherwise uses this one.
        """
        if self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape is already running.")
        self.target_channel = channel or ctx.channel
        self.scrape_loop.start()
        await ctx.send(f"✅ Started daily scraping. Posting to {self.target_channel.mention}")

    @commands.is_owner()
    @commands.command(name="stop-scrape")
    async def _stop(self, ctx):
        """Stop the daily Rightmove scrape."""
        if not self.scrape_loop.is_running():
            return await ctx.send("❌ Scrape is not running.")
        self.scrape_loop.cancel()
        await ctx.send("✅ Stopped scraping.")

    @tasks.loop(hours=24, wait=False)
    async def scrape_loop(self):
        # Your improved URL with maxDaysSinceAdded=14
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

        scraped_at = int(time.time())
        rm = RightmoveData(url)
        df = rm.get_results

        em = discord.Embed(
            title="📈 New Rightmove Listings (past 14 days)",
            description=f"Scraped at <t:{scraped_at}:F> (<t:{scraped_at}:R>)",
            color=discord.Color.blue()
        )

        if df.empty:
            em.add_field(
                name="No new listings",
                value="No properties found in the past 14 days."
            )
        else:
            for _, row in df.iterrows():
                em.add_field(
                    name=row["address"],
                    value=(
                        f"💷 **£{int(row['price']):,}**\n"
                        f"🛏 **{row['number_bedrooms']}** beds\n"
                        f"🏠 [View listing]({row['url']})\n"
                        f"🔗 [Agent page]({row['agent_url']})"
                    ),
                    inline=False
                )

        await self.target_channel.send(embed=em)


def setup(bot):
    bot.add_cog(RightmoveCog(bot))