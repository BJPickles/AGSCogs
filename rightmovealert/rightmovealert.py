import logging
import asyncio
import datetime

import pytz
import discord
from discord.ext import tasks, commands
from redbot.core import Config

from .scraper import RightmoveScraper
from .filter_utils import filter_listings

# silence Playwright DEBUG spam
logging.getLogger("playwright").setLevel(logging.CRITICAL)

class RightmoveAlert(commands.Cog):
    """Alert guilds of new Rightmove listings from a fixed search URL."""
    def __init__(self, bot):
        self.bot = bot
        self.scraper = RightmoveScraper()
        self.tz = pytz.timezone("Europe/London")

        # per-guild settings
        guild_defaults = {
            "search_url": None,
            "alert_channel": None,
            "log_channel": None,
            "summary_channel": None,
            "maxprice": None,
            "minbeds": None,
            "keywords": [],
            "customblacklist": [],
            "blacklistleasehold": True,
            "seen": []
        }
        # global metrics
        global_defaults = {
            "listings_checked": 0,
            "matched": 0,
            "blocked": 0,
            "alerts": 0
        }

        self.config = Config.get_conf(self, identifier=1234567890123456, force_registration=True)
        self.config.register_guild(**guild_defaults)
        self.config.register_global(**global_defaults)

        self.scrape_sem = asyncio.Semaphore(3)
        self._startup_logged = False

    async def cog_load(self):
        self.scraping_loop.start()
        self.daily_summary.start()
        self.bot.loop.create_task(self._delayed_startup_log())

    async def _delayed_startup_log(self):
        await self.bot.wait_until_ready()
        if not self._startup_logged:
            self._startup_logged = True
            await asyncio.sleep(5)
            await self.log_event("Cog loaded and scraping started.")

    async def cog_unload(self):
        self.scraping_loop.cancel()
        self.daily_summary.cancel()
        await self.scraper.close()

    @tasks.loop(seconds=600)
    async def scraping_loop(self):
        """Every 10 min: scrape each guild’s URL, filter, and alert all NEW listings."""
        for guild in self.bot.guilds:
            cfg = await self.config.guild(guild).all()
            url = cfg.get("search_url")
            ac  = cfg.get("alert_channel")
            if not url or not ac:
                continue

            seen = set(cfg.get("seen", []))
            # limit concurrent Playwright tabs
            async with self.scrape_sem:
                try:
                    listings = await self.scraper.scrape_url(url)
                except Exception as e:
                    await self.log_event(f"Error scraping guild {guild.id}: {e}")
                    continue

            # update global counters
            async with self.config.bot() as g:
                g["listings_checked"] += len(listings)

            # apply filters
            matches, blocked = filter_listings(listings, cfg)
            async with self.config.bot() as g:
                g["matched"] += len(matches)
                g["blocked"] += blocked

            # figure out brand-new matches
            new = [L for L in matches if L["id"] not in seen]
            if not new:
                continue

            # persist seen & update alerts metric
            seen.update(L["id"] for L in new)
            await self.config.guild(guild).seen.set(list(seen))
            async with self.config.bot() as g:
                g["alerts"] += len(new)

            # send one embed *per* new listing
            for L in new:
                await self.handle_listing(guild, L)

    @tasks.loop(time=datetime.time(hour=23, minute=59))
    async def daily_summary(self):
        """Every day at 23:59: post a summary embed to each guild’s summary channel."""
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        g      = await self.config.bot()
        lc     = g.get("listings_checked", 0)
        m      = g.get("matched", 0)
        b      = g.get("blocked", 0)
        a      = g.get("alerts", 0)

        embed = discord.Embed(title="Rightmove Daily Summary")
        embed.add_field(name="Scrapes Run",      value=str(lc), inline=True)
        embed.add_field(name="Listings Matched", value=str(m), inline=True)
        embed.add_field(name="Blocked",          value=str(b), inline=True)
        embed.add_field(name="Alerts Sent",      value=str(a), inline=True)
        embed.add_field(name="Generated",        value=f"<t:{now_ts}:F> (<t:{now_ts}:R>)", inline=False)

        for guild in self.bot.guilds:
            sc = (await self.config.guild(guild).summary_channel())
            if sc:
                ch = self.bot.get_channel(sc)
                if ch:
                    try:
                        await ch.send(embed=embed)
                    except:
                        pass

        # reset
        async with self.config.bot() as g2:
            g2["listings_checked"] = 0
            g2["matched"]          = 0
            g2["blocked"]          = 0
            g2["alerts"]           = 0

    async def log_event(self, message: str):
        """Send a timestamped log to every guild’s log_channel."""
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        full   = f"{message} — <t:{now_ts}:F> (<t:{now_ts}:R>)"
        for guild in self.bot.guilds:
            lc = (await self.config.guild(guild).log_channel())
            if lc:
                ch = self.bot.get_channel(lc)
                if ch:
                    try:
                        await ch.send(full)
                    except:
                        pass

    async def handle_listing(self, guild, listing: dict):
        """Build and send one listing embed to the guild’s alert_channel."""
        ts = int(datetime.datetime.now(self.tz).timestamp())
        embed = discord.Embed(
            title=listing["title"],
            url=listing["url"],
            timestamp=datetime.datetime.fromtimestamp(ts, tz=self.tz)
        )
        embed.add_field(name="Price",      value=f"£{listing['price']}", inline=True)
        embed.add_field(name="Beds",       value=str(listing["beds"]),  inline=True)
        embed.add_field(name="Location",   value=listing["location"],  inline=False)
        embed.add_field(name="Scraped At", value=f"<t:{ts}:F> (<t:{ts}:R>)", inline=False)

        ac = (await self.config.guild(guild).alert_channel())
        if ac:
            ch = self.bot.get_channel(ac)
            if ch:
                try:
                    await ch.send(embed=embed)
                except:
                    await self.log_event(f"Failed to send alert in guild {guild.id}")

    @commands.group(name="rmalert", invoke_without_command=True)
    async def rmalert(self, ctx):
        await ctx.send_help()

    @rmalert.group(name="set", invoke_without_command=True)
    async def set(self, ctx):
        await ctx.send_help()

    @set.command(name="url")
    @commands.guild_only()
    async def set_url(self, ctx, *, url: str):
        """Set the exact Rightmove search URL to scrape."""
        await self.config.guild(ctx.guild).search_url.set(url)
        await ctx.send(f"🔗 Search URL set to:\n`{url}`")

    @set.command(name="maxprice")
    @commands.guild_only()
    async def set_maxprice(self, ctx, amount: int):
        """Set the maximum price filter."""
        await self.config.guild(ctx.guild).maxprice.set(amount)
        await ctx.send(f"Max price set to £{amount}.")

    @set.command(name="minbeds")
    @commands.guild_only()
    async def set_minbeds(self, ctx, count: int):
        """Set the minimum bedrooms filter."""
        await self.config.guild(ctx.guild).minbeds.set(count)
        await ctx.send(f"Min beds set to {count}.")

    @set.command(name="keyword")
    @commands.guild_only()
    async def set_keyword(self, ctx, *, keyword: str):
        """Set a single whitelist keyword (resets the list)."""
        kw = keyword.lower().strip()
        await self.config.guild(ctx.guild).keywords.set([kw])
        await ctx.send(f"Whitelist keyword set to `{kw}`.")

    @set.command(name="customblacklist")
    @commands.guild_only()
    async def set_customblacklist(self, ctx, *, items: str):
        """Set your custom blacklist (comma-separated)."""
        terms = [t.lower().strip() for t in items.split(",") if t.strip()]
        await self.config.guild(ctx.guild).customblacklist.set(terms)
        await ctx.send(f"Custom blacklist set to: `{', '.join(terms)}`")

    @set.command(name="blacklistleasehold")
    @commands.guild_only()
    async def set_blacklistleasehold(self, ctx, toggle: bool):
        """Toggle blocking of leasehold listings."""
        await self.config.guild(ctx.guild).blacklistleasehold.set(toggle)
        await ctx.send(f"Blacklist leasehold set to {toggle}.")

    @set.group(name="channels", invoke_without_command=True)
    async def set_channels(self, ctx):
        await ctx.send_help()

    @set_channels.command(name="alert")
    @commands.guild_only()
    async def set_channel_alert(self, ctx, channel: discord.TextChannel):
        """Set the channel where new listings are posted."""
        await self.config.guild(ctx.guild).alert_channel.set(channel.id)
        await ctx.send(f"✅ Alert channel set to {channel.mention}")

    @set_channels.command(name="log")
    @commands.guild_only()
    async def set_channel_log(self, ctx, channel: discord.TextChannel):
        """Set your internal log channel."""
        await self.config.guild(ctx.guild).log_channel.set(channel.id)
        await ctx.send(f"✅ Log channel set to {channel.mention}")

    @set_channels.command(name="summary")
    @commands.guild_only()
    async def set_channel_summary(self, ctx, channel: discord.TextChannel):
        """Set the daily summary channel."""
        await self.config.guild(ctx.guild).summary_channel.set(channel.id)
        await ctx.send(f"✅ Summary channel set to {channel.mention}")

    @rmalert.command()
    async def test(self, ctx):
        """One-off: scrape + filter + alert *all* matching listings immediately."""
        cfg = await self.config.guild(ctx.guild).all()
        url = cfg.get("search_url")
        ac  = cfg.get("alert_channel")
        if not url or not ac:
            return await ctx.send("⚠️ Please set both the URL and an alert channel first.")

        try:
            listings = await self.scraper.scrape_url(url)
        except Exception as e:
            return await ctx.send(f"❌ Scrape error: {e}")
        matches, _ = filter_listings(listings, cfg)
        if not matches:
            return await ctx.send("ℹ️ No matching listings found for test.")
        for L in matches:
            await self.handle_listing(ctx.guild, L)
        await ctx.send(f"✅ Test delivered {len(matches)} listings.")

    @rmalert.command()
    async def status(self, ctx):
        """Show this guild’s current settings."""
        cfg = await self.config.guild(ctx.guild).all()
        embed = discord.Embed(title=f"{ctx.guild.name} Rightmove Status")
        embed.add_field(name="Search URL",      value=cfg.get("search_url") or "None", inline=False)
        embed.add_field(name="Max Price",       value=(f"£{cfg['maxprice']}" if cfg["maxprice"] else "None"), inline=True)
        embed.add_field(name="Min Beds",        value=(str(cfg["minbeds"]) if cfg["minbeds"] else "None"), inline=True)
        kws = cfg.get("keywords", [])
        embed.add_field(name="Whitelist KW",    value=(", ".join(kws) or "None"), inline=False)
        cbl = cfg.get("customblacklist", [])
        embed.add_field(name="Custom BL",       value=(", ".join(cbl) or "None"), inline=False)
        blh = cfg.get("blacklistleasehold")
        embed.add_field(name="Blacklist Lease.",value=str(blh), inline=True)
        ac = cfg.get("alert_channel")
        embed.add_field(name="Alert Channel",   value=(f"<#{ac}>" if ac else "None"), inline=True)
        lc = cfg.get("log_channel")
        embed.add_field(name="Log Channel",     value=(f"<#{lc}>" if lc else "None"), inline=True)
        sc = cfg.get("summary_channel")
        embed.add_field(name="Summary Channel", value=(f"<#{sc}>" if sc else "None"), inline=True)
        seen = len(cfg.get("seen", []))
        embed.add_field(name="Seen Listings",   value=str(seen), inline=True)
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        embed.add_field(name="Generated",       value=f"<t:{now_ts}:F> (<t:{now_ts}:R>)", inline=False)
        await ctx.send(embed=embed)