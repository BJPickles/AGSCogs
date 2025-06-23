import logging
import asyncio
import datetime

import pytz
import discord
from discord.ext import tasks
from redbot.core import commands, Config

from .scraper import RightmoveScraper, CaptchaError
from .filter_utils import seconds_until_next_scrape, filter_listings, now_in_windows

logging.getLogger('playwright').setLevel(logging.CRITICAL)

class RightmoveAlert(commands.Cog):
    """Cog to alert users of new Rightmove listings based on configurable filters."""
    def __init__(self, bot):
        self.bot = bot
        self.scraper = RightmoveScraper()
        self.tz = pytz.timezone('Europe/London')
        self.scrape_sem = None
        self.config = Config.get_conf(self, identifier=1234567890123456, force_registration=True)
        user_defaults = {
            "enabled": False,
            "maxprice": None,
            "minbeds": None,
            "area": None,
            "region_code": None,
            "keywords": [],
            "customblacklist": [],
            "blacklistleasehold": True,
            "seen": [],
            "active_hours": [["08:00", "12:30"], ["14:00", "22:00"]],
            "night_interval": [900, 2700]
        }
        guild_defaults = {
            "alert_channel": None,
            "log_channel": None,
            "summary_channel": None
        }
        global_defaults = {
            "listings_checked": 0,
            "matched": 0,
            "blocked": 0,
            "user_alerts": {}
        }
        self.config.register_user(**user_defaults)
        self.config.register_guild(**guild_defaults)
        self.config.register_global(**global_defaults)
        self._startup_logged = False

    async def cog_load(self):
        """Start scraping and summary tasks when the cog loads."""
        self.scrape_sem = asyncio.Semaphore(3)
        self.scraping_loop.start()
        self.daily_summary.start()
        self.bot.loop.create_task(self._delayed_startup_log())

    async def _delayed_startup_log(self):
        """Allow the bot to become ready before logging startup."""
        await self.bot.wait_until_ready()
        if self._startup_logged:
            return
        self._startup_logged = True
        await asyncio.sleep(5)
        await self.log_event("Cog loaded and tasks started.")

    async def cog_unload(self):
        """Clean up tasks and browser when the cog unloads."""
        self.scraping_loop.cancel()
        self.daily_summary.cancel()
        await self.scraper.close()

    @tasks.loop(seconds=600)
    async def scraping_loop(self):
        """Main loop: scrape per-area and send alerts."""
        try:
            interval = seconds_until_next_scrape()
            self.scraping_loop.change_interval(seconds=interval)

            users = await self.config.all_users()
            enabled = {
                int(uid): data
                for uid, data in users.items()
                if data.get("enabled") and data.get("area")
            }
            if not enabled:
                return

            # group by area
            area_map = {}
            for uid, data in enabled.items():
                area_map.setdefault(data["area"], []).append((uid, data))

            async def process_area(area, user_list):
                async with self.scrape_sem:
                    for uid, data in user_list:
                        # check user-defined active hours
                        if not now_in_windows(data.get("active_hours")):
                            continue

                        # pull out all three scrape args
                        maxp = data.get("maxprice")
                        minb = data.get("minbeds")
                        rc   = data.get("region_code")

                        try:
                            listings = await self.scraper.scrape_area(area, maxp, minb, rc)
                            self.scraper.backoff_count = 0
                        except CaptchaError as e:
                            await self.log_event(f"Captcha detected for area '{area}': {e}")
                            self.scraper.backoff_count += 1
                            delay = min((2 ** self.scraper.backoff_count) * 60, 3600)
                            await asyncio.sleep(delay)
                            continue
                        except Exception as e:
                            await self.log_event(f"Error scraping area '{area}': {e}")
                            self.scraper.backoff_count += 1
                            delay = min((2 ** self.scraper.backoff_count) * 10, 600)
                            await asyncio.sleep(delay)
                            continue

                        # increment global counters
                        async with self.config.bot() as g:
                            g["listings_checked"] += len(listings)

                        matches, blocked = filter_listings(listings, data)

                        async with self.config.bot() as g:
                            g["matched"] += len(matches)
                            g["blocked"] += blocked

                        seen = set(data.get("seen", []))
                        new = [l for l in matches if l["id"] not in seen]
                        if not new:
                            continue

                        for listing in new:
                            await self.handle_listing(uid, listing)
                            seen.add(listing["id"])

                        await self.config.user(uid).seen.set(list(seen))

                        async with self.config.bot() as g:
                            ua = g.get("user_alerts", {})
                            ua[str(uid)] = ua.get(str(uid), 0) + len(new)
                            g["user_alerts"] = ua

            # fire off all area tasks in parallel
            tasks_list = [
                asyncio.create_task(process_area(area, ul))
                for area, ul in area_map.items()
            ]
            if tasks_list:
                await asyncio.gather(*tasks_list)

        except Exception as e:
            await self.log_event(f"Error in scraping loop: {e}")

    @tasks.loop(time=datetime.time(hour=23, minute=59))
    async def daily_summary(self):
        """Post a daily summary of statistics each day at 23:59."""
        try:
            now_ts = int(datetime.datetime.now(self.tz).timestamp())
            g = await self.config.bot()
            listings_checked = g.get("listings_checked", 0)
            matched           = g.get("matched", 0)
            blocked           = g.get("blocked", 0)
            user_alerts       = g.get("user_alerts", {})
            unique_users      = len([u for u, v in user_alerts.items() if v > 0])

            embed = discord.Embed(title="Daily Summary")
            embed.add_field(name="Listings Checked",       value=str(listings_checked), inline=True)
            embed.add_field(name="Listings Matched",       value=str(matched),           inline=True)
            embed.add_field(name="Blocked by Blacklist",   value=str(blocked),           inline=True)
            embed.add_field(name="Unique Users Alerted",   value=str(unique_users),      inline=True)
            embed.add_field(
                name="Generated",
                value=f"<t:{now_ts}:F> (<t:{now_ts}:R>)",
                inline=False
            )

            for guild in self.bot.guilds:
                cfg = await self.config.guild(guild).all()
                ch_id = cfg.get("summary_channel")
                if ch_id:
                    ch = self.bot.get_channel(ch_id)
                    if ch:
                        try:
                            await ch.send(embed=embed)
                        except:
                            pass

            # reset globals
            async with self.config.bot() as g2:
                g2["listings_checked"] = 0
                g2["matched"]          = 0
                g2["blocked"]          = 0
                g2["user_alerts"]      = {}

        except Exception as e:
            await self.log_event(f"Error in daily summary: {e}")

    async def log_event(self, message: str):
        """Send a log message to all configured log channels."""
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        full_msg = f"{message} — <t:{now_ts}:F> (<t:{now_ts}:R>)"
        for guild in self.bot.guilds:
            cfg = await self.config.guild(guild).all()
            ch_id = cfg.get("log_channel")
            if ch_id:
                ch = self.bot.get_channel(ch_id)
                if ch:
                    try:
                        await ch.send(full_msg)
                    except:
                        pass

    async def handle_listing(self, uid: int, listing: dict):
        """Send a listing alert via DM and in the guild alert channel."""
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        url = listing.get("url")
        embed = discord.Embed(
            title=listing.get("title", "Listing"),
            url=url,
            timestamp=datetime.datetime.fromtimestamp(now_ts, tz=self.tz)
        )
        embed.add_field(name="Price", value=f"£{listing.get('price')}", inline=True)
        embed.add_field(name="Beds", value=str(listing.get("beds")), inline=True)
        embed.add_field(name="Location", value=listing.get("location", "Unknown"), inline=False)
        embed.add_field(
            name="Scraped At",
            value=f"<t:{now_ts}:F> (<t:{now_ts}:R>)",
            inline=False
        )

        user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
        if user:
            try:
                await user.send(url, embed=embed)
            except:
                await self.log_event(f"Failed to DM user {uid}")

        for guild in self.bot.guilds:
            member = guild.get_member(uid)
            if not member:
                continue
            cfg = await self.config.guild(guild).all()
            alert_ch_id = cfg.get("alert_channel")
            if alert_ch_id:
                ch = self.bot.get_channel(alert_ch_id)
                if ch:
                    try:
                        await ch.send(f"<@{uid}> {url}", embed=embed)
                    except:
                        await self.log_event(f"Failed to send alert to channel {alert_ch_id} for user {uid}")
            log_ch_id = cfg.get("log_channel")
            if log_ch_id:
                log_ch = self.bot.get_channel(log_ch_id)
                if log_ch:
                    try:
                        await log_ch.send(f"Alert for user {uid}: Listing {listing.get('id')} — <t:{now_ts}:R> {url}")
                    except:
                        pass

    @commands.group(name="rmalert", invoke_without_command=True)
    async def rmalert(self, ctx):
        """Manage Rightmove alerts."""
        await ctx.send_help()

    @rmalert.command()
    async def enable(self, ctx):
        """Enable Rightmove alerts."""
        await self.config.user(ctx.author).enabled.set(True)
        await ctx.send("Rightmove alerts enabled.")

    @rmalert.command()
    async def disable(self, ctx):
        """Disable Rightmove alerts."""
        await self.config.user(ctx.author).enabled.set(False)
        await ctx.send("Rightmove alerts disabled.")

    @rmalert.group(name="set", invoke_without_command=True)
    async def set(self, ctx):
        """Configure your alert settings."""
        await ctx.send_help()

    @set.command(name="maxprice")
    async def set_maxprice(self, ctx, amount: int):
        """Set maximum price."""
        await self.config.user(ctx.author).maxprice.set(amount)
        await ctx.send(f"Max price set to £{amount}.")

    @set.command(name="minbeds")
    async def set_minbeds(self, ctx, count: int):
        """Set minimum bedrooms."""
        await self.config.user(ctx.author).minbeds.set(count)
        await ctx.send(f"Minimum bedrooms set to {count}.")

    @set.command(name="area")
    async def set_area(self, ctx, *, area: str):
        """Set the area or display name for your search."""
        await self.config.user(ctx.author).area.set(area)
        await ctx.send(f"Area set to {area}.")

    @set.command(name="region")
    async def set_region(self, ctx, *, code: str):
        """Set the URL region code (percent-encoded)."""
        await self.config.user(ctx.author).region_code.set(code)
        await ctx.send(f"Region code set to `{code}`.")

    @set.command(name="keyword")
    async def set_keyword(self, ctx, *, keyword: str):
        """Set a single keyword (deprecated; use [p]rmalert set keywords add)."""
        await self.config.user(ctx.author).keywords.set([keyword.lower().strip()])
        await ctx.send(f"Keyword set to `{keyword}`.")

    @set.command(name="blacklistleasehold")
    async def set_blacklistleasehold(self, ctx, toggle: bool):
        """Toggle leasehold blacklist."""
        await self.config.user(ctx.author).blacklistleasehold.set(toggle)
        await ctx.send(f"Blacklist leasehold set to {toggle}.")

    @set.command(name="activehours")
    async def set_activehours(self, ctx, start1: str, end1: str, start2: str, end2: str):
        """Set your active scraping windows."""
        hours = [[start1, end1], [start2, end2]]
        await self.config.user(ctx.author).active_hours.set(hours)
        await ctx.send(f"Active hours set to {hours}.")

    @set.command(name="nightinterval")
    async def set_nightinterval(self, ctx, min_sec: int, max_sec: int):
        """Set off-hours scrape interval (sec)."""
        await self.config.user(ctx.author).night_interval.set([min_sec, max_sec])
        await ctx.send(f"Night interval set to {min_sec}-{max_sec}s.")

    @set.group(name="channels", invoke_without_command=True)
    async def set_channels(self, ctx):
        """Configure channels."""
        await ctx.send_help()

    @set_channels.command(name="alert")
    @commands.guild_only()
    async def set_channel_alert(self, ctx, channel: discord.TextChannel):
        """Set alert channel."""
        await self.config.guild(ctx.guild).alert_channel.set(channel.id)
        await ctx.send(f"Alert channel set to {channel.mention}.")

    @set_channels.command(name="log")
    @commands.guild_only()
    async def set_channel_log(self, ctx, channel: discord.TextChannel):
        """Set log channel."""
        await self.config.guild(ctx.guild).log_channel.set(channel.id)
        await ctx.send(f"Log channel set to {channel.mention}.")

    @set_channels.command(name="summary")
    @commands.guild_only()
    async def set_channel_summary(self, ctx, channel: discord.TextChannel):
        """Set summary channel."""
        await self.config.guild(ctx.guild).summary_channel.set(channel.id)
        await ctx.send(f"Summary channel set to {channel.mention}.")

    @rmalert.command()
    async def test(self, ctx):
        """Run a test scrape and alert."""
        data = await self.config.user(ctx.author).all()
        if not data.get("area"):
            return await ctx.send("Please set your area or region first.")

        maxp = data.get("maxprice")
        minb = data.get("minbeds")
        rc   = data.get("region_code")

        listings = await self.scraper.scrape_area(data["area"], maxp, minb, rc)
        if listings:
            await self.handle_listing(ctx.author.id, listings[0])
            await ctx.send("Test alert sent.")
        else:
            await ctx.send("No matching listings found for test.")

    @rmalert.command()
    async def status(self, ctx):
        """Show your settings."""
        data = await self.config.user(ctx.author).all()
        embed = discord.Embed(title=f"{ctx.author.display_name}'s Settings")
        embed.add_field(name="Enabled", value=str(data.get("enabled")), inline=True)
        embed.add_field(name="Area", value=data.get("area") or "Not set", inline=True)
        embed.add_field(name="Region Code", value=data.get("region_code") or "None", inline=True)
        embed.add_field(name="Max Price", value=f"£{data.get('maxprice')}" if data.get("maxprice") else "None", inline=True)
        embed.add_field(name="Min Beds", value=str(data.get("minbeds")) or "None", inline=True)
        kws = data.get("keywords", [])
        embed.add_field(name="Keywords", value=", ".join(kws) or "None", inline=False)
        bl = data.get("customblacklist", [])
        embed.add_field(name="Custom Blacklist", value=", ".join(bl) or "None", inline=False)
        embed.add_field(name="Blacklist Leasehold", value=str(data.get("blacklistleasehold")), inline=True)
        ah = ", ".join(f"{w[0]}-{w[1]}" for w in data.get("active_hours", []))
        embed.add_field(name="Active Hours", value=ah or "Default", inline=True)
        ni = data.get("night_interval", [])
        embed.add_field(name="Night Interval", value=(f"{ni[0]}s-{ni[1]}s" if ni else "Default"), inline=True)
        embed.add_field(name="Seen", value=str(len(data.get("seen", []))), inline=True)
        guild_cfg = await self.config.guild(ctx.guild).all()
        ac = guild_cfg.get("alert_channel")
        lc = guild_cfg.get("log_channel")
        sc = guild_cfg.get("summary_channel")
        embed.add_field(name="Alert Channel", value=(f"<#{ac}>" if ac else "None"), inline=True)
        embed.add_field(name="Log Channel", value=(f"<#{lc}>" if lc else "None"), inline=True)
        embed.add_field(name="Summary Channel", value=(f"<#{sc}>" if sc else "None"), inline=True)
        now_ts = int(datetime.datetime.now(self.tz).timestamp())
        embed.add_field(name="Status Generated", value=f"<t:{now_ts}:F> (<t:{now_ts}:R>)", inline=False)
        await ctx.send(embed=embed)