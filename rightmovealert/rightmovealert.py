import logging
import asyncio
import datetime
import io
import random

import pytz
import discord
from discord.ext import tasks
from redbot.core import commands, Config

from .scraper import RightmoveScraper, CaptchaError
from .utils import seconds_until_next_scrape, filter_listings, now_in_windows

logging.getLogger('playwright').setLevel(logging.CRITICAL)

class RightmoveAlert(commands.Cog):
    """Cog to alert users of new Rightmove listings based on criteria."""
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
            "keyword": None,
            "blacklistleasehold": True,
            "seen": [],
            "active_hours": [["08:00","12:30"],["14:00","22:00"]],
            "night_interval": [900,2700]
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

    async def cog_load(self):
        self.scrape_sem = asyncio.Semaphore(3)
        self.scraping_loop.start()
        self.daily_summary.start()
        await self.log_event("Cog loaded and tasks started.")

    async def cog_unload(self):
        self.scraping_loop.cancel()
        self.daily_summary.cancel()
        await self.scraper.close()

    @tasks.loop(seconds=600)
    async def scraping_loop(self):
        try:
            interval = seconds_until_next_scrape()
            self.scraping_loop.change_interval(seconds=interval)
            users = await self.config.all_users()
            enabled = {int(uid): data for uid, data in users.items() if data.get("enabled") and data.get("area")}
            if not enabled:
                return
            area_map = {}
            for uid, data in enabled.items():
                area = data.get("area")
                area_map.setdefault(area, []).append((uid, data))
            async def process_area(area, user_list):
                async with self.scrape_sem:
                    try:
                        listings = await self.scraper.scrape_area(area)
                        self.scraper.backoff_count = 0
                    except CaptchaError as e:
                        await self.log_event(f"Captcha detected scraping {area}, backing off: {e}")
                        self.scraper.backoff_count += 1
                        delay = min((2 ** self.scraper.backoff_count) * 60, 3600)
                        await asyncio.sleep(delay)
                        return
                    except Exception as e:
                        await self.log_event(f"Error scraping area {area}: {e}")
                        self.scraper.backoff_count += 1
                        delay = min((2 ** self.scraper.backoff_count) * 10, 600)
                        await asyncio.sleep(delay)
                        return
                    async with self.config.global() as g:
                        g['listings_checked'] += len(listings)
                    for uid, data in user_list:
                        if not now_in_windows(data.get("active_hours")):
                            continue
                        try:
                            matches, blocked = filter_listings(listings, data)
                            async with self.config.global() as g:
                                g['matched'] += len(matches)
                                g['blocked'] += blocked
                            seen = set(data.get("seen", []))
                            new = [l for l in matches if l["id"] not in seen]
                            if not new:
                                continue
                            for listing in new:
                                await self.handle_listing(uid, listing)
                                seen.add(listing["id"])
                            async with self.config.user(uid) as u:
                                u['seen'] = list(seen)
                            async with self.config.global() as g:
                                ua = g.get('user_alerts', {})
                                ua[str(uid)] = ua.get(str(uid), 0) + len(new)
                                g['user_alerts'] = ua
                        except Exception as e:
                            await self.log_event(f"Error processing user {uid} listings: {e}")
            tasks_list = [asyncio.create_task(process_area(area, ul)) for area, ul in area_map.items()]
            if tasks_list:
                await asyncio.gather(*tasks_list)
        except Exception as e:
            await self.log_event(f"Error in scraping loop: {e}")

    @tasks.loop(time=datetime.time(hour=23, minute=59), timezone=pytz.timezone('Europe/London'))
    async def daily_summary(self):
        try:
            g = await self.config.global()
            listings_checked = g.get('listings_checked', 0)
            matched = g.get('matched', 0)
            blocked = g.get('blocked', 0)
            user_alerts = g.get('user_alerts', {})
            unique_users = len([u for u, v in user_alerts.items() if v > 0])
            embed = discord.Embed(title="Daily Summary", timestamp=datetime.datetime.now(self.tz))
            embed.add_field(name="Listings Checked", value=str(listings_checked), inline=True)
            embed.add_field(name="Listings Matched", value=str(matched), inline=True)
            embed.add_field(name="Blocked by Blacklist", value=str(blocked), inline=True)
            embed.add_field(name="Unique Users Alerted", value=str(unique_users), inline=True)
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
            async with self.config.global() as g2:
                g2['listings_checked'] = 0
                g2['matched'] = 0
                g2['blocked'] = 0
                g2['user_alerts'] = {}
        except Exception as e:
            await self.log_event(f"Error in daily summary: {e}")

    async def log_event(self, message: str):
        ts = datetime.datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{ts}] {message}"
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
        embed = discord.Embed(title=listing.get("title", "Listing"), url=listing.get("url"), timestamp=datetime.datetime.now(self.tz))
        embed.add_field(name="Price", value=f"£{listing.get('price')}", inline=True)
        embed.add_field(name="Beds", value=str(listing.get('beds')), inline=True)
        embed.add_field(name="Location", value=listing.get('location', "Unknown"), inline=False)
        file_bytes = listing.get('screenshot')
        filename = f"{listing.get('id')}.png" if file_bytes else None
        if file_bytes:
            embed.set_image(url=f"attachment://{filename}")
        user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
        if user:
            try:
                if file_bytes:
                    file_dm = discord.File(io.BytesIO(file_bytes), filename=filename)
                    await user.send(embed=embed, file=file_dm)
                else:
                    await user.send(embed=embed)
            except:
                await self.log_event(f"Failed to DM user {uid}")
        for guild in self.bot.guilds:
            member = guild.get_member(uid)
            if member:
                cfg = await self.config.guild(guild).all()
                alert_ch_id = cfg.get("alert_channel")
                if alert_ch_id:
                    ch = self.bot.get_channel(alert_ch_id)
                    if ch:
                        try:
                            if file_bytes:
                                file_ch = discord.File(io.BytesIO(file_bytes), filename=filename)
                                await ch.send(f"<@{uid}>", embed=embed, file=file_ch)
                            else:
                                await ch.send(f"<@{uid}>", embed=embed)
                        except:
                            await self.log_event(f"Failed to send alert to channel {alert_ch_id} for user {uid}")
                log_ch_id = cfg.get("log_channel")
                if log_ch_id:
                    log_ch = self.bot.get_channel(log_ch_id)
                    if log_ch:
                        try:
                            await log_ch.send(f"Alert for user {uid}: Listing {listing.get('id')}")
                        except:
                            pass

    @commands.group(name="rmalert", invoke_without_command=True)
    async def rmalert(self, ctx):
        await ctx.send_help()

    @rmalert.command()
    async def enable(self, ctx):
        await self.config.user(ctx.author).enabled.set(True)
        await ctx.send("Rightmove alerts enabled.")

    @rmalert.command()
    async def disable(self, ctx):
        await self.config.user(ctx.author).enabled.set(False)
        await ctx.send("Rightmove alerts disabled.")

    @rmalert.group(name="set", invoke_without_command=True)
    async def set(self, ctx):
        await ctx.send_help()

    @set.command(name="maxprice")
    async def set_maxprice(self, ctx, amount: int):
        await self.config.user(ctx.author).maxprice.set(amount)
        await ctx.send(f"Max price set to £{amount}.")

    @set.command(name="minbeds")
    async def set_minbeds(self, ctx, count: int):
        await self.config.user(ctx.author).minbeds.set(count)
        await ctx.send(f"Minimum bedrooms set to {count}.")

    @set.command(name="area")
    async def set_area(self, ctx, *, area: str):
        await self.config.user(ctx.author).area.set(area)
        await ctx.send(f"Area set to {area}.")

    @set.command(name="keyword")
    async def set_keyword(self, ctx, *, keyword: str):
        await self.config.user(ctx.author).keyword.set(keyword)
        await ctx.send(f"Keyword set to '{keyword}'.")

    @set.command(name="blacklistleasehold")
    async def set_blacklistleasehold(self, ctx, toggle: bool):
        await self.config.user(ctx.author).blacklistleasehold.set(toggle)
        await ctx.send(f"Blacklist leasehold set to {toggle}.")

    @set.command(name="activehours")
    async def set_activehours(self, ctx, start1: str, end1: str, start2: str, end2: str):
        hours = [[start1, end1], [start2, end2]]
        await self.config.user(ctx.author).active_hours.set(hours)
        await ctx.send(f"Active hours set to {start1}-{end1} and {start2}-{end2}.")

    @set.command(name="nightinterval")
    async def set_nightinterval(self, ctx, min_sec: int, max_sec: int):
        await self.config.user(ctx.author).night_interval.set([min_sec, max_sec])
        await ctx.send(f"Night interval set to between {min_sec}s and {max_sec}s.")

    @set.group(name="channels", invoke_without_command=True)
    async def set_channels(self, ctx):
        await ctx.send_help()

    @set_channels.command(name="alert")
    @commands.guild_only()
    async def set_channel_alert(self, ctx, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).alert_channel.set(channel.id)
        await ctx.send(f"Alert channel set to {channel.mention}.")

    @set_channels.command(name="log")
    @commands.guild_only()
    async def set_channel_log(self, ctx, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).log_channel.set(channel.id)
        await ctx.send(f"Log channel set to {channel.mention}.")

    @set_channels.command(name="summary")
    @commands.guild_only()
    async def set_channel_summary(self, ctx, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).summary_channel.set(channel.id)
        await ctx.send(f"Summary channel set to {channel.mention}.")

    @rmalert.command()
    async def test(self, ctx):
        data = await self.config.user(ctx.author).all()
        if not data.get("area"):
            return await ctx.send("Please set your area first.")
        try:
            listings = await self.scraper.scrape_area(data.get("area"))
            matches, _ = filter_listings(listings, data)
            if matches:
                await self.handle_listing(ctx.author.id, matches[0])
                await ctx.send("Test alert sent.")
            else:
                await ctx.send("No matching listings found for test.")
        except Exception as e:
            await self.log_event(f"Error during test command for user {ctx.author.id}: {e}")
            await ctx.send("An error occurred during test.")

    @rmalert.command()
    async def status(self, ctx):
        data = await self.config.user(ctx.author).all()
        embed = discord.Embed(title=f"{ctx.author.display_name}'s Settings")
        embed.add_field(name="Enabled", value=str(data.get("enabled")), inline=True)
        embed.add_field(name="Area", value=data.get("area") or "Not set", inline=True)
        embed.add_field(name="Max Price", value=(f"£{data.get('maxprice')}" if data.get("maxprice") else "Not set"), inline=True)
        embed.add_field(name="Min Beds", value=(str(data.get("minbeds")) if data.get("minbeds") else "Not set"), inline=True)
        embed.add_field(name="Keyword", value=data.get("keyword") or "None", inline=True)
        embed.add_field(name="Blacklist Leasehold", value=str(data.get("blacklistleasehold")), inline=True)
        ah = ", ".join([f"{a[0]}-{a[1]}" for a in data.get("active_hours", [])])
        embed.add_field(name="Active Hours", value=ah or "Default", inline=True)
        ni = data.get("night_interval", [])
        ni_text = f"{ni[0]}s-{ni[1]}s" if ni else "Default"
        embed.add_field(name="Night Interval", value=ni_text, inline=True)
        embed.add_field(name="Seen Listings", value=str(len(data.get("seen", []))), inline=True)
        guild_cfg = await self.config.guild(ctx.guild).all()
        ac = guild_cfg.get("alert_channel")
        lc = guild_cfg.get("log_channel")
        sc = guild_cfg.get("summary_channel")
        embed.add_field(name="Alert Channel", value=(f"<#{ac}>" if ac else "Not set"), inline=True)
        embed.add_field(name="Log Channel", value=(f"<#{lc}>" if lc else "Not set"), inline=True)
        embed.add_field(name="Summary Channel", value=(f"<#{sc}>" if sc else "Not set"), inline=True)
        await ctx.send(embed=embed)