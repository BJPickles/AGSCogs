import asyncio
import aiohttp
import async_timeout
import discord
import hashlib
import re
from bs4 import BeautifulSoup
from discord.ext import tasks
from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting as cf

# Optional Selenium imports for screenshots:
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import tempfile, os

DEFAULT_SELECTOR = "section:has(h2:contains('Available Houses'))"

class AGSHousingScraper(commands.Cog):
    """Monitor a website for new social-housing posts and alert your server."""
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, 1234567890, force_registration=True)
        self.config.register_guild(
            url="https://www.eost.org.uk/housing",
            poll_interval=300,
            selector=DEFAULT_SELECTOR,
            channel_id=None,
            role_id=None,
            dm_user_id=None,
            seen_posts=[],
            last_hash=None,
            use_section_hash=False,
            use_screenshot=False,
        )
        self._tasks = {}

    def cog_unload(self):
        for t in self._tasks.values():
            t.cancel()

    async def ensure_task(self, guild):
        if guild.id in self._tasks:
            return
        interval = await self.config.guild(guild).poll_interval()
        loop = tasks.Loop(self.check_site, seconds=interval, reconnect=True)
        loop.start(guild)
        self._tasks[guild.id] = loop

    @tasks.loop(seconds=300.0)
    async def check_site(self, guild):
        settings = await self.config.guild(guild).all()
        url = settings["url"]
        selector = settings["selector"]
        # …[fetch page, parse with BeautifulSoup]…
        # …[either do hash-mode or per-post link detection as in the earlier example]…
        # …[on new posts or changes call self.dispatch_alerts()]…

    # …[All helper methods: scrape_post, dispatch_alerts, capture_screenshot]…

    # ─── Commands ───

    @commands.group(name="agshousingscraper")
    @checks.admin_or_permissions(manage_guild=True)
    async def agshousingscraper(self, ctx):
        """Manage the AGS Housing Scraper."""
        if not ctx.invoked_subcommand:
            await ctx.send_help(ctx.command)

    @agshousingscraper.command()
    async def start(self, ctx):
        """Start the monitor."""
        await self.ensure_task(ctx.guild)
        await ctx.tick()

    @agshousingscraper.command()
    async def stop(self, ctx):
        """Stop the monitor."""
        t = self._tasks.pop(ctx.guild.id, None)
        if t:
            t.cancel()
            await ctx.tick()
        else:
            await ctx.send("Not running.")

    @agshousingscraper.command()
    async def status(self, ctx):
        """Show current settings."""
        s = await self.config.guild(ctx.guild).all()
        e = discord.Embed(title="AGS Housing Scraper Status", color=discord.Color.blue())
        e.add_field(name="URL", value=s["url"], inline=False)
        e.add_field(name="Interval", value=f"{s['poll_interval']}s", inline=True)
        e.add_field(name="Selector", value=s["selector"], inline=True)
        e.add_field(name="Channel", value=f"<#{s['channel_id']}>" if s["channel_id"] else "None", inline=True)
        e.add_field(name="Role", value=f"<@&{s['role_id']}>" if s["role_id"] else "None", inline=True)
        e.add_field(name="DM User", value=f"<@{s['dm_user_id']}>" if s["dm_user_id"] else "None", inline=True)
        e.add_field(name="Hash-mode", value=str(s["use_section_hash"]), inline=True)
        e.add_field(name="Screenshots", value=str(s["use_screenshot"]), inline=True)
        e.add_field(name="Seen Posts", value=str(len(s["seen_posts"])), inline=True)
        await ctx.send(embed=e)

    @agshousingscraper.command()
    async def seturl(self, ctx, url: str):
        """Set the target URL."""
        await self.config.guild(ctx.guild).url.set(url)
        await ctx.tick()

    @agshousingscraper.command()
    async def interval(self, ctx, seconds: int):
        """Set polling interval."""
        await self.config.guild(ctx.guild).poll_interval.set(seconds)
        # restart if running
        t = self._tasks.pop(ctx.guild.id, None)
        if t:
            t.cancel()
            await self.ensure_task(ctx.guild)
        await ctx.tick()

    @agshousingscraper.command()
    async def selector(self, ctx, *, css: str):
        """Set CSS selector for the monitored section."""
        await self.config.guild(ctx.guild).selector.set(css)
        await ctx.tick()

    @agshousingscraper.command()
    async def channel(self, ctx, channel: discord.TextChannel):
        """Set alert channel."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def role(self, ctx, role: discord.Role):
        """Set role to ping."""
        await self.config.guild(ctx.guild).role_id.set(role.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def dm(self, ctx, user: discord.User):
        """Set user to DM."""
        await self.config.guild(ctx.guild).dm_user_id.set(user.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def hashmode(self, ctx, on: bool):
        """Toggle any-change hash mode."""
        await self.config.guild(ctx.guild).use_section_hash.set(on)
        await ctx.tick()

    @agshousingscraper.command()
    async def screenshot(self, ctx, on: bool):
        """Toggle screenshots on alerts."""
        await self.config.guild(ctx.guild).use_screenshot.set(on)
        await ctx.tick()

    @agshousingscraper.command()
    async def clear(self, ctx):
        """Forget all seen posts."""
        await self.config.guild(ctx.guild).seen_posts.set([])
        await ctx.send("✅ Cleared history.")

    @agshousingscraper.command()
    async def force(self, ctx):
        """Force an immediate check."""
        await self.check_site(ctx.guild)
        await ctx.tick()