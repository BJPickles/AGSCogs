# cogs/agshousingscraper/agshousingscraper.py

import asyncio
import os
import tempfile
import re
import hashlib

import aiohttp
import async_timeout
import discord
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting as cf

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
        # map guild_id -> asyncio.Task
        self.monitor_tasks = {}

    def cog_unload(self):
        for task in self.monitor_tasks.values():
            task.cancel()

    async def _monitor_loop(self, guild):
        """Background loop that checks every poll_interval seconds."""
        await self.bot.wait_until_ready()
        settings = self.config.guild(guild)
        while True:
            try:
                await self.check_site(guild)
            except Exception:
                self.bot.log.exception(f"[agshousingscraper:{guild.name}] error in monitor loop")
            interval = await settings.poll_interval()
            await asyncio.sleep(interval)

    async def check_site(self, guild):
        """Fetch the page, detect changes or new posts, and alert if needed."""
        cfg = await self.config.guild(guild).all()
        url = cfg["url"]
        selector = cfg["selector"]
        use_hash = cfg["use_section_hash"]
        use_ss = cfg["use_screenshot"]

        # 1) fetch HTML
        try:
            async with aiohttp.ClientSession() as session:
                with async_timeout.timeout(30):
                    resp = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    html = await resp.text()
        except Exception:
            self.bot.log.warning(f"[agshousingscraper:{guild.name}] failed to fetch {url}")
            return

        soup = BeautifulSoup(html, "html.parser")

        # 2) hash-mode: alert on any change in the section
        if use_hash:
            sec = soup.select_one(selector)
            if not sec:
                return
            new_hash = hashlib.sha256(str(sec).encode("utf-8")).hexdigest()
            if new_hash != cfg["last_hash"]:
                await self.config.guild(guild).last_hash.set(new_hash)
                embed = discord.Embed(
                    title="Section changed!",
                    description="The monitored section has been updated.",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow()
                )
                ss = await self.capture_screenshot(url, selector) if use_ss else None
                await self.dispatch_alert(guild, None, embed, ss)
            return

        # 3) per-post detection
        section = soup.select_one(selector)
        if not section:
            return
        # skip placeholder
        if section.select_one("[data-hook='empty-state-container']"):
            return

        # gather candidate links
        candidates = set()
        for a in section.find_all("a", href=True):
            href = a["href"]
            txt = a.get_text("", True).lower()
            if "blog" in href.lower() or "read more" in txt:
                candidates.add(href)
        # fallback on blog-post class
        if not candidates:
            for art in section.find_all(["article", "div"], class_=re.compile(r"blog-post", re.I)):
                a = art.find("a", href=True)
                if a:
                    candidates.add(a["href"])

        seen = set(cfg["seen_posts"])
        new_posts = candidates - seen
        if not new_posts:
            return

        # update seen
        await self.config.guild(guild).seen_posts.set(list(seen | new_posts))

        # alert for each new link
        for href in new_posts:
            title, snippet, thumb = await self.scrape_post(href)
            embed = discord.Embed(
                title=title or "New Housing Post",
                url=href,
                description=snippet or "No snippet available.",
                color=discord.Color.blue(),
                timestamp=discord.utils.utcnow()
            )
            if thumb:
                embed.set_thumbnail(url=thumb)
            ss = await self.capture_screenshot(href) if use_ss else None
            await self.dispatch_alert(guild, None, embed, ss)

    async def scrape_post(self, url):
        """Fetch an individual post and extract title, snippet, og:image."""
        title = snippet = thumb = None
        try:
            async with aiohttp.ClientSession() as session:
                with async_timeout.timeout(15):
                    r = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    txt = await r.text()
        except:
            return title, snippet, thumb

        soup = BeautifulSoup(txt, "html.parser")
        h = soup.find(re.compile(r"h[1-3]"))
        if h:
            title = h.get_text(strip=True)
        p = soup.find("p")
        if p:
            snippet = p.get_text(strip=True)[:200] + "…"
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            thumb = og["content"]
        return title, snippet, thumb

    async def dispatch_alert(self, guild, content, embed, screenshot_path=None):
        """Send embed (and optional screenshot) to channel and/or DM."""
        cfg = await self.config.guild(guild).all()
        ch = guild.get_channel(cfg["channel_id"]) if cfg["channel_id"] else None
        usr = self.bot.get_user(cfg["dm_user_id"]) if cfg["dm_user_id"] else None

        # ping role if set
        if cfg["role_id"] and ch:
            content = f"<@&{cfg['role_id']}>"

        file = None
        if screenshot_path:
            file = discord.File(screenshot_path, filename="screenshot.png")
            embed.set_image(url="attachment://screenshot.png")

        if ch:
            await ch.send(content=content, embed=embed, file=file)
        if usr:
            try:
                await usr.send(embed=embed, file=file)
            except:
                pass

        # cleanup
        if file and os.path.exists(screenshot_path):
            os.remove(screenshot_path)

    async def capture_screenshot(self, url, css_selector=None):
        """Capture headless-chrome screenshot of page or specific element."""
        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--window-size=1280,2000")
        driver = webdriver.Chrome(options=opts)
        driver.get(url)
        if css_selector:
            try:
                el = driver.find_element("css selector", css_selector)
                driver.execute_script("arguments[0].scrollIntoView();", el)
            except:
                pass
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        driver.save_screenshot(tmp.name)
        driver.quit()
        return tmp.name

    # ───────────────── Commands ─────────────────

    @commands.group(name="agshousingscraper")
    @checks.admin_or_permissions(manage_guild=True)
    async def agshousingscraper(self, ctx):
        """Manage the AGS Housing Scraper."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agshousingscraper.command()
    async def start(self, ctx):
        """Start monitoring this server."""
        if ctx.guild.id in self.monitor_tasks:
            return await ctx.send("🔄 Already running.")
        task = self.bot.loop.create_task(self._monitor_loop(ctx.guild))
        self.monitor_tasks[ctx.guild.id] = task
        await ctx.tick()

    @agshousingscraper.command()
    async def stop(self, ctx):
        """Stop monitoring."""
        task = self.monitor_tasks.pop(ctx.guild.id, None)
        if not task:
            return await ctx.send("⚠️ Not running.")
        task.cancel()
        await ctx.tick()

    @agshousingscraper.command()
    async def status(self, ctx):
        """Show current settings & running state."""
        s = await self.config.guild(ctx.guild).all()
        e = discord.Embed(title="AGS Housing Scraper Status", color=discord.Color.blurple())
        e.add_field(name="URL", value=s["url"], inline=False)
        e.add_field(name="Interval", value=f"{s['poll_interval']}s", inline=True)
        e.add_field(name="Selector", value=s["selector"], inline=True)
        e.add_field(name="Channel", value=f"<#{s['channel_id']}>" if s["channel_id"] else "None", inline=True)
        e.add_field(name="Role", value=f"<@&{s['role_id']}>" if s["role_id"] else "None", inline=True)
        e.add_field(name="DM User", value=f"<@{s['dm_user_id']}>" if s["dm_user_id"] else "None", inline=True)
        e.add_field(name="Hash Mode", value=str(s["use_section_hash"]), inline=True)
        e.add_field(name="Screenshots", value=str(s["use_screenshot"]), inline=True)
        e.add_field(name="Seen Posts", value=str(len(s["seen_posts"])), inline=True)
        e.add_field(name="Monitoring", value=str(ctx.guild.id in self.monitor_tasks), inline=True)
        await ctx.send(embed=e)

    @agshousingscraper.command()
    async def seturl(self, ctx, url: str):
        """Set the monitored URL."""
        await self.config.guild(ctx.guild).url.set(url)
        await ctx.tick()

    @agshousingscraper.command()
    async def interval(self, ctx, seconds: int):
        """Set polling interval (in seconds)."""
        await self.config.guild(ctx.guild).poll_interval.set(seconds)
        # if running, restart
        task = self.monitor_tasks.pop(ctx.guild.id, None)
        if task:
            task.cancel()
            self.bot.loop.create_task(self._monitor_loop(ctx.guild))
        await ctx.tick()

    @agshousingscraper.command()
    async def selector(self, ctx, *, css: str):
        """Set the CSS selector for the watched section."""
        await self.config.guild(ctx.guild).selector.set(css)
        await ctx.tick()

    @agshousingscraper.command()
    async def channel(self, ctx, channel: discord.TextChannel):
        """Set the alert channel."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def role(self, ctx, role: discord.Role):
        """Set the role to ping."""
        await self.config.guild(ctx.guild).role_id.set(role.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def dm(self, ctx, user: discord.User):
        """Set a user to DM on alerts."""
        await self.config.guild(ctx.guild).dm_user_id.set(user.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def hashmode(self, ctx, on: bool):
        """Toggle “any-change” hash mode."""
        await self.config.guild(ctx.guild).use_section_hash.set(on)
        await ctx.tick()

    @agshousingscraper.command()
    async def screenshot(self, ctx, on: bool):
        """Toggle screenshots in alerts."""
        await self.config.guild(ctx.guild).use_screenshot.set(on)
        await ctx.tick()

    @agshousingscraper.command()
    async def clear(self, ctx):
        """Clear seen-post history."""
        await self.config.guild(ctx.guild).seen_posts.set([])
        await ctx.send("✅ Cleared history.")

    @agshousingscraper.command()
    async def force(self, ctx):
        """Force an immediate check: always send an embed + screenshot."""
        await ctx.trigger_typing()

        # 1) load config
        cfg = await self.config.guild(ctx.guild).all()
        url = cfg["url"]
        selector = cfg["selector"]
        seen = set(cfg["seen_posts"])

        # 2) fetch page
        try:
            async with aiohttp.ClientSession() as session:
                with async_timeout.timeout(30):
                    r = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    html = await r.text()
        except Exception as e:
            return await ctx.send(f"❌ Could not fetch **{url}**:\n```{e}```")

        # 3) parse section
        soup = BeautifulSoup(html, "html.parser")
        section = soup.select_one(selector)
        if not section:
            return await ctx.send(f"❌ Selector `{selector}` matched nothing on the page.")

        # 4) detect post-links
        candidates = set()
        for a in section.find_all("a", href=True):
            href = a["href"]
            txt = a.get_text("", True).lower()
            if "blog" in href.lower() or "read more" in txt:
                candidates.add(href)
        if not candidates:
            for art in section.find_all(["article", "div"], class_=re.compile(r"blog-post", re.I)):
                a = art.find("a", href=True)
                if a:
                    candidates.add(a["href"])

        new_posts = candidates - seen

        # 5) build description & update seen
        lines = []
        color = discord.Color.blurple()
        if new_posts:
            lines.append(f"🔔 **{len(new_posts)} new post(s)** detected:")
            for link in new_posts:
                lines.append(f"• {link}")
            await self.config.guild(ctx.guild).seen_posts.set(list(seen | new_posts))
            color = discord.Color.green()
        else:
            lines.append("✅ No new posts detected.")

        desc = "\n".join(lines)

        # 6) build embed
        embed = discord.Embed(
            title="🏠 Housing Monitor Force Check",
            url=url,
            description=desc,
            color=color,
            timestamp=discord.utils.utcnow()
        )

        # 7) screenshot
        try:
            ss_path = await self.capture_screenshot(url, selector)
            file = discord.File(ss_path, filename="snapshot.png")
            embed.set_image(url="attachment://snapshot.png")
        except Exception as e:
            file = None
            embed.add_field(name="⚠️ Screenshot failed", value=str(e), inline=False)

        # 8) send
        await ctx.send(embed=embed, file=file)
        await ctx.tick()

        # 9) cleanup
        if file and os.path.exists(ss_path):
            os.remove(ss_path)