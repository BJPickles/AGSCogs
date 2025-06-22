import asyncio
import os
import tempfile
import re
import hashlib
import datetime

import aiohttp
import async_timeout
import discord
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from redbot.core import commands, Config, checks

DEFAULT_SELECTOR = "section:has(h2:contains('Available Houses'))"

class AGSHousingScraper(commands.Cog):
    """Monitor a website for new social-housing posts, with logging & weekly snapshots."""

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
            log_channel_id=None,      # where to send per-check logs
            seen_posts=[],
            last_hash=None,
            use_section_hash=False,
            use_screenshot=False,
        )
        self.monitor_tasks = {}  # guild.id -> asyncio.Task
        self._auto_start_task = bot.loop.create_task(self._auto_start())
        self._weekly_task = bot.loop.create_task(self._weekly_snapshot_loop())

    def cog_unload(self):
        self._auto_start_task.cancel()
        self._weekly_task.cancel()
        for t in self.monitor_tasks.values():
            t.cancel()

    async def _auto_start(self):
        """On bot ready, resume monitoring for any guild with channels configured."""
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            cfg = await self.config.guild(guild).all()
            if cfg["channel_id"] or cfg["log_channel_id"]:
                if guild.id not in self.monitor_tasks:
                    self.monitor_tasks[guild.id] = self.bot.loop.create_task(self._monitor_loop(guild))

    async def _weekly_snapshot_loop(self):
        """Every Monday at 07:00 UTC post a snapshot to each guild's log channel."""
        await self.bot.wait_until_ready()
        while True:
            now = datetime.datetime.utcnow()
            # next Monday
            days = (0 - now.weekday() + 7) % 7
            target = now + datetime.timedelta(days=days)
            run_at = target.replace(hour=7, minute=0, second=0, microsecond=0)
            if run_at <= now:
                run_at += datetime.timedelta(days=7)
            await asyncio.sleep((run_at - now).total_seconds())

            for guild in self.bot.guilds:
                cfg = await self.config.guild(guild).all()
                lc = cfg["log_channel_id"]
                if not lc:
                    continue
                ch = guild.get_channel(lc)
                if not ch:
                    continue
                ss = None
                try:
                    ss = await self.capture_screenshot(cfg["url"], cfg["selector"])
                    emb = discord.Embed(
                        title="🗓 Weekly Snapshot",
                        description=f"Automated snapshot of monitored section (for week starting {run_at.date()})",
                        color=discord.Color.gold(),
                        timestamp=run_at,
                    )
                    file = discord.File(ss, filename="snapshot.png")
                    emb.set_image(url="attachment://snapshot.png")
                    await ch.send(embed=emb, file=file)
                except Exception as e:
                    await ch.send(f"❌ Weekly snapshot failed:\n```{e}```")
                finally:
                    if ss and os.path.exists(ss):
                        os.remove(ss)

    async def _monitor_loop(self, guild):
        """Background loop: check_site → log → sleep."""
        await self.bot.wait_until_ready()
        settings = self.config.guild(guild)
        while True:
            stats = {
                "time": datetime.datetime.utcnow(),
                "url": await settings.url(),
                "selector": await settings.selector(),
                "mode": "hash" if await settings.use_section_hash() else "posts",
                "success": False,
                "error": None,
                "new_posts": [],
                "section_changed": False,
                "placeholder_skipped": False,
                "candidates": 0,
                "seen_count": len(await settings.seen_posts()),
            }
            try:
                await self.check_site(guild, stats)
                stats["success"] = True
            except Exception as e:
                stats["error"] = repr(e)
            stats["seen_count"] = len(await settings.seen_posts())
            await self._log_to_channel(guild, stats)
            await asyncio.sleep(await settings.poll_interval())

    async def check_site(self, guild, stats):
        """Fetch & parse the page; alert on changes or new posts."""
        cfg = await self.config.guild(guild).all()
        url, selector = stats["url"], stats["selector"]
        use_hash, use_ss = cfg["use_section_hash"], cfg["use_screenshot"]

        # fetch
        async with aiohttp.ClientSession() as session:
            with async_timeout.timeout(30):
                r = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                html = await r.text()
        soup = BeautifulSoup(html, "html.parser")

        # hash mode
        if use_hash:
            section = soup.select_one(selector)
            if not section:
                raise ValueError(f"Selector `{selector}` not found")
            new_h = hashlib.sha256(str(section).encode("utf-8")).hexdigest()
            if new_h != cfg["last_hash"]:
                stats["section_changed"] = True
                await self.config.guild(guild).last_hash.set(new_h)
                emb = discord.Embed(
                    title="Section changed!",
                    description="The monitored section has been updated.",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow(),
                )
                ss = await self.capture_screenshot(url, selector) if use_ss else None
                await self.dispatch_alert(guild, None, emb, ss)
            return

        # post-link mode
        section = soup.select_one(selector)
        if not section:
            raise ValueError(f"Selector `{selector}` not found")
        if section.select_one("[data-hook='empty-state-container']"):
            stats["placeholder_skipped"] = True
            return

        # gather candidates
        cands = set()
        for a in section.find_all("a", href=True):
            href, txt = a["href"], a.get_text("", True).lower()
            if "blog" in href.lower() or "read more" in txt:
                cands.add(href)
        if not cands:
            for art in section.find_all(["article", "div"], class_=re.compile(r"blog-post", re.I)):
                a = art.find("a", href=True)
                if a:
                    cands.add(a["href"])

        stats["candidates"] = len(cands)
        seen = set(cfg["seen_posts"])
        new = cands - seen
        stats["new_posts"] = list(new)
        if not new:
            return

        # update seen
        await self.config.guild(guild).seen_posts.set(list(seen | new))
        for href in new:
            title, snippet, thumb = await self.scrape_post(href)
            emb = discord.Embed(
                title=title or "New Housing Post",
                url=href,
                description=snippet or "No snippet available.",
                color=discord.Color.blue(),
                timestamp=discord.utils.utcnow(),
            )
            if thumb:
                emb.set_thumbnail(url=thumb)
            ss = await self.capture_screenshot(href) if use_ss else None
            await self.dispatch_alert(guild, None, emb, ss)

    async def _log_to_channel(self, guild, stats):
        """Send a no-ping log embed to the configured log channel."""
        cfg = await self.config.guild(guild).all()
        lc = cfg["log_channel_id"]
        if not lc:
            return
        ch = guild.get_channel(lc)
        if not ch:
            return
        e = discord.Embed(
            title="🏷️ Housing Scraper Log",
            timestamp=stats["time"],
            color=discord.Color.dark_gray(),
        )
        e.add_field(name="URL", value=stats["url"], inline=False)
        e.add_field(name="Selector", value=stats["selector"], inline=True)
        e.add_field(name="Mode", value=stats["mode"], inline=True)
        e.add_field(name="Success", value=str(stats["success"]), inline=True)
        if stats["error"]:
            e.add_field(name="Error", value=stats["error"], inline=False)
        if stats["mode"] == "hash":
            e.add_field(name="Section changed", value=str(stats["section_changed"]), inline=True)
        else:
            e.add_field(name="Candidates", value=str(stats["candidates"]), inline=True)
            e.add_field(name="New posts", value=str(len(stats["new_posts"])), inline=True)
            if stats["placeholder_skipped"]:
                e.add_field(name="Placeholder skipped", value="Yes", inline=True)
        e.add_field(name="Seen total", value=str(stats["seen_count"]), inline=True)
        await ch.send(embed=e)

    async def scrape_post(self, url):
        """Fetch a post page and extract title, snippet & og:image."""
        title = snippet = thumb = None
        try:
            async with aiohttp.ClientSession() as session:
                with async_timeout.timeout(15):
                    r = await session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    txt = await r.text()
        except:
            return title, snippet, thumb

        s = BeautifulSoup(txt, "html.parser")
        h = s.find(re.compile(r"h[1-3]"))
        if h:
            title = h.get_text(strip=True)
        p = s.find("p")
        if p:
            snippet = p.get_text(strip=True)[:200] + "…"
        og = s.find("meta", property="og:image")
        if og and og.get("content"):
            thumb = og["content"]
        return title, snippet, thumb

    async def dispatch_alert(self, guild, content, embed, ss_path=None):
        """Send alert embed (and optional screenshot) to alert channel/DM/role."""
        cfg = await self.config.guild(guild).all()
        ch = guild.get_channel(cfg["channel_id"]) if cfg["channel_id"] else None
        usr = self.bot.get_user(cfg["dm_user_id"]) if cfg["dm_user_id"] else None
        if cfg["role_id"] and ch:
            content = f"<@&{cfg['role_id']}>"
        file = None
        if ss_path:
            file = discord.File(ss_path, filename="snapshot.png")
            embed.set_image(url="attachment://snapshot.png")
        if ch:
            await ch.send(content=content, embed=embed, file=file)
        if usr:
            try:
                await usr.send(embed=embed, file=file)
            except:
                pass
        if file and os.path.exists(ss_path):
            os.remove(ss_path)

    async def capture_screenshot(self, url, css_selector=None):
        """Headless Chrome screenshot of page or specific element."""
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

    # ───────────── Commands ─────────────

    @commands.group(name="agshousingscraper")
    @checks.admin_or_permissions(manage_guild=True)
    async def agshousingscraper(self, ctx):
        """Manage the AGS Housing Scraper."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agshousingscraper.command()
    async def start(self, ctx):
        """Start monitoring."""
        if ctx.guild.id in self.monitor_tasks:
            return await ctx.send("🔄 Already running.")
        self.monitor_tasks[ctx.guild.id] = self.bot.loop.create_task(self._monitor_loop(ctx.guild))
        await ctx.tick()

    @agshousingscraper.command()
    async def stop(self, ctx):
        """Stop monitoring."""
        t = self.monitor_tasks.pop(ctx.guild.id, None)
        if not t:
            return await ctx.send("⚠️ Not running.")
        t.cancel()
        await ctx.tick()

    @agshousingscraper.command()
    async def status(self, ctx):
        """Show current settings & running status."""
        s = await self.config.guild(ctx.guild).all()
        e = discord.Embed(title="AGS Housing Scraper Status", color=discord.Color.blurple())
        e.add_field(name="URL", value=s["url"], inline=False)
        e.add_field(name="Interval", value=f"{s['poll_interval']}s", inline=True)
        e.add_field(name="Selector", value=s["selector"], inline=True)
        e.add_field(name="Alert channel", value=f"<#{s['channel_id']}>" if s["channel_id"] else "None", inline=True)
        e.add_field(name="Log channel", value=f"<#{s['log_channel_id']}>" if s["log_channel_id"] else "None", inline=True)
        e.add_field(name="Ping role", value=f"<@&{s['role_id']}>" if s["role_id"] else "None", inline=True)
        e.add_field(name="DM user", value=f"<@{s['dm_user_id']}>" if s["dm_user_id"] else "None", inline=True)
        e.add_field(name="Hash mode", value=str(s["use_section_hash"]), inline=True)
        e.add_field(name="Screenshots", value=str(s["use_screenshot"]), inline=True)
        e.add_field(name="Seen posts", value=str(len(s["seen_posts"])), inline=True)
        e.add_field(name="Monitoring", value=str(ctx.guild.id in self.monitor_tasks), inline=True)
        await ctx.send(embed=e)

    @agshousingscraper.command()
    async def seturl(self, ctx, url: str):
        """Set the monitored URL."""
        await self.config.guild(ctx.guild).url.set(url); await ctx.tick()

    @agshousingscraper.command()
    async def interval(self, ctx, seconds: int):
        """Set polling interval (seconds)."""
        await self.config.guild(ctx.guild).poll_interval.set(seconds)
        t = self.monitor_tasks.pop(ctx.guild.id, None)
        if t:
            t.cancel()
            self.monitor_tasks[ctx.guild.id] = self.bot.loop.create_task(self._monitor_loop(ctx.guild))
        await ctx.tick()

    @agshousingscraper.command()
    async def selector(self, ctx, *, css: str):
        """Set the CSS selector."""
        await self.config.guild(ctx.guild).selector.set(css); await ctx.tick()

    @agshousingscraper.command()
    async def channel(self, ctx, channel: discord.TextChannel):
        """Set the alert channel."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id); await ctx.tick()

    @agshousingscraper.command()
    async def logchannel(self, ctx, channel: discord.TextChannel):
        """Set the log channel (no pings)."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id); await ctx.tick()

    @agshousingscraper.command()
    async def role(self, ctx, role: discord.Role):
        """Set role to ping on alerts."""
        await self.config.guild(ctx.guild).role_id.set(role.id); await ctx.tick()

    @agshousingscraper.command()
    async def dm(self, ctx, user: discord.User):
        """Set user to DM on alerts."""
        await self.config.guild(ctx.guild).dm_user_id.set(user.id); await ctx.tick()

    @agshousingscraper.command()
    async def hashmode(self, ctx, on: bool):
        """Toggle any-change hash mode."""
        await self.config.guild(ctx.guild).use_section_hash.set(on); await ctx.tick()

    @agshousingscraper.command()
    async def screenshot(self, ctx, on: bool):
        """Toggle screenshots in alerts."""
        await self.config.guild(ctx.guild).use_screenshot.set(on); await ctx.tick()

    @agshousingscraper.command()
    async def clear(self, ctx):
        """Clear seen-posts history."""
        await self.config.guild(ctx.guild).seen_posts.set([]); await ctx.send("✅ Cleared history.")

    @agshousingscraper.command()
    async def force(self, ctx):
        """Force an immediate check: always embed & screenshot."""
        try:
            await ctx.channel.trigger_typing()
        except:
            pass
        stats = {
            "time": datetime.datetime.utcnow(),
            "url": await self.config.guild(ctx.guild).url(),
            "selector": await self.config.guild(ctx.guild).selector(),
            "mode": "hash" if await self.config.guild(ctx.guild).use_section_hash() else "posts",
            "success": False,
            "error": None,
        }
        try:
            await self.check_site(ctx.guild, stats)
            stats["success"] = True
        except Exception as e:
            stats["error"] = repr(e)
        # build embed
        desc = []
        if stats["mode"] == "hash":
            desc.append(f"Section changed? {stats.get('section_changed', False)}")
        else:
            desc.append(f"New posts? {len(stats.get('new_posts', []))}")
        if stats["error"]:
            desc.append(f"Error: {stats['error']}")
        emb = discord.Embed(
            title="🏠 Housing Monitor Force Check",
            url=stats["url"],
            description="\n".join(desc),
            color=discord.Color.green() if stats["success"] else discord.Color.red(),
            timestamp=stats["time"]
        )
        file = None
        try:
            ss = await self.capture_screenshot(stats["url"], stats["selector"])
            file = discord.File(ss, filename="snapshot.png")
            emb.set_image(url="attachment://snapshot.png")
        except Exception as e:
            emb.add_field(name="⚠️ Screenshot failed", value=str(e), inline=False)
        await ctx.send(embed=emb, file=file)
        await ctx.tick()
        if file and os.path.exists(ss):
            os.remove(ss)

    # ─────────── Test Commands ───────────

    @agshousingscraper.group(name="test")
    @checks.admin_or_permissions(manage_guild=True)
    async def test(self, ctx):
        """Test各種機能 without waiting."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @test.command()
    async def check(self, ctx):
        """Run one iteration of check_site (alerts + log)."""
        stats = {
            "time": datetime.datetime.utcnow(),
            "url": await self.config.guild(ctx.guild).url(),
            "selector": await self.config.guild(ctx.guild).selector(),
            "mode": "hash" if await self.config.guild(ctx.guild).use_section_hash() else "posts",
            "success": False,
            "error": None,
            "new_posts": [],
            "section_changed": False,
            "placeholder_skipped": False,
            "candidates": 0,
            "seen_count": len(await self.config.guild(ctx.guild).seen_posts()),
        }
        try:
            await self.check_site(ctx.guild, stats)
            stats["success"] = True
        except Exception as e:
            stats["error"] = repr(e)
        stats["seen_count"] = len(await self.config.guild(ctx.guild).seen_posts())
        await self._log_to_channel(ctx.guild, stats)
        await ctx.tick()

    @test.command()
    async def log(self, ctx):
        """Send a dummy log embed right now."""
        stats = {
            "time": datetime.datetime.utcnow(),
            "url": await self.config.guild(ctx.guild).url(),
            "selector": await self.config.guild(ctx.guild).selector(),
            "mode": "hash" if await self.config.guild(ctx.guild).use_section_hash() else "posts",
            "success": True,
            "error": None,
            "new_posts": [],
            "section_changed": False,
            "placeholder_skipped": False,
            "candidates": 0,
            "seen_count": len(await self.config.guild(ctx.guild).seen_posts()),
        }
        await self._log_to_channel(ctx.guild, stats)
        await ctx.tick()

    @test.command()
    async def weekly(self, ctx):
        """Send a test weekly snapshot to your log channel."""
        cfg = await self.config.guild(ctx.guild).all()
        lc = cfg["log_channel_id"]
        if not lc:
            return await ctx.send("🚨 No log channel set.")
        ch = ctx.guild.get_channel(lc)
        if not ch:
            return await ctx.send("🚨 Log channel invalid.")
        ss = await self.capture_screenshot(cfg["url"], cfg["selector"])
        emb = discord.Embed(
            title="🗓 Test Weekly Snapshot",
            description="Simulated automatic weekly snapshot.",
            color=discord.Color.gold(),
            timestamp=datetime.datetime.utcnow(),
        )
        file = discord.File(ss, filename="snapshot.png")
        emb.set_image(url="attachment://snapshot.png")
        await ch.send(embed=emb, file=file)
        await ctx.tick()
        if os.path.exists(ss):
            os.remove(ss)

    @test.command()
    async def screenshot(self, ctx):
        """Test screenshot capture and send it here."""
        cfg = await self.config.guild(ctx.guild).all()
        ss = await self.capture_screenshot(cfg["url"], cfg["selector"])
        file = discord.File(ss, filename="snapshot.png")
        await ctx.send(file=file)
        await ctx.tick()
        if os.path.exists(ss):
            os.remove(ss)

    @test.command(name="alert")
    async def test_alert(self, ctx):
        """Send a dummy alert embed (and screenshot) to your alert channel."""
        cfg = await self.config.guild(ctx.guild).all()
        emb = discord.Embed(
            title="🏠 Test Alert",
            description="This is a *test* alert embed.",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow(),
        )
        ss = None
        if cfg["use_screenshot"]:
            ss = await self.capture_screenshot(cfg["url"], cfg["selector"])
        await self.dispatch_alert(ctx.guild, None, emb, ss)
        await ctx.tick()
        if ss and os.path.exists(ss):
            os.remove(ss)