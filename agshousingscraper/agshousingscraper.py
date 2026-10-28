import asyncio
import os
import tempfile
import re
import hashlib
import datetime
import logging

import aiohttp
import discord
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from redbot.core import commands, Config, checks

# Defaults
DEFAULT_SECTION_SELECTOR = "section:has(h2:contains('Available Houses'))"
DEFAULT_FULL_SELECTOR    = "html"
DEFAULT_KEYWORDS         = [
    "titchfield", "warsash", "park gate", "fareham",
    "house", "bedrooms", "almshouse"
]
DEFAULT_MODES = ["posts", "keywords", "section_hash", "full_hash"]
DEFAULT_POLL_INTERVAL_SECONDS = 300
MIN_POLL_INTERVAL_SECONDS = 60
MAX_POLL_INTERVAL_SECONDS = 24 * 60 * 60
FETCH_TIMEOUT_SECONDS = 30
SCREENSHOT_PAGE_LOAD_TIMEOUT_SECONDS = 30
SCREENSHOT_SCRIPT_TIMEOUT_SECONDS = 15

log = logging.getLogger("red.agshousingscraper")


def delete_screenshot(path):
    """Remove a generated screenshot without masking an earlier send error."""
    if path is None:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError:
        log.exception("Failed to remove Housing Scraper screenshot %s", path)


def safe_poll_interval(seconds):
    """Bound saved poll settings, including legacy or malformed values.

    Only integer durations are accepted; bad types revert to the default.
    Out-of-range integers are clamped to the configured safe limits.
    """
    if type(seconds) is not int:
        return DEFAULT_POLL_INTERVAL_SECONDS
    return max(MIN_POLL_INTERVAL_SECONDS, min(seconds, MAX_POLL_INTERVAL_SECONDS))


class AGSHousingScraper(commands.Cog):
    """Advanced monitor for social-housing posts with redundancies, keywords, logging, weekly snapshots."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, 1234567890, force_registration=True)
        # register per-guild defaults
        self.config.register_guild(
            url="https://www.eost.org.uk/housing",
            poll_interval=DEFAULT_POLL_INTERVAL_SECONDS,
            section_selector=DEFAULT_SECTION_SELECTOR,
            full_selector=DEFAULT_FULL_SELECTOR,
            detection_modes=DEFAULT_MODES.copy(),
            keywords=DEFAULT_KEYWORDS.copy(),
            channel_id=None,
            role_id=None,
            dm_user_id=None,
            log_channel_id=None,
            seen_posts=[],
            seen_keywords=[],
            last_section_hash=None,
            last_full_hash=None,
            use_screenshot=False,
        )
        self.monitor_tasks = {}  # guild.id -> asyncio.Task
        # Chromium is memory-heavy: permit one screenshot browser at a time.
        self._screenshot_lock = asyncio.Lock()
        # auto-start on restart
        self._auto_start = bot.loop.create_task(self._auto_start_loop())
        # weekly snapshot task
        self._weekly_snap = bot.loop.create_task(self._weekly_snapshot_loop())

    def cog_unload(self):
        self._auto_start.cancel()
        self._weekly_snap.cancel()
        for t in self.monitor_tasks.values():
            t.cancel()

    # ───────────── Persistence & Scheduling ─────────────

    async def _auto_start_loop(self):
        """Auto-start monitors for any guilds that have an alert or log channel set."""
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            cfg = await self.config.guild(guild).all()
            if cfg["channel_id"] or cfg["log_channel_id"]:
                if guild.id not in self.monitor_tasks:
                    self.monitor_tasks[guild.id] = self.bot.loop.create_task(self._monitor_loop(guild))

    async def _weekly_snapshot_loop(self):
        """Every Monday at 07:00 UTC send a screenshot to each guild's log channel."""
        await self.bot.wait_until_ready()
        while True:
            now = datetime.datetime.utcnow()
            # calculate next Monday 07:00
            days = (0 - now.weekday() + 7) % 7
            run_at = (now + datetime.timedelta(days=days)) \
                        .replace(hour=7, minute=0, second=0, microsecond=0)
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
                ss_path = None
                try:
                    ss_path = await self.capture_screenshot(cfg["url"], cfg["section_selector"])
                    emb = discord.Embed(
                        title="🗓 Weekly Snapshot",
                        description=f"Automated weekly snapshot ({run_at.strftime('%Y-%m-%d %H:%M UTC')})",
                        color=discord.Color.gold(),
                        timestamp=run_at
                    )
                    emb.set_image(url="attachment://snapshot.png")
                    await self._send_with_screenshot(ch, embed=emb, screenshot_path=ss_path)
                except Exception as e:
                    await ch.send(f"❌ Weekly snapshot failed:\n```{e}```")
                finally:
                    delete_screenshot(ss_path)

    # ───────────── Monitor Loop ─────────────

    async def _monitor_loop(self, guild):
        """Background loop: run a check, log it, sleep, repeat."""
        await self.bot.wait_until_ready()
        settings = self.config.guild(guild)
        while True:
            stats = {
                "time": datetime.datetime.utcnow(),
                "url": await settings.url(),
                "section_selector": await settings.section_selector(),
                "full_selector": await settings.full_selector(),
                "modes": await settings.detection_modes(),
                "keywords": await settings.keywords(),
                "success": False,
                "error": None,
                "new_posts": [],
                "new_keywords": [],
                "section_changed": False,
                "full_changed": False,
                "seen_posts": len(await settings.seen_posts()),
                "seen_keywords": len(await settings.seen_keywords()),
            }
            try:
                await self.check_site(guild, stats)
                stats["success"] = True
            except Exception as e:
                stats["error"] = repr(e)
            # refresh seen counts
            stats["seen_posts"]    = len(await settings.seen_posts())
            stats["seen_keywords"] = len(await settings.seen_keywords())
            await self._log_to_channel(guild, stats)

            # Validate at the point of use too: older saved configs may still
            # contain 0, negative or malformed intervals from before this fix.
            stored_interval = await settings.poll_interval()
            delay = safe_poll_interval(stored_interval)
            if type(stored_interval) is not int or stored_interval != delay:
                log.warning(
                    "Correcting unsafe Housing Scraper poll interval for guild %s: %r -> %ss",
                    guild.id, stored_interval, delay,
                )
                await settings.poll_interval.set(delay)
            await asyncio.sleep(delay)

    # ───────────── Detection Pipeline ─────────────

    async def check_site(self, guild, stats):
        """Fetch & parse; for each enabled mode, detect and alert if triggered."""
        cfg = await self.config.guild(guild).all()
        url         = stats["url"]
        section_sel = stats["section_selector"]
        full_sel    = stats["full_selector"]
        modes       = stats["modes"]
        keywords    = stats["keywords"]
        use_ss      = cfg["use_screenshot"]

        # 1) Fetch HTML. The timeout covers response headers AND body reading.
        # Closing both context managers releases the connection even on errors.
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
                response.raise_for_status()
                html = await response.text()
        soup = BeautifulSoup(html, "html.parser")

        # 2) POSTS DETECTION
        if "posts" in modes:
            sec = soup.select_one(section_sel)
            if sec and not sec.select_one("[data-hook='empty-state-container']"):
                cands = set()
                for a in sec.find_all("a", href=True):
                    href = a["href"]; txt = a.get_text("", True).lower()
                    if "blog" in href.lower() or "read more" in txt:
                        cands.add(href)
                if not cands:
                    for art in sec.find_all(["article","div"], class_=re.compile(r"blog-post", re.I)):
                        a2 = art.find("a", href=True)
                        if a2:
                            cands.add(a2["href"])
                seen = set(cfg["seen_posts"])
                new_posts = list(cands - seen)
                if new_posts:
                    stats["new_posts"] = new_posts
                    await self.config.guild(guild).seen_posts.set(list(seen | set(new_posts)))
                    emb = discord.Embed(
                        title="🔔 New Housing Posts",
                        description="\n".join(f"• {u}" for u in new_posts),
                        color=discord.Color.blue(),
                        timestamp=discord.utils.utcnow(),
                    )
                    ss = await self.capture_screenshot(url, section_sel) if use_ss else None
                    await self.dispatch_alert(guild, None, emb, ss)

        # 3) KEYWORDS DETECTION
        if "keywords" in modes:
            text = soup.get_text(" ", strip=True).lower()
            seen_k = set(cfg["seen_keywords"])
            found = [kw for kw in keywords if kw.lower() in text]
            new_k = list(set(found) - seen_k)
            if new_k:
                stats["new_keywords"] = new_k
                await self.config.guild(guild).seen_keywords.set(list(seen_k | set(new_k)))
                emb = discord.Embed(
                    title="🔍 New Keywords Detected",
                    description="\n".join(f"• {k}" for k in new_k),
                    color=discord.Color.orange(),
                    timestamp=discord.utils.utcnow(),
                )
                ss = await self.capture_screenshot(url, section_sel) if use_ss else None
                await self.dispatch_alert(guild, None, emb, ss)

        # 4) SECTION-HASH DETECTION
        if "section_hash" in modes:
            sec = soup.select_one(section_sel)
            if sec:
                new_h = hashlib.sha256(str(sec).encode("utf-8")).hexdigest()
                if new_h != cfg["last_section_hash"]:
                    stats["section_changed"] = True
                    await self.config.guild(guild).last_section_hash.set(new_h)
                    emb = discord.Embed(
                        title="🔄 Section Changed",
                        description="Monitored section HTML has changed.",
                        color=discord.Color.green(),
                        timestamp=discord.utils.utcnow(),
                    )
                    ss = await self.capture_screenshot(url, section_sel) if use_ss else None
                    await self.dispatch_alert(guild, None, emb, ss)

        # 5) FULL-HASH DETECTION
        if "full_hash" in modes:
            full = soup.select_one(full_sel) or soup
            new_f = hashlib.sha256(str(full).encode("utf-8")).hexdigest()
            if new_f != cfg["last_full_hash"]:
                stats["full_changed"] = True
                await self.config.guild(guild).last_full_hash.set(new_f)
                emb = discord.Embed(
                    title="🔄 Full Page Changed",
                    description="Entire page HTML has changed.",
                    color=discord.Color.dark_green(),
                    timestamp=discord.utils.utcnow(),
                )
                # full screenshot of page
                ss = await self.capture_screenshot(url) if use_ss else None
                await self.dispatch_alert(guild, None, emb, ss)

    # ───────────── Logging ─────────────

    async def _log_to_channel(self, guild, stats):
        """Send a no-ping log embed on every single check."""
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
        e.add_field(name="URL",            value=stats["url"], inline=False)
        e.add_field(name="Section sel",    value=stats["section_selector"], inline=True)
        e.add_field(name="Full sel",       value=stats["full_selector"], inline=True)
        e.add_field(name="Modes",          value=", ".join(stats["modes"]), inline=False)
        e.add_field(name="Keywords tracked", value=", ".join(stats["keywords"]), inline=False)
        e.add_field(name="Success",        value=str(stats["success"]), inline=True)
        if stats["error"]:
            e.add_field(name="Error",       value=stats["error"], inline=False)
        e.add_field(name="New posts",      value=str(len(stats["new_posts"])), inline=True)
        e.add_field(name="New keywords",   value=str(len(stats["new_keywords"])), inline=True)
        e.add_field(name="Section changed", value=str(stats["section_changed"]), inline=True)
        e.add_field(name="Full changed",   value=str(stats["full_changed"]), inline=True)
        e.add_field(name="Seen posts",     value=str(stats["seen_posts"]), inline=True)
        e.add_field(name="Seen keywords",  value=str(stats["seen_keywords"]), inline=True)
        await ch.send(embed=e)

    # ───────────── Utilities ─────────────

    def _capture_screenshot_sync(self, url, css_selector=None):
        """Run all synchronous Selenium operations in a worker thread.

        The caller owns a successful result path and must delete it after
        sending. Partial files are deleted here on any capture failure.
        """
        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--window-size=1280,2000")

        driver = None
        screenshot_path = None
        try:
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(SCREENSHOT_PAGE_LOAD_TIMEOUT_SECONDS)
            driver.set_script_timeout(SCREENSHOT_SCRIPT_TIMEOUT_SECONDS)
            driver.get(url)

            if css_selector:
                try:
                    element = driver.find_element("css selector", css_selector)
                    driver.execute_script("arguments[0].scrollIntoView();", element)
                except Exception as exc:
                    # The page screenshot is still useful without the selector.
                    log.debug("Could not scroll screenshot selector %r: %s", css_selector, exc)

            # Close the descriptor before Chrome writes to the same path,
            # including on Windows. Do not keep empty/partial images on error.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                screenshot_path = tmp.name
            if not driver.save_screenshot(screenshot_path):
                raise RuntimeError("Chrome failed to save the screenshot")
            return screenshot_path
        except BaseException:
            if screenshot_path is not None:
                try:
                    os.remove(screenshot_path)
                except FileNotFoundError:
                    pass
                except OSError:
                    log.exception("Failed to delete incomplete screenshot %s", screenshot_path)
            raise
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    log.exception("Could not close Selenium Chrome driver")

    async def capture_screenshot(self, url, css_selector=None):
        """Capture without blocking Red's loop or starting parallel Chromes.

        Cancelling this coroutine cannot stop Selenium's running OS thread.
        Keep the screenshot lock until it finishes and discard any output
        produced after cancellation, so aborted jobs do not leak temp files.
        """
        async with self._screenshot_lock:
            worker = asyncio.create_task(
                asyncio.to_thread(self._capture_screenshot_sync, url, css_selector)
            )
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                def discard_cancelled_output(finished):
                    try:
                        path = finished.result()
                    except (Exception, asyncio.CancelledError):
                        return
                    if path:
                        try:
                            os.remove(path)
                        except FileNotFoundError:
                            pass
                        except OSError:
                            log.exception("Failed to discard cancelled screenshot %s", path)

                # A done callback also handles repeated task cancellations.
                worker.add_done_callback(discard_cancelled_output)
                try:
                    await asyncio.shield(worker)
                except Exception:
                    # The synchronous worker logs and cleans its own failures.
                    pass
                raise

    async def _send_with_screenshot(self, destination, *, embed, screenshot_path=None, content=None):
        """Open a fresh Discord upload for exactly one send, then close it."""
        attachment = None
        try:
            if screenshot_path:
                attachment = discord.File(screenshot_path, filename="snapshot.png")
            await destination.send(content=content, embed=embed, file=attachment)
        finally:
            if attachment is not None:
                attachment.close()

    async def dispatch_alert(self, guild, content, embed, screenshot_path=None):
        """Send independent channel and DM uploads; always clean the PNG."""
        try:
            cfg = await self.config.guild(guild).all()
            ch = guild.get_channel(cfg["channel_id"]) if cfg["channel_id"] else None
            usr = self.bot.get_user(cfg["dm_user_id"]) if cfg["dm_user_id"] else None

            if cfg["role_id"] and ch:
                content = f"<@&{cfg['role_id']}>"
            if screenshot_path:
                embed.set_image(url="attachment://snapshot.png")

            channel_error = None
            if ch:
                try:
                    await self._send_with_screenshot(
                        ch, content=content, embed=embed, screenshot_path=screenshot_path
                    )
                except Exception as exc:
                    log.exception("Housing Scraper channel alert failed for guild %s", guild.id)
                    channel_error = exc

            if usr:
                try:
                    # Discord consumes/closes upload handles; never reuse the
                    # channel's discord.File for a separate destination.
                    await self._send_with_screenshot(
                        usr, embed=embed, screenshot_path=screenshot_path
                    )
                except Exception:
                    log.exception("Housing Scraper DM alert failed for guild %s", guild.id)

            if channel_error is not None:
                # Preserve existing failed-check semantics for channel failures.
                raise channel_error
        finally:
            delete_screenshot(screenshot_path)

    # ───────────── Commands ─────────────

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
        """Show current settings & running state."""
        s = await self.config.guild(ctx.guild).all()
        e = discord.Embed(title="AGS Housing Scraper Status", color=discord.Color.blurple())
        e.add_field(name="URL",            value=s["url"], inline=False)
        e.add_field(name="Interval",       value=f"{s['poll_interval']}s", inline=True)
        e.add_field(name="Section sel",    value=s["section_selector"], inline=True)
        e.add_field(name="Full sel",       value=s["full_selector"], inline=True)
        e.add_field(name="Modes",          value=", ".join(s["detection_modes"]), inline=False)
        e.add_field(name="Keywords",       value=", ".join(s["keywords"]), inline=False)
        e.add_field(name="Alert channel",  value=f"<#{s['channel_id']}>" if s["channel_id"] else "None", inline=True)
        e.add_field(name="Log channel",    value=f"<#{s['log_channel_id']}>" if s["log_channel_id"] else "None", inline=True)
        e.add_field(name="Ping role",      value=f"<@&{s['role_id']}>" if s["role_id"] else "None", inline=True)
        e.add_field(name="DM user",        value=f"<@{s['dm_user_id']}>" if s["dm_user_id"] else "None", inline=True)
        e.add_field(name="Screenshots",    value=str(s["use_screenshot"]), inline=True)
        e.add_field(name="Seen posts",     value=str(len(s["seen_posts"])), inline=True)
        e.add_field(name="Seen keywords",  value=str(len(s["seen_keywords"])), inline=True)
        e.add_field(name="Monitoring",     value=str(ctx.guild.id in self.monitor_tasks), inline=True)
        await ctx.send(embed=e)

    @agshousingscraper.command()
    async def seturl(self, ctx, url: str):
        """Set the monitored URL."""
        await self.config.guild(ctx.guild).url.set(url)
        await ctx.tick()

    @agshousingscraper.command()
    async def interval(self, ctx, seconds: int):
        """Set polling interval from 60 seconds to 24 hours."""
        if not MIN_POLL_INTERVAL_SECONDS <= seconds <= MAX_POLL_INTERVAL_SECONDS:
            return await ctx.send(
                f"⚠️ Polling interval must be between {MIN_POLL_INTERVAL_SECONDS} "
                f"and {MAX_POLL_INTERVAL_SECONDS} seconds (24 hours). "
                "Your existing interval was not changed."
            )

        await self.config.guild(ctx.guild).poll_interval.set(seconds)
        t = self.monitor_tasks.pop(ctx.guild.id, None)
        if t:
            t.cancel()
            self.monitor_tasks[ctx.guild.id] = self.bot.loop.create_task(self._monitor_loop(ctx.guild))
        await ctx.tick()

    @agshousingscraper.command()
    async def selector(self, ctx, *, css: str):
        """Set the CSS selector for the monitored section."""
        await self.config.guild(ctx.guild).section_selector.set(css)
        await ctx.tick()

    @agshousingscraper.command()
    async def fullselector(self, ctx, *, css: str):
        """Set the CSS selector for full-page hashing."""
        await self.config.guild(ctx.guild).full_selector.set(css)
        await ctx.tick()

    @agshousingscraper.group(name="modes")
    async def modes(self, ctx):
        """Manage detection modes (posts, keywords, section_hash, full_hash)."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @modes.command(name="list")
    async def modes_list(self, ctx):
        """List current detection modes."""
        m = await self.config.guild(ctx.guild).detection_modes()
        await ctx.send("Detection modes: " + ", ".join(m))

    @modes.command(name="set")
    async def modes_set(self, ctx, *, modes: str):
        """Set detection modes, comma-separated."""
        parts = [p.strip() for p in modes.split(",")]
        valid = [m for m in parts if m in DEFAULT_MODES]
        if not valid:
            return await ctx.send("No valid modes in: " + ", ".join(parts))
        await self.config.guild(ctx.guild).detection_modes.set(valid)
        await ctx.tick()

    @agshousingscraper.group(name="keywords")
    async def keywords(self, ctx):
        """Manage keyword watchlist."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @keywords.command(name="list")
    async def kw_list(self, ctx):
        """List tracked keywords."""
        k = await self.config.guild(ctx.guild).keywords()
        await ctx.send("Keywords: " + ", ".join(k))

    @keywords.command(name="add")
    async def kw_add(self, ctx, *, word: str):
        """Add a keyword."""
        w = word.lower().strip()
        k = await self.config.guild(ctx.guild).keywords()
        if w in k:
            return await ctx.send("Already tracking “%s”." % w)
        k.append(w)
        await self.config.guild(ctx.guild).keywords.set(k)
        await ctx.tick()

    @keywords.command(name="remove")
    async def kw_remove(self, ctx, *, word: str):
        """Remove a keyword."""
        w = word.lower().strip()
        k = await self.config.guild(ctx.guild).keywords()
        if w not in k:
            return await ctx.send("“%s” not found in keyword list." % w)
        k.remove(w)
        await self.config.guild(ctx.guild).keywords.set(k)
        await ctx.tick()

    @agshousingscraper.command()
    async def channel(self, ctx, channel: discord.TextChannel):
        """Set the alert channel."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def logchannel(self, ctx, channel: discord.TextChannel):
        """Set the log channel (no pings)."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def role(self, ctx, role: discord.Role):
        """Set role to ping on alerts."""
        await self.config.guild(ctx.guild).role_id.set(role.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def dm(self, ctx, user: discord.User):
        """Set user to DM on alerts."""
        await self.config.guild(ctx.guild).dm_user_id.set(user.id)
        await ctx.tick()

    @agshousingscraper.command()
    async def screenshot(self, ctx, on: bool):
        """Toggle whether to attach screenshots on alerts."""
        await self.config.guild(ctx.guild).use_screenshot.set(on)
        await ctx.tick()

    @agshousingscraper.command()
    async def clear(self, ctx):
        """Clear seen-posts and seen-keywords history."""
        await self.config.guild(ctx.guild).seen_posts.set([])
        await self.config.guild(ctx.guild).seen_keywords.set([])
        await ctx.send("✅ Cleared history.")

    @agshousingscraper.command()
    async def force(self, ctx):
        """Force an immediate check: always send a summary + screenshot."""
        try:
            await ctx.channel.trigger_typing()
        except:
            pass
        stats = {
            "time": datetime.datetime.utcnow(),
            "url": await self.config.guild(ctx.guild).url(),
            "section_selector": await self.config.guild(ctx.guild).section_selector(),
            "full_selector": await self.config.guild(ctx.guild).full_selector(),
            "modes": await self.config.guild(ctx.guild).detection_modes(),
            "keywords": await self.config.guild(ctx.guild).keywords(),
            "success": False,
            "error": None,
            "new_posts": [],
            "new_keywords": [],
            "section_changed": False,
            "full_changed": False,
        }
        try:
            await self.check_site(ctx.guild, stats)
            stats["success"] = True
        except Exception as e:
            stats["error"] = repr(e)
        # build summary embed
        desc = []
        if "posts" in stats["modes"]:
            desc.append(f"🔔 New posts: {len(stats['new_posts'])}")
        if "keywords" in stats["modes"]:
            desc.append(f"🔍 New keywords: {len(stats['new_keywords'])}")
        if "section_hash" in stats["modes"]:
            desc.append(f"🔄 Section changed: {stats['section_changed']}")
        if "full_hash" in stats["modes"]:
            desc.append(f"🌐 Full changed: {stats['full_changed']}")
        if stats["error"]:
            desc.append(f"❌ Error: {stats['error']}")
        if not desc:
            desc.append("No detections configured.")
        emb = discord.Embed(
            title="🏠 Housing Monitor Force Check",
            url=stats["url"],
            description="\n".join(desc),
            color=discord.Color.green() if stats["success"] else discord.Color.red(),
            timestamp=stats["time"]
        )
        ss = None
        try:
            if await self.config.guild(ctx.guild).use_screenshot():
                ss = await self.capture_screenshot(stats["url"], stats["section_selector"])
                emb.set_image(url="attachment://snapshot.png")
            await self._send_with_screenshot(ctx, embed=emb, screenshot_path=ss)
            await ctx.tick()
        finally:
            delete_screenshot(ss)