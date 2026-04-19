# agsonthisday.py

from __future__ import annotations
import asyncio
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
from lxml import html
import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box
from redbot.core.utils import get_end_user_data_statement

__red_end_user_data_statement__ = get_end_user_data_statement(__file__)

TODAY_URL = "https://www.onthisday.com/today"

SECTION_ICONS: dict[str, str] = {
    "Today in History":      "https://www.onthisday.com/images/calendar.svg",
    "Did You Know?":         "https://www.onthisday.com/images/did-you-know.svg",
    "Fun Fact About Today":  "https://www.onthisday.com/images/comedian.svg",
    "Featured Article":      "https://www.onthisday.com/images/article.svg",
}


def userday_to_pyweekday(userday: int) -> int:
    # 1=Monday .. 7=Sunday → 0..6
    return (userday - 1) % 7


def compute_next_occurrence(
    user_day: int,
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    """
    Next occurrence strictly after 'now' of given weekday+time in tzinfo.
    """
    if now is None:
        now = datetime.now(tzinfo)
    else:
        now = now.astimezone(tzinfo)
    target_wd = userday_to_pyweekday(user_day)
    days_ahead = (target_wd - now.weekday()) % 7
    candidate = datetime(
        now.year, now.month, now.day, hour, minute, tzinfo=tzinfo
    ) + timedelta(days=days_ahead)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


def compute_last_occurrence(
    user_day: int,
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    """
    Most recent occurrence at or before 'now' of given weekday+time in tzinfo.
    """
    if now is None:
        now = datetime.now(tzinfo)
    else:
        now = now.astimezone(tzinfo)
    target_wd = userday_to_pyweekday(user_day)
    days_ago = (now.weekday() - target_wd) % 7
    candidate = datetime(
        now.year, now.month, now.day, hour, minute, tzinfo=tzinfo
    ) - timedelta(days=days_ago)
    if candidate > now:
        candidate -= timedelta(days=7)
    return candidate


async def mod_check(ctx: commands.Context) -> bool:
    return ctx.author.guild_permissions.manage_guild


class ButtonView(discord.ui.View):
    """A simple View of URL buttons for Wikipedia links."""

    def __init__(self, buttons: dict[str, str]):
        super().__init__(timeout=None)
        for label, url in buttons.items():
            # truncate label if too long
            safe_label = label if len(label) < 80 else label[:77] + "..."
            self.add_item(discord.ui.Button(label=safe_label, url=url))


class AGSOnThisDay(commands.Cog):
    """Automatic daily 'On This Day' posts from onthisday.com."""

    __author__ = "AEGIS Team"
    __version__ = "1.0.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.session: aiohttp.ClientSession | None = None
        self.config = Config.get_conf(
            self, identifier=0xA5C3B4D6F1, force_registration=True
        )
        self.config.register_guild(
            enabled=False,
            channel_id=None,
            timezone="UTC",
            post_day=0,       # unused, we post daily
            post_hour=8,
            post_minute=0,
            last_posted_unix=0,
        )
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def format_help_for_context(self, ctx: commands.Context) -> str:
        help_text = super().format_help_for_context(ctx)
        return (
            f"{help_text}\n\n"
            f"Author: {self.__author__}\n"
            f"Version: {self.__version__}"
        )

    async def red_delete_data_for_user(self, **kwargs):
        """This cog does not store per-user data."""
        return

    async def cog_load(self) -> None:
        self.session = aiohttp.ClientSession()
        self._task = self.bot.loop.create_task(self._background_loop())

    async def cog_unload(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
        if self.session:
            await self.session.close()
            self.session = None

    async def _background_loop(self) -> None:
        await self.bot.wait_until_ready()
        while True:
            try:
                now_utc = datetime.now(timezone.utc)
                for guild in list(self.bot.guilds):
                    try:
                        await self._handle_guild(guild, now_utc)
                    except Exception:
                        self.bot.logger.exception("AGSOnThisDay error for guild %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("AGSOnThisDay background loop crashed")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild, now_utc: datetime) -> None:
        cfg = await self.config.guild(guild).all()
        if not cfg.get("enabled"):
            return
        chan_id = cfg.get("channel_id")
        if not chan_id:
            return
        channel = guild.get_channel(chan_id)
        if not isinstance(channel, discord.TextChannel):
            return

        # timezone
        tz_str = cfg.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        ph = int(cfg.get("post_hour", 8))
        pm = int(cfg.get("post_minute", 0))

        # compute last and next post times
        last_local = compute_last_occurrence(1, ph, pm, now=now_utc, tzinfo=tz)
        last_utc = last_local.astimezone(timezone.utc)
        next_local = compute_next_occurrence(1, ph, pm, now=now_utc, tzinfo=tz)
        next_utc = next_local.astimezone(timezone.utc)

        last_posted = int(cfg.get("last_posted_unix", 0))
        # reset guard if clock moved backwards
        if last_posted > int(next_utc.timestamp()):
            await self.config.guild(guild).last_posted_unix.set(0)
            last_posted = 0

        # if we're within 5 minutes of last_local and haven't posted it yet
        now_ts = now_utc.timestamp()
        window_start = last_utc.timestamp()
        if window_start <= now_ts <= window_start + 300:
            post_ts = int(last_utc.timestamp())
            if last_posted < post_ts:
                async with self._lock:
                    # re-fetch inside lock
                    lp = int(await self.config.guild(guild).last_posted_unix())
                    if lp < post_ts:
                        await self._post_today(channel)
                        await self.config.guild(guild).last_posted_unix.set(post_ts)

    async def _fetch_page(self) -> html.HtmlElement | None:
        if not self.session:
            return None
        try:
            async with self.session.get(TODAY_URL, timeout=20) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
        except Exception:
            return None
        try:
            return html.fromstring(text)
        except Exception:
            return None

    def _parse_section(
        self, tree: html.HtmlElement, title: str
    ) -> tuple[str, str | None, dict[str, str]] | None:
        # find the <section> by its <h2> text
        hdr = tree.xpath(f"//h2[contains(normalize-space(), '{title}')]")
        if not hdr:
            return None
        sec = hdr[0]
        # climb to section
        while sec is not None and sec.tag.lower() != "section":
            sec = sec.getparent()
        if sec is None:
            return None

        # description: join all <p>
        paras = sec.xpath(".//p")
        desc_texts = [p.text_content().strip() for p in paras if p.text_content().strip()]
        description = "\n\n".join(desc_texts)[:4096] or None

        # collect wikipedia links
        wiki: dict[str, str] = {}
        for a in sec.xpath(".//a[contains(@href,'wikipedia.org')]"):
            href = a.get("href")
            label = a.text_content().strip() or href
            wiki[label] = href

        # find main image: last non-SVG <img>
        img_srcs = sec.xpath(".//img[not(contains(@src,'.svg'))]/@data-src | .//img[not(contains(@src,'.svg'))]/@src")
        image_url = img_srcs[-1] if img_srcs else None

        return description or "", image_url, wiki

    def _parse_today_in_history(
        self, tree: html.HtmlElement
    ) -> tuple[str, str | None, dict[str, str]] | None:
        # Title is "Today in History"
        hdr = tree.xpath("//h2[contains(normalize-space(), 'Today in History')]")
        if not hdr:
            return None
        sec = hdr[0]
        while sec is not None and sec.tag.lower() != "section":
            sec = sec.getparent()
        if sec is None:
            return None

        # pick top 5 <li>
        items = sec.xpath(".//ul/li")[:5]
        lines: list[str] = []
        wiki: dict[str, str] = {}
        for li in items:
            text = li.xpath("string()").strip()
            lines.append(f"• {text}")
            for a in li.xpath(".//a[contains(@href,'wikipedia.org')]"):
                href = a.get("href")
                label = a.text_content().strip() or href
                wiki[label] = href

        description = "\n".join(lines) or ""
        # image
        img_srcs = sec.xpath(".//img[not(contains(@src,'.svg'))]/@data-src | .//img[not(contains(@src,'.svg'))]/@src")
        image_url = img_srcs[-1] if img_srcs else None

        return description, image_url, wiki

    async def _post_today(self, channel: discord.TextChannel) -> None:
        tree = await self._fetch_page()
        if tree is None:
            await channel.send("❌ Failed to retrieve On This Day content.")
            return

        sections = [
            ("Today in History",      self._parse_today_in_history),
            ("Did You Know?",         self._parse_section),
            ("Fun Fact About Today",  self._parse_section),
            ("Featured Article",      self._parse_section),
        ]

        for title, parser in sections:
            data = parser(tree, title)
            if not data:
                continue
            description, image_url, wiki = data
            icon = SECTION_ICONS.get(title)
            embed = discord.Embed(
                title=title.upper(),
                description=description,
                color=discord.Color.blurple(),
            )
            if icon:
                embed.set_thumbnail(url=icon)
            if image_url:
                embed.set_image(url=image_url)
            embed.set_footer(text="from onthisday.com")
            view = ButtonView(wiki) if wiki else None
            try:
                await channel.send(embed=embed, view=view)
            except Exception:
                # fallback to embed only
                await channel.send(embed=embed)

    @commands.group(name="agsonthisday", invoke_without_command=True)
    @commands.guild_only()
    async def agsonthisday(self, ctx: commands.Context) -> None:
        """Configure or view your AGS On This Day daily posts."""
        await ctx.send_help(ctx.command)

    @agsonthisday.command()
    @commands.check(mod_check)
    async def enable(self, ctx: commands.Context) -> None:
        """Enable daily On This Day posts."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send("✅ AGS OnThisDay enabled.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def disable(self, ctx: commands.Context) -> None:
        """Disable daily On This Day posts."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ AGS OnThisDay disabled.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """Set the channel for the daily post."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.send(f"✅ Channel set to {channel.mention}.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def settime(self, ctx: commands.Context, time_str: str) -> None:
        """
        Set the local time to post each day. Format HH:MM (24h).
        """
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Time must be in HH:MM format.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).post_hour.set(hr)
        await self.config.guild(ctx.guild).post_minute.set(mn)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Post time set to {hr:02d}:{mn:02d} (in guild timezone).")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def settimezone(self, ctx: commands.Context, timezone_name: str) -> None:
        """
        Set the IANA timezone for this guild’s posts.
        Example: Europe/London, America/New_York, UTC, etc.
        """
        try:
            ZoneInfo(timezone_name)
        except Exception:
            return await ctx.send("❌ Invalid timezone. Please supply a valid IANA name.")
        await self.config.guild(ctx.guild).timezone.set(timezone_name)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Timezone set to `{timezone_name}`.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def status(self, ctx: commands.Context) -> None:
        """Show current configuration and next post time."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data.get("enabled", False)
        ch_id = data.get("channel_id")
        tz_str = data.get("timezone", "UTC")
        ph = int(data.get("post_hour", 8))
        pm = int(data.get("post_minute", 0))

        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        next_local = compute_next_occurrence(1, ph, pm, tzinfo=tz)
        embed = discord.Embed(title="AGS OnThisDay Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)
        embed.add_field(
            name="Channel",
            value=(ctx.guild.get_channel(ch_id).mention if ch_id and ctx.guild.get_channel(ch_id) else "Not set"),
            inline=True,
        )
        embed.add_field(name="Timezone", value=tz_str, inline=True)
        embed.add_field(
            name="Next Post (local)",
            value=f"{next_local:%Y-%m-%d %H:%M %Z}",
            inline=False,
        )
        embed.add_field(
            name="Next Post (discord t: tag)",
            value=f"<t:{int(next_local.timestamp())}:t> (<t:{int(next_local.timestamp())}:R>)",
            inline=False,
        )
        await ctx.send(embed=embed)

    @commands.is_owner()
    @agsonthisday.command(name="raw")
    async def _raw_config(self, ctx: commands.Context) -> None:
        """[Owner only] Dump the raw guild config."""
        data = await self.config.all_guilds()
        await ctx.send(box(str(data)))
    

async def setup(bot: Red) -> None:
    """Load the AGSOnThisDay cog."""
    await bot.add_cog(AGSOnThisDay(bot))