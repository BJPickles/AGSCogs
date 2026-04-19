# agsonthisday.py

from __future__ import annotations
import asyncio
import random
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
ENDPOINT  = "https://byabbe.se/on-this-day/{}/events.json"

# Browser UA
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0 Safari/537.36"
)

# Embed colour
EMBED_COLOR = 0xCE9DC6

# Section thumbnails (PNG)
THUMBNAILS = {
    "events":            "https://aegisgamestudios.co.uk/wp-content/uploads/2026/04/person-of-interest.png",
    "did-you-know":      "https://aegisgamestudios.co.uk/wp-content/uploads/2026/04/did-you-know.png",
    "fun-fact":          "https://aegisgamestudios.co.uk/wp-content/uploads/2026/04/comedian.png",
    "featured-article":  "https://aegisgamestudios.co.uk/wp-content/uploads/2026/04/article.png",
}

# Pre‐messages
DEFAULT_PRE_MESSAGES: list[str] = [
    "Caw caw 🐦, did you hear that {ping}? It's the sound of Wild History™!",
    "Grab your towel 🛸, {ping}! We're hitch-hiking through yesterday's headlines!",
    "Spanners at the ready 🔧, {ping}! Time to wrench open past mysteries!",
    "By the Librarian's tusks 🐘, {ping}, today's history is positively bananas!",
    "Sound the klaxon 🚨, {ping}! A history emergency has arrived!",
    "Ahoy mateys ⚓, {ping}! A tide of bygone tales washes ashore!",
    "Don your top hat 🎩, {ping}! A historical caper awaits!",
    "To the time machine 🕰️, {ping}! We're off to yesterday!",
    "Tea kettle's whistling 🍵, {ping}! History's brewing something grand!",
    "Blast off 🚀, {ping}! Prepare for a cosmic history tour!",
    "Hear the hamster wheel 🐹, {ping}? That's the chronicle hamster running!",
    "By the Great A'Tuin 🌏, {ping}, let's turtle-walk through time!"
]

def compute_next_daily(
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    if now is None:
        now_local = datetime.now(tzinfo)
    else:
        now_local = now.astimezone(tzinfo)
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate

def compute_last_daily(
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    if now is None:
        now_local = datetime.now(tzinfo)
    else:
        now_local = now.astimezone(tzinfo)
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate > now_local:
        candidate -= timedelta(days=1)
    return candidate

async def mod_check(ctx: commands.Context) -> bool:
    return ctx.author.guild_permissions.manage_guild

class ButtonView(discord.ui.View):
    """A View of URL buttons for Wikipedia links."""
    def __init__(self, buttons: dict[str, str]):
        super().__init__(timeout=None)
        for label, url in buttons.items():
            safe_label = label if len(label) < 80 else label[:77] + "..."
            self.add_item(discord.ui.Button(label=safe_label, url=url))

class AGSOnThisDay(commands.Cog):
    """Automatic daily 'On This Day' posts from onthisday.com."""

    __author__ = "AEGIS Team"
    __version__ = "1.1.3"

    def __init__(self, bot: Red):
        self.bot = bot
        self.session: aiohttp.ClientSession | None = None
        self.config = Config.get_conf(
            self, identifier=0xA5C3B4D6F2, force_registration=True
        )
        self.config.register_guild(
            enabled=False,
            channel_id=None,
            timezone="UTC",
            post_hour=8,
            post_minute=0,
            last_posted_unix=0,
            ping_role_id=None,
            pre_messages=DEFAULT_PRE_MESSAGES,
            prefix_order=[],
            prefix_index=0,
        )
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def format_help_for_context(self, ctx: commands.Context) -> str:
        base = super().format_help_for_context(ctx)
        return f"{base}\n\nAuthor: {self.__author__}\nVersion: {self.__version__}"

    async def red_delete_data_for_user(self, **kwargs):
        return

    async def cog_load(self) -> None:
        self.session = aiohttp.ClientSession(headers={"User-Agent": DEFAULT_UA})
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
                now = datetime.now(timezone.utc)
                for guild in self.bot.guilds:
                    try:
                        await self._handle_guild(guild, now)
                    except Exception:
                        self.bot.logger.exception("AGSOnThisDay error %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("BG loop crashed")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild, now_utc: datetime) -> None:
        cfg = await self.config.guild(guild).all()
        if not cfg["enabled"]:
            return
        ch_id = cfg["channel_id"]
        if not ch_id:
            return
        channel = guild.get_channel(ch_id)
        if not isinstance(channel, discord.TextChannel):
            return

        tz_str = cfg["timezone"]
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        ph = cfg["post_hour"]
        pm = cfg["post_minute"]

        last_local = compute_last_daily(ph, pm, now=now_utc, tzinfo=tz)
        last_utc   = last_local.astimezone(timezone.utc)
        next_local = compute_next_daily(ph, pm, now=now_utc, tzinfo=tz)
        next_utc   = next_local.astimezone(timezone.utc)

        last_posted = cfg["last_posted_unix"]
        if last_posted > int(next_utc.timestamp()):
            await self.config.guild(guild).last_posted_unix.set(0)
            last_posted = 0

        now_ts = now_utc.timestamp()
        window_start = last_utc.timestamp()
        if window_start <= now_ts <= window_start + 300:
            post_ts = int(window_start)
            if last_posted < post_ts:
                async with self._lock:
                    fresh = await self.config.guild(guild).all()
                    if fresh["last_posted_unix"] < post_ts:
                        pre = await self._get_next_prefix(guild, fresh)
                        if pre:
                            await channel.send(pre)
                        await self._post_today(channel)
                        await self.config.guild(guild).last_posted_unix.set(post_ts)

    async def _fetch_json_events(self) -> list[dict]:
        if not self.session:
            return []
        now = datetime.now()
        try:
            async with self.session.get(ENDPOINT.format(f"{now.month}/{now.day}")) as r:
                if r.status != 200:
                    return []
                data = await r.json()
        except Exception:
            return []
        return [e for e in data.get("events", []) if e.get("year","").lstrip("-").isdigit()]

    async def _fetch_page(self) -> html.HtmlElement | None:
        if not self.session:
            return None
        try:
            async with self.session.get(TODAY_URL, timeout=20) as resp:
                resp.raise_for_status()
                raw = await resp.read()
        except Exception:
            return None
        try:
            return html.fromstring(raw)
        except Exception:
            return None

    async def _post_today(self, channel: discord.TextChannel) -> None:
        tree = await self._fetch_page()
        if tree is None:
            return await channel.send("❌ Failed to retrieve On This Day content.")

        # parse main page image for embeds
        main_img = None
        imgs = tree.xpath(
            "//figure[contains(@class,'calendar__figure-iob')]//img/@data-src | "
            "//figure[contains(@class,'calendar__figure-iob')]//img/@src"
        )
        if imgs:
            main_img = imgs[-1]

        # 1) TODAY IN HISTORY: top 5 via JSON, embed with main_img
        events = await self._fetch_json_events()
        if events:
            top5 = events[:5]
            lines = [f"**{e['year']}**: {e['description']}" for e in top5]
            desc = "\n".join(lines)
            wiki: dict[str, str] = {}
            for e in top5:
                for w in e.get("wikipedia", []):
                    t = w.get("title","").strip()
                    u = w.get("wikipedia")
                    if t and u:
                        wiki[t] = u
            embed = discord.Embed(
                title="TODAY IN HISTORY",
                description=desc,
                color=EMBED_COLOR,
            )
            embed.set_thumbnail(url=THUMBNAILS["events"])
            if main_img:
                embed.set_image(url=main_img)
            embed.set_footer(text="from onthisday.com")
            view = ButtonView(wiki) if wiki else None
            await channel.send(embed=embed, view=view)

        # 2) DID YOU KNOW?, FUN FACT, FEATURED ARTICLE from HTML
        html_sections = [
            ("section--did-you-know",     "DID YOU KNOW?",        THUMBNAILS["did-you-know"]),
            ("section--fun-fact",         "FUN FACT ABOUT TODAY", THUMBNAILS["fun-fact"]),
            ("section--featured-article", "FEATURED ARTICLE",     THUMBNAILS["featured-article"]),
        ]
        for cls, title, thumb in html_sections:
            sec = tree.xpath(f"//section[contains(@class,'{cls}')]")
            if not sec:
                continue
            sec = sec[0]
            paras = sec.xpath(".//p")
            descs = [p.text_content().strip() for p in paras if p.text_content().strip()]
            description = "\n\n".join(descs)[:4096]
            # optional section image
            imgs = sec.xpath(
                ".//img[@src and not(contains(@src,'.svg'))]/@src | "
                ".//img[@data-src and not(contains(@data-src,'.svg'))]/@data-src"
            )
            section_img = imgs[-1] if imgs else None
            # any wiki links
            wiki: dict[str, str] = {}
            for a in sec.xpath(".//a[contains(@href,'wikipedia.org')]"):
                href = a.get("href")
                label = a.text_content().strip() or href
                wiki[label] = href

            embed = discord.Embed(
                title=title,
                description=description,
                color=EMBED_COLOR,
            )
            embed.set_thumbnail(url=thumb)
            if section_img:
                embed.set_image(url=section_img)
            elif main_img:
                embed.set_image(url=main_img)
            embed.set_footer(text="from onthisday.com")
            view = ButtonView(wiki) if wiki else None
            await channel.send(embed=embed, view=view)

    async def _get_next_prefix(self, guild: discord.Guild, data: dict | None = None) -> str:
        if data is None:
            data = await self.config.guild(guild).all()
        pre = data["pre_messages"] or DEFAULT_PRE_MESSAGES
        N = len(pre)
        if N == 0:
            return ""
        order = data["prefix_order"] or []
        idx = data["prefix_index"] or 0
        if not order or idx >= N:
            order = list(range(N))
            random.shuffle(order)
            idx = 0
        pick = order[idx]
        idx += 1
        await self.config.guild(guild).prefix_order.set(order)
        await self.config.guild(guild).prefix_index.set(idx)
        tmpl = pre[pick]
        role_id = data["ping_role_id"]
        if not role_id:
            return tmpl.format(ping="")
        role = guild.get_role(role_id)
        if not role:
            return tmpl.format(ping="")
        return tmpl.format(ping=role.mention)

    # ─── COMMANDS ────────────────────────────────────────────────────────────────

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
        """Set the local time to post each day. Format HH:MM."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Time must be in HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).post_hour.set(hr)
        await self.config.guild(ctx.guild).post_minute.set(mn)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Post time set to {hr:02d}:{mn:02d}.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def settimezone(self, ctx: commands.Context, tz_name: str) -> None:
        """Set IANA timezone for posts."""
        try:
            ZoneInfo(tz_name)
        except Exception:
            return await ctx.send("❌ Invalid timezone.")
        await self.config.guild(ctx.guild).timezone.set(tz_name)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Timezone set to `{tz_name}`.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def setpingrole(self, ctx: commands.Context, role: discord.Role) -> None:
        """Set the role to ping with the daily pre-message."""
        await self.config.guild(ctx.guild).ping_role_id.set(role.id)
        await ctx.send(f"✅ Ping role set to {role.mention}.")

    @agsonthisday.command()
    @commands.check(mod_check)
    async def fetch(self, ctx: commands.Context) -> None:
        """Fetch & post today's content immediately (testing)."""
        prefix = await self._get_next_prefix(ctx.guild)
        if prefix:
            await ctx.send(prefix)
        await self._post_today(ctx.channel)

    @agsonthisday.command()
    @commands.check(mod_check)
    async def status(self, ctx: commands.Context) -> None:
        """Show config & next post time."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data["enabled"]
        ch_id = data["channel_id"]
        tz_str = data["timezone"]
        ph = data["post_hour"]
        pm = data["post_minute"]
        role = ctx.guild.get_role(data["ping_role_id"]) if data["ping_role_id"] else None

        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        next_local = compute_next_daily(ph, pm, tzinfo=tz)
        embed = discord.Embed(title="AGS OnThisDay Status", color=EMBED_COLOR)
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
            name="Next Post (t: tag)",
            value=f"<t:{int(next_local.timestamp())}:t> (<t:{int(next_local.timestamp())}:R>)",
            inline=False,
        )
        embed.add_field(name="Ping Role", value=(role.mention if role else "Not set"), inline=True)
        await ctx.send(embed=embed)

    @commands.is_owner()
    @agsonthisday.command(name="raw")
    async def _raw_config(self, ctx: commands.Context) -> None:
        """[Owner only] Dump raw guild config."""
        data = await self.config.all_guilds()
        await ctx.send(box(str(data)))

async def setup(bot: Red) -> None:
    """Load the AGSOnThisDay cog."""
    await bot.add_cog(AGSOnThisDay(bot))