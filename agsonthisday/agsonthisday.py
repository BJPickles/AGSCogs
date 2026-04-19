# agsonthisday.py

from __future__ import annotations
import asyncio
import random
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

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

    # ────────────────────────────────────────────────────────
    #  Helpers for scraping and embed building
    # ────────────────────────────────────────────────────────

    def _normalize_url(self, url: str | None) -> str | None:
        if not url:
            return None
        url = url.strip()
        if url.startswith("//"):
            return "https:" + url
        if url.startswith("/"):
            return urljoin(TODAY_URL, url)
        return url

    def _pick_best_from_srcset(self, raw: str) -> str:
        parts = [p.strip().split() for p in raw.split(",")]
        def score(p):
            if len(p) > 1 and p[1].endswith("w"):
                try:
                    return int(p[1][:-1])
                except:
                    return 0
            return 0
        parts.sort(key=score)
        return parts[-1][0]

    def _extract_best_image(self, node) -> str | None:
        imgs = node.xpath(
            ".//picture/source[@data-srcset]/@data-srcset |"
            ".//img[@data-src]/@data-src |"
            ".//img[@src]/@src"
        )
        if not imgs:
            return None
        raw = imgs[-1]
        if "," in raw:
            url = self._pick_best_from_srcset(raw)
        else:
            url = raw.split()[0]
        url = self._normalize_url(url)
        if url and any(x in url.lower() for x in ("svg", "logo", "icon")):
            return None
        return url

    def _extract_wiki_links(self, node) -> dict[str, str]:
        wiki: dict[str, str] = {}
        for a in node.xpath(".//a[contains(@href,'wikipedia.org')]"):
            href = self._normalize_url(a.get("href"))
            label = a.text_content().strip() or href
            if href and label not in wiki:
                wiki[label[:80]] = href
        return wiki

    def _build_embed(
        self,
        title: str,
        desc: str,
        thumb: str | None,
        image: str | None,
        wiki: dict[str, str],
    ):
        embed = discord.Embed(title=title, description=desc, color=EMBED_COLOR)
        if thumb:
            embed.set_thumbnail(url=thumb)
        if image:
            embed.set_image(url=image)
        embed.set_footer(text="from onthisday.com")
        items = list(wiki.items())[:24]
        view = ButtonView(dict(items)) if items else None
        return embed, view

    # ────────────────────────────────────────────────────────
    #  Dispatch to three isolated scrapers
    # ────────────────────────────────────────────────────────

    async def _post_today(self, channel: discord.TextChannel) -> None:
        tree = await self._fetch_page()
        if tree is None:
            return await channel.send("❌ Failed to retrieve On This Day content.")
        await self._post_today_in_history(tree, channel)
        await self._post_fun_fact(tree, channel)
        await self._post_featured_article(tree, channel)

    async def _post_today_in_history(self, tree, channel):
        try:
            events = tree.xpath("//li[contains(@class,'event')]")
            chosen = None
            for ev in events:
                if ev.xpath(".//div[contains(@class,'event-photo')]"):
                    chosen = ev
                    break
            if not chosen and events:
                chosen = events[0]
            if not chosen:
                self.bot.logger.warning("OnThisDay: no li.event found")
                return

            # Extract year from the event
            year_nodes = chosen.xpath(".//a[contains(@class,'date')]/text()")
            year = year_nodes[0].strip() if year_nodes else None

            # Extract full text and strip the leading year if present
            full_text = chosen.xpath("string()").strip()
            desc_text = full_text
            if year and full_text.startswith(year):
                desc_text = full_text[len(year):].strip()

            # Build today's date + the event year
            today = datetime.now().date()
            month_name = today.strftime("%B")
            day = today.day
            if year:
                date_text = f"{month_name} {day}, {year}"
            else:
                date_text = f"{month_name} {day}"

            # Append calendar emoji and formatted date
            final_desc = f"{desc_text}\n\n📅 {date_text}"

            image = self._extract_best_image(chosen)
            wiki = self._extract_wiki_links(chosen)

            embed, view = self._build_embed(
                "TODAY IN HISTORY",
                final_desc[:4096],
                THUMBNAILS["events"],
                image,
                wiki,
            )
            await channel.send(embed=embed, view=view)
        except Exception:
            self.bot.logger.exception("Failed to build TODAY IN HISTORY section")

    async def _post_fun_fact(self, tree, channel):
        try:
            section = tree.xpath("//section[contains(@class,'section--did-you-know')]")
            if not section:
                self.bot.logger.debug("OnThisDay: no did-you-know section")
                return
            sec = section[0]

            # Loop over each wrapper (Fun Fact or Did You Know)
            wrappers = sec.xpath(".//div[contains(@class,'wrapper')]")
            for wrapper in wrappers:
                # Extract the heading/title
                h2_nodes = wrapper.xpath(".//h2[contains(@class,'did-you-know__heading')]")
                if not h2_nodes:
                    continue
                title = h2_nodes[0].text_content().strip()

                # Extract paragraphs: the fact text vs. the date line
                paras = wrapper.xpath(".//p")
                fact_text = None
                fact_date = None
                for p in paras:
                    cls = p.get("class", "").strip()
                    txt = p.text_content().strip()
                    if not txt:
                        continue
                    if "fun-fact" in cls:
                        fact_date = txt
                    else:
                        fact_text = txt

                if not fact_text:
                    continue

                desc = fact_text
                if fact_date:
                    desc += f"\n\n📅 {fact_date}"

                # Pick thumbnail based on whether it's a Fun Fact or Did You Know
                key = "fun-fact" if "fun fact" in title.lower() else "did-you-know"
                thumb = THUMBNAILS.get(key)

                image = self._extract_best_image(wrapper)
                wiki = self._extract_wiki_links(wrapper)

                embed, view = self._build_embed(
                    title,
                    desc[:4096],
                    thumb,
                    image,
                    wiki,
                )
                await channel.send(embed=embed, view=view)
        except Exception:
            self.bot.logger.exception("Failed to build FUN FACT / DID YOU KNOW section")

    async def _post_featured_article(self, tree, channel):
        try:
            node = tree.xpath(
                "(//section[contains(@class,'featured-article')]"
                " | //div[contains(@class,'featured-article')])[1]"
            )
            if not node:
                return
            art = node[0]

            p = art.xpath(".//p[contains(@class,'linked_text')]")
            if p:
                p = p[0]
                full = p.xpath("string()").strip()
                date_node = p.xpath(".//span[contains(@class,'linked_date')]/text()")
                date_text = date_node[0].strip() if date_node else None
                if date_text and full.endswith(date_text):
                    desc_text = full[: -len(date_text)].rstrip()
                else:
                    desc_text = full
            else:
                paras = art.xpath(".//p")
                parts = [x.text_content().strip() for x in paras if x.text_content().strip()]
                if not parts:
                    return
                desc_text = "\n\n".join(parts)
                date_text = None

            if date_text:
                desc = f"{desc_text[:3900]}\n\n📅 {date_text}"
            else:
                desc = desc_text[:4096]

            image = self._extract_best_image(art)
            wiki = self._extract_wiki_links(art)

            embed, view = self._build_embed(
                "FEATURED ARTICLE",
                desc,
                THUMBNAILS["featured-article"],
                image,
                wiki,
            )
            await channel.send(embed=embed, view=view)

        except Exception:
            self.bot.logger.exception("Failed to build FEATURED ARTICLE section")

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