from __future__ import annotations
import asyncio
import random
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

DEFAULT_PRE_MESSAGES = [
    "Hey everyone, it's dad joke time:",
    "Brace yourselves—dad joke incoming:",
    "I hope you're ready for some pun-ishment:",
    "It's time to groan—dad joke ahead:",
    "Ready for your daily dose of dad humor?",
    "Warning: cheesy joke approaching:",
    "Time to roll your eyes—dad joke coming:",
    "Dad joke incoming, stand by:",
    "All aboard the dad joke express:",
    "Prepare yourself for maximum dadness:",
    "Attention: dad joke drop imminent:",
    ""
]

def compute_next_daily(
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    """
    Return the next occurrence strictly after 'now' of the given time
    in the given tzinfo (handles DST automatically).
    """
    if now is None:
        now_local = datetime.now(tzinfo)
    else:
        now_local = now.astimezone(tzinfo)
    candidate = datetime(
        now_local.year,
        now_local.month,
        now_local.day,
        hour,
        minute,
        tzinfo=tzinfo,
    )
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate

def compute_last_daily(
    hour: int,
    minute: int,
    now: datetime | None = None,
    tzinfo: timezone | ZoneInfo = timezone.utc,
) -> datetime:
    """
    Return the most recent occurrence at or before 'now' of the given time
    in the given tzinfo (handles DST folds/unfolds).
    """
    if now is None:
        now_local = datetime.now(tzinfo)
    else:
        now_local = now.astimezone(tzinfo)
    candidate = datetime(
        now_local.year,
        now_local.month,
        now_local.day,
        hour,
        minute,
        tzinfo=tzinfo,
    )
    if candidate > now_local:
        candidate -= timedelta(days=1)
    return candidate


class DailyDadJokes(commands.Cog):
    """
    Fetch random dad jokes on command or post daily with rotating prefixes.
    """

    __author__ = "you"
    __version__ = "1.0.0"

    def __init__(self, bot: Red):
        self.bot = bot
        # Unique hex identifier for this cog's Config
        self.config = Config.get_conf(self, identifier=0xDAD10CEB, force_registration=True)
        # Per-guild defaults
        self.config.register_guild(
            enabled=False,
            channel_id=None,
            timezone="UTC",
            post_hour=12,
            post_minute=0,
            last_posted_unix=0,
            pre_messages=DEFAULT_PRE_MESSAGES,
            prefix_order=[],
            prefix_index=0,
        )
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def cog_load(self):
        # Start the background loop
        self._task = self.bot.loop.create_task(self._background_loop())

    async def cog_unload(self):
        # Cancel the background loop
        if self._task:
            self._task.cancel()
            self._task = None

    async def _background_loop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                now_utc = datetime.now(timezone.utc)
                for guild in list(self.bot.guilds):
                    try:
                        await self._handle_guild(guild, now_utc)
                    except Exception:
                        self.bot.logger.exception("DailyDadJokes _handle_guild error for %s", guild.id)
                # check twice a minute
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("DailyDadJokes background loop crashed")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild, now_utc: datetime):
        data = await self.config.guild(guild).all()
        if not data.get("enabled"):
            return
        chan_id = data.get("channel_id")
        if not chan_id:
            return
        channel = guild.get_channel(chan_id)
        if channel is None:
            return

        # Resolve timezone
        tz_str = data.get("timezone", "UTC")
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        post_hour = int(data.get("post_hour", 12))
        post_minute = int(data.get("post_minute", 0))
        last_posted = int(data.get("last_posted_unix", 0) or 0)

        # Compute the last daily occurrence at or before now
        last_local = compute_last_daily(post_hour, post_minute, now=now_utc, tzinfo=tz)
        last_utc = last_local.astimezone(timezone.utc)

        # If we're within 5 minutes after that occurrence, and haven't posted yet:
        if last_utc <= now_utc <= last_utc + timedelta(minutes=5):
            ts = int(last_local.timestamp())
            if last_posted < ts:
                async with self._lock:
                    # Re-check under lock
                    fresh = await self.config.guild(guild).all()
                    if int(fresh.get("last_posted_unix", 0) or 0) < ts:
                        joke = await self._fetch_joke()
                        if not joke:
                            return
                        prefix = await self._get_next_prefix(guild, fresh)
                        content = f"{prefix}\n`{joke}`" if prefix else f"`{joke}`"
                        try:
                            await channel.send(content)
                        except discord.Forbidden:
                            self.bot.logger.warning(
                                "DailyDadJokes cannot send to %s in guild %s", chan_id, guild.id
                            )
                        except Exception:
                            self.bot.logger.exception(
                                "DailyDadJokes failed send in %s:%s", guild.id, chan_id
                            )
                        await self.config.guild(guild).last_posted_unix.set(ts)

    async def _fetch_joke(self) -> str | None:
        """Fetches a plain‐text joke or returns None on failure."""
        try:
            async with aiohttp.request(
                "GET",
                "https://icanhazdadjoke.com/",
                headers={"Accept": "text/plain"},
            ) as r:
                if r.status != 200:
                    return None
                return await r.text(encoding="utf-8")
        except aiohttp.ClientError:
            return None

    async def _get_next_prefix(self, guild: discord.Guild, data: dict | None = None) -> str:
        """
        Picks the next prefix from the guild's cycle, reshuffling when needed.
        """
        if data is None:
            data = await self.config.guild(guild).all()
        pre_messages = data.get("pre_messages", DEFAULT_PRE_MESSAGES) or []
        N = len(pre_messages)
        if N == 0:
            return ""
        prefix_order = data.get("prefix_order") or []
        prefix_index = int(data.get("prefix_index", 0) or 0)
        # If we've exhausted the cycle (or never built one), reshuffle
        if not prefix_order or prefix_index >= N:
            prefix_order = list(range(N))
            random.shuffle(prefix_order)
            prefix_index = 0
        idx = prefix_order[prefix_index]
        prefix_index += 1
        # Save back our updated cycle state
        await self.config.guild(guild).prefix_order.set(prefix_order)
        await self.config.guild(guild).prefix_index.set(prefix_index)
        # Return the chosen prefix (empty string → raw joke)
        return pre_messages[idx] or ""

    # ─── COMMANDS ────────────────────────────────────────────────────────────────

    @commands.group(name="dadjoke", invoke_without_command=True)
    @commands.guild_only()
    async def dadjoke(self, ctx: commands.Context):
        """
        Fetch a random dad joke right now (with rotating prefix).
        """
        joke = await self._fetch_joke()
        if not joke:
            return await ctx.send("Oops! Cannot get a dad joke…")
        async with self._lock:
            prefix = await self._get_next_prefix(ctx.guild)
        content = f"{prefix}\n`{joke}`" if prefix else f"`{joke}`"
        await ctx.send(content)

    @dadjoke.command()
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def enable(self, ctx: commands.Context):
        """Enable daily dad joke posting."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("✅ DailyDadJokes enabled.")

    @dadjoke.command()
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def disable(self, ctx: commands.Context):
        """Disable daily dad joke posting."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ DailyDadJokes disabled.")

    @dadjoke.command()
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def status(self, ctx: commands.Context):
        """Show current configuration and next daily post time."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data.get("enabled", False)
        chan_id = data.get("channel_id")
        tz_str = data.get("timezone", "UTC")
        post_hour = int(data.get("post_hour", 12))
        post_minute = int(data.get("post_minute", 0))
        pre_messages = data.get("pre_messages", DEFAULT_PRE_MESSAGES) or []
        prefix_order = data.get("prefix_order") or []
        prefix_index = int(data.get("prefix_index", 0) or 0)

        # Resolve timezone
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = timezone.utc

        next_local = compute_next_daily(post_hour, post_minute, now=None, tzinfo=tz)

        embed = discord.Embed(title="DailyDadJokes Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)
        embed.add_field(
            name="Channel",
            value=(
                ctx.guild.get_channel(chan_id).mention
                if chan_id and ctx.guild.get_channel(chan_id)
                else "Not set"
            ),
            inline=True,
        )
        embed.add_field(name="Timezone", value=tz_str, inline=True)
        embed.add_field(name="Post Time", value=f"{post_hour:02}:{post_minute:02}", inline=True)
        embed.add_field(
            name="Next Post (local)",
            value=f"{next_local:%a %Y-%m-%d %H:%M %Z}",
            inline=False,
        )
        embed.add_field(
            name="Next Post (Discord t: tag)",
            value=f"<t:{int(next_local.timestamp())}:t> (<t:{int(next_local.timestamp())}:R>)",
            inline=False,
        )
        embed.add_field(name="Prefixes Total", value=str(len(pre_messages)), inline=True)
        embed.add_field(
            name="Cycle Position",
            value=f"{prefix_index}/{len(prefix_order) if prefix_order else len(pre_messages)}",
            inline=True,
        )

        await ctx.send(embed=embed)

    @dadjoke.command(name="setchannel")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def setchannel(self, ctx: commands.Context, *, channel: str):
        """Set the channel for daily postings."""
        try:
            ch = await commands.TextChannelConverter().convert(ctx, channel)
            if ch.guild.id != ctx.guild.id:
                return await ctx.send("❌ That channel isn’t in this guild.")
        except Exception:
            return await ctx.send("❌ Could not parse that channel.")
        await self.config.guild(ctx.guild).channel_id.set(ch.id)
        await ctx.send(f"✅ Channel set to {ch.mention}.")

    @dadjoke.command(name="settime")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def settime(self, ctx: commands.Context, time_str: str):
        """Set the daily post time (HH:MM in guild timezone)."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Format must be HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).post_hour.set(hr)
        await self.config.guild(ctx.guild).post_minute.set(mn)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Post time set to {hr:02}:{mn:02}.")

    @dadjoke.command(name="settimezone")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def settimezone(self, ctx: commands.Context, timezone_name: str):
        """
        Set the IANA timezone for daily postings.
        Example: Europe/London, America/New_York, UTC, etc.
        """
        try:
            ZoneInfo(timezone_name)
        except Exception:
            return await ctx.send(
                "❌ Invalid timezone. Please supply a valid IANA name "
                "(e.g. Europe/London, America/New_York, UTC)."
            )
        await self.config.guild(ctx.guild).timezone.set(timezone_name)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Timezone set to `{timezone_name}`.")

    @dadjoke.group(name="prefix", invoke_without_command=True)
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def prefix(self, ctx: commands.Context):
        """Manage the rotating pre-messages."""
        await ctx.send_help(ctx.command)

    @prefix.command(name="list")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def prefix_list(self, ctx: commands.Context):
        """List all current prefixes."""
        data = await self.config.guild(ctx.guild).all()
        pre_messages = data.get("pre_messages", DEFAULT_PRE_MESSAGES) or []
        lines = [f"{i+1}: {msg or '[empty]'}" for i, msg in enumerate(pre_messages)]
        await ctx.send(box("\n".join(lines)))

    @prefix.command(name="add")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def prefix_add(self, ctx: commands.Context, *, message: str):
        """Add a new prefix message (cycle resets)."""
        msg = message.strip()
        if msg == "":
            return await ctx.send(
                "❌ Prefix cannot be empty. To get a raw joke, just do `[p]dadjoke`."
            )
        data = await self.config.guild(ctx.guild).all()
        pre_messages = data.get("pre_messages", DEFAULT_PRE_MESSAGES).copy()
        pre_messages.append(msg)
        await self.config.guild(ctx.guild).pre_messages.set(pre_messages)
        await self.config.guild(ctx.guild).prefix_order.set([])
        await self.config.guild(ctx.guild).prefix_index.set(0)
        await ctx.send("✅ Prefix added and cycle reset.")

    @prefix.command(name="remove")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def prefix_remove(self, ctx: commands.Context, index: int):
        """Remove a prefix by its number from `[p]dadjoke prefix list`."""
        data = await self.config.guild(ctx.guild).all()
        pre_messages = data.get("pre_messages", DEFAULT_PRE_MESSAGES).copy()
        if not (1 <= index <= len(pre_messages)):
            return await ctx.send("❌ Invalid prefix number.")
        removed = pre_messages.pop(index - 1)
        await self.config.guild(ctx.guild).pre_messages.set(pre_messages)
        await self.config.guild(ctx.guild).prefix_order.set([])
        await self.config.guild(ctx.guild).prefix_index.set(0)
        await ctx.send(f"✅ Removed prefix #{index}: {removed or '[empty]'} and cycle reset.")

    @prefix.command(name="clear")
    @commands.has_guild_permissions(manage_guild=True)
    @commands.guild_only()
    async def prefix_clear(self, ctx: commands.Context):
        """Reset prefixes to the original 12 defaults (cycle resets)."""
        await self.config.guild(ctx.guild).pre_messages.set(DEFAULT_PRE_MESSAGES)
        await self.config.guild(ctx.guild).prefix_order.set([])
        await self.config.guild(ctx.guild).prefix_index.set(0)
        await ctx.send("✅ Prefixes reset to defaults and cycle reset.")