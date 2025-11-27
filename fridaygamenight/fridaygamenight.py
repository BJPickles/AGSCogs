from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
import re
from typing import Optional

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

DEFAULT_MESSAGE = (
    "## FRIDAY NIGHT GAME NIGHT\n"
    "Hello folks, happy Friday! Weekly game night starts at <t:{unix}:t> (<t:{unix}:R>).\n\n"
    "Pop a reaction below if you're planning on joining 🔥"
)

def userday_to_pyweekday(userday: int) -> int:
    # 1=Monday .. 7=Sunday  →  0..6
    return (userday - 1) % 7

def compute_next_occurrence(
    user_day: int,
    hour: int,
    minute: int,
    now: Optional[datetime] = None
) -> datetime:
    """
    Returns the next occurrence *strictly after* 'now' of the given
    weekday+time.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    target_wd = userday_to_pyweekday(user_day)
    today_wd = now.weekday()
    days_ahead = (target_wd - today_wd) % 7
    candidate = datetime(
        now.year, now.month, now.day,
        hour, minute, 0,
        tzinfo=timezone.utc
    ) + timedelta(days=days_ahead)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate

async def mod_check(ctx: commands.Context) -> bool:
    return ctx.author.guild_permissions.manage_guild

class FridayGameNight(commands.Cog):
    """Automated weekly game night announcer with advance posting."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA1B2C3D4E6, force_registration=True)
        self.config.register_guild(
            enabled=False,
            channel_id=None,
            message=DEFAULT_MESSAGE,
            day=5,                # Friday
            announce_hour=9,      # post at 09:00 UTC
            announce_minute=0,
            hour=19,              # event at 19:30 UTC
            minute=30,
            last_posted_unix=0,
        )
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def cog_load(self) -> None:
        self._task = self.bot.loop.create_task(self._background_loop())

    async def cog_unload(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    async def _background_loop(self) -> None:
        await self.bot.wait_until_ready()
        while True:
            try:
                for guild in list(self.bot.guilds):
                    try:
                        await self._handle_guild(guild)
                    except Exception:
                        self.bot.logger.exception("FGN _handle_guild failed for %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("FGN background loop error")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild) -> None:
        data = await self.config.guild(guild).all()
        if not data.get("enabled"):
            return
        chan_id = data.get("channel_id")
        if not chan_id:
            return
        channel = guild.get_channel(chan_id)
        if channel is None:
            return

        # gather config
        user_day = int(data.get("day", 5))
        ann_hr = int(data.get("announce_hour", 9))
        ann_min = int(data.get("announce_minute", 0))
        evt_hr = int(data.get("hour", 19))
        evt_min = int(data.get("minute", 30))

        now = datetime.now(timezone.utc)
        target_wd = userday_to_pyweekday(user_day)
        today_wd = now.weekday()
        days_ahead = (target_wd - today_wd) % 7
        base_date = (now + timedelta(days=days_ahead)).date()

        # announcement window
        ann_dt = datetime(
            base_date.year, base_date.month, base_date.day,
            ann_hr, ann_min, 0,
            tzinfo=timezone.utc
        )
        window_start = ann_dt
        window_end = ann_dt + timedelta(minutes=5)
        if not (window_start <= now <= window_end):
            return

        # compute that same day's event datetime (or next week if it would be before announce)
        evt_dt = datetime(
            base_date.year, base_date.month, base_date.day,
            evt_hr, evt_min, 0,
            tzinfo=timezone.utc
        )
        if evt_dt <= ann_dt:
            # event time is earlier than announce → schedule for next week
            evt_dt += timedelta(days=7)

        next_event_unix = int(evt_dt.timestamp())
        last_posted = int(data.get("last_posted_unix", 0))
        if last_posted >= next_event_unix:
            return  # already posted for this event

        # lock & double-check
        async with self._lock:
            data2 = await self.config.guild(guild).all()
            if int(data2.get("last_posted_unix", 0)) >= next_event_unix:
                return

            template = data2.get("message", DEFAULT_MESSAGE)
            final = re.sub(r"\{\s*unix\s*\}", str(next_event_unix), template, flags=re.IGNORECASE)
            allowed = discord.AllowedMentions(roles=True, users=True, everyone=False)
            try:
                sent = await channel.send(final, allowed_mentions=allowed)
                try:
                    await sent.add_reaction("🔥")
                except Exception:
                    pass
                await self.config.guild(guild).last_posted_unix.set(next_event_unix)
            except discord.Forbidden:
                self.bot.logger.warning(
                    "FGN cannot send to %s in guild %s", chan_id, guild.id
                )
            except Exception:
                self.bot.logger.exception(
                    "FGN failed posting in %s:%s", guild.id, chan_id
                )

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def gamenight(self, ctx: commands.Context):
        """Configure/view the weekly Game Night announcer."""
        await ctx.send_help(ctx.command)

    @gamenight.command()
    @commands.check(mod_check)
    async def enable(self, ctx: commands.Context):
        """Enable the weekly announcer."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("✅ FridayGameNight enabled.")

    @gamenight.command()
    @commands.check(mod_check)
    async def disable(self, ctx: commands.Context):
        """Disable the weekly announcer."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ FridayGameNight disabled.")

    @gamenight.command()
    @commands.check(mod_check)
    async def status(self, ctx: commands.Context):
        """Show current settings and next times."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data.get("enabled", False)
        ch_id = data.get("channel_id")
        msg = data.get("message", DEFAULT_MESSAGE)
        day = int(data.get("day", 5))
        ann_hr = int(data.get("announce_hour", 9))
        ann_min = int(data.get("announce_minute", 0))
        evt_hr = int(data.get("hour", 19))
        evt_min = int(data.get("minute", 30))
        last_posted = int(data.get("last_posted_unix", 0))

        next_ann = compute_next_occurrence(day, ann_hr, ann_min)
        next_evt = compute_next_occurrence(day, evt_hr, evt_min)

        embed = discord.Embed(title="FridayGameNight Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)
        if ch_id:
            ch = ctx.guild.get_channel(ch_id)
            embed.add_field(name="Channel", value=(ch.mention if ch else f"<#{ch_id}>"), inline=True)
        else:
            embed.add_field(name="Channel", value="Not set", inline=True)

        embed.add_field(
            name="Next Announcement (UTC)",
            value=f"{next_ann:%Y-%m-%d %H:%M}\n<t:{int(next_ann.timestamp())}:t> (<t:{int(next_ann.timestamp())}:R>)",
            inline=False,
        )
        embed.add_field(
            name="Next Event (UTC)",
            value=f"{next_evt:%Y-%m-%d %H:%M}\n<t:{int(next_evt.timestamp())}:t> (<t:{int(next_evt.timestamp())}:R>)",
            inline=False,
        )

        embed.add_field(name="Day (1=Mon..7=Sun)", value=str(day), inline=True)
        embed.add_field(name="Announcement Time (HH:MM UTC)", value=f"{ann_hr:02d}:{ann_min:02d}", inline=True)
        embed.add_field(name="Event Time (HH:MM UTC)", value=f"{evt_hr:02d}:{evt_min:02d}", inline=True)

        preview = re.sub(r"\{\s*unix\s*\}", str(int(next_evt.timestamp())), msg, flags=re.IGNORECASE)
        if len(preview) > 1000:
            preview = preview[:990] + "…"
        embed.add_field(name="Message Preview", value=box(preview, lang=""), inline=False)

        if last_posted:
            lp = datetime.fromtimestamp(last_posted, tz=timezone.utc)
            embed.set_footer(text=f"Last posted for event at {lp:%Y-%m-%d %H:%M UTC} (unix {last_posted})")

        await ctx.send(embed=embed)

    @gamenight.command(name="setchannel")
    @commands.check(mod_check)
    async def setchannel(self, ctx: commands.Context, *, channel: str):
        """Set the channel for the announcement."""
        chan_id: Optional[int] = None
        m = re.search(r"/channels/\d+/(\d+)", channel)
        if m:
            chan_id = int(m.group(1))
        else:
            m = re.match(r"<#(\d+)>$", channel)
            if m:
                chan_id = int(m.group(1))
            elif channel.isdigit():
                chan_id = int(channel)
        if chan_id is None:
            try:
                ch = await commands.TextChannelConverter().convert(ctx, channel)
                chan_id = ch.id
            except Exception:
                return await ctx.send("❌ Could not parse that channel.")
        ch_obj = ctx.guild.get_channel(chan_id)
        if ch_obj is None:
            return await ctx.send("❌ Channel not found.")
        await self.config.guild(ctx.guild).channel_id.set(chan_id)
        await ctx.send(f"✅ Channel set to {ch_obj.mention}.")

    @gamenight.command(name="setmessage")
    @commands.check(mod_check)
    async def setmessage(self, ctx: commands.Context, *, message: str):
        """
        Set the announcement template.
        Use `{unix}` in it to interpolate the event timestamp.
        """
        await self.config.guild(ctx.guild).message.set(message)
        await ctx.send("✅ Message template updated.")

    @gamenight.command(name="day")
    @commands.check(mod_check)
    async def set_day(self, ctx: commands.Context, day: int):
        """Set weekday of announcement/event (1=Mon .. 7=Sun)."""
        if not 1 <= day <= 7:
            return await ctx.send("❌ Day must be between 1 and 7.")
        await self.config.guild(ctx.guild).day.set(day)
        await ctx.send(f"✅ Day set to {day}.")

    @gamenight.command(name="time")
    @commands.check(mod_check)
    async def set_time(self, ctx: commands.Context, time_str: str):
        """Set the *event* time (HH:MM UTC)."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Format must be HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).hour.set(hr)
        await self.config.guild(ctx.guild).minute.set(mn)
        await ctx.send(f"✅ Event time set to {hr:02d}:{mn:02d} UTC.")

    @gamenight.command(name="announce_time")
    @commands.check(mod_check)
    async def set_announce(self, ctx: commands.Context, time_str: str):
        """Set the *announcement* time (HH:MM UTC)."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Format must be HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).announce_hour.set(hr)
        await self.config.guild(ctx.guild).announce_minute.set(mn)
        await ctx.send(f"✅ Announcement time set to {hr:02d}:{mn:02d} UTC.")

    @commands.is_owner()
    @gamenight.command(name="raw")
    async def _raw_config(self, ctx: commands.Context):
        """[Owner] Dump raw guild config."""
        data = await self.config.guild(ctx.guild).all()
        await ctx.send(box(str(data)))
