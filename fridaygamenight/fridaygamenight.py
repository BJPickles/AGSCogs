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
    "Hello <@&1441728661307920477>! It's almost Friday! **Game Night™** starts tomorrow at <t:{unix}:t> (<t:{unix}:R>).\n\n"
    "Pop a reaction below if you're planning on joining 🔥"
)

DEFAULT_EVENT_MESSAGE = (
    "## GAME NIGHT STARTING NOW\n"
    "Hello <@&1441728661307920477>! **Game Night™** is starting now! <t:{unix}:t> (<t:{unix}:R>)."
)

def userday_to_pyweekday(userday: int) -> int:
    # 1=Monday .. 7=Sunday → 0..6
    return (userday - 1) % 7

def compute_next_occurrence(
    user_day: int,
    hour: int,
    minute: int,
    now: Optional[datetime] = None
) -> datetime:
    """
    Return the next occurrence strictly after 'now' of the given weekday+time.
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

def compute_last_occurrence(
    user_day: int,
    hour: int,
    minute: int,
    now: Optional[datetime] = None
) -> datetime:
    """
    Return the most recent occurrence at or before 'now' of the given weekday+time.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    target_wd = userday_to_pyweekday(user_day)
    today_wd = now.weekday()
    days_ago = (today_wd - target_wd) % 7
    candidate = datetime(
        now.year, now.month, now.day,
        hour, minute, 0,
        tzinfo=timezone.utc
    ) - timedelta(days=days_ago)
    if candidate > now:
        candidate -= timedelta(days=7)
    return candidate

async def mod_check(ctx: commands.Context) -> bool:
    return ctx.author.guild_permissions.manage_guild

class FridayGameNight(commands.Cog):
    """Automated weekly game night announcer with separate announce/event schedules."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA1B2C3D4E7, force_registration=True)
        self.config.register_guild(
            enabled=False,
            channel_id=None,
            message=DEFAULT_MESSAGE,
            announce_day=4,      # Thursday
            announce_hour=9,
            announce_minute=0,
            event_day=5,         # Friday
            event_hour=19,
            event_minute=30,
            last_posted_unix=0,
            event_message=DEFAULT_EVENT_MESSAGE,
            last_event_posted_unix=0,
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
                now = datetime.now(timezone.utc)
                for guild in list(self.bot.guilds):
                    try:
                        await self._handle_guild(guild, now)
                    except Exception:
                        self.bot.logger.exception("FGN _handle_guild error for %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("FGN background loop crashed")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild, now: datetime) -> None:
        data = await self.config.guild(guild).all()
        if not data.get("enabled"):
            return
        chan_id = data.get("channel_id")
        if not chan_id:
            return
        channel = guild.get_channel(chan_id)
        if channel is None:
            return

        # load schedules
        ad = int(data.get("announce_day", 4))
        ah = int(data.get("announce_hour", 9))
        am = int(data.get("announce_minute", 0))
        ed = int(data.get("event_day", 5))
        eh = int(data.get("event_hour", 19))
        em = int(data.get("event_minute", 30))

        # ─── ANNOUNCEMENT ───
        last_announce = compute_last_occurrence(ad, ah, am, now)
        if last_announce <= now <= last_announce + timedelta(minutes=5):
            event_dt = compute_next_occurrence(ed, eh, em, now=last_announce)
            next_event_unix = int(event_dt.timestamp())
            last_posted = int(data.get("last_posted_unix", 0))
            if last_posted < next_event_unix:
                async with self._lock:
                    data2 = await self.config.guild(guild).all()
                    if int(data2.get("last_posted_unix", 0)) < next_event_unix:
                        template = data2.get("message", DEFAULT_MESSAGE)
                        final = re.sub(r"\{\s*unix\s*\}", str(next_event_unix), template, flags=re.IGNORECASE)
                        allowed = discord.AllowedMentions(roles=True, users=True, everyone=False)
                        try:
                            sent = await channel.send(final, allowed_mentions=allowed)
                            try:
                                await sent.add_reaction("🔥")
                            except:
                                pass
                            await self.config.guild(guild).last_posted_unix.set(next_event_unix)
                        except discord.Forbidden:
                            self.bot.logger.warning(
                                "FGN cannot send announcement to %s in guild %s", chan_id, guild.id
                            )
                        except Exception:
                            self.bot.logger.exception(
                                "FGN failed announcement in %s:%s", guild.id, chan_id
                            )

        # ─── EVENT‐START REMINDER ───
        last_event = compute_last_occurrence(ed, eh, em, now)
        if last_event <= now <= last_event + timedelta(minutes=5):
            next_event_unix = int(last_event.timestamp())
            last_ev_posted = int(data.get("last_event_posted_unix", 0))
            if last_ev_posted < next_event_unix:
                async with self._lock:
                    data3 = await self.config.guild(guild).all()
                    if int(data3.get("last_event_posted_unix", 0)) < next_event_unix:
                        ev_tmpl = data3.get("event_message", DEFAULT_EVENT_MESSAGE)
                        ev_msg = re.sub(r"\{\s*unix\s*\}", str(next_event_unix), ev_tmpl, flags=re.IGNORECASE)
                        allowed = discord.AllowedMentions(roles=True, users=True, everyone=False)
                        try:
                            sent2 = await channel.send(ev_msg, allowed_mentions=allowed)
                            try:
                                await sent2.add_reaction("🎉")
                            except:
                                pass
                            await self.config.guild(guild).last_event_posted_unix.set(next_event_unix)
                        except discord.Forbidden:
                            self.bot.logger.warning(
                                "FGN cannot send event reminder to %s in guild %s", chan_id, guild.id
                            )
                        except Exception:
                            self.bot.logger.exception(
                                "FGN failed event reminder in %s:%s", guild.id, chan_id
                            )

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def gamenight(self, ctx: commands.Context):
        """Configure or view your weekly Game Night announcer."""
        await ctx.send_help(ctx.command)

    @gamenight.command()
    @commands.check(mod_check)
    async def enable(self, ctx: commands.Context):
        """Enable automatic game night announcements."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("✅ FridayGameNight enabled.")

    @gamenight.command()
    @commands.check(mod_check)
    async def disable(self, ctx: commands.Context):
        """Disable automatic game night announcements."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ FridayGameNight disabled.")

    @gamenight.command()
    @commands.check(mod_check)
    async def status(self, ctx: commands.Context):
        """Show current configuration and next announce/event times."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data.get("enabled", False)
        ch_id = data.get("channel_id")
        tmpl = data.get("message", DEFAULT_MESSAGE)
        ev_tmpl = data.get("event_message", DEFAULT_EVENT_MESSAGE)
        ad = int(data.get("announce_day", 4))
        ah = int(data.get("announce_hour", 9))
        am = int(data.get("announce_minute", 0))
        ed = int(data.get("event_day", 5))
        eh = int(data.get("event_hour", 19))
        em = int(data.get("event_minute", 30))
        last_posted = int(data.get("last_posted_unix", 0))
        last_ev_posted = int(data.get("last_event_posted_unix", 0))

        next_ann = compute_next_occurrence(ad, ah, am)
        next_evt = compute_next_occurrence(ed, eh, em)

        embed = discord.Embed(title="FridayGameNight Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)
        embed.add_field(
            name="Channel",
            value=(ctx.guild.get_channel(ch_id).mention if ch_id and ctx.guild.get_channel(ch_id) else "Not set"),
            inline=True
        )
        embed.add_field(
            name="Next Announcement",
            value=(
                f"{next_ann:%a %Y-%m-%d %H:%M UTC}\n"
                f"<t:{int(next_ann.timestamp())}:t> (<t:{int(next_ann.timestamp())}:R>)"
            ),
            inline=False
        )
        embed.add_field(
            name="Next Event",
            value=(
                f"{next_evt:%a %Y-%m-%d %H:%M UTC}\n"
                f"<t:{int(next_evt.timestamp())}:t> (<t:{int(next_evt.timestamp())}:R>)"
            ),
            inline=False
        )
        embed.add_field(name="Announce Day", value=str(ad), inline=True)
        embed.add_field(name="Announce Time", value=f"{ah:02d}:{am:02d} UTC", inline=True)
        embed.add_field(name="Event Day", value=str(ed), inline=True)
        embed.add_field(name="Event Time", value=f"{eh:02d}:{em:02d} UTC", inline=True)

        # Preview announcement
        preview = re.sub(r"\{\s*unix\s*\}", str(int(next_evt.timestamp())), tmpl, flags=re.IGNORECASE)
        if len(preview) > 1000:
            preview = preview[:990] + "…"
        embed.add_field(name="Message Preview", value=box(preview, lang=""), inline=False)

        # Preview event-start message
        ev_preview = re.sub(r"\{\s*unix\s*\}", str(int(next_evt.timestamp())), ev_tmpl, flags=re.IGNORECASE)
        if len(ev_preview) > 1000:
            ev_preview = ev_preview[:990] + "…"
        embed.add_field(name="Event-Start Preview", value=box(ev_preview, lang=""), inline=False)

        if last_posted:
            lp = datetime.fromtimestamp(last_posted, tz=timezone.utc)
            embed.set_footer(text=(
                f"Last announced for event at {lp:%Y-%m-%d %H:%M UTC} (unix {last_posted}), "
                f"last reminder at unix {last_ev_posted}"
            ))
        await ctx.send(embed=embed)

    @gamenight.command(name="setchannel")
    @commands.check(mod_check)
    async def setchannel(self, ctx: commands.Context, *, channel: str):
        """Set the channel for announcements."""
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
            return await ctx.send("❌ Channel not found in this guild.")
        await self.config.guild(ctx.guild).channel_id.set(chan_id)
        await ctx.send(f"✅ Channel set to {ch_obj.mention}.")

    @gamenight.command(name="setmessage")
    @commands.check(mod_check)
    async def setmessage(self, ctx: commands.Context, *, message: str):
        """
        Set the announcement template. Use `{unix}` to insert the event timestamp.
        """
        await self.config.guild(ctx.guild).message.set(message)
        # reset guard so you can retest immediately
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send("✅ Message template updated.")

    @gamenight.command(name="seteventmessage")
    @commands.check(mod_check)
    async def set_event_message(self, ctx: commands.Context, *, message: str):
        """
        Set the event‐start reminder template. Use `{unix}` to insert the event timestamp.
        """
        await self.config.guild(ctx.guild).event_message.set(message)
        await self.config.guild(ctx.guild).last_event_posted_unix.set(0)
        await ctx.send("✅ Event‐start reminder template updated.")

    @gamenight.command(name="announce_day")
    @commands.check(mod_check)
    async def set_announce_day(self, ctx: commands.Context, day: int):
        """Set the weekday to post the announcement (1=Mon..7=Sun)."""
        if not 1 <= day <= 7:
            return await ctx.send("❌ Day must be between 1 and 7.")
        await self.config.guild(ctx.guild).announce_day.set(day)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Announcement day set to {day}.")

    @gamenight.command(name="announce_time")
    @commands.check(mod_check)
    async def set_announce_time(self, ctx: commands.Context, time_str: str):
        """Set the time (UTC) to post the announcement. Format HH:MM."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Format must be HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).announce_hour.set(hr)
        await self.config.guild(ctx.guild).announce_minute.set(mn)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await ctx.send(f"✅ Announcement time set to {hr:02d}:{mn:02d} UTC.")

    @gamenight.command(name="event_day")
    @commands.check(mod_check)
    async def set_event_day(self, ctx: commands.Context, day: int):
        """Set the weekday for the event itself (1=Mon..7=Sun)."""
        if not 1 <= day <= 7:
            return await ctx.send("❌ Day must be between 1 and 7.")
        await self.config.guild(ctx.guild).event_day.set(day)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await self.config.guild(ctx.guild).last_event_posted_unix.set(0)
        await ctx.send(f"✅ Event day set to {day}.")

    @gamenight.command(name="event_time")
    @commands.check(mod_check)
    async def set_event_time(self, ctx: commands.Context, time_str: str):
        """Set the time (UTC) for the event. Format HH:MM."""
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            return await ctx.send("❌ Format must be HH:MM.")
        hr, mn = int(m.group(1)), int(m.group(2))
        if not (0 <= hr < 24 and 0 <= mn < 60):
            return await ctx.send("❌ Invalid time.")
        await self.config.guild(ctx.guild).event_hour.set(hr)
        await self.config.guild(ctx.guild).event_minute.set(mn)
        await self.config.guild(ctx.guild).last_posted_unix.set(0)
        await self.config.guild(ctx.guild).last_event_posted_unix.set(0)
        await ctx.send(f"✅ Event time set to {hr:02d}:{mn:02d} UTC.")

    @commands.is_owner()
    @gamenight.command(name="raw")
    async def _raw_config(self, ctx: commands.Context):
        """[Owner only] Dump the raw guild config."""
        data = await self.config.guild(ctx.guild).all()
        await ctx.send(box(str(data)))
