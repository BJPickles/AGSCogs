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
    return (userday - 1) % 7

def compute_next_occurrence(user_day: int, hour: int, minute: int, now: Optional[datetime] = None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)

    target_weekday = userday_to_pyweekday(user_day)
    today_weekday = now.weekday()

    candidate = datetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=hour,
        minute=minute,
        second=0,
        tzinfo=timezone.utc,
    )

    days_ahead = (target_weekday - today_weekday) % 7
    if days_ahead == 0 and candidate <= now:
        days_ahead = 7

    if days_ahead:
        candidate = candidate + timedelta(days=days_ahead)

    return candidate


async def mod_check(ctx: commands.Context) -> bool:
    return ctx.author.guild_permissions.manage_guild


class FridayGameNight(commands.Cog):
    """FridayGameNight - Automated weekly game night announcer."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA1B2C3D4E5, force_registration=True)

        self.config.register_guild(
            enabled=False,
            channel_id=None,
            message=DEFAULT_MESSAGE,
            day=5,
            hour=19,
            minute=0,
            last_posted_unix=0,
        )

        self._task = None
        self._loop_lock = asyncio.Lock()

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
                        self.bot.logger.exception("Error handling gamenight for guild %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("Unexpected error in FridayGameNight background loop")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild) -> None:
        data = await self.config.guild(guild).all()

        if not data.get("enabled"):
            return

        channel_id = data.get("channel_id")
        if not channel_id:
            return

        channel = guild.get_channel(channel_id)
        if channel is None:
            return

        user_day = int(data.get("day", 5))
        hour = int(data.get("hour", 19))
        minute = int(data.get("minute", 0))

        next_dt = compute_next_occurrence(user_day, hour, minute)
        next_unix = int(next_dt.timestamp())

        last_posted = int(data.get("last_posted_unix", 0))
        now_unix = int(datetime.now(timezone.utc).timestamp())

        if last_posted >= next_unix:
            return

        if not (next_unix <= now_unix <= next_unix + 300):
            return

        async with self._loop_lock:
            data2 = await self.config.guild(guild).all()
            if int(data2.get("last_posted_unix", 0)) >= next_unix:
                return

            message_template = data2.get("message", DEFAULT_MESSAGE)
            final_message = re.sub(r"\{\s*unix\s*\}", str(next_unix), message_template, flags=re.IGNORECASE)

            allowed = discord.AllowedMentions(roles=True, users=True, everyone=False)

            try:
                sent = await channel.send(final_message, allowed_mentions=allowed)
                try:
                    await sent.add_reaction("🔥")
                except Exception:
                    pass

                await self.config.guild(guild).last_posted_unix.set(next_unix)

            except discord.Forbidden:
                self.bot.logger.warning(
                    "FridayGameNight: missing permissions to send in %s (guild %s)",
                    channel.id,
                    guild.id,
                )
            except Exception:
                self.bot.logger.exception(
                    "Failed to post FridayGameNight message in guild %s channel %s",
                    guild.id,
                    channel.id,
                )

    @commands.group()
    @commands.guild_only()
    async def gamenight(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @gamenight.command()
    @commands.check(mod_check)
    async def enable(self, ctx: commands.Context):
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("✅ FridayGameNight enabled for this server.")

    @gamenight.command()
    @commands.check(mod_check)
    async def disable(self, ctx: commands.Context):
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ FridayGameNight disabled for this server.")

    @gamenight.command()
    @commands.check(mod_check)
    async def status(self, ctx: commands.Context):
        data = await self.config.guild(ctx.guild).all()

        enabled = data.get("enabled", False)
        channel_id = data.get("channel_id")
        message = data.get("message", DEFAULT_MESSAGE)
        day = int(data.get("day", 5))
        hour = int(data.get("hour", 19))
        minute = int(data.get("minute", 0))
        last_posted = int(data.get("last_posted_unix", 0))

        next_dt = compute_next_occurrence(day, hour, minute)
        next_unix = int(next_dt.timestamp())

        embed = discord.Embed(title="FridayGameNight Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)

        if channel_id:
            ch = ctx.guild.get_channel(channel_id)
            embed.add_field(name="Channel", value=(ch.mention if ch else f"<#{channel_id}>"), inline=True)
        else:
            embed.add_field(name="Channel", value="Not set", inline=True)

        embed.add_field(
            name="Next Post (UTC)",
            value=f"{next_dt.strftime('%Y-%m-%d %H:%M UTC')}\n<t:{next_unix}:t> (<t:{next_unix}:R>)",
            inline=False,
        )
        embed.add_field(name="Day (1=Mon..7=Sun)", value=str(day), inline=True)
        embed.add_field(name="Time (HH:MM UTC)", value=f"{hour:02d}:{minute:02d}", inline=True)

        preview = re.sub(r"\{\s*unix\s*\}", str(next_unix), message, flags=re.IGNORECASE)
        if len(preview) > 1000:
            preview = preview[:990] + "…"

        embed.add_field(name="Message (preview)", value=box(preview, lang=""), inline=False)

        if last_posted:
            lp = datetime.fromtimestamp(last_posted, tz=timezone.utc)
            embed.set_footer(text=f"Last posted: {lp.strftime('%Y-%m-%d %H:%M UTC')} (unix {last_posted})")

        await ctx.send(embed=embed)

    @gamenight.command(name="setchannel")
    @commands.check(mod_check)
    async def setchannel(self, ctx: commands.Context, *, channel: str):
        channel_id = None

        match = re.search(r"/channels/\d+/(\d+)", channel)
        if match:
            channel_id = int(match.group(1))
        else:
            match = re.search(r"<#(\d+)>", channel)
            if match:
                channel_id = int(match.group(1))
            elif channel.isdigit():
                channel_id = int(channel)

        if channel_id is None:
            try:
                conv = commands.TextChannelConverter()
                ch = await conv.convert(ctx, channel)
                channel_id = ch.id
            except Exception:
                await ctx.send("❌ Could not parse that channel.")
                return

        ch_obj = ctx.guild.get_channel(channel_id)
        if ch_obj is None:
            await ctx.send("❌ Channel not found in this guild.")
            return

        await self.config.guild(ctx.guild).channel_id.set(channel_id)
        await ctx.send(f"✅ Game night channel set to {ch_obj.mention}")

    @gamenight.command(name="setmessage")
    @commands.check(mod_check)
    async def setmessage(self, ctx: commands.Context, *, message: str):
        await self.config.guild(ctx.guild).message.set(message)
        await ctx.send("✅ Message updated.")

    @gamenight.command(name="day")
    @commands.check(mod_check)
    async def set_day(self, ctx: commands.Context, day: int):
        if day < 1 or day > 7:
            await ctx.send("❌ Day must be between 1 and 7.")
            return

        await self.config.guild(ctx.guild).day.set(day)
        await ctx.send(f"✅ Day set to {day}.")

    @gamenight.command(name="time")
    @commands.check(mod_check)
    async def set_time(self, ctx: commands.Context, time_str: str):
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            await ctx.send("❌ Time must be in HH:MM format.")
            return

        hour = int(m.group(1))
        minute = int(m.group(2))

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            await ctx.send("❌ Invalid time.")
            return

        await self.config.guild(ctx.guild).hour.set(hour)
        await self.config.guild(ctx.guild).minute.set(minute)

        await ctx.send(f"✅ Time set to {hour:02d}:{minute:02d} UTC.")

    @commands.is_owner()
    @gamenight.command(name="raw")
    async def _raw_config(self, ctx: commands.Context):
        data = await self.config.guild(ctx.guild).all()
        await ctx.send(box(str(data)))
