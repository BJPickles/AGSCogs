from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, time as dtime, timezone
import re
from typing import Optional

import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

DEFAULT_MESSAGE = (
    "## FRIDAY NIGHT GAME NIGHT\n"
    "Hello folks, happy Friday! Weekly game night starts at <t:{unix}:t> (<t:{unix}:R>).\n\n"
    "Pop a reaction below if you're planning on joining 🔥"
)

# day mapping: user input 1=Monday,...,7=Sunday -> python weekday 0=Monday..6=Sunday
def userday_to_pyweekday(userday: int) -> int:
    return (userday - 1) % 7

def compute_next_occurrence(user_day: int, hour: int, minute: int, now: Optional[datetime] = None) -> datetime:
    """
    Compute the next occurrence (UTC) of the given user_day (1=Mon..7=Sun), hour/minute (UTC).
    Returns a timezone-aware datetime in UTC.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    target_weekday = userday_to_pyweekday(user_day)
    today_weekday = now.weekday()  # 0=Mon
    # Build candidate for today at the requested time
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
        # same day but time already passed -> next week
        days_ahead = 7
    elif days_ahead == 0 and candidate > now:
        days_ahead = 0
    # if not today, advance
    if days_ahead:
        candidate = candidate + timedelta(days=days_ahead)
    return candidate

class FridayGameNight(commands.Cog):
    """
    FridayGameNight - Automated weekly game night announcer.

    Commands:
    [p]gamenight enable
    [p]gamenight disable
    [p]gamenight status
    [p]gamenight setchannel <channel>
    [p]gamenight setmessage <message>
    [p]gamenight day <1-7>   (1=Monday ... 5=Friday default)
    [p]gamenight time <HH:MM> (24-hour GMT/UTC time, default 19:00)

    Message placeholders:
    - {unix} will be replaced with the unix timestamp for the next scheduled post,
      e.g. "<t:{unix}:t> (<t:{unix}:R>)" in the default message.
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA1B2C3D4E5, force_registration=True)
        default_guild = {
            "enabled": False,
            "channel_id": None,
            "message": DEFAULT_MESSAGE,
            "day": 5,  # 1=Mon..5=Fri default
            "hour": 19,
            "minute": 0,
            "last_posted_unix": 0,
        }
        self.config.register_guild(**default_guild)
        self._task = None
        self._loop_lock = asyncio.Lock()

    async def cog_load(self) -> None:
        # start background task
        self._task = self.bot.loop.create_task(self._background_loop())

    async def cog_unload(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    async def _background_loop(self) -> None:
        """
        Background loop checks every 30 seconds for due posts.
        Uses last_posted_unix to avoid double-posting.
        """
        await self.bot.wait_until_ready()
        while True:
            try:
                # iterate guilds (we use guild-specific config)
                for guild in list(self.bot.guilds):
                    try:
                        await self._handle_guild(guild)
                    except Exception:
                        # avoid task dying for a single guild error
                        self.bot.logger.exception("Error handling gamenight for guild %s", guild.id)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                self.bot.logger.exception("Unexpected error in FridayGameNight background loop")
                await asyncio.sleep(30)

    async def _handle_guild(self, guild: discord.Guild) -> None:
        data = await self.config.guild(guild).all()
        if not data.get("enabled", False):
            return
        channel_id = data.get("channel_id")
        if not channel_id:
            return
        channel = guild.get_channel(channel_id)
        if channel is None:
            # channel missing/not found
            return
        # compute next occurrence
        user_day = int(data.get("day", 5))
        hour = int(data.get("hour", 19))
        minute = int(data.get("minute", 0))
        next_dt = compute_next_occurrence(user_day, hour, minute)
        next_unix = int(next_dt.replace(tzinfo=timezone.utc).timestamp())
        last_posted = int(data.get("last_posted_unix", 0))
        now_unix = int(datetime.now(timezone.utc).timestamp())
        # if next_unix is in the past (shouldn't happen), skip
        # Post if we've not posted for this target yet and time is now or passed within 90s
        if last_posted >= next_unix:
            return
        # allow a small window for posting: if now >= next_unix and now <= next_unix + 300 (5 mins)
        if now_unix >= next_unix and now_unix <= next_unix + 300:
            # Acquire a lightweight lock to avoid race conditions across guild handling
            async with self._loop_lock:
                # re-check last_posted under lock
                data2 = await self.config.guild(guild).all()
                if int(data2.get("last_posted_unix", 0)) >= next_unix:
                    return
                message_template = data2.get("message", DEFAULT_MESSAGE)
                # replace {unix} placeholder if present
                final_message = message_template.replace("{unix}", str(next_unix))
                # if there are any stray {unix} patterns with spaces, cover them too
                final_message = re.sub(r"\{ *unix *\}", str(next_unix), final_message, flags=re.IGNORECASE)

                # send message allowing role pings and user pings (but not everyone by default)
                allowed = discord.AllowedMentions(roles=True, users=True, everyone=False)
                try:
                    sent = await channel.send(final_message, allowed_mentions=allowed)
                    # add reaction for joining
                    try:
                        await sent.add_reaction("🔥")
                    except Exception:
                        # ignore reaction failures
                        pass
                    # persist last_posted_unix
                    await self.config.guild(guild).last_posted_unix.set(next_unix)
                except discord.Forbidden:
                    # bot cannot send in channel - ignore
                    self.bot.logger.warning("FridayGameNight: missing permissions to send in %s (guild %s)", channel.id, guild.id)
                except Exception:
                    self.bot.logger.exception("Failed to post FridayGameNight message in guild %s channel %s", guild.id, channel.id)

    # -----------------------
    # Commands (group)
    # -----------------------
    @commands.group()
    @commands.guild_only()
    async def gamenight(self, ctx: commands.Context):
        """
        Manage the FridayGameNight automated postings.

        Use subcommands: enable, disable, status, setchannel, setmessage, day, time
        """
        if ctx.invoked_subcommand is None:
            # show help/usage
            await ctx.send_help(ctx.command)

    # permission: guild admins / manage_guild
    def _mod_check():
        return commands.has_permissions(manage_guild=True)

    @gamenight.command()
    @commands.check(_mod_check())
    async def enable(self, ctx: commands.Context):
        """Enable weekly automatic posting."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send(":white_check_mark: FridayGameNight enabled for this server.")

    @gamenight.command()
    @commands.check(_mod_check())
    async def disable(self, ctx: commands.Context):
        """Disable weekly automatic posting."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send(":x: FridayGameNight disabled for this server.")

    @gamenight.command()
    @commands.check(_mod_check())
    async def status(self, ctx: commands.Context):
        """Show current gamenight configuration and next scheduled post."""
        data = await self.config.guild(ctx.guild).all()
        enabled = data.get("enabled", False)
        channel_id = data.get("channel_id")
        message = data.get("message", DEFAULT_MESSAGE)
        day = int(data.get("day", 5))
        hour = int(data.get("hour", 19))
        minute = int(data.get("minute", 0))
        last_posted = int(data.get("last_posted_unix", 0))

        # compute next occurrence
        next_dt = compute_next_occurrence(day, hour, minute)
        next_unix = int(next_dt.timestamp())

        embed = discord.Embed(title="FridayGameNight Status", color=discord.Color.blurple())
        embed.add_field(name="Enabled", value=str(enabled), inline=True)
        if channel_id:
            ch = ctx.guild.get_channel(channel_id)
            embed.add_field(name="Channel", value=f"{ch.mention if ch else f'<#{channel_id}>'}", inline=True)
        else:
            embed.add_field(name="Channel", value="Not set", inline=True)
        embed.add_field(name="Next Post (UTC)", value=f"{next_dt.strftime('%Y-%m-%d %H:%M UTC')}\n<t:{next_unix}:t> (<t:{next_unix}:R>)", inline=False)
        embed.add_field(name="Day (1=Mon..7=Sun)", value=str(day), inline=True)
        embed.add_field(name="Time (HH:MM UTC)", value=f"{hour:02d}:{minute:02d}", inline=True)
        # show message preview with replaced timestamp
        preview = message.replace("{unix}", str(next_unix))
        preview = re.sub(r"\{ *unix *\}", str(next_unix), preview, flags=re.IGNORECASE)
        # truncate preview if too long
        if len(preview) > 1000:
            preview = preview[:990] + "…"
        embed.add_field(name="Message (preview)", value=box(preview, lang=""), inline=False)
        if last_posted:
            lp = datetime.fromtimestamp(last_posted, tz=timezone.utc)
            embed.set_footer(text=f"Last posted: {lp.strftime('%Y-%m-%d %H:%M UTC')} (unix {last_posted})")
        await ctx.send(embed=embed)

    @gamenight.command(name="setchannel")
    @commands.check(_mod_check())
    async def setchannel(self, ctx: commands.Context, *, channel: str):
        """
        Set the channel where the message will be posted.

        Accepts:
         - channel mention (#channel)
         - channel id
         - channel link format: https://discord.com/channels/<guild_id>/<channel_id>
        """
        # try to extract id from a link or mention
        channel_id = None
        # link format
        match = re.search(r"/channels/\d+/(\d+)", channel)
        if match:
            channel_id = int(match.group(1))
        else:
            # mention like <#123456>
            match = re.search(r"<#(\d+)>", channel)
            if match:
                channel_id = int(match.group(1))
            else:
                # direct numeric id?
                if channel.isdigit():
                    channel_id = int(channel)
        if channel_id is None:
            # try to convert using TextChannel converter (handles names and mentions)
            try:
                conv = commands.TextChannelConverter()
                ch = await conv.convert(ctx, channel)
                channel_id = ch.id
            except Exception:
                await ctx.send(":x: Could not parse that channel. Provide a mention, id, or channel link.")
                return
        ch_obj = ctx.guild.get_channel(channel_id)
        if ch_obj is None:
            await ctx.send(":x: Channel not found in this guild.")
            return
        # set config
        await self.config.guild(ctx.guild).channel_id.set(channel_id)
        await ctx.send(f":white_check_mark: Game night channel set to {ch_obj.mention} (ID {channel_id}).")

    @gamenight.command(name="setmessage")
    @commands.check(_mod_check())
    async def setmessage(self, ctx: commands.Context, *, message: str):
        """
        Set the message the bot will post.

        Important: Use the placeholder {unix} where you want the unix timestamp for the next scheduled post inserted.
        Example default message uses: <t:{unix}:t> (<t:{unix}:R>)
        Role mentions (e.g. <@&roleid>) in this message will actually ping those roles.
        """
        # store the message as provided
        await self.config.guild(ctx.guild).message.set(message)
        await ctx.send(":white_check_mark: Message updated. Use `gamenight status` to preview with the next timestamp.")

    @gamenight.command(name="day")
    @commands.check(_mod_check())
    async def set_day(self, ctx: commands.Context, day: int):
        """
        Set the weekday for posting.
        day: 1 = Monday, ... 5 = Friday (default), ... 7 = Sunday
        """
        if day < 1 or day > 7:
            await ctx.send(":x: Day must be between 1 and 7 (1=Monday ... 7=Sunday).")
            return
        await self.config.guild(ctx.guild).day.set(day)
        await ctx.send(f":white_check_mark: Scheduled day set to {day} (1=Mon ... 7=Sun).")

    @gamenight.command(name="time")
    @commands.check(_mod_check())
    async def set_time(self, ctx: commands.Context, time_str: str):
        """
        Set the time (24-hour) in GMT/UTC for posting.
        Format: HH:MM (e.g. 19:00)
        """
        m = re.match(r"^(\d{1,2}):(\d{2})$", time_str.strip())
        if not m:
            await ctx.send(":x: Time must be in HH:MM 24-hour format (e.g. 19:00).")
            return
        hour = int(m.group(1))
        minute = int(m.group(2))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            await ctx.send(":x: Invalid hour/minute.")
            return
        await self.config.guild(ctx.guild).hour.set(hour)
        await self.config.guild(ctx.guild).minute.set(minute)
        await ctx.send(f":white_check_mark: Scheduled time set to {hour:02d}:{minute:02d} UTC/GMT.")

    @gamenight.command(name="help")
    async def gamenight_help(self, ctx: commands.Context):
        """Show help and best-practice tips for the cog."""
        help_text = (
            "**FridayGameNight Help**\n\n"
            "Commands (require Manage Server permission):\n"
            "- `gamenight enable` — start automatic weekly posts\n"
            "- `gamenight disable` — stop automatic posts\n"
            "- `gamenight status` — show next post, channel, message preview\n"
            "- `gamenight setchannel <channel>` — set posting channel (mention, id, or link)\n"
            "- `gamenight setmessage <message>` — set the message. Use `{unix}` where you want the unix timestamp inserted.\n"
            "- `gamenight day <1-7>` — 1=Mon ... 5=Fri (default)\n"
            "- `gamenight time <HH:MM>` — 24-hour UTC/GMT (default 19:00)\n\n"
            "Notes:\n"
            "- The default message includes `<t:{unix}:t> (<t:{unix}:R>)` which will be replaced at post time.\n"
            "- Role mentions like `<@&ROLEID>` included in the message WILL ping that role (the cog allows role pings).\n"
            "- Make sure the bot has Send Messages and Add Reactions permissions in the target channel.\n"
        )
        await ctx.send(help_text)

    # nice convenience: allow owner to view raw guild config values (optional)
    @commands.is_owner()
    @gamenight.command(name="raw")
    async def _raw_config(self, ctx: commands.Context):
        """(Owner only) Dump raw config for the guild — debugging helper."""
        data = await self.config.guild(ctx.guild).all()
        await ctx.send(box(str(data)))
