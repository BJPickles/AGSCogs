from __future__ import annotations

import asyncio
import logging
import random
import string
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

log = logging.getLogger("red.agsvoiceannounce")

DEFAULT_MESSAGES = ["{role} {user.mention} is in {channel.name}. Come join in!"]


class Placeholder:
    """Tiny object that supports {root} and {root.attr} template rendering."""

    def __init__(self, text: str, **attrs: Any):
        self._text = text
        for key, value in attrs.items():
            setattr(self, key, value)

    def __str__(self) -> str:
        return self._text

    def __format__(self, format_spec: str) -> str:
        return format(str(self), format_spec)

ALLOWED_PLACEHOLDERS = {
    "role",
    "role.mention",
    "user",
    "user.name",
    "user.display_name",
    "user.mention",
    "user.id",
    "channel",
    "channel.name",
    "channel.mention",
    "channel.id",
    "guild",
    "guild.name",
}


class AGSVoiceAnnounce(commands.Cog):
    """Announce qualifying public/invite-only AutoRoom voice sessions."""

    __author__ = "AEGIS Game Studios"
    __version__ = "1.0.0"

    def __init__(self, bot: Red):
        self.bot = bot
        # Do not change this identifier after release; it owns this cog's persisted Config.
        self.config = Config.get_conf(self, identifier=0xA651CE001, force_registration=True)
        self.config.register_guild(
            announcement_channel_id=None,
            ping_role_id=None,
            opt_out_role_id=None,
            blacklisted_voice_channel_ids=[],
            delay_seconds=15,
            rejoin_grace_seconds=300,
            messages=DEFAULT_MESSAGES,
            message_queue=[],
            last_message_index=None,
            custom_messages_started=False,
        )
        # Runtime voice-session tracking. Keyed by guild_id -> channel_id.
        self._sessions: Dict[int, Dict[int, Dict[str, Any]]] = defaultdict(dict)
        self._pending_tasks: Dict[Tuple[int, int], asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._ready_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._ready_task = self.bot.loop.create_task(self._reconcile_after_ready())

    async def cog_unload(self):
        if self._ready_task:
            self._ready_task.cancel()
            self._ready_task = None
        for task in list(self._pending_tasks.values()):
            task.cancel()
        self._pending_tasks.clear()

    async def _reconcile_after_ready(self):
        """Mark existing occupied voice channels as active so reloads do not duplicate announce."""
        await self.bot.wait_until_ready()
        now = time.monotonic()
        for guild in self.bot.guilds:
            for channel in guild.voice_channels:
                if self._human_members(channel):
                    self._sessions[guild.id][channel.id] = {
                        "active": True,
                        "announced": True,
                        "grace_until": 0.0,
                        "last_human_left": None,
                        "created_at": now,
                    }

    # ---------------------------------------------------------------------
    # Voice event/session logic
    # ---------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        if member.bot:
            return
        if before.channel and before.channel != after.channel:
            await self._refresh_channel_after_departure(before.channel)
        if after.channel and before.channel != after.channel:
            await self._consider_voice_join(member, after.channel)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        if isinstance(channel, discord.VoiceChannel):
            self._sessions[channel.guild.id].pop(channel.id, None)
            task = self._pending_tasks.pop((channel.guild.id, channel.id), None)
            if task:
                task.cancel()

    async def _refresh_channel_after_departure(self, channel: discord.abc.GuildChannel):
        if not isinstance(channel, discord.VoiceChannel):
            return
        guild_sessions = self._sessions[channel.guild.id]
        session = guild_sessions.get(channel.id)
        if not session:
            return
        if self._human_members(channel):
            session["active"] = True
            session["grace_until"] = 0.0
            return
        grace = await self.config.guild(channel.guild).rejoin_grace_seconds()
        session["active"] = False
        session["last_human_left"] = time.monotonic()
        session["grace_until"] = time.monotonic() + max(0, int(grace or 0))
        # If there are no bots either, keep the grace anyway. AutoRoom deletion will clear it.

    async def _consider_voice_join(self, member: discord.Member, channel: discord.abc.GuildChannel):
        if not isinstance(channel, discord.VoiceChannel):
            return

        guild = channel.guild
        now = time.monotonic()
        sessions = self._sessions[guild.id]
        existing = sessions.get(channel.id)

        if existing:
            if existing.get("active") or float(existing.get("grace_until", 0.0)) > now:
                # Same ongoing room/session; do not announce again. Mark human activity as active.
                existing["active"] = True
                existing["grace_until"] = 0.0
                return
            # Grace expired and the room stayed around with bots/no humans: new opportunity.
            sessions.pop(channel.id, None)

        # If Discord already shows multiple humans, do not let a late event spam an old room.
        if len(self._human_members(channel)) > 1:
            sessions[channel.id] = {
                "active": True,
                "announced": True,
                "grace_until": 0.0,
                "last_human_left": None,
                "created_at": now,
            }
            return

        data = await self.config.guild(guild).all()
        prelim_ok, _reason = await self._is_announcement_candidate(member, channel, data)
        # Any human starts a session, even if they personally opted out, to prevent later join spam.
        sessions[channel.id] = {
            "active": True,
            "announced": bool(not prelim_ok),
            "grace_until": 0.0,
            "last_human_left": None,
            "created_at": now,
        }
        if not prelim_ok:
            return

        key = (guild.id, channel.id)
        old_task = self._pending_tasks.pop(key, None)
        if old_task:
            old_task.cancel()
        task = self.bot.loop.create_task(self._delayed_announce(member.id, guild.id, channel.id))
        self._pending_tasks[key] = task

    async def _delayed_announce(self, member_id: int, guild_id: int, channel_id: int):
        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return
            delay = int(await self.config.guild(guild).delay_seconds() or 0)
            if delay > 0:
                await asyncio.sleep(delay)

            guild = self.bot.get_guild(guild_id)
            if not guild:
                return
            channel = guild.get_channel(channel_id)
            member = guild.get_member(member_id)
            if not isinstance(channel, discord.VoiceChannel) or member is None:
                return
            if member.voice is None or member.voice.channel is None or member.voice.channel.id != channel_id:
                return

            data = await self.config.guild(guild).all()
            ok, reason = await self._is_announcement_candidate(member, channel, data)
            if not ok:
                log.debug("AGSVoiceAnnounce skipped delayed announcement in %s: %s", guild.id, reason)
                return

            announce_channel = guild.get_channel(data.get("announcement_channel_id"))
            ping_role = guild.get_role(data.get("ping_role_id"))
            if not isinstance(announce_channel, discord.TextChannel) or ping_role is None:
                return

            message = await self._render_next_message(guild, member, channel, ping_role)
            allowed = discord.AllowedMentions(everyone=False, users=[member], roles=[ping_role], replied_user=False)
            await announce_channel.send(message, allowed_mentions=allowed)

            session = self._sessions[guild.id].setdefault(channel.id, {})
            session["active"] = True
            session["announced"] = True
            session["grace_until"] = 0.0
        except asyncio.CancelledError:
            raise
        except discord.Forbidden as exc:
            log.warning("AGSVoiceAnnounce lacked permission to announce in guild %s: %s", guild_id, exc)
            await self._notify_owners(guild_id, "I could not send a voice announcement because Discord denied permissions. Check my access to the configured announcement channel and role mention permissions.")
        except Exception:
            log.exception("AGSVoiceAnnounce failed while announcing in guild %s", guild_id)
            await self._notify_owners(guild_id, "I hit an unexpected error while trying to send a voice announcement. Please check the bot logs.")
        finally:
            self._pending_tasks.pop((guild_id, channel_id), None)

    async def _is_announcement_candidate(
        self,
        member: discord.Member,
        voice_channel: discord.VoiceChannel,
        data: Optional[dict] = None,
    ) -> Tuple[bool, str]:
        guild = member.guild
        if member.bot:
            return False, "member is a bot"
        data = data or await self.config.guild(guild).all()

        if voice_channel.id in set(data.get("blacklisted_voice_channel_ids") or []):
            return False, "voice channel is blacklisted"

        announce_channel = guild.get_channel(data.get("announcement_channel_id"))
        if not isinstance(announce_channel, discord.TextChannel):
            return False, "announcement channel is not configured or missing"

        ping_role = guild.get_role(data.get("ping_role_id"))
        if ping_role is None:
            return False, "ping role is not configured or missing"

        opt_role = guild.get_role(data.get("opt_out_role_id"))
        if opt_role is None:
            return False, "opt-out role is not configured or missing"
        if opt_role in member.roles:
            return False, "member is opted out"

        if self._is_private_voice_channel(voice_channel, ping_role):
            return False, "voice channel is private for the ping role"

        ok, reason = self._check_announce_channel_permissions(announce_channel, ping_role)
        if not ok:
            return False, reason

        return True, "ok"

    @staticmethod
    def _human_members(channel: discord.VoiceChannel) -> List[discord.Member]:
        return [m for m in channel.members if not m.bot]

    @staticmethod
    def _is_private_voice_channel(channel: discord.VoiceChannel, audience_role: discord.Role) -> bool:
        perms = channel.permissions_for(audience_role)
        return not perms.view_channel

    def _check_announce_channel_permissions(
        self, channel: discord.TextChannel, ping_role: Optional[discord.Role] = None
    ) -> Tuple[bool, str]:
        me = channel.guild.me
        if me is None:
            return False, "I cannot resolve my guild member permissions"
        perms = channel.permissions_for(me)
        if not perms.view_channel:
            return False, "I do not have access to the configured announcement channel"
        if not perms.send_messages:
            return False, "I can see the configured announcement channel, but I cannot send messages there"
        if ping_role is not None and not ping_role.mentionable and not perms.mention_everyone:
            return False, "I cannot true-ping the configured role. Make the role mentionable or give me Mention Everyone permission in the announcement channel"
        return True, "ok"

    # ---------------------------------------------------------------------
    # Messages/placeholders
    # ---------------------------------------------------------------------

    async def _render_next_message(
        self,
        guild: discord.Guild,
        member: discord.Member,
        voice_channel: discord.VoiceChannel,
        ping_role: discord.Role,
    ) -> str:
        template = await self._next_message_template(guild)
        return self._render_template(template, member, voice_channel, ping_role)

    async def _next_message_template(self, guild: discord.Guild) -> str:
        async with self._lock:
            data = await self.config.guild(guild).all()
            messages = list(data.get("messages") or DEFAULT_MESSAGES)
            if not messages:
                messages = list(DEFAULT_MESSAGES)
                await self.config.guild(guild).messages.set(messages)

            queue = list(data.get("message_queue") or [])
            last = data.get("last_message_index")
            valid_indices = set(range(len(messages)))
            queue = [idx for idx in queue if isinstance(idx, int) and idx in valid_indices]

            if not queue:
                queue = list(range(len(messages)))
                random.shuffle(queue)
                if len(queue) > 1 and last is not None and queue[0] == last:
                    # Swap first with another item to avoid boundary repeat.
                    swap_idx = next((i for i, idx in enumerate(queue[1:], start=1) if idx != last), None)
                    if swap_idx is not None:
                        queue[0], queue[swap_idx] = queue[swap_idx], queue[0]

            idx = queue.pop(0)
            await self.config.guild(guild).message_queue.set(queue)
            await self.config.guild(guild).last_message_index.set(idx)
            return messages[idx]

    @staticmethod
    def _render_template(
        template: str,
        member: discord.Member,
        channel: discord.VoiceChannel,
        role: discord.Role,
    ) -> str:
        mapping = {
            "role": Placeholder(role.mention, mention=role.mention),
            "user": Placeholder(
                member.display_name,
                name=member.name,
                display_name=member.display_name,
                mention=member.mention,
                id=str(member.id),
            ),
            "channel": Placeholder(
                channel.mention,
                name=channel.name,
                mention=channel.mention,
                id=str(channel.id),
            ),
            "guild": Placeholder(member.guild.name, name=member.guild.name),
        }

        class SafeDict(dict):
            def __missing__(self, key):
                return "{" + key + "}"

        return template.format_map(SafeDict(mapping))

    @staticmethod
    def _validate_template(template: str) -> Tuple[bool, str]:
        formatter = string.Formatter()
        try:
            for _literal, field_name, _format_spec, _conversion in formatter.parse(template):
                if not field_name:
                    continue
                # Disallow indexing/conversions/format specs to keep templates predictable.
                if "[" in field_name or "]" in field_name:
                    return False, "Indexing placeholders are not supported."
                if field_name not in ALLOWED_PLACEHOLDERS:
                    return False, f"Unknown placeholder `{field_name}`."
        except ValueError as exc:
            return False, f"Invalid template: {exc}"
        return True, "ok"

    async def _reset_message_cycle(self, guild: discord.Guild):
        await self.config.guild(guild).message_queue.set([])
        await self.config.guild(guild).last_message_index.set(None)

    # ---------------------------------------------------------------------
    # Utility/feedback helpers
    # ---------------------------------------------------------------------

    async def _notify_owners(self, guild_id: int, message: str):
        owner_ids: Sequence[int] = []
        try:
            owner_ids = list(getattr(self.bot, "owner_ids", set()) or [])
            owner_id = getattr(self.bot, "owner_id", None)
            if owner_id:
                owner_ids = list(set(owner_ids) | {owner_id})
        except Exception:
            owner_ids = []
        if not owner_ids:
            return
        content = f"AGSVoiceAnnounce notice for guild `{guild_id}`: {message}"
        for uid in owner_ids:
            try:
                user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                await user.send(content)
            except Exception:
                continue

    async def _role_manage_check(self, ctx: commands.Context, role: discord.Role) -> bool:
        me = ctx.guild.me
        if me is None:
            await ctx.send("❌ I cannot resolve my own guild member object.")
            return False
        perms = ctx.channel.permissions_for(me) if isinstance(ctx.channel, discord.TextChannel) else me.guild_permissions
        if not me.guild_permissions.manage_roles:
            await ctx.send("❌ I do not have the **Manage Roles** permission.")
            return False
        if role >= me.top_role:
            await ctx.send("❌ I cannot manage that role because it is higher than or equal to my top role.")
            return False
        return True

    async def _get_member_from_optional(self, ctx: commands.Context, member: Optional[discord.Member]) -> Optional[discord.Member]:
        if member is None:
            return ctx.author if isinstance(ctx.author, discord.Member) else None
        if member != ctx.author and not await self.bot.is_owner(ctx.author):
            await ctx.send("❌ Only the bot owner can opt other users in or out.")
            return None
        return member

    async def _send_command_error(self, ctx: commands.Context, message: str):
        try:
            await ctx.send(message)
        except discord.Forbidden:
            try:
                await ctx.author.send(message)
            except Exception:
                log.warning("Could not send command feedback in guild %s", getattr(ctx.guild, "id", None))

    # ---------------------------------------------------------------------
    # Commands
    # ---------------------------------------------------------------------

    @commands.group(name="agsvoiceannounce", aliases=["agsva", "voiceannounce"], invoke_without_command=True)
    @commands.guild_only()
    async def agsvoiceannounce(self, ctx: commands.Context):
        """Configure AGS voice announcements."""
        await ctx.send_help(ctx.command)

    @agsvoiceannounce.command(name="channel")
    @commands.is_owner()
    @commands.guild_only()
    async def set_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the text channel where voice announcements are posted."""
        if channel.guild.id != ctx.guild.id:
            return await ctx.send("❌ That channel is not in this guild.")
        ok, reason = self._check_announce_channel_permissions(channel, ctx.guild.get_role(await self.config.guild(ctx.guild).ping_role_id()))
        if not ok:
            return await ctx.send(f"❌ {reason}")
        await self.config.guild(ctx.guild).announcement_channel_id.set(channel.id)
        await ctx.send(f"✅ Announcement channel set to {channel.mention}.")

    @agsvoiceannounce.command(name="pingrole")
    @commands.is_owner()
    @commands.guild_only()
    async def set_ping_role(self, ctx: commands.Context, role: discord.Role):
        """Set the role that will be true-pinged in announcements."""
        chan_id = await self.config.guild(ctx.guild).announcement_channel_id()
        if chan_id:
            channel = ctx.guild.get_channel(chan_id)
            if isinstance(channel, discord.TextChannel):
                ok, reason = self._check_announce_channel_permissions(channel, role)
                if not ok:
                    return await ctx.send(f"❌ {reason}")
        await self.config.guild(ctx.guild).ping_role_id.set(role.id)
        await ctx.send(f"✅ Ping role set to {role.mention}. I will use controlled allowed mentions so this role is the one pinged.")

    @agsvoiceannounce.command(name="optrole")
    @commands.is_owner()
    @commands.guild_only()
    async def set_opt_role(self, ctx: commands.Context, role: discord.Role):
        """Set the role that marks a member as opted out."""
        if not await self._role_manage_check(ctx, role):
            return
        await self.config.guild(ctx.guild).opt_out_role_id.set(role.id)
        await ctx.send(f"✅ Opt-out role set to {role.mention}.")

    @agsvoiceannounce.command(name="delay")
    @commands.is_owner()
    @commands.guild_only()
    async def set_delay(self, ctx: commands.Context, seconds: int):
        """Set the announcement delay in seconds. Use 0 for immediate posting."""
        if seconds < 0:
            return await ctx.send("❌ Delay cannot be negative.")
        if seconds > 3600:
            return await ctx.send("❌ Delay cannot exceed 3600 seconds.")
        await self.config.guild(ctx.guild).delay_seconds.set(seconds)
        await ctx.send(f"✅ Announcement delay set to {seconds} second(s).")

    @agsvoiceannounce.command(name="rejoingrace", aliases=["rejoin_grace", "grace"])
    @commands.is_owner()
    @commands.guild_only()
    async def set_rejoin_grace(self, ctx: commands.Context, seconds: int):
        """Set the grace period before a bot-held room can announce again."""
        if seconds < 0:
            return await ctx.send("❌ Rejoin grace cannot be negative.")
        if seconds > 86400:
            return await ctx.send("❌ Rejoin grace cannot exceed 86400 seconds.")
        await self.config.guild(ctx.guild).rejoin_grace_seconds.set(seconds)
        await ctx.send(f"✅ Rejoin grace set to {seconds} second(s).")

    @agsvoiceannounce.group(name="blacklist", invoke_without_command=True)
    @commands.is_owner()
    @commands.guild_only()
    async def blacklist(self, ctx: commands.Context):
        """Manage voice channels that should never announce."""
        await ctx.send_help(ctx.command)

    @blacklist.command(name="add")
    @commands.is_owner()
    @commands.guild_only()
    async def blacklist_add(self, ctx: commands.Context, channel: discord.VoiceChannel):
        ids = list(await self.config.guild(ctx.guild).blacklisted_voice_channel_ids())
        if channel.id in ids:
            return await ctx.send("ℹ️ That voice channel is already blacklisted.")
        ids.append(channel.id)
        await self.config.guild(ctx.guild).blacklisted_voice_channel_ids.set(ids)
        await ctx.send(f"✅ Blacklisted {channel.mention}.")

    @blacklist.command(name="remove")
    @commands.is_owner()
    @commands.guild_only()
    async def blacklist_remove(self, ctx: commands.Context, channel: discord.VoiceChannel):
        ids = list(await self.config.guild(ctx.guild).blacklisted_voice_channel_ids())
        if channel.id not in ids:
            return await ctx.send("ℹ️ That voice channel is not blacklisted.")
        ids.remove(channel.id)
        await self.config.guild(ctx.guild).blacklisted_voice_channel_ids.set(ids)
        await ctx.send(f"✅ Removed {channel.mention} from the blacklist.")

    @blacklist.command(name="list")
    @commands.is_owner()
    @commands.guild_only()
    async def blacklist_list(self, ctx: commands.Context):
        ids = list(await self.config.guild(ctx.guild).blacklisted_voice_channel_ids())
        if not ids:
            return await ctx.send("No blacklisted voice channels are configured.")
        lines = []
        for cid in ids:
            ch = ctx.guild.get_channel(cid)
            lines.append(f"- {ch.mention if ch else f'Missing channel `{cid}`'}")
        await ctx.send(box("\n".join(lines)))

    @agsvoiceannounce.group(name="message", invoke_without_command=True)
    @commands.is_owner()
    @commands.guild_only()
    async def message(self, ctx: commands.Context):
        """Manage rotating announcement messages."""
        await ctx.send_help(ctx.command)

    @message.command(name="add")
    @commands.is_owner()
    @commands.guild_only()
    async def message_add(self, ctx: commands.Context, *, template: str):
        template = template.strip()
        if not template:
            return await ctx.send("❌ Message cannot be empty.")
        ok, reason = self._validate_template(template)
        if not ok:
            return await ctx.send(f"❌ {reason}\nAllowed placeholders: `{', '.join(sorted(ALLOWED_PLACEHOLDERS))}`")
        data = await self.config.guild(ctx.guild).all()
        messages = list(data.get("messages") or DEFAULT_MESSAGES)
        if not data.get("custom_messages_started"):
            messages = [template]
            await self.config.guild(ctx.guild).custom_messages_started.set(True)
        else:
            messages.append(template)
        await self.config.guild(ctx.guild).messages.set(messages)
        await self._reset_message_cycle(ctx.guild)
        await ctx.send("✅ Message added and the random cycle was reset.")

    @message.command(name="remove")
    @commands.is_owner()
    @commands.guild_only()
    async def message_remove(self, ctx: commands.Context, index: int):
        messages = list(await self.config.guild(ctx.guild).messages() or DEFAULT_MESSAGES)
        if not (1 <= index <= len(messages)):
            return await ctx.send("❌ Invalid message number.")
        removed = messages.pop(index - 1)
        if not messages:
            messages = list(DEFAULT_MESSAGES)
            await self.config.guild(ctx.guild).custom_messages_started.set(False)
        await self.config.guild(ctx.guild).messages.set(messages)
        await self._reset_message_cycle(ctx.guild)
        await ctx.send(f"✅ Removed message #{index}: {removed}")

    @message.command(name="list")
    @commands.is_owner()
    @commands.guild_only()
    async def message_list(self, ctx: commands.Context):
        messages = list(await self.config.guild(ctx.guild).messages() or DEFAULT_MESSAGES)
        lines = [f"{i + 1}: {msg}" for i, msg in enumerate(messages)]
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(box(page))
            await asyncio.sleep(0.5)

    @message.command(name="reset")
    @commands.is_owner()
    @commands.guild_only()
    async def message_reset(self, ctx: commands.Context):
        await self.config.guild(ctx.guild).messages.set(list(DEFAULT_MESSAGES))
        await self.config.guild(ctx.guild).custom_messages_started.set(False)
        await self._reset_message_cycle(ctx.guild)
        await ctx.send("✅ Messages reset to the default and the random cycle was reset.")

    @agsvoiceannounce.command(name="optout")
    @commands.guild_only()
    async def optout(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        """Opt yourself out, or as bot owner opt another member out."""
        target = await self._get_member_from_optional(ctx, member)
        if target is None:
            return
        role_id = await self.config.guild(ctx.guild).opt_out_role_id()
        role = ctx.guild.get_role(role_id) if role_id else None
        if role is None:
            return await ctx.send("❌ No opt-out role is configured, or the configured role was deleted.")
        if not await self._role_manage_check(ctx, role):
            return
        if role in target.roles:
            return await ctx.send(f"ℹ️ {target.mention} is already opted out.")
        try:
            await target.add_roles(role, reason=f"AGSVoiceAnnounce opt-out requested by {ctx.author}")
        except discord.Forbidden:
            return await ctx.send("❌ Discord denied the role change. Check my role hierarchy and Manage Roles permission.")
        except discord.HTTPException as exc:
            return await ctx.send(f"❌ Discord rejected the role change: `{exc}`")
        await ctx.send(f"✅ {target.mention} is now opted out.")

    @agsvoiceannounce.command(name="optin")
    @commands.guild_only()
    async def optin(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        """Opt yourself back in, or as bot owner opt another member in."""
        target = await self._get_member_from_optional(ctx, member)
        if target is None:
            return
        role_id = await self.config.guild(ctx.guild).opt_out_role_id()
        role = ctx.guild.get_role(role_id) if role_id else None
        if role is None:
            return await ctx.send("❌ No opt-out role is configured, or the configured role was deleted.")
        if not await self._role_manage_check(ctx, role):
            return
        if role not in target.roles:
            return await ctx.send(f"ℹ️ {target.mention} is already opted in.")
        try:
            await target.remove_roles(role, reason=f"AGSVoiceAnnounce opt-in requested by {ctx.author}")
        except discord.Forbidden:
            return await ctx.send("❌ Discord denied the role change. Check my role hierarchy and Manage Roles permission.")
        except discord.HTTPException as exc:
            return await ctx.send(f"❌ Discord rejected the role change: `{exc}`")
        await ctx.send(f"✅ {target.mention} is now opted in.")

    @agsvoiceannounce.command(name="status")
    @commands.is_owner()
    @commands.guild_only()
    async def status(self, ctx: commands.Context):
        """Show opted-in/out status for guild members."""
        role_id = await self.config.guild(ctx.guild).opt_out_role_id()
        role = ctx.guild.get_role(role_id) if role_id else None
        if role is None:
            return await ctx.send("❌ No valid opt-out role is configured.")

        humans = [m for m in ctx.guild.members if not m.bot]
        opted_out = [m for m in humans if role in m.roles]
        opted_in = [m for m in humans if role not in m.roles]

        header = f"Humans: {len(humans)} | Opted in: {len(opted_in)} | Opted out: {len(opted_out)}"
        lines = [header, "", "Opted out:"]
        lines.extend(f"- {m.display_name} (`{m.id}`)" for m in sorted(opted_out, key=lambda x: x.display_name.lower()))
        lines.append("")
        lines.append("Opted in:")
        lines.extend(f"- {m.display_name} (`{m.id}`)" for m in sorted(opted_in, key=lambda x: x.display_name.lower()))

        for page_no, page in enumerate(pagify("\n".join(lines), page_length=3500), start=1):
            embed = discord.Embed(
                title=f"AGSVoiceAnnounce Status — Page {page_no}",
                description=page,
                color=discord.Color.blurple(),
            )
            await ctx.send(embed=embed)
            await asyncio.sleep(1.0)

    @agsvoiceannounce.command(name="settings")
    @commands.is_owner()
    @commands.guild_only()
    async def settings(self, ctx: commands.Context):
        """Show current AGSVoiceAnnounce configuration."""
        data = await self.config.guild(ctx.guild).all()
        announce = ctx.guild.get_channel(data.get("announcement_channel_id"))
        ping = ctx.guild.get_role(data.get("ping_role_id"))
        opt = ctx.guild.get_role(data.get("opt_out_role_id"))
        blacklist_ids = data.get("blacklisted_voice_channel_ids") or []
        embed = discord.Embed(title="AGSVoiceAnnounce Settings", color=discord.Color.blurple())
        embed.add_field(name="Announcement channel", value=announce.mention if announce else "Not set / missing", inline=False)
        embed.add_field(name="Ping role", value=ping.mention if ping else "Not set / missing", inline=True)
        embed.add_field(name="Opt-out role", value=opt.mention if opt else "Not set / missing", inline=True)
        embed.add_field(name="Delay", value=f"{data.get('delay_seconds', 15)}s", inline=True)
        embed.add_field(name="Rejoin grace", value=f"{data.get('rejoin_grace_seconds', 300)}s", inline=True)
        embed.add_field(name="Messages", value=str(len(data.get("messages") or DEFAULT_MESSAGES)), inline=True)
        embed.add_field(name="Blacklisted channels", value=str(len(blacklist_ids)), inline=True)
        if announce and ping:
            ok, reason = self._check_announce_channel_permissions(announce, ping)
            embed.add_field(name="Announcement permission check", value=("✅ OK" if ok else f"❌ {reason}"), inline=False)
        await ctx.send(embed=embed)

    @agsvoiceannounce.command(name="test")
    @commands.is_owner()
    @commands.guild_only()
    async def test(
        self,
        ctx: commands.Context,
        target_or_channel: Optional[Union[discord.Member, discord.VoiceChannel]] = None,
        channel: Optional[discord.VoiceChannel] = None,
    ):
        """Send a test announcement using the configured channel, role, and templates."""
        target: Optional[discord.Member]
        if isinstance(target_or_channel, discord.VoiceChannel):
            target = ctx.author if isinstance(ctx.author, discord.Member) else None
            channel = target_or_channel
        elif isinstance(target_or_channel, discord.Member):
            target = target_or_channel
        else:
            target = ctx.author if isinstance(ctx.author, discord.Member) else None

        if target is None:
            return await ctx.send("❌ I could not resolve the test member.")
        if channel is None:
            if target.voice and isinstance(target.voice.channel, discord.VoiceChannel):
                channel = target.voice.channel
            else:
                return await ctx.send("❌ Provide a voice channel, or run this while the test member is in voice.")
        data = await self.config.guild(ctx.guild).all()
        ok, reason = await self._is_announcement_candidate(target, channel, data)
        if not ok:
            return await ctx.send(f"❌ Test failed: {reason}")
        announce = ctx.guild.get_channel(data.get("announcement_channel_id"))
        ping = ctx.guild.get_role(data.get("ping_role_id"))
        if not isinstance(announce, discord.TextChannel) or ping is None:
            return await ctx.send("❌ Announcement channel or ping role is missing.")
        msg = await self._render_next_message(ctx.guild, target, channel, ping)
        await announce.send(msg, allowed_mentions=discord.AllowedMentions(everyone=False, users=[target], roles=[ping], replied_user=False))
        await ctx.send(f"✅ Test announcement sent to {announce.mention}.")
