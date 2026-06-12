import asyncio
from typing import List, Optional, Tuple

import discord
from redbot.core import Config, commands, modlog
from redbot.core.bot import Red


class AGSVRoleKick(commands.Cog):
    """
    AGSVRoleKick

    Bot-owner-only manual role kick tool for culling inactive or flagged users.

    Main command:
        [p]agsrolekick @role <reason>
    """

    # IMPORTANT:
    # Do not change this identifier after release.
    # Red Config uses it to find this cog's persisted settings.
    CONFIG_IDENTIFIER = 0xA651D001

    DEFAULT_INVITE_LINK = "https://discord.gg/smvhW9t"

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=self.CONFIG_IDENTIFIER,
            force_registration=True,
        )

        self.config.register_guild(
            log_channel=None,
            invite_link=self.DEFAULT_INVITE_LINK,
            blacklisted_roles=[],
            blacklisted_users=[],
        )

    # -------------------------------------------------------------------------
    # Utility helpers
    # -------------------------------------------------------------------------

    async def _safe_send(
        self,
        ctx: commands.Context,
        message: str,
        *,
        channel: Optional[discord.abc.Messageable] = None,
        allowed_mentions: Optional[discord.AllowedMentions] = None,
    ) -> bool:
        """
        Try to send feedback somewhere useful.

        Order:
        1. Explicit channel, if provided.
        2. Command channel.
        3. Command author's DM.

        Returns True if anything was sent.
        """
        allowed_mentions = allowed_mentions or discord.AllowedMentions.none()

        destinations = []

        if channel is not None:
            destinations.append(channel)

        if ctx.channel not in destinations:
            destinations.append(ctx.channel)

        if ctx.author not in destinations:
            destinations.append(ctx.author)

        for destination in destinations:
            try:
                await destination.send(message, allowed_mentions=allowed_mentions)
                return True
            except (discord.Forbidden, discord.HTTPException, AttributeError):
                continue

        return False

    async def _send_long_message(
        self,
        ctx: commands.Context,
        message: str,
        *,
        channel: Optional[discord.abc.Messageable] = None,
    ) -> bool:
        """
        Send a long report safely without exceeding Discord's message length.
        """
        chunks = []

        while len(message) > 1900:
            split_at = message.rfind("\n", 0, 1900)
            if split_at == -1:
                split_at = 1900

            chunks.append(message[:split_at])
            message = message[split_at:].lstrip()

        if message:
            chunks.append(message)

        sent_any = False

        for chunk in chunks:
            sent = await self._safe_send(ctx, chunk, channel=channel)
            sent_any = sent_any or sent

        return sent_any

    def _format_channel_problem(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
    ) -> Optional[str]:
        """
        Return a human-readable channel permission/access problem, or None.
        """
        if channel.guild.id != guild.id:
            return "That channel is not in this server."

        me = guild.me
        perms = channel.permissions_for(me)

        if not perms.view_channel:
            return f"I do not have access to {channel.mention}. I need `View Channel`."

        if not perms.send_messages:
            return f"I can see {channel.mention}, but I cannot post there. I need `Send Messages`."

        if not perms.embed_links:
            return (
                f"I can post in {channel.mention}, but I do not have `Embed Links`. "
                "This cog can still work, but Red's ModLog may need embed permissions depending on your setup."
            )

        return None

    async def _get_report_channel(
        self,
        guild: discord.Guild,
    ) -> Tuple[Optional[discord.TextChannel], Optional[str]]:
        """
        Return configured report channel, plus an optional warning.
        """
        channel_id = await self.config.guild(guild).log_channel()

        if not channel_id:
            return None, "No AGSVRoleKick report channel is configured."

        channel = guild.get_channel(channel_id)

        if channel is None:
            return None, "The configured AGSVRoleKick report channel no longer exists."

        if not isinstance(channel, discord.TextChannel):
            return None, "The configured AGSVRoleKick report channel is not a text channel."

        problem = self._format_channel_problem(guild, channel)

        if problem:
            return None, problem

        return channel, None

    def _audit_reason(
        self,
        ctx: commands.Context,
        role: discord.Role,
        reason: str,
    ) -> str:
        """
        Discord audit log reasons have a practical length limit.
        Keep it concise while preserving owner and role context.
        """
        prefix = f"AGSVRoleKick by {ctx.author} ({ctx.author.id}) for role {role.name}: "
        max_total = 512

        remaining = max_total - len(prefix)

        if remaining <= 0:
            return prefix[:max_total]

        clean_reason = reason.strip()

        if len(clean_reason) > remaining:
            clean_reason = clean_reason[: remaining - 3] + "..."

        return prefix + clean_reason

    async def _try_chunk_guild(self, guild: discord.Guild) -> Optional[str]:
        """
        Try to improve member cache completeness.

        This requires the bot to have member access/intents available.
        If it fails, the command can still proceed with the current cache,
        but the final report should make this clear.
        """
        if guild.chunked:
            return None

        try:
            await guild.chunk(cache=True)
            return None
        except Exception as exc:
            return (
                "I could not fully chunk the member list before scanning the role. "
                "This usually means the bot is missing the Server Members Intent or Discord refused the request. "
                f"I will continue with the members currently visible in cache. Error: `{type(exc).__name__}: {exc}`"
            )

    async def _members_with_role(
        self,
        guild: discord.Guild,
        role: discord.Role,
    ) -> Tuple[List[discord.Member], Optional[str]]:
        """
        Return members with the role, plus optional cache warning.
        """
        cache_warning = await self._try_chunk_guild(guild)
        members = list(role.members)
        return members, cache_warning

    async def _is_user_blacklisted(
        self,
        guild: discord.Guild,
        member: discord.Member,
    ) -> bool:
        user_ids = await self.config.guild(guild).blacklisted_users()
        return member.id in user_ids

    async def _blacklisted_role_names_for(
        self,
        guild: discord.Guild,
        member: discord.Member,
    ) -> List[str]:
        blacklisted_role_ids = await self.config.guild(guild).blacklisted_roles()
        member_role_ids = {role.id for role in member.roles}

        matched = []

        for role_id in blacklisted_role_ids:
            if role_id in member_role_ids:
                role = guild.get_role(role_id)
                matched.append(role.name if role else f"Deleted role `{role_id}`")

        return matched

    def _basic_skip_reason(
        self,
        guild: discord.Guild,
        member: discord.Member,
    ) -> Optional[str]:
        """
        Skip reasons that do not require async config lookup.
        """
        me = guild.me

        if member.id == guild.owner_id:
            return "server owner"

        if member.id == me.id:
            return "bot itself"

        if member.bot and member.id == self.bot.user.id:
            return "bot itself"

        if member.top_role >= me.top_role:
            return "role hierarchy prevents me from kicking this member"

        return None

    async def _skip_reason_for_member(
        self,
        guild: discord.Guild,
        member: discord.Member,
    ) -> Optional[str]:
        """
        Return reason this member must be skipped, or None if kickable.
        """
        basic_reason = self._basic_skip_reason(guild, member)

        if basic_reason:
            return basic_reason

        if await self._is_user_blacklisted(guild, member):
            return "user blacklist"

        blacklisted_roles = await self._blacklisted_role_names_for(guild, member)

        if blacklisted_roles:
            return "role blacklist: " + ", ".join(blacklisted_roles)

        return None

    async def _confirm(
        self,
        ctx: commands.Context,
        role: discord.Role,
        reason: str,
        kickable_count: int,
        skipped_count: int,
    ) -> bool:
        prompt = (
            "**AGSVRoleKick confirmation required**\n\n"
            f"Target role: `{role.name}` (`{role.id}`)\n"
            f"Members eligible to kick: `{kickable_count}`\n"
            f"Members skipped by safeguards: `{skipped_count}`\n\n"
            f"Reason:\n```text\n{reason}\n```\n"
            "Type `confirm` within 30 seconds to continue.\n"
            "Anything else, or no response, will cancel this action."
        )

        await self._safe_send(ctx, prompt)

        def check(message: discord.Message) -> bool:
            return (
                message.author.id == ctx.author.id
                and message.channel.id == ctx.channel.id
                and message.content.lower().strip() == "confirm"
            )

        try:
            await self.bot.wait_for("message", timeout=30, check=check)
            return True
        except asyncio.TimeoutError:
            await self._safe_send(ctx, "AGSVRoleKick cancelled. No members were kicked.")
            return False

    async def _attempt_dm(
        self,
        member: discord.Member,
        guild: discord.Guild,
        role: discord.Role,
        invite_link: str,
    ) -> Tuple[bool, Optional[str]]:
        message = (
            f"Hi there {member.display_name}. "
            f"You've been kicked from {guild.name} because you've been tagged {role.name}.\n"
            f"If you'd like to rejoin, please use this invite link: {invite_link}"
        )

        try:
            await member.send(message)
            return True, None
        except discord.Forbidden:
            return False, "DM failed: user has DMs closed or blocked the bot"
        except discord.HTTPException as exc:
            return False, f"DM failed: Discord HTTP error `{exc}`"
        except Exception as exc:
            return False, f"DM failed: `{type(exc).__name__}: {exc}`"

    async def _create_modlog_case(
        self,
        ctx: commands.Context,
        member: discord.Member,
        role: discord.Role,
        reason: str,
    ) -> Tuple[bool, Optional[str]]:
        full_reason = f"{reason}\n\nAGSVRoleKick target role: {role.name} ({role.id})"

        try:
            await modlog.create_case(
                self.bot,
                ctx.guild,
                ctx.message.created_at,
                action_type="kick",
                user=member,
                moderator=ctx.author,
                reason=full_reason,
                channel=ctx.channel,
            )
            return True, None
        except Exception as exc:
            return False, f"ModLog case failed: `{type(exc).__name__}: {exc}`"

    # -------------------------------------------------------------------------
    # Main command group
    # -------------------------------------------------------------------------

    @commands.guild_only()
    @commands.is_owner()
    @commands.group(name="agsrolekick", invoke_without_command=True)
    async def agsrolekick(
        self,
        ctx: commands.Context,
        role: Optional[discord.Role] = None,
        *,
        reason: Optional[str] = None,
    ):
        """
        Kick every member with a role, with blacklist and confirmation safeguards.

        Usage:
            [p]agsrolekick @role <reason>
        """
        guild = ctx.guild

        if role is None:
            await self._safe_send(
                ctx,
                "Usage: `[p]agsrolekick @role <reason>`\n"
                "Example: `[p]agsrolekick @Inactive 90+ days inactive cleanup`",
            )
            return

        if reason is None or not reason.strip:
            await self._safe_send(ctx, "You must provide a reason: `[p]agsrolekick @role <reason>`")
            return

        reason = reason.strip()

        if not reason:
            await self._safe_send(ctx, "You must provide a non-empty reason.")
            return

        me = guild.me

        if role.id == guild.default_role.id:
            await self._safe_send(ctx, "I refuse to target `@everyone`.")
            return

        if not guild.me.guild_permissions.kick_members:
            await self._safe_send(
                ctx,
                "I cannot kick members in this server because I do not have `Kick Members`.",
            )
            return

        if role >= me.top_role:
            await self._safe_send(
                ctx,
                "Warning: the target role is above or equal to my highest role. "
                "I may still kick lower-ranked members who have it, but any member whose highest role "
                "is above or equal to mine will be skipped.",
            )

        report_channel, report_channel_warning = await self._get_report_channel(guild)

        if report_channel_warning:
            await self._safe_send(
                ctx,
                "Report channel warning: "
                f"{report_channel_warning}\n"
                "I will try to report in this command channel or DM you if needed.",
            )

        cache_warning = None
        members, cache_warning = await self._members_with_role(guild, role)

        if cache_warning:
            await self._safe_send(ctx, cache_warning)

        if not members:
            await self._safe_send(
                ctx,
                f"No visible members currently have the role `{role.name}`. No one was kicked.",
            )
            return

        kickable = []
        skipped = []

        for member in members:
            skip_reason = await self._skip_reason_for_member(guild, member)

            if skip_reason:
                skipped.append((member, skip_reason))
            else:
                kickable.append(member)

        if not kickable:
            skipped_preview = "\n".join(
                f"- {member} (`{member.id}`): {skip_reason}" for member, skip_reason in skipped[:20]
            )

            if len(skipped) > 20:
                skipped_preview += f"\n...and {len(skipped) - 20} more."

            await self._send_long_message(
                ctx,
                "No members are eligible to kick after safeguards.\n\n"
                f"Skipped members:\n{skipped_preview}",
            )
            return

        confirmed = await self._confirm(
            ctx,
            role,
            reason,
            kickable_count=len(kickable),
            skipped_count=len(skipped),
        )

        if not confirmed:
            return

        invite_link = await self.config.guild(guild).invite_link()
        audit_reason = self._audit_reason(ctx, role, reason)

        kicked = []
        kick_failures = []
        dm_success = []
        dm_failures = []
        modlog_success = []
        modlog_failures = []

        for member in kickable:
            dm_ok, dm_error = await self._attempt_dm(member, guild, role, invite_link)

            if dm_ok:
                dm_success.append(member)
            else:
                dm_failures.append((member, dm_error))

            try:
                await member.kick(reason=audit_reason)
                kicked.append(member)
            except discord.Forbidden:
                kick_failures.append(
                    (member, "Kick failed: Discord denied permission, likely role hierarchy or missing Kick Members")
                )
                await asyncio.sleep(1)
                continue
            except discord.HTTPException as exc:
                kick_failures.append((member, f"Kick failed: Discord HTTP error `{exc}`"))
                await asyncio.sleep(1)
                continue
            except Exception as exc:
                kick_failures.append((member, f"Kick failed: `{type(exc).__name__}: {exc}`"))
                await asyncio.sleep(1)
                continue

            modlog_ok, modlog_error = await self._create_modlog_case(ctx, member, role, reason)

            if modlog_ok:
                modlog_success.append(member)
            else:
                modlog_failures.append((member, modlog_error))

            # Small delay to avoid hammering Discord with DM/kick/modlog activity.
            await asyncio.sleep(1)

        report_lines = [
            "**AGSVRoleKick completed**",
            "",
            f"Server: `{guild.name}` (`{guild.id}`)",
            f"Target role: `{role.name}` (`{role.id}`)",
            f"Reason: `{reason}`",
            "",
            f"Visible members with role: `{len(members)}`",
            f"Eligible after safeguards: `{len(kickable)}`",
            f"Kicked: `{len(kicked)}`",
            f"Kick failures: `{len(kick_failures)}`",
            f"DMs sent: `{len(dm_success)}`",
            f"DM failures: `{len(dm_failures)}`",
            f"ModLog cases created: `{len(modlog_success)}`",
            f"ModLog failures: `{len(modlog_failures)}`",
            f"Skipped by safeguards: `{len(skipped)}`",
        ]

        if report_channel_warning:
            report_lines.extend(["", f"Report channel warning: {report_channel_warning}"])

        if cache_warning:
            report_lines.extend(["", f"Member cache warning: {cache_warning}"])

        if skipped:
            report_lines.append("")
            report_lines.append("**Skipped members**")

            for member, skip_reason in skipped[:40]:
                report_lines.append(f"- `{member}` (`{member.id}`): {skip_reason}")

            if len(skipped) > 40:
                report_lines.append(f"- ...and `{len(skipped) - 40}` more skipped members.")

        if kick_failures:
            report_lines.append("")
            report_lines.append("**Kick failures**")

            for member, failure in kick_failures[:40]:
                report_lines.append(f"- `{member}` (`{member.id}`): {failure}")

            if len(kick_failures) > 40:
                report_lines.append(f"- ...and `{len(kick_failures) - 40}` more kick failures.")

        if dm_failures:
            report_lines.append("")
            report_lines.append("**DM failures**")

            for member, failure in dm_failures[:40]:
                report_lines.append(f"- `{member}` (`{member.id}`): {failure}")

            if len(dm_failures) > 40:
                report_lines.append(f"- ...and `{len(dm_failures) - 40}` more DM failures.")

        if modlog_failures:
            report_lines.append("")
            report_lines.append("**ModLog failures**")

            for member, failure in modlog_failures[:40]:
                report_lines.append(f"- `{member}` (`{member.id}`): {failure}")

            if len(modlog_failures) > 40:
                report_lines.append(f"- ...and `{len(modlog_failures) - 40}` more ModLog failures.")

        report = "\n".join(report_lines)

        await self._send_long_message(ctx, report, channel=report_channel)

    # -------------------------------------------------------------------------
    # Settings commands
    # -------------------------------------------------------------------------

    @agsrolekick.command(name="setchannel")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_setchannel(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
    ):
        """
        Set the AGSVRoleKick report channel.

        Usage:
            [p]agsrolekick setchannel #channel
        """
        problem = self._format_channel_problem(ctx.guild, channel)

        if problem:
            await self._safe_send(ctx, problem)
            return

        await self.config.guild(ctx.guild).log_channel.set(channel.id)

        await self._safe_send(
            ctx,
            f"AGSVRoleKick report channel set to {channel.mention}.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @agsrolekick.command(name="clearchannel")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_clearchannel(self, ctx: commands.Context):
        """
        Clear the AGSVRoleKick report channel.

        Usage:
            [p]agsrolekick clearchannel
        """
        await self.config.guild(ctx.guild).log_channel.clear()
        await self._safe_send(ctx, "AGSVRoleKick report channel cleared.")

    @agsrolekick.command(name="setinvite")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_setinvite(
        self,
        ctx: commands.Context,
        invite_link: str,
    ):
        """
        Set the invite link used in pre-kick DMs.

        Usage:
            [p]agsrolekick setinvite https://discord.gg/example
        """
        invite_link = invite_link.strip()

        if not invite_link.startswith(("https://discord.gg/", "https://discord.com/invite/")):
            await self._safe_send(
                ctx,
                "That does not look like a Discord invite link. "
                "Please use a link like `https://discord.gg/smvhW9t`.",
            )
            return

        await self.config.guild(ctx.guild).invite_link.set(invite_link)
        await self._safe_send(ctx, f"AGSVRoleKick invite link set to: {invite_link}")

    @agsrolekick.command(name="settings")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_settings(self, ctx: commands.Context):
        """
        Show AGSVRoleKick settings.

        Usage:
            [p]agsrolekick settings
        """
        guild = ctx.guild
        data = await self.config.guild(guild).all()

        channel = guild.get_channel(data["log_channel"]) if data["log_channel"] else None

        blacklisted_roles = []

        for role_id in data["blacklisted_roles"]:
            role = guild.get_role(role_id)
            blacklisted_roles.append(f"{role.name} (`{role_id}`)" if role else f"Deleted role (`{role_id}`)")

        blacklisted_users = []

        for user_id in data["blacklisted_users"]:
            member = guild.get_member(user_id)
            blacklisted_users.append(f"{member} (`{user_id}`)" if member else f"User ID `{user_id}`")

        message = [
            "**AGSVRoleKick settings**",
            "",
            f"Report channel: {channel.mention if channel else '`Not set`'}",
            f"Invite link: {data['invite_link']}",
            "",
            "**Blacklisted roles:**",
            "\n".join(f"- {item}" for item in blacklisted_roles) if blacklisted_roles else "- None",
            "",
            "**Blacklisted users:**",
            "\n".join(f"- {item}" for item in blacklisted_users) if blacklisted_users else "- None",
        ]

        await self._send_long_message(ctx, "\n".join(message))

    # -------------------------------------------------------------------------
    # Blacklist commands
    # -------------------------------------------------------------------------

    @agsrolekick.group(name="blacklist")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist(self, ctx: commands.Context):
        """
        Manage AGSVRoleKick blacklists.

        Usage:
            [p]agsrolekick blacklist role add @role
            [p]agsrolekick blacklist role remove @role
            [p]agsrolekick blacklist role list
            [p]agsrolekick blacklist user add @user
            [p]agsrolekick blacklist user remove @user
            [p]agsrolekick blacklist user list
        """
        pass

    @agsrolekick_blacklist.group(name="role")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_role(self, ctx: commands.Context):
        """
        Manage protected roles.
        """
        pass

    @agsrolekick_blacklist_role.command(name="add")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_role_add(
        self,
        ctx: commands.Context,
        role: discord.Role,
    ):
        """
        Add a protected role.

        Members with this role will not be kicked by AGSVRoleKick.

        Usage:
            [p]agsrolekick blacklist role add @Staff
        """
        if role.id == ctx.guild.default_role.id:
            await self._safe_send(ctx, "I will not add `@everyone` to the role blacklist.")
            return

        async with self.config.guild(ctx.guild).blacklisted_roles() as roles:
            if role.id in roles:
                await self._safe_send(ctx, f"`{role.name}` is already blacklisted.")
                return

            roles.append(role.id)

        await self._safe_send(ctx, f"`{role.name}` added to the AGSVRoleKick role blacklist.")

    @agsrolekick_blacklist_role.command(name="remove")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_role_remove(
        self,
        ctx: commands.Context,
        role: discord.Role,
    ):
        """
        Remove a protected role.

        Usage:
            [p]agsrolekick blacklist role remove @Staff
        """
        async with self.config.guild(ctx.guild).blacklisted_roles() as roles:
            if role.id not in roles:
                await self._safe_send(ctx, f"`{role.name}` is not currently blacklisted.")
                return

            roles.remove(role.id)

        await self._safe_send(ctx, f"`{role.name}` removed from the AGSVRoleKick role blacklist.")

    @agsrolekick_blacklist_role.command(name="list")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_role_list(self, ctx: commands.Context):
        """
        List protected roles.

        Usage:
            [p]agsrolekick blacklist role list
        """
        role_ids = await self.config.guild(ctx.guild).blacklisted_roles()

        if not role_ids:
            await self._safe_send(ctx, "No roles are currently blacklisted.")
            return

        lines = ["**AGSVRoleKick role blacklist**"]

        for role_id in role_ids:
            role = ctx.guild.get_role(role_id)
            lines.append(f"- `{role.name}` (`{role_id}`)" if role else f"- Deleted role (`{role_id}`)")

        await self._send_long_message(ctx, "\n".join(lines))

    @agsrolekick_blacklist.group(name="user")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_user(self, ctx: commands.Context):
        """
        Manage protected users.
        """
        pass

    @agsrolekick_blacklist_user.command(name="add")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_user_add(
        self,
        ctx: commands.Context,
        user: discord.User,
    ):
        """
        Add a protected user.

        This user will not be kicked by AGSVRoleKick.

        Usage:
            [p]agsrolekick blacklist user add @User
        """
        async with self.config.guild(ctx.guild).blacklisted_users() as users:
            if user.id in users:
                await self._safe_send(ctx, f"`{user}` is already blacklisted.")
                return

            users.append(user.id)

        await self._safe_send(ctx, f"`{user}` added to the AGSVRoleKick user blacklist.")

    @agsrolekick_blacklist_user.command(name="remove")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_user_remove(
        self,
        ctx: commands.Context,
        user: discord.User,
    ):
        """
        Remove a protected user.

        Usage:
            [p]agsrolekick blacklist user remove @User
        """
        async with self.config.guild(ctx.guild).blacklisted_users() as users:
            if user.id not in users:
                await self._safe_send(ctx, f"`{user}` is not currently blacklisted.")
                return

            users.remove(user.id)

        await self._safe_send(ctx, f"`{user}` removed from the AGSVRoleKick user blacklist.")

    @agsrolekick_blacklist_user.command(name="list")
    @commands.guild_only()
    @commands.is_owner()
    async def agsrolekick_blacklist_user_list(self, ctx: commands.Context):
        """
        List protected users.

        Usage:
            [p]agsrolekick blacklist user list
        """
        user_ids = await self.config.guild(ctx.guild).blacklisted_users()

        if not user_ids:
            await self._safe_send(ctx, "No users are currently blacklisted.")
            return

        lines = ["**AGSVRoleKick user blacklist**"]

        for user_id in user_ids:
            member = ctx.guild.get_member(user_id)
            if member:
                lines.append(f"- `{member}` (`{user_id}`)")
            else:
                user = self.bot.get_user(user_id)
                lines.append(f"- `{user}` (`{user_id}`)" if user else f"- User ID `{user_id}`")

        await self._send_long_message(ctx, "\n".join(lines))