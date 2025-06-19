import asyncio
import uuid
import time
import logging
from datetime import datetime

import discord
from discord import TextChannel, Guild, Member
from discord.ui import View, Button

from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.activity")

DEFAULT_GUILD = {
    "default_channel_id": None,      # Default channel for public activities
    "log_channel_id":    None,       # Audit log channel
    "prune_summary_channel": None,   # Monthly‐prune summary channel
    "templates": {},                 # Saved templates
    "instances": {},                 # All live & scheduled instances
}


class ActionButton(Button):
    """A tiny Button subclass that takes a callback function."""
    def __init__(self, *, label: str, style: discord.ButtonStyle, custom_id: str, cb):
        super().__init__(label=label, style=style, custom_id=custom_id)
        self._cb = cb

    async def callback(self, interaction: discord.Interaction):
        await self._cb(interaction)


class PublicActivityView(View):
    """Join/Leave buttons for public activities."""
    def __init__(self, cog: "Activities", iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.add_item(ActionButton(
            label="Join",
            style=discord.ButtonStyle.green,
            custom_id=f"act:public:join:{iid}",
            cb=self.join,
        ))
        self.add_item(ActionButton(
            label="Leave",
            style=discord.ButtonStyle.red,
            custom_id=f"act:public:leave:{iid}",
            cb=self.leave,
        ))

    async def join(self, interaction: discord.Interaction):
        await self.cog._handle_public_join(interaction, self.iid)

    async def leave(self, interaction: discord.Interaction):
        await self.cog._handle_public_leave(interaction, self.iid)


class InviteView(View):
    """
    Invite buttons for private/scheduled activities.
      • rsvp=True   → Accept/Decline for RSVP stage.
      • reminder=True → Leave only (for reminders at start).
      • otherwise  → Accept/Decline/Reply for live private invites.
    """
    def __init__(
        self,
        cog: "Activities",
        iid: str,
        target_id: int,
        *,
        rsvp: bool = False,
        reminder: bool = False,
    ):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.target_id = target_id

        if rsvp:
            # scheduled‐RSVP stage
            self.add_item(ActionButton(
                label="Accept",
                style=discord.ButtonStyle.green,
                custom_id=f"act:rsvp:yes:{iid}:{target_id}",
                cb=self.rsvp_yes,
            ))
            self.add_item(ActionButton(
                label="Decline",
                style=discord.ButtonStyle.red,
                custom_id=f"act:rsvp:no:{iid}:{target_id}",
                cb=self.rsvp_no,
            ))
        elif reminder:
            # reminder at start → only Leave
            self.add_item(ActionButton(
                label="Leave",
                style=discord.ButtonStyle.red,
                custom_id=f"act:reminder:leave:{iid}:{target_id}",
                cb=self.reminder_leave,
            ))
        else:
            # live private invite
            self.add_item(ActionButton(
                label="Accept",
                style=discord.ButtonStyle.green,
                custom_id=f"act:invite:yes:{iid}:{target_id}",
                cb=self.invite_yes,
            ))
            self.add_item(ActionButton(
                label="Decline",
                style=discord.ButtonStyle.red,
                custom_id=f"act:invite:no:{iid}:{target_id}",
                cb=self.invite_no,
            ))
            self.add_item(ActionButton(
                label="Reply",
                style=discord.ButtonStyle.gray,
                custom_id=f"act:invite:reply:{iid}:{target_id}",
                cb=self.invite_reply,
            ))

    async def rsvp_yes(self, interaction: discord.Interaction):
        await self.cog._handle_rsvp(interaction, self.iid, self.target_id, True)

    async def rsvp_no(self, interaction: discord.Interaction):
        await self.cog._handle_rsvp(interaction, self.iid, self.target_id, False)

    async def reminder_leave(self, interaction: discord.Interaction):
        await self.cog._handle_reminder_leave(interaction, self.iid, self.target_id)

    async def invite_yes(self, interaction: discord.Interaction):
        await self.cog._handle_invite_accept(interaction, self.iid, self.target_id)

    async def invite_no(self, interaction: discord.Interaction):
        await self.cog._handle_invite_decline(interaction, self.iid, self.target_id)

    async def invite_reply(self, interaction: discord.Interaction):
        # open a Modal to type a reply
        class ReplyModal(discord.ui.Modal):
            def __init__(self_inner):
                super().__init__(title="Send a message to the owner")
                self_inner.response = discord.ui.TextInput(
                    label="Your message",
                    style=discord.TextStyle.paragraph,
                    placeholder="Type your message…",
                    max_length=500,
                    required=True,
                )
                self_inner.add_item(self_inner.response)

            async def on_submit(self_inner, modal_interaction: discord.Interaction):
                await modal_interaction.response.send_message(
                    "Your message has been sent to the activity owner.", ephemeral=True
                )
                await self.cog._handle_invite_reply(
                    modal_interaction,
                    self.iid,
                    self.target_id,
                    self_inner.response.value,
                )

        await interaction.response.send_modal(ReplyModal())


class ExtendView(View):
    """Extend/Finalize buttons after auto-end."""
    def __init__(self, cog: "Activities", iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.add_item(ActionButton(
            label="Extend 12 h",
            style=discord.ButtonStyle.green,
            custom_id=f"act:extend:{iid}",
            cb=self.extend,
        ))
        self.add_item(ActionButton(
            label="Finalize now",
            style=discord.ButtonStyle.red,
            custom_id=f"act:finalize:{iid}",
            cb=self.finalize,
        ))

    async def extend(self, interaction: discord.Interaction):
        await self.cog._handle_extend(interaction, self.iid)

    async def finalize(self, interaction: discord.Interaction):
        await self.cog._handle_finalize(interaction, self.iid)


class Activities(commands.Cog):
    """Activities Cog with scheduling, RSVP, dynamic embeds, logs."""
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=9876543210123456, force_registration=True
        )
        self.config.register_guild(**DEFAULT_GUILD)
        # Restore persistent views & schedule tasks
        bot.loop.create_task(self._startup_tasks())
        bot.loop.create_task(self._monthly_prune_scheduler())

    # ──────────────────────────────────────────────────────────────────────────
    # Startup / View Restoration / Schedulers
    # ──────────────────────────────────────────────────────────────────────────
    async def _startup_tasks(self):
        await self.bot.wait_until_ready()
        now = time.time()

        for guild in self.bot.guilds:
            data = await self.config.guild(guild).all()
            insts = data["instances"]
            for iid, inst in insts.items():
                status = inst["status"]
                msgs = inst["message_ids"]

                # Public OPEN
                if status == "OPEN" and inst["public"] and msgs.get("public"):
                    self.bot.add_view(
                        PublicActivityView(self, iid),
                        message_id=msgs["public"],
                    )

                # Live private invites
                if status == "OPEN" and not inst["public"]:
                    for uid, mid in msgs.get("invites", {}).items():
                        self.bot.add_view(
                            InviteView(self, iid, int(uid)),
                            message_id=mid,
                        )

                # Scheduled RSVP invites
                if status == "SCHEDULED":
                    for uid, mid in msgs.get("rsvps", {}).items():
                        self.bot.add_view(
                            InviteView(self, iid, int(uid), rsvp=True),
                            message_id=mid,
                        )
                    sched = inst.get("scheduled_time", 0)
                    if sched > now:
                        self.bot.loop.create_task(
                            self._schedule_start(guild.id, iid, sched - now)
                        )

                # Reminder buttons
                if status == "OPEN" and not inst["public"]:
                    for uid, mid in msgs.get("reminders", {}).items():
                        self.bot.add_view(
                            InviteView(self, iid, int(uid), reminder=True),
                            message_id=mid,
                        )

                # Extend/Finalize after auto-end
                if msgs.get("extend"):
                    self.bot.add_view(
                        ExtendView(self, iid),
                        message_id=msgs["extend"],
                    )

                # Schedule auto-end
                if status == "OPEN" and inst.get("end_time"):
                    delay = inst["end_time"] - now
                    if delay < 0:
                        delay = 0
                    self.bot.loop.create_task(
                        self._auto_end_task(guild.id, iid, delay)
                    )

    async def _monthly_prune_scheduler(self):
        await self.bot.wait_until_ready()
        while True:
            now = datetime.utcnow()
            # Compute next 1st of month
            if now.month == 12:
                nxt = datetime(now.year + 1, 1, 1)
            else:
                nxt = datetime(now.year, now.month + 1, 1)
            delay = (nxt - now).total_seconds()
            await asyncio.sleep(delay)

            for guild in self.bot.guilds:
                insts = await self.config.guild(guild).instances()
                pruned = []
                for iid, inst in list(insts.items()):
                    if inst["status"] == "ENDED":
                        insts.pop(iid)
                        pruned.append((iid, inst))
                await self.config.guild(guild).instances.set(insts)

                cid = await self.config.guild(guild).prune_summary_channel()
                if pruned and cid:
                    ch = guild.get_channel(cid)
                    if ch:
                        lines = "\n".join(
                            f"`{i[:8]}` • {inst['title']}" for i, inst in pruned
                        )
                        try:
                            await ch.send(f"Auto-pruned {len(pruned)} activities:\n{lines}")
                        except Exception:
                            log.exception("Failed sending prune summary")

    # ──────────────────────────────────────────────────────────────────────────
    # Embed Builder & Logging
    # ──────────────────────────────────────────────────────────────────────────
    
        def _build_embed(self, inst: dict, guild: Guild) -> discord.Embed:
        parts = []
        for uid in inst["participants"]:
            m = guild.get_member(int(uid))
            parts.append(m.display_name if m else f"User#{uid}")

        curr = len(parts)
        maxs = inst.get("max_slots")
        if maxs:
            ratio = curr / maxs
            emoji = "🟢" if ratio < 0.5 else ("🟠" if ratio < 1 else "🔴")
            slots = f"{curr}/{maxs}"
        else:
            emoji = "🟢"
            slots = f"{curr}/∞"

        title = f"{emoji} {inst['title']}"
        e = discord.Embed(
            title=title,
            description=inst.get("description", "No description."),
            color=discord.Color.blurple(),
        )

        # ---- FIXED add_field calls ----
        owner = guild.get_member(inst["owner_id"]) or self.bot.get_user(inst["owner_id"])
        e.add_field(
            name="Owner",
            value=owner.mention if owner else "Unknown",
            inline=True,
        )
        e.add_field(
            name="Slots",
            value=slots,
            inline=True,
        )
        # ------------------------------

        sched = inst.get("scheduled_time")
        if sched:
            e.add_field(
                name="Scheduled",
                value=f"<t:{int(sched)}:F> (<t:{int(sched)}:R>)",
                inline=False,
            )

        if parts:
            e.add_field(
                name="Participants",
                value="\n".join(parts),
                inline=False,
            )

        chan_id = inst.get("channel_id")
        if chan_id:
            ch = guild.get_channel(chan_id)
            if ch:
                e.set_footer(text=f"In {ch.mention}")

        return e

    async def _log(self, guild: Guild, message: str):
        cid = await self.config.guild(guild).log_channel_id()
        if not cid:
            return
        ch = guild.get_channel(cid)
        if not ch:
            return
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            await ch.send(f"[{ts}] {message}")
        except Exception:
            log.exception("Failed to send log message")

    # ──────────────────────────────────────────────────────────────────────────
    # Auto-end & Scheduling
    # ──────────────────────────────────────────────────────────────────────────
    async def _auto_end_task(self, guild_id: int, iid: str, delay: float):
        await asyncio.sleep(delay)
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst["status"] != "OPEN":
            return

        # Mark ENDED
        inst["status"] = "ENDED"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # Remove public buttons
        pm = inst["message_ids"].get("public")
        cid = inst.get("channel_id")
        if pm and cid:
            ch = guild.get_channel(cid)
            if ch:
                try:
                    msg = await ch.fetch_message(pm)
                    await msg.edit(embed=self._build_embed(inst, guild), view=None)
                except Exception:
                    pass

        # DM owner for extend/finalize
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                e2 = discord.Embed(
                    title=f"Activity auto-ended: {inst['title']}",
                    description=(
                        "This activity has automatically ended after 12 hours.\n\n"
                        "Click **Extend 12 h** to keep it open, or **Finalize now**."
                    ),
                    color=discord.Color.orange(),
                )
                view = ExtendView(self, iid)
                dm = await owner.send(embed=e2, view=view)
                inst["message_ids"]["extend"] = dm.id
                insts[iid] = inst
                await self.config.guild(guild).instances.set(insts)
            except Exception:
                log.exception("Failed to DM owner about auto-end")

        await self._log(
            guild,
            f"Auto-ended activity `{iid[:8]}` (“{inst['title']}”)."
        )

    async def _schedule_start(self, guild_id: int, iid: str, delay: float):
        await asyncio.sleep(delay)
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst["status"] != "SCHEDULED":
            return

        now = time.time()
        inst["status"] = "OPEN"
        inst["start_time"] = now
        inst["end_time"] = now + 12 * 3600
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # Public or private
        if inst["public"]:
            # Post public embed
            ch = guild.get_channel(inst["channel_id"])
            if ch:
                e = self._build_embed(inst, guild)
                view = PublicActivityView(self, iid)
                try:
                    msg = await ch.send(embed=e, view=view)
                    inst["message_ids"]["public"] = msg.id
                    insts[iid] = inst
                    await self.config.guild(guild).instances.set(insts)
                    self.bot.add_view(view, message_id=msg.id)
                except Exception:
                    pass
            await self._log(
                guild,
                f"Scheduled public `{iid[:8]}` has now started."
            )
        else:
            # Send reminders only to accepted RSVPs
            for uid_str, state in list(inst["rsvps"].items()):
                if state != "ACCEPTED":
                    continue
                uid = int(uid_str)
                inst["participants"].append(uid_str)
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    e = discord.Embed(
                        title=f"🔔 Reminder: {inst['title']} is starting now",
                        description=inst.get("description", ""),
                        color=discord.Color.blurple(),
                    )
                    e.set_footer(text="Click Leave if you can’t make it.")
                    view = InviteView(self, iid, uid, reminder=True)
                    msg = await dm.send(embed=e, view=view)
                    inst["message_ids"].setdefault("reminders", {})[uid_str] = msg.id
                    self.bot.add_view(view, message_id=msg.id)
                except Exception:
                    pass
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            await self._log(
                guild,
                f"Scheduled private `{iid[:8]}` started; reminders sent."
            )

        # Schedule auto-end
        self.bot.loop.create_task(
            self._auto_end_task(guild.id, iid, 12 * 3600)
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Commands: activity, setdefault, logchannel, prunechannel, list, info, prune
    # ──────────────────────────────────────────────────────────────────────────
    @commands.group(name="activity", invoke_without_command=True)
    @commands.guild_only()
    async def activity(self, ctx):
        """Create, schedule, or manage activities."""
        await ctx.send_help(ctx.command)

    @activity.command(name="setdefault")
    @checks.guildowner()
    async def set_default(self, ctx, channel: TextChannel = None):
        """Set or clear the default public-post channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).default_channel_id.set(cid)
        await ctx.send(
            f"Default public channel {'set to ' + channel.mention if channel else 'cleared'}."
        )

    @activity.command(name="logchannel")
    @checks.guildowner()
    async def set_logchannel(self, ctx, channel: TextChannel = None):
        """Set or clear the audit log channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).log_channel_id.set(cid)
        await ctx.send(
            f"Audit log channel {'set to ' + channel.mention if channel else 'cleared'}."
        )

    @activity.command(name="prunechannel")
    @checks.guildowner()
    async def set_prunechannel(self, ctx, channel: TextChannel = None):
        """Set or clear the monthly prune summary channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).prune_summary_channel.set(cid)
        await ctx.send(
            f"Prune summary channel {'set to ' + channel.mention if channel else 'cleared'}."
        )

    @activity.command(name="list")
    async def list_activities(self, ctx):
        """List all activities (live & scheduled)."""
        insts = await self.config.guild(ctx.guild).instances()
        if not insts:
            return await ctx.send("No activities found.")
        embed = discord.Embed(
            title="Activities",
            color=discord.Color.green(),
        )
        for iid, inst in insts.items():
            owner = ctx.guild.get_member(inst["owner_id"])
            status = inst["status"]
            sched = inst.get("scheduled_time")
            sched_str = (
                f" • starts <t:{int(sched)}:R>"
                if sched and status == "SCHEDULED"
                else ""
            )
            embed.add_field(
                name=f"{iid[:8]}: {inst['title']}",
                value=(
                    f"Owner: {owner.mention if owner else inst['owner_id']}\n"
                    f"Status: {status}{sched_str}"
                ),
                inline=False,
            )
        await ctx.send(embed=embed)

    @activity.command(name="info")
    async def info_activity(self, ctx, iid: str):
        """Show detailed information on an activity."""
        insts = await self.config.guild(ctx.guild).instances()
        full = next((k for k in insts if k.startswith(iid.lower())), None)
        if not full:
            return await ctx.send("No such activity.")
        inst = insts[full]
        embed = self._build_embed(inst, ctx.guild)
        embed.title = f"Info: {embed.title}"
        embed.set_footer(text=f"ID: {full[:8]} • Status: {inst['status']}")
        await ctx.send(embed=embed)

    @activity.command(name="prune")
    @checks.guildowner()
    async def prune_activities(self, ctx, status: str = "ENDED", older_than: int = None):
        """
        Manually prune activities by status and optional minimum age (days).
        """
        insts = await self.config.guild(ctx.guild).instances()
        now = time.time()
        removed = []
        for iid, inst in list(insts.items()):
            if inst["status"] != status.upper():
                continue
            created = inst.get("created_at", now)
            if older_than is not None and (now - created) < older_than * 86400:
                continue
            # delete public message if any
            pm = inst["message_ids"].get("public")
            pc = inst.get("channel_id")
            if pm and pc:
                ch = ctx.guild.get_channel(pc)
                if ch:
                    try:
                        msg = await ch.fetch_message(pm)
                        await msg.delete()
                    except Exception:
                        pass
            insts.pop(iid)
            removed.append(iid)
        await self.config.guild(ctx.guild).instances.set(insts)
        await ctx.send(f"Pruned {len(removed)} activities.")

    # ──────────────────────────────────────────────────────────────────────────
    # Templates: save, list, remove
    # ──────────────────────────────────────────────────────────────────────────
    @activity.group(name="template", invoke_without_command=True)
    @checks.guildowner()
    async def template(self, ctx):
        """Manage saved activity templates."""
        await ctx.send_help(ctx.command)

    @template.command(name="save")
    @checks.guildowner()
    async def template_save(self, ctx, name: str):
        """
        Save a template:
        title, description, public/private, channel or DM targets, max slots, schedule.
        """
        name = name.lower()
        existing = await self.config.guild(ctx.guild).templates()
        if name in existing:
            return await ctx.send("That template already exists.")

        await ctx.send("**Template Setup:** 300 s per question; type `skip` to omit optional.")

        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        try:
            # 1) Title
            await ctx.send("1) Title:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            title = m.content.strip()[:100]
            await m.add_reaction("✅")

            # 2) Description
            await ctx.send("2) Description (or `skip`):")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            desc = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]
            await m.add_reaction("✅")

            # 3) Public or Private
            await ctx.send("3) Public or Private? (`public`/`private`)")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                val = m.content.strip().lower()
                if val in ("public", "p", "private", "priv"):
                    public = val.startswith("p") and not val.startswith("pr")
                    await m.add_reaction("✅")
                    break
                else:
                    await ctx.send("Invalid. Type `public` or `private`.")

            channel_id = None
            dm_targets = []
            if public:
                # 4a) Channel
                def chan_check(m): return m.author == ctx.author and m.channel == ctx.channel
                await ctx.send("4) Channel? Mention it or type `default`:")
                while True:
                    m = await self.bot.wait_for("message", check=chan_check, timeout=120)
                    if m.content.lower().startswith("default"):
                        channel_id = await self.config.guild(ctx.guild).default_channel_id()
                        await m.add_reaction("✅")
                        break
                    elif m.channel_mentions:
                        channel_id = m.channel_mentions[0].id
                        await m.add_reaction("✅")
                        break
                    else:
                        await ctx.send("Invalid. Mention a text channel or `default`.")
            else:
                # 4b) DM targets
                await ctx.send("4) DM whom? Mention @role/@users or type `all`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    cont = m.content.lower()
                    if cont.startswith("all"):
                        dm_targets = [m.id for m in ctx.guild.members if not m.bot]
                        await m.add_reaction("✅")
                        break
                    elif m.role_mentions:
                        dm_targets = [u.id for u in m.role_mentions[0].members if not u.bot]
                        await m.add_reaction("✅")
                        break
                    elif m.mentions:
                        dm_targets = [u.id for u in m.mentions if not u.bot]
                        await m.add_reaction("✅")
                        break
                    else:
                        await ctx.send("Invalid. Mention users/role or `all`.")

            # 5) Max slots
            await ctx.send("5) Max slots? Number or `none`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                cont = m.content.strip()
                if cont.lower() in ("none", "n"):
                    max_s = None
                    await m.add_reaction("✅")
                    break
                elif cont.isdigit():
                    max_s = int(cont)
                    await m.add_reaction("✅")
                    break
                else:
                    await ctx.send("Invalid. Enter a number or `none`.")

            # 6) Scheduled
            await ctx.send("6) Scheduled? `YYYY-MM-DD HH:MM` UTC or `skip`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=300)
                cont = m.content.strip()
                if cont.lower().startswith("skip"):
                    sched = None
                    await m.add_reaction("✅")
                    break
                try:
                    dt = datetime.strptime(cont, "%Y-%m-%d %H:%M")
                    sched = dt.timestamp()
                    await m.add_reaction("✅")
                    break
                except Exception:
                    await ctx.send("Invalid format. Use `YYYY-MM-DD HH:MM` UTC or `skip`.")

        except asyncio.TimeoutError:
            return await ctx.send("❌ Timed out; aborting template setup.")

        tpl = {
            "title": title,
            "description": desc,
            "public": public,
            "channel_id": channel_id,
            "dm_targets": dm_targets,
            "max_slots": max_s,
            "scheduled_time": sched,
        }
        existing[name] = tpl
        await self.config.guild(ctx.guild).templates.set(existing)
        await ctx.send(f"Template `{name}` saved.")

    @template.command(name="list")
    async def template_list(self, ctx):
        """List saved templates."""
        tpls = await self.config.guild(ctx.guild).templates()
        if not tpls:
            return await ctx.send("No templates.")
        lines = [
            f"`{n}` • {'Pub' if t['public'] else 'Priv'} • “{t['title']}”"
            for n, t in tpls.items()
        ]
        await ctx.send("\n".join(lines))

    @template.command(name="remove")
    @checks.guildowner()
    async def template_remove(self, ctx, name: str):
        """Remove a saved template."""
        name = name.lower()
        tpls = await self.config.guild(ctx.guild).templates()
        if name not in tpls:
            return await ctx.send("No such template.")
        tpls.pop(name)
        await self.config.guild(ctx.guild).templates.set(tpls)
        await ctx.send(f"Template `{name}` removed.")

    # ──────────────────────────────────────────────────────────────────────────
    # Start / Schedule Command (with interactive wizard + use‐template)
    # ──────────────────────────────────────────────────────────────────────────
    @activity.command(name="start")
    async def activity_start(self, ctx, template: str = None):
        """
        Start or schedule an activity.
        Optionally specify a saved template name to pre-fill fields.
        """
        guild = ctx.guild
        author = ctx.author
        tpls = await self.config.guild(guild).templates()
        tpl = tpls.get(template.lower()) if template else None

        # Build `inst` either from template or via wizard
        inst: dict = {}
        if tpl:
            # Copy template fields
            inst.update(tpl)
            # fill defaults
            if inst["public"] and not inst.get("channel_id"):
                inst["channel_id"] = await self.config.guild(guild).default_channel_id()
        else:
            # Wizard
            await ctx.send("**Activity Wizard** (300 s/question; type `skip` to omit optional)")
            def check(m): return m.author == author and m.channel == ctx.channel
            try:
                # 1) Title
                await ctx.send("1) Title:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=300)
                    title = m.content.strip()
                    if title:
                        inst["title"] = title[:100]
                        await m.add_reaction("✅")
                        break
                    else:
                        await ctx.send("Title cannot be empty; please enter a title.")

                # 2) Description
                await ctx.send("2) Description (or `skip`):")
                m = await self.bot.wait_for("message", check=check, timeout=300)
                inst["description"] = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]
                await m.add_reaction("✅")

                # 3) Public/private
                await ctx.send("3) Public or Private? (`public`/`private`):")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    val = m.content.strip().lower()
                    if val in ("public", "p", "private", "priv"):
                        inst["public"] = val.startswith("p") and not val.startswith("pr")
                        await m.add_reaction("✅")
                        break
                    else:
                        await ctx.send("Invalid. Type `public` or `private`.")

                # 4a) Channel if public
                if inst["public"]:
                    await ctx.send("4) Channel? Mention it or `default`:")
                    while True:
                        m = await self.bot.wait_for("message", check=check, timeout=120)
                        txt = m.content.strip().lower()
                        if txt.startswith("default"):
                            inst["channel_id"] = await self.config.guild(guild).default_channel_id()
                            await m.add_reaction("✅")
                            break
                        elif m.channel_mentions:
                            inst["channel_id"] = m.channel_mentions[0].id
                            await m.add_reaction("✅")
                            break
                        else:
                            await ctx.send("Invalid. Mention a channel or `default`.")
                else:
                    # 4b) DM targets if private
                    await ctx.send("4) DM whom? Mention @role/@users or `all`:")
                    while True:
                        m = await self.bot.wait_for("message", check=check, timeout=120)
                        cont = m.content.lower()
                        if cont.startswith("all"):
                            inst["dm_targets"] = [m.id for m in guild.members if not m.bot]
                            await m.add_reaction("✅")
                            break
                        elif m.role_mentions:
                            inst["dm_targets"] = [
                                u.id for u in m.role_mentions[0].members if not u.bot
                            ]
                            await m.add_reaction("✅")
                            break
                        elif m.mentions:
                            inst["dm_targets"] = [u.id for u in m.mentions if not u.bot]
                            await m.add_reaction("✅")
                            break
                        else:
                            await ctx.send("Invalid. Mention a role/users or `all`.")

                # 5) Max slots
                await ctx.send("5) Max slots? Number or `none`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    txt = m.content.strip()
                    if txt.lower() in ("none", "n"):
                        inst["max_slots"] = None
                        await m.add_reaction("✅")
                        break
                    elif txt.isdigit():
                        inst["max_slots"] = int(txt)
                        await m.add_reaction("✅")
                        break
                    else:
                        await ctx.send("Invalid. Enter a number or `none`.")

                # 6) Scheduled
                await ctx.send("6) Scheduled? `YYYY-MM-DD HH:MM` UTC or `skip`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=300)
                    txt = m.content.strip()
                    if txt.lower().startswith("skip"):
                        inst["scheduled_time"] = None
                        await m.add_reaction("✅")
                        break
                    try:
                        dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
                        inst["scheduled_time"] = dt.timestamp()
                        await m.add_reaction("✅")
                        break
                    except Exception:
                        await ctx.send("Invalid. Use `YYYY-MM-DD HH:MM` UTC or `skip`.")

            except asyncio.TimeoutError:
                return await ctx.send("❌ Timed out; aborting creation.")

        # Fill common instance fields
        now = time.time()
        sched = inst.get("scheduled_time")
        status = (
            "SCHEDULED" if sched and sched > now else "OPEN"
        )
        iid = uuid.uuid4().hex
        inst.update({
            "owner_id": author.id,
            "created_at": now,
            "status": status,
            "participants": [],
            "rsvps": {},                   # str(uid) -> "PENDING"/"ACCEPTED"/"DECLINED"
            "message_ids": {
                "public": None,
                "extend": None,
                "invites": {},
                "rsvps": {},
                "reminders": {},
            },
            "channel_id": inst.get("channel_id"),
            "max_slots": inst.get("max_slots"),
            "scheduled_time": inst.get("scheduled_time"),
            "end_time": now + 12 * 3600,
        })

        # Persist
        allinst = await self.config.guild(guild).instances()
        allinst[iid] = inst
        await self.config.guild(guild).instances.set(allinst)

        # Scheduled?
        if status == "SCHEDULED":
            delay = sched - now
            self.bot.loop.create_task(self._schedule_start(guild.id, iid, delay))

            if inst["public"]:
                await ctx.send(
                    f"✅ Scheduled public `{iid[:8]}` for <t:{int(sched)}:F>."
                )
                await self._log(
                    guild,
                    f"{author.mention} scheduled public `{iid[:8]}` for <t:{int(sched)}:F>."
                )
            else:
                # Send RSVP DMs
                fails = []
                for uid in inst["dm_targets"]:
                    try:
                        user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                        dm = await user.create_dm()
                        e = discord.Embed(
                            title=f"RSVP: {inst['title']}",
                            description=inst.get("description", ""),
                            color=discord.Color.blurple(),
                        )
                        e.add_field(
                            name="Scheduled for",
                            value=f"<t:{int(sched)}:F>",
                            inline=False,
                        )
                        view = InviteView(self, iid, uid, rsvp=True)
                        msg = await dm.send(embed=e, view=view)
                        inst["message_ids"]["rsvps"][str(uid)] = msg.id
                        inst["rsvps"][str(uid)] = "PENDING"
                        self.bot.add_view(view, message_id=msg.id)
                    except Exception:
                        fails.append(uid)

                # Update persist
                allinst[iid] = inst
                await self.config.guild(guild).instances.set(allinst)

                reply = f"✅ Scheduled private `{iid[:8]}`; RSVP invites sent."
                if fails:
                    failed_mentions = " ".join(f"<@{u}>" for u in fails)
                    reply += f"\nFailed to DM: {failed_mentions}"
                await ctx.send(reply)
                await self._log(
                    guild,
                    f"{author.mention} scheduled private `{iid[:8]}`; failed to DM {fails}."
                )
            return

        # Immediate dispatch for OPEN
        await self._dispatch_open(guild, iid, ctx)

    async def _dispatch_open(self, guild: Guild, iid: str, ctx):
        """Send out the embed + buttons for an OPEN activity (public or private)."""
        insts = await self.config.guild(guild).instances()
        inst = insts[iid]
        author = ctx.author

        if inst["public"]:
            # Public post
            ch = guild.get_channel(inst["channel_id"])
            if not ch:
                return await ctx.send("Invalid public channel.")
            e = self._build_embed(inst, guild)
            view = PublicActivityView(self, iid)
            msg = await ch.send(embed=e, view=view)
            inst["message_ids"]["public"] = msg.id
            await self.config.guild(guild).instances.set(insts)
            self.bot.add_view(view, message_id=msg.id)
            await ctx.send(f"✅ Public activity created (ID `{iid[:8]}`).")
            await self._log(
                guild,
                f"{author.mention} created public `{iid[:8]}` “{inst['title']}”."
            )
        else:
            # Private invites
            fails = []
            for uid in inst["dm_targets"]:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    e = discord.Embed(
                        title=inst["title"],
                        description=inst.get("description", ""),
                        color=discord.Color.blurple(),
                    )
                    view = InviteView(self, iid, uid)
                    msg = await dm.send(embed=e, view=view)
                    inst["message_ids"]["invites"][str(uid)] = msg.id
                    self.bot.add_view(view, message_id=msg.id)
                    await self._log(
                        guild,
                        f"Invited {user.mention} to private `{iid[:8]}`."
                    )
                except Exception:
                    fails.append(uid)

            await self.config.guild(guild).instances.set(insts)
            if fails:
                await ctx.send(
                    f"Created private `{iid[:8]}`, but failed to DM: "
                    + ", ".join(str(u) for u in fails)
                )
            else:
                await ctx.send(f"✅ Private activity created and invites sent (ID `{iid[:8]}`).")

    # ──────────────────────────────────────────────────────────────────────────
    # Button Callbacks: Public / RSVP / Invite / Reminder / Extend / Finalize
    # ──────────────────────────────────────────────────────────────────────────
    async def _handle_public_join(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst["status"] != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)

        uid = str(interaction.user.id)
        if uid in inst["participants"]:
            return await interaction.response.send_message("Already joined.", ephemeral=True)

        if inst["max_slots"] and len(inst["participants"]) >= inst["max_slots"]:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("Now full!", ephemeral=True)

        inst["participants"].append(uid)
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # Update the embed
        pm = inst["message_ids"]["public"]
        cid = inst["channel_id"]
        if pm and cid:
            ch = guild.get_channel(cid)
            if ch:
                try:
                    msg = await ch.fetch_message(pm)
                    await msg.edit(embed=self._build_embed(inst, guild), view=PublicActivityView(self, iid))
                except Exception:
                    pass

        await interaction.response.send_message("✅ Joined!", ephemeral=True)
        await self._log(
            guild,
            f"{interaction.user.mention} joined public `{iid[:8]}`."
        )

    async def _handle_public_leave(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        uid = str(interaction.user.id)
        if not inst or uid not in inst["participants"]:
            return await interaction.response.send_message("You’re not in this activity.", ephemeral=True)

        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # Update embed
        pm = inst["message_ids"]["public"]
        cid = inst["channel_id"]
        if pm and cid:
            ch = guild.get_channel(cid)
            if ch:
                try:
                    msg = await ch.fetch_message(pm)
                    await msg.edit(embed=self._build_embed(inst, guild), view=PublicActivityView(self, iid))
                except Exception:
                    pass

        await interaction.response.send_message("🗑️ Left.", ephemeral=True)
        await self._log(
            guild,
            f"{interaction.user.mention} left public `{iid[:8]}`."
        )

    async def _handle_rsvp(
        self,
        interaction: discord.Interaction,
        iid: str,
        target: int,
        accepted: bool,
    ):
        # RSVP stage for scheduled private activities
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst or inst["status"] != "SCHEDULED":
            return await interaction.response.send_message("No such scheduled activity.", ephemeral=True)

        uid = str(target)
        state = inst["rsvps"].get(uid)
        if state != "PENDING":
            return await interaction.response.send_message("Already responded.", ephemeral=True)

        inst["rsvps"][uid] = "ACCEPTED" if accepted else "DECLINED"
        # remove buttons
        try:
            await interaction.message.edit(view=None)
        except Exception:
            pass
        await interaction.response.send_message(
            "✅ RSVP: Yes" if accepted else "❌ RSVP: No",
            ephemeral=True,
        )
        await self.config.guild(guild).instances.set(insts := await self.config.guild(guild).instances())
        await self._log(
            guild,
            f"{interaction.user.mention} RSVPed {'YES' if accepted else 'NO'} to `{iid[:8]}`."
        )

    async def _handle_invite_accept(self, interaction: discord.Interaction, iid: str, target: int):
        # Live private join
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst or inst["status"] != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)

        uid = str(target)
        if uid in inst["participants"]:
            return await interaction.response.send_message("Already joined.", ephemeral=True)

        if inst["max_slots"] and len(inst["participants"]) >= inst["max_slots"]:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("Now full!", ephemeral=True)

        inst["participants"].append(uid)
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # remove invite buttons
        try:
            await interaction.message.edit(view=None)
        except Exception:
            pass

        # notify owner
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(
                    f"{interaction.user.mention} joined your private `{iid[:8]}`."
                )
            except Exception:
                pass

        await interaction.response.send_message("✅ Joined!", ephemeral=True)
        await self._log(
            guild,
            f"{interaction.user.mention} joined private `{iid[:8]}`."
        )

    async def _handle_invite_decline(self, interaction: discord.Interaction, iid: str, target: int):
        # Live private decline
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)

        # remove buttons
        try:
            await interaction.message.edit(view=None)
        except Exception:
            pass
        await interaction.response.send_message("❌ Declined.", ephemeral=True)

        # notify owner
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(
                    f"{interaction.user.mention} declined your private `{iid[:8]}`."
                )
            except Exception:
                pass

        await self._log(
            guild,
            f"{interaction.user.mention} declined private `{iid[:8]}`."
        )

    async def _handle_invite_reply(
        self,
        interaction: discord.Interaction,
        iid: str,
        target: int,
        content: str,
    ):
        # Forward custom text to owner
        inst = None
        guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return

        owner = self.bot.get_user(inst["owner_id"])
        if not owner:
            return
        try:
            await owner.send(
                f"**Message from {interaction.user.mention}** about `{iid[:8]}`:\n{content}"
            )
        except Exception:
            log.exception("Could not send reply to owner")

    async def _handle_reminder_leave(self, interaction: discord.Interaction, iid: str, target: int):
        # Reminder leave at start for private scheduled
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst or inst["status"] != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)

        uid = str(target)
        if uid not in inst["participants"]:
            return await interaction.response.send_message("You never joined.", ephemeral=True)

        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # remove reminder button
        try:
            await interaction.message.edit(view=None)
        except Exception:
            pass

        await interaction.response.send_message("🗑️ You left.", ephemeral=True)
        await self._log(
            guild,
            f"{interaction.user.mention} left private (reminder) `{iid[:8]}`."
        )

    async def _handle_extend(self, interaction: discord.Interaction, iid: str):
        """Extend an auto-ended activity by 12 hours."""
        # find instance & guild
        inst = None
        guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)

        new_end = time.time() + 12 * 3600
        inst["end_time"] = new_end
        inst["status"] = "OPEN"
        inst["message_ids"]["extend"] = None

        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        await interaction.response.edit_message(
            content="Extended 12 hours.",
            view=None,
            embed=None,
        )
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))
        await self._log(
            guild,
            f"{interaction.user.mention} extended `{iid[:8]}` by 12 hours."
        )

    async def _handle_finalize(self, interaction: discord.Interaction, iid: str):
        """Finalize (end) an activity immediately."""
        inst = None
        guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)

        inst["status"] = "ENDED"
        inst["message_ids"]["extend"] = None
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # remove public buttons if any
        pm = inst["message_ids"].get("public")
        cid = inst.get("channel_id")
        if pm and cid:
            ch = guild.get_channel(cid)
            if ch:
                try:
                    msg = await ch.fetch_message(pm)
                    await msg.edit(view=None)
                except Exception:
                    pass

        await interaction.response.edit_message(
            content="Finalized.",
            view=None,
            embed=None,
        )
        await self._log(
            guild,
            f"{interaction.user.mention} finalized `{iid[:8]}`."
        )


async def setup(bot: Red):
    await bot.add_cog(Activities(bot))