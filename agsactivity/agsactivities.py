import asyncio
import uuid
import time
import logging
from datetime import datetime

import discord
from discord import TextChannel, Guild
from discord.ui import View, Button

from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.activity")

DEFAULT_GUILD = {
    "default_channel_id": None,       # Default public channel
    "log_channel_id":    None,       # Audit log channel
    "prune_summary_channel": None,   # Monthly‐prune summary
    "templates": {},                 # Saved templates
    "instances": {},                 # All live & scheduled instances
}


class PublicJoinButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.green,
            label="Join",
            custom_id=f"act:join:{iid}",
        )
        self.iid = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_join(interaction, self.iid)


class PublicLeaveButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Leave",
            custom_id=f"act:leave:{iid}",
        )
        self.iid = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_leave(interaction, self.iid)


class PublicActivityView(View):
    """Join/Leave buttons for a public activity embed."""
    def __init__(self, cog, iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.add_item(PublicJoinButton(iid))
        self.add_item(PublicLeaveButton(iid))


class DMAcceptButton(Button):
    def __init__(self, iid: str, target: int):
        super().__init__(
            style=discord.ButtonStyle.green,
            label="Accept",
            custom_id=f"act:dmaccept:{iid}:{target}",
        )
        self.iid = iid
        self.target = target

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_dm_accept(interaction, self.iid, self.target)


class DMDeclineButton(Button):
    def __init__(self, iid: str, target: int):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Decline",
            custom_id=f"act:dmdecline:{iid}:{target}",
        )
        self.iid = iid
        self.target = target

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_dm_decline(interaction, self.iid, self.target)


class DMLeaveButton(Button):
    def __init__(self, iid: str, target: int):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Leave",
            custom_id=f"act:dmleave:{iid}:{target}",
        )
        self.iid = iid
        self.target = target

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_dm_leave(interaction, self.iid, self.target)


class DMInviteView(View):
    """RSVP / Accept‐Decline‐Leave view for private invites."""
    def __init__(self, cog, iid: str, target: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.target = target
        self.add_item(DMAcceptButton(iid, target))
        self.add_item(DMDeclineButton(iid, target))
        self.add_item(DMLeaveButton(iid, target))


class ExtendButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.green,
            label="Extend 12 h",
            custom_id=f"act:extend:{iid}",
        )
        self.iid = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_extend(interaction, self.iid)


class FinalizeButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Finalize now",
            custom_id=f"act:finalize:{iid}",
        )
        self.iid = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_finalize(interaction, self.iid)


class ExtendView(View):
    """Extend/Finalize view after auto‐end."""
    def __init__(self, cog, iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.add_item(ExtendButton(iid))
        self.add_item(FinalizeButton(iid))


class Activities(commands.Cog):
    """Activities cog with scheduling + RSVPs + dynamic embeds + logs."""
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=9876543210123456, force_registration=True
        )
        self.config.register_guild(**DEFAULT_GUILD)
        # Re‐add views + schedule tasks
        self.bot.loop.create_task(self._startup_tasks())
        self.bot.loop.create_task(self._monthly_prune_scheduler())

    async def _startup_tasks(self):
        """Restore views and schedule any future starts or auto‐ends."""
        await self.bot.wait_until_ready()
        now = time.time()
        for guild in self.bot.guilds:
            data = await self.config.guild(guild).all()
            insts = data["instances"]
            for iid, inst in insts.items():
                # Public OPEN
                if inst.get("status") == "OPEN" and inst.get("public_message_id"):
                    self.bot.add_view(
                        PublicActivityView(self, iid),
                        message_id=inst["public_message_id"],
                    )
                # Scheduled RSVPs
                if inst.get("status") == "SCHEDULED":
                    for ts, mid in inst.get("rsvp_message_ids", {}).items():
                        try:
                            self.bot.add_view(
                                DMInviteView(self, iid, int(ts)),
                                message_id=mid,
                            )
                        except:
                            continue
                    sched = inst.get("scheduled_time", 0)
                    if sched > now:
                        self.bot.loop.create_task(
                            self._schedule_start(guild.id, iid, sched - now)
                        )
                # Live private reminders
                if inst.get("status") == "OPEN" and not inst.get("public"):
                    for ts, mid in inst.get("start_message_ids", {}).items():
                        try:
                            self.bot.add_view(
                                DMInviteView(self, iid, int(ts)),
                                message_id=mid,
                            )
                        except:
                            continue
                # Extend/Finalize
                if inst.get("extend_message_id"):
                    self.bot.add_view(
                        ExtendView(self, iid),
                        message_id=inst["extend_message_id"],
                    )
                # Auto-end
                if inst.get("status") == "OPEN" and inst.get("end_time"):
                    delay = inst["end_time"] - now
                    if delay < 0:
                        delay = 0
                    self.bot.loop.create_task(
                        self._auto_end_task(guild.id, iid, delay)
                    )

    def _build_embed(self, inst: dict, guild: Guild) -> discord.Embed:
        """
        Build a dynamic embed for an activity:
         • Uses .mention for users & channels
         • Shows colored circle for fill
         • Uses <t:…:F> and <t:…:R> for schedules
        """
        # Participants
        parts = []
        for uid_str in inst.get("participants", []):
            m = guild.get_member(int(uid_str))
            parts.append(m.mention if m else uid_str)
        curr = len(parts)
        max_s = inst.get("max_slots")
        if max_s:
            ratio = curr / max_s
            emoji = "🟢" if ratio < 0.5 else "🟠" if ratio < 1 else "🔴"
            slots = f"{curr}/{max_s}"
        else:
            emoji = "🟢"
            slots = f"{curr}/∞"

        title = f"{emoji} {inst['title']}"
        e = discord.Embed(
            title=title,
            description=inst.get("description", "") or "No description.",
            color=discord.Color.blurple(),
        )
        owner = self.bot.get_user(inst["owner_id"])
        e.add_field(
            name="Owner",
            value=owner.mention if owner else str(inst["owner_id"]),
            inline=True,
        )
        e.add_field(name="Slots", value=slots, inline=True)

        sched = inst.get("scheduled_time")
        if sched:
            e.add_field(
                name="Scheduled",
                value=f"<t:{int(sched)}:F> (<t:{int(sched)}:R>)",
                inline=False,
            )

        if parts:
            e.add_field(name="Participants", value="\n".join(parts), inline=False)

        # Footer with channel if set
        if inst.get("channel_id"):
            ch = guild.get_channel(inst["channel_id"])
            if ch:
                e.set_footer(text=f"In {ch.mention}")

        return e

    async def _log(self, guild: Guild, message: str):
        """Send an audit log entry to the configured log_channel."""
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
             # -------------------------------------------------------------------------
    # AUTO‐END TASK
    # -------------------------------------------------------------------------
    async def _auto_end_task(self, guild_id: int, iid: str, delay: float):
        await asyncio.sleep(delay)
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst.get("status") != "OPEN":
            return
        inst["status"] = "ENDED"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # Remove public buttons
        if inst.get("public_message_id"):
            ch = guild.get_channel(inst["public_channel_id"])
            if ch:
                try:
                    msg = await ch.fetch_message(inst["public_message_id"])
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=None)
                except:
                    pass
        # DM owner with Extend/Finalize
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                e2 = discord.Embed(
                    title=f"Activity auto-ended: {inst['title']}",
                    description=(
                        "This activity has automatically ended after 12 hours.\n\n"
                        "Click **Extend 12 h** to keep it open another 12 h, or **Finalize now**."
                    ),
                    color=discord.Color.orange(),
                )
                view = ExtendView(self, iid)
                dm = await owner.send(embed=e2, view=view)
                inst["extend_message_id"] = dm.id
                insts[iid] = inst
                await self.config.guild(guild).instances.set(insts)
            except:
                log.exception("Failed to DM owner about auto-end")
        await self._log(guild, f"Auto-ended activity `{iid[:8]}` (“{inst['title']}”).")

    # -------------------------------------------------------------------------
    # SCHEDULED START
    # -------------------------------------------------------------------------
    async def _schedule_start(self, guild_id: int, iid: str, delay: float):
        await asyncio.sleep(delay)
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst.get("status") != "SCHEDULED":
            return
        now = time.time()
        inst["status"] = "OPEN"
        inst["start_time"] = now
        inst["end_time"] = now + 12 * 3600

        if inst.get("public"):
            ch = guild.get_channel(inst["public_channel_id"])
            if ch:
                e = self._build_embed(inst, guild)
                view = PublicActivityView(self, iid)
                try:
                    msg = await ch.send(embed=e, view=view)
                    inst["public_message_id"] = msg.id
                    self.bot.add_view(view, message_id=msg.id)
                except:
                    pass
            await self._log(guild, f"Scheduled public `{iid[:8]}` has now started.")
        else:
            # DM only those who RSVPed “ACCEPTED”
            for uid_str, state in list(inst["rsvps"].items()):
                if state != "ACCEPTED":
                    continue
                inst["participants"].append(uid_str)
                uid = int(uid_str)
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    e = discord.Embed(
                        title=f"🔔 Reminder: {inst['title']} is starting now",
                        description=inst.get("description", ""),
                        color=discord.Color.blurple(),
                    )
                    e.set_footer(text="Click Leave below if you can’t make it.")
                    reminder = View(timeout=None)
                    reminder.add_item(DMLeaveButton(iid, uid))
                    msg = await dm.send(embed=e, view=reminder)
                    inst["start_message_ids"][str(uid)] = msg.id
                    self.bot.add_view(reminder, message_id=msg.id)
                except:
                    pass
            await self._log(guild, f"Scheduled private `{iid[:8]}` started; reminders sent.")

        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))

    # -------------------------------------------------------------------------
    # MONTHLY PRUNE
    # -------------------------------------------------------------------------
    async def _monthly_prune_scheduler(self):
        await self.bot.wait_until_ready()
        while True:
            now = datetime.utcnow()
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
                    if inst.get("status") == "ENDED":
                        insts.pop(iid)
                        pruned.append((iid, inst))
                await self.config.guild(guild).instances.set(insts)
                chan_id = await self.config.guild(guild).prune_summary_channel()
                if pruned and chan_id:
                    ch = guild.get_channel(chan_id)
                    if ch:
                        lines = "\n".join(f"`{iid[:8]}` • {inst['title']}" for iid, inst in pruned)
                        try:
                            await ch.send(f"Auto-pruned {len(pruned)} activities:\n{lines}")
                        except:
                            pass

    # -------------------------------------------------------------------------
    # COMMANDS
    # -------------------------------------------------------------------------
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
        await ctx.send(f"Default channel {'set to ' + channel.mention if channel else 'cleared'}.")

    @activity.command(name="logchannel")
    @checks.guildowner()
    async def set_logchannel(self, ctx, channel: TextChannel = None):
        """Set or clear the log channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).log_channel_id.set(cid)
        await ctx.send(f"Log channel {'set to ' + channel.mention if channel else 'cleared'}.")

    @activity.command(name="prunechannel")
    @checks.guildowner()
    async def set_prunechannel(self, ctx, channel: TextChannel = None):
        """Set or clear the monthly prune-summary channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).prune_summary_channel.set(cid)
        await ctx.send(f"Prune summary channel {'set to ' + channel.mention if channel else 'cleared'}.")

    @activity.command(name="list")
    async def list_activities(self, ctx):
        """List all live or scheduled activities."""
        insts = await self.config.guild(ctx.guild).instances()
        if not insts:
            return await ctx.send("No activities found.")
        embed = discord.Embed(title="Activities", color=discord.Color.green())
        for iid, inst in insts.items():
            owner = ctx.guild.get_member(inst["owner_id"])
            status = inst["status"]
            sched = inst.get("scheduled_time")
            sched_str = f" • starts <t:{int(sched)}:R>" if sched and status == "SCHEDULED" else ""
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
        """Show detailed info on an activity."""
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
        Manually prune activities by status (OPEN/FULL/SCHEDULED/ENDED) and optional min age (days).
        """
        insts = await self.config.guild(ctx.guild).instances()
        now = time.time()
        removed = []
        for iid, inst in list(insts.items()):
            if inst.get("status") != status.upper():
                continue
            if older_than is not None:
                created = inst.get("created_at", now)
                if now - created < older_than * 86400:
                    continue
            pmid = inst.get("public_message_id")
            pcid = inst.get("public_channel_id")
            if pmid and pcid:
                ch = ctx.guild.get_channel(pcid)
                if ch:
                    try:
                        msg = await ch.fetch_message(pmid)
                        await msg.delete()
                    except:
                        pass
            insts.pop(iid)
            removed.append(iid)
        await self.config.guild(ctx.guild).instances.set(insts)
        await ctx.send(f"Pruned {len(removed)} activities.")

    # -------------------------------------------------------------------------
    # TEMPLATES
    # -------------------------------------------------------------------------
    @activity.group(name="template", invoke_without_command=True)
    @checks.guildowner()
    async def template(self, ctx):
        """Manage activity templates."""
        await ctx.send_help(ctx.command)

    @template.command(name="save")
    @checks.guildowner()
    async def template_save(self, ctx, name: str):
        """
        Save a template: title, description, public/private, channel or targets, slots, schedule.
        """
        name = name.lower()
        existing = await self.config.guild(ctx.guild).templates()
        if name in existing:
            return await ctx.send("That template already exists.")
        await ctx.send("Template setup: 300 s/question; ‘skip’ to omit optional fields.")
        def check(m): return m.author == ctx.author and m.channel == ctx.channel
        try:
            await ctx.send("1) Title:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            title = m.content.strip()[:100]

            await ctx.send("2) Description (or ‘skip’):")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            desc = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]

            await ctx.send("3) Public or Private? (public/private)")
            m = await self.bot.wait_for("message", check=check, timeout=120)
            public = m.content.lower().startswith("p")

            channel_id = None
            dm_targets = []
            if public:
                await ctx.send("4) Channel? Mention it or ‘default’:")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if m.channel_mentions:
                    channel_id = m.channel_mentions[0].id
            else:
                await ctx.send("4) DM whom? Mention role/users or ‘all’:")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if m.content.lower().startswith("all"):
                    dm_targets = [u.id for u in ctx.channel.members if not u.bot]
                elif m.role_mentions:
                    dm_targets = [u.id for u in m.role_mentions[0].members if not u.bot]
                elif m.mentions:
                    dm_targets = [u.id for u in m.mentions if not u.bot]

            await ctx.send("5) Max slots? Number or ‘none’:")
            m = await self.bot.wait_for("message", check=check, timeout=120)
            try:
                max_s = int(m.content.strip())
            except:
                max_s = None

            await ctx.send("6) Scheduled? YYYY-MM-DD HH:MM UTC or ‘skip’:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            if m.content.lower().startswith("skip"):
                sched = None
            else:
                try:
                    dt = datetime.strptime(m.content.strip(), "%Y-%m-%d %H:%M")
                    sched = dt.timestamp()
                except:
                    sched = None

        except asyncio.TimeoutError:
            return await ctx.send("Timed out; aborting template.")

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
            f"`{name}` • {'Pub' if t['public'] else 'Priv'} • “{t['title']}”"
            for name, t in tpls.items()
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

    # -------------------------------------------------------------------------
    # START / SCHEDULE WIZARD
    # -------------------------------------------------------------------------
    @activity.command(name="start")
    async def activity_start(self, ctx, template: str = None):
        """
        Start or schedule an activity.
        Optionally pass a template name.
        """
        guild = ctx.guild
        tpls = await self.config.guild(guild).templates()
        tpl = tpls.get(template.lower()) if template else None

        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        inst = {}
        if tpl:
            inst.update(tpl)
            if inst["public"] and not inst.get("channel_id"):
                inst["public_channel_id"] = await self.config.guild(guild).default_channel_id()
            else:
                inst["public_channel_id"] = inst.get("channel_id")
            inst["dm_targets"] = tpl.get("dm_targets", [])
            inst["scheduled_time"] = tpl.get("scheduled_time")
        else:
            await ctx.send("**Activity Wizard** (300 s/question, ‘skip’ to omit):")
            try:
                await ctx.send("1) Title:")
                m = await self.bot.wait_for("message", check=check, timeout=300)
                inst["title"] = m.content.strip()[:100]

                await ctx.send("2) Description (or ‘skip’):")
                m = await self.bot.wait_for("message", check=check, timeout=300)
                inst["description"] = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]

                await ctx.send("3) Public or Private? (public/private)")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                inst["public"] = m.content.lower().startswith("p")

                if inst["public"]:
                    await ctx.send("4) Channel? Mention or ‘default’:")
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    if m.channel_mentions:
                        inst["public_channel_id"] = m.channel_mentions[0].id
                    else:
                        inst["public_channel_id"] = await self.config.guild(guild).default_channel_id()
                else:
                    await ctx.send("4) DM whom? Mention role/users or ‘all’:")
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    if m.content.lower().startswith("all"):
                        inst["dm_targets"] = [u.id for u in ctx.channel.members if not u.bot]
                    elif m.role_mentions:
                        inst["dm_targets"] = [u.id for u in m.role_mentions[0].members if not u.bot]
                    elif m.mentions:
                        inst["dm_targets"] = [u.id for u in m.mentions if not u.bot]
                    else:
                        return await ctx.send("No valid targets; abort.")

                await ctx.send("5) Max slots? Number or ‘none’:")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                try:
                    inst["max_slots"] = int(m.content.strip())
                except:
                    inst["max_slots"] = None

                await ctx.send("6) Scheduled? YYYY-MM-DD HH:MM UTC or ‘skip’:")
                m = await self.bot.wait_for("message", check=check, timeout=300)
                if m.content.lower().startswith("skip"):
                    inst["scheduled_time"] = None
                else:
                    try:
                        dt = datetime.strptime(m.content.strip(), "%Y-%m-%d %H:%M")
                        inst["scheduled_time"] = dt.timestamp()
                    except:
                        inst["scheduled_time"] = None

            except asyncio.TimeoutError:
                return await ctx.send("Timed out; aborting.")

        # finalize
        now = time.time()
        sched = inst.get("scheduled_time")
        status = "SCHEDULED" if sched is not None and sched > now else "OPEN"
        iid = uuid.uuid4().hex
        inst.update({
            "owner_id": ctx.author.id,
            "created_at": now,
            "status": status,
            "participants": [],
            "public_message_id": None,
            "dm_message_ids": {},
            "rsvp_message_ids": {},
            "rsvps": {},
            "start_message_ids": {},
            "extend_message_id": None,
            "channel_id": inst.get("public_channel_id"),
            "end_time": now + 12 * 3600,
        })
        allinst = await self.config.guild(guild).instances()
        allinst[iid] = inst
        await self.config.guild(guild).instances.set(allinst)

        if status == "SCHEDULED":
            delay = sched - now
            self.bot.loop.create_task(self._schedule_start(guild.id, iid, delay))
            if inst["public"]:
                await ctx.send(f"✅ Scheduled public `{iid[:8]}` for <t:{int(sched)}:F>.")
                await self._log(guild, f"{ctx.author.mention} scheduled public `{iid[:8]}`.")
            else:
                fails = []
                for uid in inst["dm_targets"]:
                    try:
                        user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                        dm = await user.create_dm()
                        e = discord.Embed(
                            title=f"RSVP: {inst['title']}",
                            description=inst.get("description",""),
                            color=discord.Color.blurple(),
                        )
                        e.add_field(
                            name="Scheduled for",
                            value=f"<t:{int(sched)}:F>",
                            inline=False,
                        )
                        view = DMInviteView(self, iid, uid)
                        msg = await dm.send(embed=e, view=view)
                        inst["rsvp_message_ids"][str(uid)] = msg.id
                        inst["rsvps"][str(uid)] = "PENDING"
                        self.bot.add_view(view, message_id=msg.id)
                    except:
                        fails.append(str(uid))
                await self.config.guild(guild).instances.set(allinst)
                txt = f"✅ Scheduled private `{iid[:8]}`; RSVP invites sent."
                if fails:
                    txt += f"\nFailed to DM: {', '.join(fails)}"
                await ctx.send(txt)
                await self._log(guild, f"{ctx.author.mention} scheduled private `{iid[:8]}`.")
            return

        # Immediate OPEN
        await self._dispatch_immediate(guild, iid, ctx)

    # -------------------------------------------------------------------------
    # IMMEDIATE DISPATCH HELPER
    # -------------------------------------------------------------------------
    async def _dispatch_immediate(self, guild: Guild, iid: str, ctx):
        insts = await self.config.guild(guild).instances()
        inst = insts[iid]
        if inst.get("public"):
            ch = guild.get_channel(inst["public_channel_id"])
            if not ch:
                return await ctx.send("Invalid channel.")
            e = self._build_embed(inst, guild)
            view = PublicActivityView(self, iid)
            msg = await ch.send(embed=e, view=view)
            inst["public_message_id"] = msg.id
            await self.config.guild(guild).instances.set(insts)
            self.bot.add_view(view, message_id=msg.id)
            await ctx.send(f"✅ Public activity created (ID `{iid[:8]}`) in {ch.mention}.")
            await self._log(guild, f"{ctx.author.mention} created public `{iid[:8]}` “{inst['title']}”.")
        else:
            fails = []
            for uid in inst["dm_targets"]:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    e = self._build_embed(inst, guild)
                    view = DMInviteView(self, iid, uid)
                    msg = await dm.send(embed=e, view=view)
                    inst["dm_message_ids"][str(uid)] = msg.id
                    self.bot.add_view(view, message_id=msg.id)
                    await self._log(guild, f"Invited {user.mention} to private `{iid[:8]}`.")
                except:
                    fails.append(str(uid))
            await self.config.guild(guild).instances.set(insts)
            if fails:
                await ctx.send(f"Created private `{iid[:8]}`, but failed to DM: {', '.join(fails)}")
            else:
                await ctx.send(f"✅ Private activity created and invites sent (ID `{iid[:8]}`).")

    # -------------------------------------------------------------------------
    # BUTTON CALLBACKS
    # -------------------------------------------------------------------------
    async def _handle_extend(self, interaction: discord.Interaction, iid: str):
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("Not found.", ephemeral=True)
        new_end = time.time() + 12 * 3600
        inst["end_time"] = new_end
        inst["status"] = "OPEN"
        inst["extend_message_id"] = None
        insts = await self.config.guild(guild).instances()
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        await interaction.response.edit_message(content="Extended 12 h.", view=None, embed=None)
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))
        await self._log(guild, f"{interaction.user.mention} extended `{iid[:8]}`.")

    async def _handle_finalize(self, interaction: discord.Interaction, iid: str):
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("Not found.", ephemeral=True)
        inst["status"] = "ENDED"
        inst["extend_message_id"] = None
        insts = await self.config.guild(guild).instances()
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        pmid = inst.get("public_message_id"); pcid = inst.get("public_channel_id")
        if pmid and pcid:
            ch = guild.get_channel(pcid)
            if ch:
                try:
                    msg = await ch.fetch_message(pmid)
                    await msg.edit(view=None)
                except:
                    pass
        await interaction.response.edit_message(content="Finalized.", view=None, embed=None)
        await self._log(guild, f"{interaction.user.mention} finalized `{iid[:8]}`.")

    async def _handle_join(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst.get("status") != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)
        uid = str(interaction.user.id)
        if uid in inst["participants"]:
            return await interaction.response.send_message("Already joined.", ephemeral=True)
        max_s = inst.get("max_slots")
        if max_s and len(inst["participants"]) >= max_s:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("Now full.", ephemeral=True)
        inst["participants"].append(uid)
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        pmid = inst.get("public_message_id"); pcid = inst.get("public_channel_id")
        if pmid and pcid:
            ch = guild.get_channel(pcid)
            if ch:
                try:
                    msg = await ch.fetch_message(pmid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        await interaction.response.send_message("✅ Joined!", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} joined `{iid[:8]}`.")

    async def _handle_leave(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        uid = str(interaction.user.id)
        if not inst or uid not in inst["participants"]:
            return await interaction.response.send_message("Not in activity.", ephemeral=True)
        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        pmid = inst.get("public_message_id"); pcid = inst.get("public_channel_id")
        if pmid and pcid:
            ch = guild.get_channel(pcid)
            if ch:
                try:
                    msg = await ch.fetch_message(pmid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        await interaction.response.send_message("🗑️ Left.", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} left `{iid[:8]}`.")

    async def _handle_dm_accept(self, interaction: discord.Interaction, iid: str, target: int):
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("Not found.", ephemeral=True)
        uid = str(target)
        if inst.get("status") == "SCHEDULED":
            if inst["rsvps"].get(uid) != "PENDING":
                return await interaction.response.send_message("Already RSVPed.", ephemeral=True)
            inst["rsvps"][uid] = "ACCEPTED"
            insts = await self.config.guild(guild).instances()
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            try:
                await interaction.message.edit(view=None)
            except:
                pass
            await interaction.response.send_message("✅ RSVP Yes", ephemeral=True)
            await self._log(guild, f"{interaction.user.mention} RSVPed YES `{iid[:8]}`.")
            return
        # Normal private join
        if inst.get("status") != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)
        if uid in inst["participants"]:
            return await interaction.response.send_message("Already joined.", ephemeral=True)
        max_s = inst.get("max_slots")
        if max_s and len(inst["participants"]) >= max_s:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("Now full.", ephemeral=True)
        inst["participants"].append(uid)
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        pmid = inst.get("public_message_id"); pcid = inst.get("public_channel_id")
        if pmid and pcid:
            ch = guild.get_channel(pcid)
            if ch:
                try:
                    msg = await ch.fetch_message(pmid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        await interaction.response.send_message("✅ Joined!", ephemeral=True)
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(f"{interaction.user.mention} joined your private `{iid[:8]}`.")
            except:
                pass
        await self._log(guild, f"{interaction.user.mention} joined private `{iid[:8]}`.")

    async def _handle_dm_decline(self, interaction: discord.Interaction, iid: str, target: int):
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("Not found.", ephemeral=True)
        uid = str(target)
        if inst.get("status") == "SCHEDULED":
            if inst["rsvps"].get(uid) != "PENDING":
                return await interaction.response.send_message("Already RSVPed.", ephemeral=True)
            inst["rsvps"][uid] = "DECLINED"
            insts = await self.config.guild(guild).instances()
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            try:
                await interaction.message.edit(view=None)
            except:
                pass
            await interaction.response.send_message("❌ RSVP No", ephemeral=True)
            await self._log(guild, f"{interaction.user.mention} RSVPed NO `{iid[:8]}`.")
            return
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        await interaction.response.send_message("❌ Declined.", ephemeral=True)
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(f"{interaction.user.mention} declined your private `{iid[:8]}`.")
            except:
                pass
        await self._log(guild, f"{interaction.user.mention} declined private `{iid[:8]}`.")

    async def _handle_dm_leave(self, interaction: discord.Interaction, iid: str, target: int):
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst or inst.get("status") != "OPEN":
            return await interaction.response.send_message("Not open.", ephemeral=True)
        uid = str(target)
        if uid not in inst["participants"]:
            return await interaction.response.send_message("You never joined.", ephemeral=True)
        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        pmid = inst.get("public_message_id"); pcid = inst.get("public_channel_id")
        if pmid and pcid:
            ch = guild.get_channel(pcid)
            if ch:
                try:
                    msg = await ch.fetch_message(pmid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        await interaction.response.send_message("🗑️ Left.", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} left private `{iid[:8]}`.")

async def setup(bot: Red):
    await bot.add_cog(Activities(bot))