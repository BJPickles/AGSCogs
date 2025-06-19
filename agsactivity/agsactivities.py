import asyncio
import uuid
import time
import logging
from datetime import datetime, timedelta

import discord
from discord import TextChannel, Guild
from discord.ui import View, Button

from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.activity")

DEFAULT_GUILD = {
    "default_channel_id": None,       # where public activities go by default
    "log_channel_id":    None,       # where we log every action
    "prune_summary_channel": None,   # where monthly prune summaries go
    "templates": {},                 # saved activity templates
    "instances": {},                 # live activity instances
}


class PublicJoinButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.green, label="Join", custom_id=f"act:join:{iid}"
        )
        self.iid = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_join(interaction, self.iid)


class PublicLeaveButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.red, label="Leave", custom_id=f"act:leave:{iid}"
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
    """Accept/Decline/Leave buttons for a private‐invite DM."""
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
    """Sent to the owner when auto‐ending a 12 h activity, to extend or finalize."""
    def __init__(self, cog, iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.add_item(ExtendButton(iid))
        self.add_item(FinalizeButton(iid))


class Activities(commands.Cog):
    """A completely refactored Activities cog."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=9876543210123456, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self.bot.loop.create_task(self._startup_tasks())
        self.bot.loop.create_task(self._monthly_prune_scheduler())

    # -------------------------------------------------------------------------
    # STARTUP: re-add persistent views & schedule auto‐end tasks
    # -------------------------------------------------------------------------
    async def _startup_tasks(self):
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            data = await self.config.guild(guild).all()
            insts = data["instances"]
            for iid, inst in insts.items():
                # public view
                pmid = inst.get("public_message_id")
                pcid = inst.get("public_channel_id")
                if pmid and pcid:
                    self.bot.add_view(PublicActivityView(self, iid), message_id=pmid)
                # DM invite views
                for target_str, dm_mid in inst.get("dm_message_ids", {}).items():
                    try:
                        t = int(target_str)
                        self.bot.add_view(DMInviteView(self, iid, t), message_id=dm_mid)
                    except:
                        continue
                # extend view
                ext_mid = inst.get("extend_message_id")
                if ext_mid:
                    self.bot.add_view(ExtendView(self, iid), message_id=ext_mid)
                # schedule auto‐end if still open
                if inst.get("status") == "OPEN" and inst.get("end_time"):
                    delay = inst["end_time"] - time.time()
                    if delay < 0:
                        delay = 0
                    self.bot.loop.create_task(self._auto_end_task(guild.id, iid, delay))

    # -------------------------------------------------------------------------
    # HELPER: build a dynamic embed from an instance dict
    # -------------------------------------------------------------------------
    def _build_embed(self, inst: dict, guild: Guild) -> discord.Embed:
        # participants as mentions
        ps = []
        for uid_str in inst.get("participants", []):
            m = guild.get_member(int(uid_str))
            ps.append(m.mention if m else uid_str)
        curr = len(ps)
        max_s = inst.get("max_slots")
        if max_s:
            ratio = curr / max_s
            if ratio < 0.5:
                emoji = "🟢"
            elif ratio < 1:
                emoji = "🟠"
            else:
                emoji = "🔴"
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
        e.add_field("Owner", owner.mention if owner else str(inst["owner_id"]), inline=True)
        e.add_field("Slots", slots, inline=True)
        sched = inst.get("scheduled_time")
        if sched:
            dt = datetime.utcfromtimestamp(sched)
            e.add_field("Scheduled (UTC)", dt.strftime("%Y-%m-%d %H:%M"), inline=True)
        if ps:
            e.add_field("Participants", "\n".join(ps), inline=False)
        return e

    # -------------------------------------------------------------------------
    # HELPER: log to log_channel if set
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # AUTO‐END after 12 h
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
        # mark ended
        inst["status"] = "ENDED"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # update public embed
        chid = inst.get("public_channel_id")
        mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=None)
                except:
                    pass
        # DM owner with extend/finalize buttons
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                e2 = discord.Embed(
                    title=f"Activity auto-ended: {inst['title']}",
                    description="This activity has been automatically ended after 12 hours.  "
                                "Click **Extend 12 h** to keep it open another 12 hours, "
                                "or **Finalize now** to close permanently.",
                    color=discord.Color.orange(),
                )
                view = ExtendView(self, iid)
                dm = await owner.send(embed=e2, view=view)
                inst["extend_message_id"] = dm.id
                insts[iid] = inst
                await self.config.guild(guild).instances.set(insts)
            except Exception:
                log.exception("Failed to DM owner about auto-end")
        await self._log(guild, f"Auto-ended activity `{iid[:8]}` (‘{inst['title']}’).")

    async def _handle_extend(self, interaction: discord.Interaction, iid: str):
        """Owner clicked ‘Extend’ on the auto-end prompt."""
        # find guild/inst
        guild = discord.utils.get(self.bot.guilds, id=interaction.user.mutual_guilds[0].id) \
                if interaction.user.mutual_guilds else None
        # sloppy: we know it’s their own DM, find inst by owner
        inst = None; guild_found = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                inst = insts[iid]; guild_found = g; break
        if not inst:
            return await interaction.response.send_message("Instance not found.", ephemeral=True)
        # new end_time = now + 12h
        new_et = time.time() + 12 * 3600
        inst["end_time"] = new_et
        inst["extend_message_id"] = None  # disable old buttons
        inst["status"] = "OPEN"
        insts = await self.config.guild(guild_found).instances()
        insts[iid] = inst
        await self.config.guild(guild_found).instances.set(insts)
        # ack
        await interaction.response.edit_message(
            content="Extended another 12 hours.", embed=None, view=None
        )
        # reschedule
        delay = 12 * 3600
        self.bot.loop.create_task(self._auto_end_task(guild_found.id, iid, delay))
        await self._log(
            guild_found,
            f"{interaction.user.mention} extended activity `{iid[:8]}` another 12 h."
        )

    async def _handle_finalize(self, interaction: discord.Interaction, iid: str):
        """Owner clicked ‘Finalize now’ on the auto-end prompt."""
        # same lookup as above
        inst = None; guild_found = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts and insts[iid]["owner_id"] == interaction.user.id:
                inst = insts[iid]; guild_found = g; break
        if not inst:
            return await interaction.response.send_message("Instance not found.", ephemeral=True)
        inst["status"] = "ENDED"
        inst["extend_message_id"] = None
        insts = await self.config.guild(guild_found).instances()
        insts[iid] = inst
        await self.config.guild(guild_found).instances.set(insts)
        # update public embed
        chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild_found.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild_found)
                    await msg.edit(embed=e, view=None)
                except:
                    pass
        await interaction.response.edit_message(
            content="Activity finalized.", embed=None, view=None
        )
        await self._log(
            guild_found, f"{interaction.user.mention} finalized activity `{iid[:8]}` early."
        )

    # -------------------------------------------------------------------------
    # BUTTON HANDLERS: join / leave for public activities
    # -------------------------------------------------------------------------
    async def _handle_join(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("This button only works in-guild.", ephemeral=True)
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst.get("status") != "OPEN":
            return await interaction.response.send_message("Activity is not open.", ephemeral=True)
        uid = str(interaction.user.id)
        if uid in inst["participants"]:
            return await interaction.response.send_message("You’ve already joined.", ephemeral=True)
        # check capacity
        max_s = inst.get("max_slots")
        if max_s and len(inst["participants"]) >= max_s:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("It just became full 🙁", ephemeral=True)
        # add
        inst["participants"].append(uid)
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # update embed
        chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        await interaction.response.send_message("✅ You’ve joined!", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} joined `{iid[:8]}`.")

    async def _handle_leave(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("This button only works in-guild.", ephemeral=True)
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)
        uid = str(interaction.user.id)
        if uid not in inst["participants"]:
            return await interaction.response.send_message("You are not in this activity.", ephemeral=True)
        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # update embed
        chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        await interaction.response.send_message("🗑️ You’ve left.", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} left `{iid[:8]}`.")

    # -------------------------------------------------------------------------
    # BUTTON HANDLERS: DM invite accept / decline / leave
    # -------------------------------------------------------------------------
    async def _handle_dm_accept(self, interaction: discord.Interaction, iid: str, target: int):
        # find the guild that has this inst
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst or inst.get("status") != "OPEN":
            return await interaction.response.send_message("This invite is no longer valid.", ephemeral=True)
        uid = str(target)
        if uid in inst["participants"]:
            return await interaction.response.send_message("You’ve already accepted.", ephemeral=True)
        # capacity check
        max_s = inst.get("max_slots")
        if max_s and len(inst["participants"]) >= max_s:
            inst["status"] = "FULL"
            insts = await self.config.guild(guild).instances()
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)
            return await interaction.response.send_message("It just became full 🙁", ephemeral=True)
        inst["participants"].append(uid)
        insts = await self.config.guild(guild).instances()
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # update global embed
        chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        # disable their DM buttons
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        await interaction.response.send_message("👍 You accepted!", ephemeral=True)
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(f"{interaction.user.mention} accepted your private invite `{iid[:8]}`.")
            except:
                pass
        await self._log(guild, f"{interaction.user.mention} accepted private invite for `{iid[:8]}`.")

    async def _handle_dm_decline(self, interaction: discord.Interaction, iid: str, target: int):
        # very similar to accept but no join
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("This invite is no longer valid.", ephemeral=True)
        # disable their DM buttons
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        await interaction.response.send_message("❌ You declined.", ephemeral=True)
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                await owner.send(f"{interaction.user.mention} declined your private invite `{iid[:8]}`.")
            except:
                pass
        await self._log(guild, f"{interaction.user.mention} declined private invite for `{iid[:8]}`.")

    async def _handle_dm_leave(self, interaction: discord.Interaction, iid: str, target: int):
        # a user who previously accepted can leave via DM
        inst = None; guild = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g; inst = insts[iid]; break
        if not inst:
            return await interaction.response.send_message("This invite is no longer valid.", ephemeral=True)
        uid = str(target)
        if uid not in inst["participants"]:
            return await interaction.response.send_message("You never joined.", ephemeral=True)
        inst["participants"].remove(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts = await self.config.guild(guild).instances()
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)
        # update public embed
        chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
        if chid and mid:
            ch = guild.get_channel(chid)
            if ch:
                try:
                    msg = await ch.fetch_message(mid)
                    e = self._build_embed(inst, guild)
                    await msg.edit(embed=e, view=PublicActivityView(self, iid))
                except:
                    pass
        await interaction.response.send_message("🗑️ You’ve left.", ephemeral=True)
        await self._log(guild, f"{interaction.user.mention} left private invite `{iid[:8]}`.")

    # -------------------------------------------------------------------------
    # COMMANDS: activity group
    # -------------------------------------------------------------------------
    @commands.group(name="activity", invoke_without_command=True)
    @commands.guild_only()
    async def activity(self, ctx):
        """Manage or create activities."""
        await ctx.send_help(ctx.command)

    @activity.command(name="setdefault")
    @checks.guildowner()
    async def set_default(
        self, ctx, channel: TextChannel = None
    ):
        """Set or clear the default public‐post channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).default_channel_id.set(cid)
        msg = f"Default channel set to {channel.mention}" if channel else "Default channel cleared"
        await ctx.send(msg)

    @activity.command(name="logchannel")
    @checks.guildowner()
    async def set_logchannel(
        self, ctx, channel: TextChannel = None
    ):
        """Set or clear the log channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).log_channel_id.set(cid)
        msg = f"Log channel set to {channel.mention}" if channel else "Log channel cleared"
        await ctx.send(msg)

    @activity.command(name="prunechannel")
    @checks.guildowner()
    async def set_prunechannel(
        self, ctx, channel: TextChannel = None
    ):
        """Set or clear the monthly‐prune summary channel."""
        cid = channel.id if channel else None
        await self.config.guild(ctx.guild).prune_summary_channel.set(cid)
        msg = f"Prune summary channel set to {channel.mention}" if channel else "Prune summary channel cleared"
        await ctx.send(msg)

    @activity.command(name="list")
    async def list_activities(self, ctx):
        """List live activities."""
        insts = await self.config.guild(ctx.guild).instances()
        if not insts:
            return await ctx.send("No active activities right now.")
        embed = discord.Embed(title="Current Activities", color=discord.Color.green())
        for iid, inst in insts.items():
            owner = ctx.guild.get_member(inst["owner_id"])
            embed.add_field(
                name=f"{iid[:8]}: {inst['title']}",
                value=(
                    f"Owner: {owner.mention if owner else inst['owner_id']}\n"
                    f"Status: {inst['status']}"
                ),
                inline=False,
            )
        await ctx.send(embed=embed)

    @activity.command(name="info")
    async def info_activity(self, ctx, iid: str):
        """Show detailed info about one activity."""
        insts = await self.config.guild(ctx.guild).instances()
        inst = None
        for k in insts:
            if k.startswith(iid.lower()):
                inst = insts[k]; full = k; break
        if not inst:
            return await ctx.send("No such activity.")
        embed = self._build_embed(inst, ctx.guild)
        embed.title = f"Info: {embed.title}"
        embed.set_footer(text=f"ID: {full[:8]}")
        await ctx.send(embed=embed)

    @activity.command(name="prune")
    @checks.guildowner()
    async def prune_activities(
        self, ctx, status: str = "ENDED", older_than: int = None
    ):
        """
        Manually prune activities.  status=OPEN/FULL/CLOSED/ENDED, older_than=days
        """
        removed = []
        insts = await self.config.guild(ctx.guild).instances()
        now = time.time()
        for k, inst in list(insts.items()):
            if inst.get("status") != status.upper():
                continue
            if older_than is not None:
                if (now - inst.get("created_at", now)) < older_than * 86400:
                    continue
            # delete embeds
            chid = inst.get("public_channel_id"); mid = inst.get("public_message_id")
            if chid and mid:
                ch = ctx.guild.get_channel(chid)
                if ch:
                    try:
                        msg = await ch.fetch_message(mid)
                        await msg.delete()
                    except:
                        pass
            insts.pop(k)
            removed.append(k)
        await self.config.guild(ctx.guild).instances.set(insts)
        await ctx.send(f"Pruned {len(removed)} activities.")

    # -------------------------------------------------------------------------
    # MONTHLY PRUNE SCHEDULER
    # -------------------------------------------------------------------------
    async def _monthly_prune_scheduler(self):
        await self.bot.wait_until_ready()
        while True:
            now = datetime.utcnow()
            # next first of month at midnight UTC
            if now.month == 12:
                nxt = datetime(now.year + 1, 1, 1)
            else:
                nxt = datetime(now.year, now.month + 1, 1)
            delay = (nxt - now).total_seconds()
            await asyncio.sleep(delay)
            # run prune of ENDED
            for guild in self.bot.guilds:
                cid = await self.config.guild(guild).prune_summary_channel()
                pruned = []
                insts = await self.config.guild(guild).instances()
                for k, inst in list(insts.items()):
                    if inst.get("status") == "ENDED":
                        insts.pop(k)
                        pruned.append((k, inst))
                if pruned:
                    await self.config.guild(guild).instances.set(insts)
                    if cid:
                        ch = guild.get_channel(cid)
                        if ch:
                            lines = "\n".join(f"`{k[:8]}` • {i['title']}" for k, i in pruned)
                            try:
                                await ch.send(f"Auto-pruned {len(pruned)} activities:\n{lines}")
                            except:
                                pass

    # -------------------------------------------------------------------------
    # INTERACTIVE CREATION / TEMPLATES
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
        Save an activity template under <name>.
        You will be asked: title, description, public/private, channel (or default), max slots, schedule (or skip), role/users or all for DM.
        """
        name = name.lower()
        existing = await self.config.guild(ctx.guild).templates()
        if name in existing:
            return await ctx.send("That template already exists; remove it first if you want.")
        await ctx.send("**Template Setup**\nYou have 300 s per question.  Reply ‘skip’ to leave optional fields blank.")
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel
        try:
            await ctx.send("1) Title of the activity:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            title = m.content.strip()[:100]

            await ctx.send("2) Description (or ‘skip’):")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            desc = "" if m.content.lower() == "skip" else m.content.strip()[:500]

            await ctx.send("3) Public or Private? (public/private)")
            m = await self.bot.wait_for("message", check=check, timeout=120)
            public = m.content.lower().startswith("p")

            channel_id = None
            dm_targets = []
            if public:
                await ctx.send("4) Posting channel?  Mention it or reply ‘default’:")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if m.content.lower().startswith("default"):
                    channel_id = None
                elif m.channel_mentions:
                    channel_id = m.channel_mentions[0].id
                else:
                    return await ctx.send("No channel detected; aborting.")
            else:
                await ctx.send("4) Whom to DM?  Mention role or users, or ‘all’ for channel members:")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if m.content.lower().startswith("all"):
                    dm_targets = [u.id for u in ctx.channel.members if not u.bot]
                elif m.role_mentions:
                    dm_targets = [u.id for u in m.role_mentions[0].members if not u.bot]
                elif m.mentions:
                    dm_targets = [u.id for u in m.mentions if not u.bot]
                else:
                    return await ctx.send("No valid targets; aborting.")

            await ctx.send("5) Max slots?  Number or ‘none’:")
            m = await self.bot.wait_for("message", check=check, timeout=120)
            max_s = None
            if not m.content.lower().startswith("none"):
                try:
                    max_s = int(m.content.strip())
                except:
                    max_s = None

            await ctx.send("6) Scheduled time?  YYYY-MM-DD HH:MM UTC, or ‘skip’:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            sched = None
            if not m.content.lower().startswith("skip"):
                try:
                    dt = datetime.strptime(m.content.strip(), "%Y-%m-%d %H:%M")
                    sched = dt.timestamp()
                except:
                    sched = None

        except asyncio.TimeoutError:
            return await ctx.send("Timed out; template save aborted.")

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
            return await ctx.send("No templates saved.")
        lines = []
        for name, t in tpls.items():
            kind = "Pub" if t["public"] else "Priv"
            lines.append(f"`{name}` • {kind} • “{t['title']}”")
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

    @activity.command(name="start")
    async def activity_start(self, ctx, template: str = None):
        """
        Start a new activity.  Optionally pass the name of a saved template to prefill.
        Otherwise, you will be walked through an interactive wizard.
        """
        guild = ctx.guild
        tpls = await self.config.guild(guild).templates()
        tpl = None
        if template and template.lower() in tpls:
            tpl = tpls[template.lower()]

        # wizard
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        # if using a template, copy fields and only ask for overrides; else full wizard
        if tpl:
            inst = {
                "title": tpl["title"],
                "description": tpl["description"],
                "public": tpl["public"],
                "public_channel_id": tpl["channel_id"] or await self.config.guild(guild).default_channel_id(),
                "dm_targets": tpl["dm_targets"],
                "max_slots": tpl["max_slots"],
                "scheduled_time": tpl["scheduled_time"],
            }
            # allow override of schedule
            await ctx.send("Using template; reply with new schedule (YYYY-MM-DD HH:MM UTC) or ‘skip’:")
            try:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if not m.content.lower().startswith("skip"):
                    try:
                        dt = datetime.strptime(m.content.strip(), "%Y-%m-%d %H:%M")
                        inst["scheduled_time"] = dt.timestamp()
                    except:
                        pass
            except asyncio.TimeoutError:
                pass
        else:
            inst = {}
            await ctx.send("**Activity Wizard** (300 s per question; ‘skip’ to leave optional blank)")
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
                    await ctx.send("4) Posting channel? Mention or ‘default’")
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    if m.channel_mentions:
                        inst["public_channel_id"] = m.channel_mentions[0].id
                    else:
                        inst["public_channel_id"] = await self.config.guild(guild).default_channel_id()
                else:
                    await ctx.send("4) DM who? Mention role/users or ‘all’ for this channel:")
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    if m.content.lower().startswith("all"):
                        inst["dm_targets"] = [u.id for u in ctx.channel.members if not u.bot]
                    elif m.role_mentions:
                        inst["dm_targets"] = [u.id for u in m.role_mentions[0].members if not u.bot]
                    elif m.mentions:
                        inst["dm_targets"] = [u.id for u in m.mentions if not u.bot]
                    else:
                        return await ctx.send("No targets → abort.")

                await ctx.send("5) Max slots? Number or ‘none’")
                m = await self.bot.wait_for("message", check=check, timeout=120)
                if m.content.lower().startswith("none"):
                    inst["max_slots"] = None
                else:
                    try:
                        inst["max_slots"] = int(m.content.strip())
                    except:
                        inst["max_slots"] = None

                await ctx.send("6) Scheduled? YYYY-MM-DD HH:MM UTC or ‘skip’")
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
                return await ctx.send("Timed out; creation aborted.")

            # set default channel if none for public
            if inst["public"] and not inst.get("public_channel_id"):
                inst["public_channel_id"] = await self.config.guild(guild).default_channel_id()

        # build the rest of the instance
        iid = uuid.uuid4().hex
        inst.update({
            "owner_id": ctx.author.id,
            "created_at": time.time(),
            "end_time": time.time() + 12 * 3600,
            "status": "OPEN",
            "participants": [],
            "public_message_id": None,
            "dm_message_ids": {},
            "extend_message_id": None,
        })
        # store it
        allinst = await self.config.guild(guild).instances()
        allinst[iid] = inst
        await self.config.guild(guild).instances.set(allinst)

        # now dispatch
        if inst["public"]:
            ch = guild.get_channel(inst["public_channel_id"])
            if not ch:
                return await ctx.send("Invalid channel; abort.")
            e = self._build_embed(inst, guild)
            view = PublicActivityView(self, iid)
            msg = await ch.send(embed=e, view=view)
            inst["public_message_id"] = msg.id
            allinst[iid] = inst
            await self.config.guild(guild).instances.set(allinst)
            await self.bot.add_view(view, message_id=msg.id)
            await ctx.send(f"✅ Public activity created (ID `{iid[:8]}`) in {ch.mention}.")
            await self._log(guild, f"{ctx.author.mention} created public `{iid[:8]}` “{inst['title']}”.")
        else:
            fails = []
            for uid in inst["dm_targets"]:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dch = await user.create_dm()
                    e = self._build_embed(inst, guild)
                    view = DMInviteView(self, iid, uid)
                    dm = await dch.send(embed=e, view=view)
                    inst["dm_message_ids"][str(uid)] = dm.id
                    await self.bot.add_view(view, message_id=dm.id)
                    await self._log(guild, f"Invited {user.mention} to private `{iid[:8]}`.")
                except Exception:
                    fails.append(str(uid))
            allinst[iid] = inst
            await self.config.guild(guild).instances.set(allinst)
            if fails:
                await ctx.send(f"Created private `{iid[:8]}`, but failed to DM: {', '.join(fails)}")
            else:
                await ctx.send(f"✅ Private activity created and invites sent (ID `{iid[:8]}`).")
        # schedule auto-end
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))


async def setup(bot: Red):
    await bot.add_cog(Activities(bot))