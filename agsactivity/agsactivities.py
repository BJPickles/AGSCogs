import asyncio
import time
import logging
import random
import re
from datetime import datetime, timezone

import discord
from discord import TextChannel, Guild, Member, DMChannel
from discord.ui import View, Button, Modal, TextInput

from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.activity")

# ─────────────────────────────────────────────────────────────────────────────
# ID GENERATION
# ─────────────────────────────────────────────────────────────────────────────
ADJECTIVES = [
    "soggy", "feral", "cranky", "boiling", "fluffy",
    "dented", "howling", "needy", "glowing", "confused",
    "bitter", "stinky", "whimsical", "haunted", "yeasty",
    "limping", "snoring", "oily", "gothic", "ticklish",
    "squeaky", "melting", "boozy", "sassy", "mossy",
    "grumpy", "flustered", "spindly", "leaky", "bristling",
    "nervous", "matted", "cackling", "rusty", "greedy",
    "burping", "wobbly", "itchy", "chubby", "moody"
]
NOUNS = [
    "trombone", "marshmallow", "doorknob", "llama", "toaster",
    "cauldron", "banister", "pigeon", "wetsock", "wig",
    "clam", "banjo", "meatball", "dustbin", "plunger",
    "snail", "waffle", "typewriter", "tentacle", "cabbage",
    "goblin", "sandal", "kettle", "mop", "noodle",
    "sockdrawer", "sloth", "grapefruit", "soapdish", "beehive",
    "tupperware", "zucchini", "pylon", "skillet", "badger",
    "jukebox", "footstool", "turnip", "crayon", "giraffe"
]


def generate_id(existing_ids):
    """Generate a random two-word ID not in existing_ids."""
    while True:
        iid = f"{random.choice(ADJECTIVES)}-{random.choice(NOUNS)}"
        if iid not in existing_ids:
            return iid

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_GUILD = {
    "default_channel_id":      None,   # Default channel for public activities
    "log_channel_id":          None,   # Audit log channel
    "prune_summary_channel":   None,   # Monthly‐prune summary channel
    "templates":               {},     # Saved templates
    "instances":               {},     # All live & scheduled instances
}

# ─────────────────────────────────────────────────────────────────────────────
# UI CLASSES
# ─────────────────────────────────────────────────────────────────────────────
class ActionButton(Button):
    """A tiny Button subclass that takes a callback function."""
    def __init__(self, *, label: str, style: discord.ButtonStyle, custom_id: str, cb):
        super().__init__(label=label, style=style, custom_id=custom_id)
        self._cb = cb

    async def callback(self, interaction: discord.Interaction):
        try:
            await self._cb(interaction)
        except Exception:
            log.exception("Error in button callback")
            if not interaction.response.is_done():
                await interaction.response.send_message("An error occurred.", ephemeral=True)


class PublicActivityView(View):
    """Join/Leave buttons for public OPEN activities."""
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
            self.add_item(ActionButton(
                label="Leave",
                style=discord.ButtonStyle.red,
                custom_id=f"act:reminder:leave:{iid}:{target_id}",
                cb=self.reminder_leave,
            ))
        else:
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
        class ReplyModal(Modal):
            def __init__(self_inner):
                super().__init__(title="Send a message to the owner")
                self_inner.response = TextInput(
                    label="Your message",
                    style=discord.TextStyle.paragraph,
                    placeholder="Type your message…",
                    max_length=500,
                    required=True,
                )
                self_inner.add_item(self_inner.response)

            async def on_submit(self_inner, modal_interaction: discord.Interaction):
                # Ephemeral isn't supported in DMs, so only use it in guild channels
                if isinstance(modal_interaction.channel, discord.DMChannel):
                    await modal_interaction.response.send_message(
                        "Your message has been sent to the activity owner."
                    )
                else:
                    await modal_interaction.response.send_message(
                        "Your message has been sent to the activity owner.",
                        ephemeral=True
                    )
                try:
                    await self.cog._handle_invite_reply(
                        modal_interaction,
                        self.iid,
                        self.target_id,
                        self_inner.response.value,
                    )
                except Exception:
                    log.exception("Error in invite-reply handler")

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


class PrivateManageView(View):
    """
    Join/Leave buttons for private events in DMs.
    Allows rejoin if you accidentally leave.
    """
    def __init__(self, cog: "Activities", iid: str, user_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.iid = iid
        self.user_id = user_id
        self.add_item(ActionButton(
            label="Join",
            style=discord.ButtonStyle.green,
            custom_id=f"act:priv:join:{iid}:{user_id}",
            cb=self.join,
        ))
        self.add_item(ActionButton(
            label="Leave",
            style=discord.ButtonStyle.red,
            custom_id=f"act:priv:leave:{iid}:{user_id}",
            cb=self.leave,
        ))

    async def join(self, interaction: discord.Interaction):
        await self.cog._handle_private_join(interaction, self.iid, self.user_id)

    async def leave(self, interaction: discord.Interaction):
        await self.cog._handle_private_leave(interaction, self.iid, self.user_id)


# ─────────────────────────────────────────────────────────────────────────────
# ACTIVITIES COG
# ─────────────────────────────────────────────────────────────────────────────
class Activities(commands.Cog):
    """Fully-featured activities cog with scheduling, RSVP, embeds, logs, destinations."""
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=9876543210123456, force_registration=True
        )
        self.config.register_guild(**DEFAULT_GUILD)
        bot.loop.create_task(self._startup_tasks())
        bot.loop.create_task(self._monthly_prune_scheduler())

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

                # Live private invites & manage DMs
                if status == "OPEN" and not inst["public"]:
                    for uid, mid in msgs.get("invites", {}).items():
                        self.bot.add_view(
                            InviteView(self, iid, int(uid)),
                            message_id=mid,
                        )
                    for uid, mid in msgs.get("manages", {}).items():
                        self.bot.add_view(
                            PrivateManageView(self, iid, int(uid)),
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
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            # Next 1st of month at 00:00 UTC
            if now.month == 12:
                nxt = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                nxt = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
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
                            f"`{i}` • {inst['title']}" for i, inst in pruned
                        )
                        try:
                            await ch.send(f"Auto-pruned {len(pruned)} activities:\n{lines}")
                        except Exception:
                            log.exception("Failed sending prune summary")

    # ──────────────────────────────────────────────────────────────────────────
    # Embed Builder & Logging
    # ──────────────────────────────────────────────────────────────────────────
    def _build_embed(self, inst: dict, guild: Guild) -> discord.Embed:
        # Participants list (now storing ints)
        parts = []
        for uid in inst["participants"]:
            m = guild.get_member(uid)
            parts.append(m.display_name if m else f"User#{uid}")

        # Slots & status emoji
        curr = len(parts)
        maxs = inst.get("max_slots")
        if maxs:
            ratio = curr / maxs
            emoji = "🟢" if ratio < 0.5 else ("🟠" if ratio < 1 else "🔴")
            slots = f"{curr}/{maxs}"
        else:
            emoji = "🟢"
            slots = f"{curr}/∞"

        # Base embed
        title = f"{emoji} {inst['title']}"
        e = discord.Embed(
            title=title,
            description=inst.get("description", "No description."),
            color=discord.Color.blurple(),
        )

        # Owner
        owner = guild.get_member(inst["owner_id"]) or self.bot.get_user(inst["owner_id"])
        e.add_field(name="Owner", value=owner.mention if owner else "Unknown", inline=True)
        # Slots
        e.add_field(name="Slots", value=slots, inline=True)
        # Scheduled
        sched = inst.get("scheduled_time")
        if sched:
            e.add_field(
                name="Scheduled",
                value=f"<t:{int(sched)}:F> (<t:{int(sched)}:R>)",
                inline=False,
            )
        # Destination
        dest = inst.get("destination")
        if dest:
            e.add_field(name="Destination", value=dest, inline=False)
        # Participants list
        if parts:
            e.add_field(name="Participants", value="\n".join(parts), inline=False)
        # Footer with channel name
        cid = inst.get("channel_id")
        if cid:
            ch = guild.get_channel(cid)
            if ch:
                e.set_footer(text=f"In #{ch.name}")
        return e

    async def _log(self, guild: Guild, message: str):
        """Audit‐style log with localized timestamp."""
        cid = await self.config.guild(guild).log_channel_id()
        if not cid:
            return
        ch = guild.get_channel(cid)
        if not ch:
            return
        ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
        try:
            await ch.send(f"[<t:{ts}:F>] {message}")
        except Exception:
            log.exception("Failed to send log message")

    # ──────────────────────────────────────────────────────────────────────────
    # Auto‐end & Scheduled Start
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

        # Mark ended
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
                except:
                    pass

        # DM owner to extend/finalize
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
            except:
                log.exception("Failed to DM owner about auto-end")

        await self._log(
            guild,
            f"Auto-ended activity `{iid}` (“{inst['title']}”)."
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

        # Prompt owner to update destination
        owner = self.bot.get_user(inst["owner_id"])
        if owner:
            try:
                class DestUpdateView(View):
                    def __init__(self, cog, iid):
                        super().__init__(timeout=None)
                        self.cog = cog
                        self.iid = iid
                        self.add_item(ActionButton(
                            label="Update Destination",
                            style=discord.ButtonStyle.primary,
                            custom_id=f"act:dest:update:{iid}",
                            cb=self.update,
                        ))
                        self.add_item(ActionButton(
                            label="Skip",
                            style=discord.ButtonStyle.secondary,
                            custom_id=f"act:dest:skip:{iid}",
                            cb=self.skip,
                        ))

                    async def update(self, inter: discord.Interaction):
                        class DestModal(Modal):
                            def __init__(self_inner):
                                super().__init__(title="New Destination")
                                self_inner.dest = TextInput(
                                    label="Destination (text or #channel)",
                                    placeholder="e.g. Voice chat #🔊 or URL…",
                                    required=False,
                                )
                                self_inner.add_item(self_inner.dest)

                            async def on_submit(self_inner, mod_i: discord.Interaction):
                                # Acknowledge
                                await mod_i.response.send_message("Destination updated.", ephemeral=True)
                                await self.cog._handle_destination_update(
                                    mod_i, self_inner.dest.value, self.iid
                                )

                        await inter.response.send_modal(DestModal())

                    async def skip(self, inter: discord.Interaction):
                        await inter.response.edit_message(content="Skipped destination update.", view=None)

                view = DestUpdateView(self, iid)
                await owner.send(
                    f"🔔 Your scheduled activity **{inst['title']}** has now started.\nID: `{iid}`",
                    view=view
                )
            except:
                log.exception("Failed to DM owner for destination update")

        # Public vs Private launch
        human_start = f"<t:{int(inst['start_time'])}:F>"
        if inst["public"]:
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
                except:
                    log.exception("Failed to send public activity start message")
            await self._log(guild, f"Scheduled public `{iid}` started at {human_start}.")
        else:
            # Add accepted RSVPs as participants
            for uid_str, state in inst["rsvps"].items():
                if state == "ACCEPTED":
                    # store as int
                    inst["participants"].append(int(uid_str))
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)

            # Send reminder + manage DMs
            for uid in inst["participants"]:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    # Reminder embed
                    rem_e = self._build_embed(inst, guild)
                    rem_e.title = f"🔔 Reminder: {rem_e.title}"
                    v1 = InviteView(self, iid, uid, reminder=True)
                    rem_msg = await dm.send(embed=rem_e, view=v1)
                    inst["message_ids"].setdefault("reminders", {})[str(uid)] = rem_msg.id
                    self.bot.add_view(v1, message_id=rem_msg.id)
                    # Manage embed
                    man_e = self._build_embed(inst, guild)
                    v2 = PrivateManageView(self, iid, uid)
                    man_msg = await dm.send(embed=man_e, view=v2)
                    inst["message_ids"].setdefault("manages", {})[str(uid)] = man_msg.id
                    self.bot.add_view(v2, message_id=man_msg.id)
                except:
                    log.exception(f"Failed to DM user {uid} for reminder/manage on start")
            await self._log(guild, f"Scheduled private `{iid}` started at {human_start} (reminders & manage DMs sent).")

        # Schedule auto-end
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))

    async def _handle_destination_update(self, interaction: discord.Interaction, dest_text: str, iid: str):
        """Owner updated destination; persist & edit all embeds."""
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return
        # Parse channel mention if present
        if m := re.search(r"<#(\d+)>", dest_text):
            ch = guild.get_channel(int(m.group(1)))
            dest = f"#{ch.name}" if ch else dest_text
        else:
            dest = dest_text or None
        inst["destination"] = dest
        insts[iid] = inst
        await self.config.guild(guild).instances.set(insts)

        # Rebuild and edit every embed
        e = self._build_embed(inst, guild)
        mids = inst["message_ids"]

        # Public
        if mids.get("public"):
            try:
                msg = await guild.get_channel(inst["channel_id"]).fetch_message(mids["public"])
                await msg.edit(embed=e)
            except:
                log.exception("Failed to edit public embed on destination update")

        # Private DMs (invites, reminders, manages)
        for key in ("invites", "reminders", "manages"):
            for uid_str, mid in mids.get(key, {}).items():
                try:
                    # DMs need to fetch user then channel
                    uid = int(uid_str)
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    await dm.edit_message(mid, embed=e)
                except:
                    continue

        await interaction.response.edit_message(content="All embeds updated with new destination.", view=None)

    # ──────────────────────────────────────────────────────────────────────────
    # Dispatch OPEN (public or private)
    # ──────────────────────────────────────────────────────────────────────────
    async def _dispatch_open(self, guild: Guild, iid: str, ctx):
        insts = await self.config.guild(guild).instances()
        inst = insts[iid]
        author = ctx.author

        # Ensure creator auto-joins (store as int)
        if author.id not in inst["participants"]:
            inst["participants"].append(author.id)
            insts[iid] = inst
            await self.config.guild(guild).instances.set(insts)

        if inst["public"]:
            ch = guild.get_channel(inst["channel_id"])
            if not ch:
                return await ctx.send("Invalid public channel.")
            e = self._build_embed(inst, guild)
            view = PublicActivityView(self, iid)
            msg = await ch.send(embed=e, view=view)
            inst["message_ids"]["public"] = msg.id
            await self.config.guild(guild).instances.set(insts)
            self.bot.add_view(view, message_id=msg.id)
            await ctx.send(f"✅ Public activity created (ID `{iid}`).")
            await self._log(guild, f"{author.mention} created public **{inst['title']}** (`{iid}`).")
       #     
        else:
            fails = []

            # ───> don’t DM the owner an “invite” (they’re auto-accepted)
            owner = inst["owner_id"]
            # author.id has already been appended to participants above
            invite_targets = [uid for uid in inst["dm_targets"] if uid != owner]

            for uid in invite_targets:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    e = self._build_embed(inst, guild)
                    view1 = InviteView(self, iid, uid)
                    inv_msg = await dm.send(embed=e, view=view1)
                    inst["message_ids"].setdefault("invites", {})[str(uid)] = inv_msg.id
                    self.bot.add_view(view1, message_id=inv_msg.id)
                except:
                    fails.append(uid)

            # ───> now send the “manage” DM to everyone who is already a participant,
            #      including the owner (so they get exactly one manage embed)
            for uid in inst["participants"]:
                try:
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    man_e = self._build_embed(inst, guild)
                    v2 = PrivateManageView(self, iid, uid)
                    man_msg = await dm.send(embed=man_e, view=v2)
                    inst["message_ids"].setdefault("manages", {})[str(uid)] = man_msg.id
                    self.bot.add_view(v2, message_id=man_msg.id)
                except:
                    log.exception(f"Failed to DM manage for user {uid}")

            await self.config.guild(guild).instances.set(insts)




            if fails:
                await ctx.send(f"✅ Private created (ID `{iid}`), but failed to DM: " + ", ".join(f"<@{u}>" for u in fails))
            else:
                await ctx.send(f"✅ Private activity created and invites sent (ID `{iid}`).")

    # ──────────────────────────────────────────────────────────────────────────
    # Commands: activity group, setdefault, logchannel, prunechannel, list, info, prune, stop
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
        embed = discord.Embed(title="Activities", color=discord.Color.green())
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
                name=f"{iid}: {inst['title']}",
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
        full = next((k for k in insts if k.startswith(iid)), None)
        if not full:
            return await ctx.send("No such activity.")
        inst = insts[full]
        embed = self._build_embed(inst, ctx.guild)
        embed.title = f"Info: {embed.title}"
        embed.set_footer(text=f"ID: `{full}` • Status: {inst['status']}")
        await ctx.send(embed=embed)

    @activity.command(name="prune")
    @checks.guildowner()
    async def prune_activities(self, ctx, status: str = "ENDED", older_than: int = None):
        """
        Manually prune activities by status and optional minimum age (days).
        Does NOT delete any channel messages; preserves history.
        """
        insts = await self.config.guild(ctx.guild).instances()
        now = time.time()
        removed = []
        for iid, inst in list(insts.items()):
            if inst["status"] != status.upper():
                continue
            created = inst.get("created_at", now)
            if older_than and (now - created) < older_than * 86400:
                continue
            insts.pop(iid)
            removed.append(iid)
        await self.config.guild(ctx.guild).instances.set(insts)
        await ctx.send(f"Pruned {len(removed)} activities (embeds retained).")

    @activity.command(name="stop")
    async def stop_activity(self, ctx, iid: str):
        """Manually end (finalize) an OPEN or SCHEDULED activity."""
        insts = await self.config.guild(ctx.guild).instances()
        full = next((k for k in insts if k.startswith(iid)), None)
        if not full:
            return await ctx.send("❌ No such activity.")
        inst = insts[full]
        if inst["owner_id"] != ctx.author.id:
            return await ctx.send("❌ Only the activity owner can stop it.")
        if inst["status"] == "ENDED":
            return await ctx.send("ℹ️ That activity is already ended.")
        inst["status"] = "ENDED"
        insts[full] = inst
        await self.config.guild(ctx.guild).instances.set(insts)

        # Remove public buttons if present
        pm = inst["message_ids"].get("public")
        cid = inst.get("channel_id")
        if pm and cid:
            ch = ctx.guild.get_channel(cid)
            if ch:
                try:
                    msg = await ch.fetch_message(pm)
                    await msg.edit(embed=self._build_embed(inst, ctx.guild), view=None)
                except:
                    pass

        await ctx.send(f"✅ Activity `{full}` has been stopped.")
        await self._log(
            ctx.guild,
            f"{ctx.author.mention} manually stopped `{full}` (“{inst['title']}”)."
        )

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
        Save a template with title, description, public/private, destination,
        channel or DM targets, max slots, and schedule.
        """
        name = name.lower()
        existing = await self.config.guild(ctx.guild).templates()
        if name in existing:
            return await ctx.send("That template already exists.")
        await ctx.send("**Template Setup:** 300s/question; type `skip` to omit optional.")
        def check(m): return m.author == ctx.author and m.channel == ctx.channel

        try:
            # 1) Title
            await ctx.send("1) Title:")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            title = m.content.strip()[:100]; await m.add_reaction("✅")

            # 2) Description
            await ctx.send("2) Description (or `skip`):")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            desc = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]
            await m.add_reaction("✅")

            # 3) Destination
            await ctx.send("3) Destination? Text or #channel mention or `skip`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                txt = m.content.strip()
                if txt.lower().startswith("skip"):
                    dest = None; await m.add_reaction("✅"); break
                dest = txt; await m.add_reaction("✅"); break

            # 4) Public or Private
            await ctx.send("4) Public or Private? (`public`/`private`):")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                v = m.content.strip().lower()
                if v in ("public","p","private","priv"):
                    public = v.startswith("p") and not v.startswith("pr")
                    await m.add_reaction("✅"); break
                await ctx.send("Invalid; type `public` or `private`.")

            channel_id = None
            dm_targets = []
            if public:
                await ctx.send("5) Channel? Mention it or type `default`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    txt = m.content.strip().lower()
                    if txt.startswith("default"):
                        channel_id = await self.config.guild(ctx.guild).default_channel_id()
                        await m.add_reaction("✅"); break
                    if m.channel_mentions:
                        channel_id = m.channel_mentions[0].id
                        await m.add_reaction("✅"); break
                    await ctx.send("Invalid; mention a channel or `default`.")
            else:
                await ctx.send("5) DM whom? Mention users/role or `all`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    txt = m.content.lower()
                    if txt.startswith("all"):
                        dm_targets = [u.id for u in ctx.guild.members if not u.bot]
                        await m.add_reaction("✅"); break
                    if m.role_mentions:
                        dm_targets = [u.id for u in m.role_mentions[0].members if not u.bot]
                        await m.add_reaction("✅"); break
                    if m.mentions:
                        dm_targets = [u.id for u in m.mentions if not u.bot]
                        await m.add_reaction("✅"); break
                    await ctx.send("Invalid; mention or `all`.")

            # 6) Max slots
            await ctx.send("6) Max slots? Number or `none`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                txt = m.content.strip().lower()
                if txt in ("none","n"):
                    max_slots = None; await m.add_reaction("✅"); break
                if txt.isdigit():
                    max_slots = int(txt); await m.add_reaction("✅"); break
                await ctx.send("Invalid; number or `none`.")

            # 7) Scheduled
            await ctx.send("7) Scheduled? `YYYY-MM-DD HH:MM` UTC or `skip`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=300)
                txt = m.content.strip()
                if txt.lower().startswith("skip"):
                    scheduled_time = None; await m.add_reaction("✅"); break
                try:
                    dt = datetime.strptime(txt, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                    ts = dt.timestamp()
                    if ts < time.time():
                        await ctx.send("Cannot schedule in the past; pick a future time.")
                        continue
                    scheduled_time = ts; await m.add_reaction("✅"); break
                except:
                    await ctx.send("Invalid; use `YYYY-MM-DD HH:MM` UTC or `skip`.")
        except asyncio.TimeoutError:
            return await ctx.send("❌ Timed out; aborting template setup.")

        tpl = {
            "title": title,
            "description": desc,
            "destination": dest,
            "public": public,
            "channel_id": channel_id,
            "dm_targets": dm_targets,
            "max_slots": max_slots,
            "scheduled_time": scheduled_time,
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
            for n,t in tpls.items()
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
    # Start / Schedule Command
    # ──────────────────────────────────────────────────────────────────────────
    @activity.command(name="start")
    async def activity_start(self, ctx, template: str = None):
        """
        Start or schedule an activity.
        Optionally specify a saved template name to pre-fill fields.
        """
        guild, author = ctx.guild, ctx.author
        tpls = await self.config.guild(guild).templates()
        tpl = tpls.get(template.lower()) if (template and template.lower() in tpls) else None

        inst = {}
        if tpl:
            inst.update(tpl)
            if inst["public"] and not inst.get("channel_id"):
                inst["channel_id"] = await self.config.guild(guild).default_channel_id()
        else:
            # Wizard
            await ctx.send("**Activity Wizard** (300s/question; type `skip` to omit optional)")
            def check(m): return m.author==author and m.channel==ctx.channel

            # 1) Title
            await ctx.send("1) Title:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=300)
                title = m.content.strip()
                if title:
                    inst["title"] = title[:100]; await m.add_reaction("✅"); break
                await ctx.send("Title cannot be empty.")

            # 2) Description
            await ctx.send("2) Description (or `skip`):")
            m = await self.bot.wait_for("message", check=check, timeout=300)
            inst["description"] = "" if m.content.lower().startswith("skip") else m.content.strip()[:500]
            await m.add_reaction("✅")

            # 3) Destination
            await ctx.send("3) Destination? Text or #channel mention or `skip`:")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                txt = m.content.strip()
                if txt.lower().startswith("skip"):
                    inst["destination"]=None; await m.add_reaction("✅"); break
                inst["destination"]=txt; await m.add_reaction("✅"); break

            # 4) Public/private
            await ctx.send("4) Public or Private? (`public`/`private`):")
            while True:
                m = await self.bot.wait_for("message", check=check, timeout=120)
                v=m.content.strip().lower()
                if v in ("public","p","private","priv"):
                    inst["public"]=v.startswith("p") and not v.startswith("pr")
                    await m.add_reaction("✅"); break
                await ctx.send("Invalid; type `public` or `private`.")

            # 5a) Channel if public
            if inst["public"]:
                await ctx.send("5) Channel? Mention or `default`:")
                while True:
                    m = await self.bot.wait_for("message", check=check, timeout=120)
                    txt=m.content.strip().lower()
                    if txt.startswith("default"):
                        inst["channel_id"]=await self.config.guild(guild).default_channel_id()
                        await m.add_reaction("✅"); break
                    if m.channel_mentions:
                        inst["channel_id"]=m.channel_mentions[0].id
                        await m.add_reaction("✅"); break
                    await ctx.send("Invalid; mention or `default`.")
            else:
                # 5b) DM targets
                await ctx.send("5) DM whom? Mention users/role or `all`:")
                while True:
                    m=await self.bot.wait_for("message", check=check, timeout=120)
                    txt=m.content.lower()
                    if txt.startswith("all"):
                        inst["dm_targets"]=[u.id for u in guild.members if not u.bot]
                        await m.add_reaction("✅"); break
                    if m.role_mentions:
                        inst["dm_targets"]=[u.id for u in m.role_mentions[0].members if not u.bot]
                        await m.add_reaction("✅"); break
                    if m.mentions:
                        inst["dm_targets"]=[u.id for u in m.mentions if not u.bot]
                        await m.add_reaction("✅"); break
                    await ctx.send("Invalid; mention or `all`.")

            # 6) Max slots
            await ctx.send("6) Max slots? Number or `none`:")
            while True:
                m=await self.bot.wait_for("message", check=check, timeout=120)
                txt=m.content.strip().lower()
                if txt in ("none","n"):
                    inst["max_slots"]=None; await m.add_reaction("✅"); break
                if txt.isdigit():
                    inst["max_slots"]=int(txt); await m.add_reaction("✅"); break
                await ctx.send("Invalid; number or `none`.")

            # 7) Scheduled
            await ctx.send("7) Scheduled? `YYYY-MM-DD HH:MM` UTC or `skip`:")
            while True:
                m=await self.bot.wait_for("message", check=check, timeout=300)
                txt=m.content.strip()
                if txt.lower().startswith("skip"):
                    inst["scheduled_time"]=None; await m.add_reaction("✅"); break
                try:
                    dt=datetime.strptime(txt,"%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                    ts=dt.timestamp()
                    if ts<time.time():
                        await ctx.send("Cannot schedule in the past.")
                        continue
                    inst["scheduled_time"]=ts; await m.add_reaction("✅"); break
                except:
                    await ctx.send("Invalid; use `YYYY-MM-DD HH:MM` UTC or `skip`.")

        # Common fields
        now=time.time()
        existing=await self.config.guild(guild).instances()
        iid=generate_id(existing.keys())
        status="SCHEDULED" if inst.get("scheduled_time") and inst["scheduled_time"]>now else "OPEN"
        inst.update({
            "owner_id": author.id,
            "created_at": now,
            "status": status,
            "participants": [],
            "rsvps": {},
            "message_ids": {"public":None,"extend":None,"invites":{}, "rsvps":{}, "reminders":{}, "manages":{}},
            "channel_id": inst.get("channel_id"),
            "destination": inst.get("destination"),
            "max_slots": inst.get("max_slots"),
            "scheduled_time": inst.get("scheduled_time"),
            "end_time": now+12*3600,
        })
        existing[iid]=inst
        await self.config.guild(guild).instances.set(existing)

        if status=="SCHEDULED":
            delay=inst["scheduled_time"]-now
            self.bot.loop.create_task(self._schedule_start(guild.id,iid,delay))
            human=f"<t:{int(inst['scheduled_time'])}:F>"
            if inst["public"]:
                await ctx.send(f"✅ Scheduled public `{iid}` for {human}.")
                await self._log(guild,f"{author.mention} scheduled public `{iid}` for {human}.")
            else:
                # RSVP invites
                fails=[]
                for uid in inst["dm_targets"]:
                    try:
                        user=self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                        dm=await user.create_dm()
                        e=discord.Embed(
                            title=f"RSVP: {inst['title']}",
                            description=inst.get("description",""),
                            color=discord.Color.blurple()
                        )
                        e.add_field(name="Scheduled for",value=human,inline=False)
                        view=InviteView(self,iid,uid,rsvp=True)
                        msg=await dm.send(embed=e,view=view)
                        inst["message_ids"]["rsvps"][str(uid)]=msg.id
                        inst["rsvps"][str(uid)]="PENDING"
                        self.bot.add_view(view,message_id=msg.id)
                    except:
                        fails.append(uid)
                await self.config.guild(guild).instances.set(existing)
                reply=f"✅ Scheduled private `{iid}`; RSVP invites sent."
                if fails:
                    reply+= "\nFailed to DM: "+" ".join(f"<@{u}>" for u in fails)
                await ctx.send(reply)
                await self._log(guild,f"{author.mention} scheduled private `{iid}`; failed to DM {fails}.")
            return

        # Immediate OPEN
        await self._dispatch_open(guild,iid,ctx)

    # ─── helper ──────────────────────────────────────────────────────────────────
    async def _find_instance(self, iid: str):
        """
        Scan all guilds for an instance matching iid.
        Returns (guild, insts_dict, inst_dict) or (None, None, None).
        """
        for guild in self.bot.guilds:
            insts = await self.config.guild(guild).instances()
            if iid in insts:
                return guild, insts, insts[iid]
        return None, None, None

    # ─── refresh dms ────────────────────────────────────────────────────────────
    async def _refresh_all_dms(self, guild: discord.Guild, iid: str):
        """
        Edit *every* invite/reminder/manage DM embed for activity `iid` so 
        that its participant list (and slot count) stays in sync.
        """
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst:
            return
        new_embed = self._build_embed(inst, guild)
        # categories to update
        for cat in ("invites", "reminders", "manages"):
            for uid_str, msg_id in inst["message_ids"].get(cat, {}).items():
                try:
                    uid = int(uid_str)
                    user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                    dm = await user.create_dm()
                    msg = await dm.fetch_message(msg_id)
                    await msg.edit(embed=new_embed)
                except discord.HTTPException as e:
                    # rate‐limit? pause and retry once
                    if e.status == 429:
                        await asyncio.sleep(2)
                        try:
                            await msg.edit(embed=new_embed)
                        except:
                            pass
                except Exception:
                    log.exception(f"Failed to refresh DM embed for {uid_str} in {cat}")

    # ─── public join/leave ──────────────────────────────────────────────────────
    async def _handle_public_join(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Guild context missing.", ephemeral=True)

        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst["status"] != "OPEN" or not inst["public"]:
            return await interaction.response.send_message("You can’t join that.", ephemeral=True)

        uid = interaction.user.id
        if uid in inst["participants"]:
            return await interaction.response.send_message("You’ve already joined.", ephemeral=True)

        # ───── enforce slot limit ─────
        max_slots = inst.get("max_slots")
        if max_slots is not None and len(inst["participants"]) >= max_slots:
            return await interaction.response.send_message(
                f"⛔ Sorry, this activity is full ({max_slots}/{max_slots} slots).",
                ephemeral=True
            )

        # ───── now actually join ─────
        inst["participants"].append(uid)
        await self.config.guild(guild).instances.set(insts)

        # edit the public embed to show new slots
        try:
            ch     = guild.get_channel(inst["channel_id"])
            msg_id = inst["message_ids"].get("public")
            if ch and msg_id:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(embed=self._build_embed(inst, guild))
        except Exception:
            log.exception("Failed to update public embed after join")

        return await interaction.response.send_message("✅ You have joined!", ephemeral=True)

    async def _handle_public_leave(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Guild context missing.", ephemeral=True)
        insts = await self.config.guild(guild).instances()
        inst = insts.get(iid)
        if not inst or inst["status"] != "OPEN" or not inst["public"]:
            return await interaction.response.send_message("You can’t leave that.", ephemeral=True)
        uid = interaction.user.id
        if uid not in inst["participants"]:
            return await interaction.response.send_message("You’re not in it.", ephemeral=True)
        inst["participants"].remove(uid)
        await self.config.guild(guild).instances.set(insts)

        try:
            ch = guild.get_channel(inst["channel_id"])
            msg_id = inst["message_ids"].get("public")
            if ch and msg_id:
                msg = await ch.fetch_message(msg_id)
                await msg.edit(embed=self._build_embed(inst, guild))
        except Exception:
            log.exception("Failed to update public embed after leave")

        await interaction.response.send_message("✅ You have left.", ephemeral=True)

    # ─── private DM join/leave ─────────────────────────────────────────────────
    async def _handle_private_join(self, interaction: discord.Interaction, iid: str, user_id: int):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Activity not found.", ephemeral=False)
        if interaction.user.id != user_id:
            return await interaction.response.send_message("This button isn’t for you.", ephemeral=False)
        if inst["public"] or inst["status"] != "OPEN":
            return await interaction.response.send_message("Cannot join this.", ephemeral=False)
        if user_id in inst["participants"]:
            return await interaction.response.send_message("Already joined.", ephemeral=False)

        # ───── enforce slot limit ─────
        max_slots = inst.get("max_slots")
        if max_slots is not None and len(inst["participants"]) >= max_slots:
            return await interaction.response.send_message(
                f"⛔ Sorry, this activity is full ({max_slots}/{max_slots} slots).",
                ephemeral=False
            )

        # ───── now actually join ─────
        inst["participants"].append(user_id)
        await self.config.guild(guild).instances.set(insts)

        # update or send the manage‐DM
        embed = self._build_embed(inst, guild)
        view  = PrivateManageView(self, iid, user_id)
        try:
            # if a DM-manage message existed, edit it
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.HTTPException:
            # otherwise send a new one
            dm     = await interaction.user.create_dm()
            man_msg = await dm.send(embed=embed, view=view)
            inst["message_ids"].setdefault("manages", {})[str(user_id)] = man_msg.id
            await self.config.guild(guild).instances.set(insts)

        # <— newly added: refresh every DM embed for this activity
        self.bot.loop.create_task(self._refresh_all_dms(guild, iid))

    async def _handle_private_leave(self, interaction: discord.Interaction, iid: str, user_id: int):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Activity not found.", ephemeral=False)
        if interaction.user.id != user_id:
            return await interaction.response.send_message("This button isn’t for you.", ephemeral=False)
        if inst["public"] or inst["status"] != "OPEN":
            return await interaction.response.send_message("Cannot leave this.", ephemeral=False)
        if user_id not in inst["participants"]:
            return await interaction.response.send_message("You’re not in it.", ephemeral=False)

        inst["participants"].remove(user_id)
        await self.config.guild(guild).instances.set(insts)

        embed = self._build_embed(inst, guild)
        view = PrivateManageView(self, iid, user_id)
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception:
            log.exception("Failed to edit private‐leave manage message")

        # <— newly added: refresh every DM embed for this activity
        self.bot.loop.create_task(self._refresh_all_dms(guild, iid))

    # ─── RSVP / reminder / private‐invite ────────────────────────────────────────
    async def _handle_rsvp(self, interaction: discord.Interaction, iid: str, target_id: int, accepted: bool):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Scheduled activity not found.", ephemeral=False)
        key = str(target_id)
        inst["rsvps"][key] = "ACCEPTED" if accepted else "DECLINED"
        await self.config.guild(guild).instances.set(insts)
        await interaction.response.edit_message(
            content=f"You have {'accepted' if accepted else 'declined'} the RSVP.",
            view=None
        )

    async def _handle_reminder_leave(self, interaction: discord.Interaction, iid: str, target_id: int):
        guild, insts, inst = await self._find_instance(iid)
        if guild and target_id in inst["participants"]:
            inst["participants"].remove(target_id)
            await self.config.guild(guild).instances.set(insts)
        await interaction.response.edit_message(
            content="You have left the upcoming activity.",
            view=None
        )

    async def _handle_invite_accept(self, interaction: discord.Interaction, iid: str, target_id: int):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)
        uid = target_id
        if uid not in inst["participants"]:
            inst["participants"].append(uid)
            await self.config.guild(guild).instances.set(insts)

        # disable the invite buttons & update *that* invite message
        await interaction.response.edit_message(embed=self._build_embed(inst, guild), view=None)
        
        # send the acceptor their personal manage‐DM
        man_embed = self._build_embed(inst, guild)
        v2 = PrivateManageView(self, iid, uid)
        dm = await interaction.user.create_dm()
        man_msg = await dm.send(embed=man_embed, view=v2)
        inst["message_ids"].setdefault("manages", {})[str(uid)] = man_msg.id
        await self.config.guild(guild).instances.set(insts)

        # now refresh every other DM embed (owner, other invites/reminders, etc.)
        self.bot.loop.create_task(self._refresh_all_dms(guild, iid))

    async def _handle_invite_decline(self, interaction: discord.Interaction, iid: str, target_id: int):
        await interaction.response.edit_message(
            content="You have declined the invite.",
            view=None
        )

    async def _handle_invite_reply(self, interaction: discord.Interaction, iid: str, target_id: int, content: str):
        for guild in self.bot.guilds:
            insts = await self.config.guild(guild).instances()
            if iid in insts:
                owner_id = insts[iid]["owner_id"]
                owner = self.bot.get_user(owner_id) or await self.bot.fetch_user(owner_id)
                if owner:
                    await owner.send(f"✉️ **Reply for `{iid}`** from {interaction.user.mention}:\n> {content}")
                break

    # ─── auto‐end extend/finalize ─────────────────────────────────────────────────
    async def _handle_extend(self, interaction: discord.Interaction, iid: str):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)
        import time
        inst["end_time"] = time.time() + 12 * 3600
        inst["status"] = "OPEN"
        await self.config.guild(guild).instances.set(insts)
        await interaction.response.edit_message(content="✅ Activity extended 12h.", view=None)
        self.bot.loop.create_task(self._auto_end_task(guild.id, iid, 12 * 3600))

    async def _handle_finalize(self, interaction: discord.Interaction, iid: str):
        guild, insts, inst = await self._find_instance(iid)
        if not guild:
            return await interaction.response.send_message("Activity not found.", ephemeral=True)
        inst["status"] = "ENDED"
        await self.config.guild(guild).instances.set(insts)
        await interaction.response.edit_message(content="✅ Activity finalized.", view=None)
        try:
            ch = guild.get_channel(inst["channel_id"])
            pm = inst["message_ids"].get("public")
            if ch and pm:
                msg = await ch.fetch_message(pm)
                await msg.edit(embed=self._build_embed(inst, guild), view=None)
        except:
            pass
        for uid_str, mid in inst["message_ids"].get("manages", {}).items():
            try:
                uid = int(uid_str)
                user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                dm = await user.create_dm()
                msg = await dm.fetch_message(mid)
                await msg.edit(embed=self._build_embed(inst, guild), view=None)
            except:
                continue
                
async def setup(bot: Red):
    await bot.add_cog(Activities(bot))   