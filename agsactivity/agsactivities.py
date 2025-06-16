import asyncio
import json
import re
import uuid
import logging
import time
from datetime import datetime

import discord
from discord.ui import View, Button, Modal, TextInput

from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_list

log = logging.getLogger("red.agsactivities")

# ----------------------------------------------------------------------------
# Config defaults
# ----------------------------------------------------------------------------
DEFAULT_GUILD = {
    "activity_types": {},
    "activity_instances": {},
    "prune_summary_channel": None,
}

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------
MAX_TAG_LENGTH = 200  # max length for each tag input

# ----------------------------------------------------------------------------
# UI Components
# ----------------------------------------------------------------------------
class JoinButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.green,
            label="Join",
            custom_id=f"activity_join:{iid}",
        )
        self.instance_id = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_join(interaction, self.instance_id)


class LeaveButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Leave",
            custom_id=f"activity_leave:{iid}",
        )
        self.instance_id = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_leave(interaction, self.instance_id)


class AcceptButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.green,
            label="Accept",
            custom_id=f"activity_accept:{iid}",
        )
        self.instance_id = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_accept(
            interaction, self.instance_id, self.view.target_user_id
        )


class DeclineButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Decline",
            custom_id=f"activity_decline:{iid}",
        )
        self.instance_id = iid

    async def callback(self, interaction: discord.Interaction):
        await self.view.cog._handle_decline(
            interaction, self.instance_id, self.view.target_user_id
        )


class ReplyButton(Button):
    def __init__(self, iid: str):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Reply",
            custom_id=f"activity_reply:{iid}",
        )
        self.instance_id = iid

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            ResponseModal(
                cog=self.view.cog,
                iid=self.instance_id,
                target_user_id=self.view.target_user_id,
            )
        )


class ResponseModal(Modal):
    def __init__(self, cog: commands.Cog, iid: str, target_user_id: int):
        super().__init__(title="Send a written response")
        self.cog = cog
        self.instance_id = iid
        self.target_user_id = target_user_id
        self.response = TextInput(
            label="Your message",
            style=discord.TextStyle.paragraph,
            placeholder="Type your response here...",
            max_length=500,
            required=True,
        )
        self.add_item(self.response)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "Your message has been sent to the activity owner.", ephemeral=True
        )
        await self.cog._handle_response(
            interaction, self.instance_id, self.target_user_id, self.response.value
        )


class ActivityPublicView(View):
    """View with Join/Leave for public embeds."""
    def __init__(self, cog: commands.Cog, iid: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.add_item(JoinButton(iid))
        self.add_item(LeaveButton(iid))


class ActivityDMView(View):
    """View with Accept/Decline/Reply for DM invites."""
    def __init__(self, cog: commands.Cog, iid: str, target_user_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.target_user_id = target_user_id
        self.add_item(AcceptButton(iid))
        self.add_item(DeclineButton(iid))
        self.add_item(ReplyButton(iid))


# ----------------------------------------------------------------------------
# The Cog
# ----------------------------------------------------------------------------
class Activities(commands.Cog):
    """
    AGS Activities Cog
    Fully‐featured, JSON‐templated, button‐driven activity system
    with:
      • one-command bootstrap of default types
      • per-instance channel override
      • tag input length enforcement
      • DM invite accept/decline/reply via modal
      • manual + monthly auto‐pruning with summaries
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config: Config = Config.get_conf(
            self, identifier=1234567890123456, force_registration=True
        )
        self.config.register_guild(**DEFAULT_GUILD)
        # Re‐register persistent views on restart
        self.bot.loop.create_task(self._register_persistent_views())
        # Schedule monthly prune task
        self.bot.loop.create_task(self._prune_scheduler())

    async def _register_persistent_views(self):
        """After restart re-add all Views so buttons keep working."""
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            insts = await self.config.guild(guild).activity_instances()
            for iid, inst in insts.items():
                mid = inst.get("embed_message_id")
                if mid:
                    self.bot.add_view(ActivityPublicView(self, iid), message_id=mid)
                for uid_str, dm_mid in inst.get("dm_message_ids", {}).items():
                    try:
                        uid = int(uid_str)
                    except:
                        continue
                    self.bot.add_view(ActivityDMView(self, iid, uid), message_id=dm_mid)

    def cog_unload(self):
        self.bot.remove_view(ActivityPublicView(self, ""))
        self.bot.remove_view(ActivityDMView(self, "", 0))

    # ----------------------------
    # Helpers
    # ----------------------------
    def _match_instance_id(self, insts: dict, query: str) -> str:
        matches = [iid for iid in insts if iid.startswith(query.lower())]
        if not matches:
            return None
        if len(matches) > 1:
            return "AMBIG"
        return matches[0]

    async def _render_embed(self, guild: discord.Guild, iid: str, inst: dict) -> discord.Embed:
        types = await self.config.guild(guild).activity_types()
        tconf = types.get(inst["type_name"], {})
        template = tconf.get("embed_template")
        if not template:
            return discord.Embed(
                title=inst["type_name"].title(),
                description="*(no template configured)*",
                color=discord.Color.red(),
            )
        raw = json.dumps(template)
        placeholders = {
            "activity.owner": (
                guild.get_member(inst["owner_id"]).mention
                if guild.get_member(inst["owner_id"])
                else str(inst["owner_id"])
            ),
            "activity.id": iid[:8],
            "activity.type": inst["type_name"],
            "slots.current": str(len(inst.get("participants", {}))),
            "slots.max": str(inst.get("max_slots")) if inst.get("max_slots") else "∞",
            "custom_message": inst.get("owner_message", ""),
        }
        for tag, val in inst.get("tag_values", {}).items():
            placeholders[f"tag.{tag}"] = val
        for key, val in placeholders.items():
            raw = raw.replace(f"[{key}]", val)
        data = json.loads(raw)
        e = discord.Embed(
            title=data.get("title"),
            description=data.get("description"),
            color=int(data.get("color", 0)),
        )
        for f in data.get("fields", []):
            e.add_field(name=f.get("name"), value=f.get("value"), inline=f.get("inline", False))
        if data.get("thumbnail"):
            e.set_thumbnail(url=data["thumbnail"])
        if data.get("image"):
            e.set_image(url=data["image"])
        return e

    # ----------------------------
    # Admin: activitytype group
    # ----------------------------
    @commands.group(name="activitytype", invoke_without_command=True)
    @commands.guild_only()
    @checks.guildowner()
    async def activitytype(self, ctx):
        """Manage your server's activity types."""
        await ctx.send_help(ctx.command)

    @activitytype.command(name="list")
    async def atype_list(self, ctx):
        """List all defined activity types."""
        types = await self.config.guild(ctx.guild).activity_types()
        if not types:
            return await ctx.send("No activity types defined yet.")
        embed = discord.Embed(title="Activity Types", color=discord.Color.blue())
        for key, conf in types.items():
            dc = conf.get("default_channel")
            tags = conf.get("tags", {})
            embed.add_field(
                name=key,
                value=(
                    f"• Default channel: {f'<#{dc}>' if dc else 'None'}\n"
                    f"• Tags: {', '.join(tags.keys()) or 'None'}"
                ),
                inline=False,
            )
        await ctx.send(embed=embed)

    @activitytype.command(name="add")
    async def atype_add(self, ctx, name: str, default_channel: discord.TextChannel = None):
        """
        Add a new activity type.
        """
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        if key in types:
            return await ctx.send(f"An activity type `{key}` already exists.")
        types[key] = {
            "default_channel": default_channel.id if default_channel else None,
            "embed_template": None,
            "tags": {},
        }
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(f"Activity type `{key}` created. Run `!activitytype embed upload {key}` next.")

    @activitytype.command(name="remove")
    async def atype_remove(self, ctx, name: str):
        """Remove a type and all its instances."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        if key not in types:
            return await ctx.send(f"No activity type `{key}`.")
        types.pop(key)
        await self.config.guild(ctx.guild).activity_types.set(types)
        insts = await self.config.guild(ctx.guild).activity_instances()
        removed = [i for i, v in insts.items() if v["type_name"] == key]
        for i in removed:
            insts.pop(i, None)
        await self.config.guild(ctx.guild).activity_instances.set(insts)
        await ctx.send(f"Removed `{key}` and {len(removed)} instance(s).")

    @activitytype.group(name="embed", invoke_without_command=True)
    @checks.guildowner()
    async def atype_embed(self, ctx):
        """Manage JSON embed templates for a type."""
        await ctx.send_help(ctx.command)

    @atype_embed.command(name="upload")
    async def atype_embed_upload(self, ctx, name: str):
        """Upload a JSON embed template (paste or attach file)."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        conf = types.get(key)
        if not conf:
            return await ctx.send(f"No type `{key}`.")
        await ctx.send("Please paste the raw JSON or attach a JSON file containing your embed template.")
        try:
            msg = await self.bot.wait_for(
                "message",
                timeout=300,
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
            )
        except asyncio.TimeoutError:
            return await ctx.send("Timed out.")
        if msg.attachments:
            raw = await msg.attachments[0].read()
            content = raw.decode()
        else:
            content = msg.content
        try:
            data = json.loads(content)
        except Exception as e:
            return await ctx.send(f"Invalid JSON: {e}")
        if not isinstance(data, dict):
            return await ctx.send("Embed template must be a JSON object.")
        conf["embed_template"] = data
        types[key] = conf
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(f"Embed template for `{key}` saved.")

    @activitytype.group(name="tag", invoke_without_command=True)
    @checks.guildowner()
    async def atype_tag(self, ctx):
        """Manage tags for each activity type."""
        await ctx.send_help(ctx.command)

    @atype_tag.command(name="add")
    async def atype_tag_add(
        self,
        ctx,
        name: str,
        tag: str,
        mandatory: bool = False,
        *,
        description: str = "",
    ):
        """Add a placeholder tag to a type (mandatory/optional)."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        conf = types.get(key)
        if not conf:
            return await ctx.send(f"No type `{key}`.")
        tags = conf.get("tags", {})
        if tag in tags:
            return await ctx.send(f"Tag `{tag}` already exists.")
        tags[tag] = {"mandatory": mandatory, "global": False, "description": description}
        conf["tags"] = tags
        types[key] = conf
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(f"Added tag `{tag}` to `{key}` (mandatory={mandatory}).")

    @atype_tag.command(name="remove")
    async def atype_tag_remove(self, ctx, name: str, tag: str):
        """Remove a placeholder tag from a type."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        conf = types.get(key)
        if not conf:
            return await ctx.send(f"No type `{key}`.")
        tags = conf.get("tags", {})
        if tag not in tags:
            return await ctx.send(f"Tag `{tag}` not found.")
        tags.pop(tag)
        conf["tags"] = tags
        types[key] = conf
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(f"Removed tag `{tag}` from `{key}`.")

    @atype_tag.command(name="list")
    async def atype_tag_list(self, ctx, name: str):
        """List placeholder tags for a type."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        conf = types.get(key)
        if not conf:
            return await ctx.send(f"No type `{key}`.")
        tags = conf.get("tags", {})
        if not tags:
            return await ctx.send("No tags defined.")
        lines = []
        for t, v in tags.items():
            lines.append(f"`{t}` mandatory={v['mandatory']} — {v['description']}")
        await ctx.send("\n".join(lines))

    @activitytype.command(name="setdefault")
    @checks.guildowner()
    async def atype_setdefault(self, ctx, name: str, channel: discord.TextChannel):
        """Set the default channel where embeds will be posted."""
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        conf = types.get(key)
        if not conf:
            return await ctx.send(f"No type `{key}`.")
        conf["default_channel"] = channel.id
        types[key] = conf
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(f"Default channel for `{key}` is now {channel.mention}.")

    @activitytype.command(name="init")
    @checks.guildowner()
    async def atype_init(self, ctx):
        """
        Bootstrap default activity types in one command:
          • hangout
          • voicecall-public
          • voicecall-private
        """
        types = await self.config.guild(ctx.guild).activity_types()
        created = []

        # Hangout
        if "hangout" not in types:
            hangout_template = {
                "title": "[activity.owner] started a hangout!",
                "description": "[custom_message]",
                "color": 3447003
            }
            types["hangout"] = {
                "default_channel": ctx.channel.id,
                "embed_template": hangout_template,
                "tags": {}
            }
            created.append("hangout")

        # Voicecall-Public
        if "voicecall-public" not in types:
            vc_pub_template = {
                "title": "[activity.owner] started a public voice call!",
                "description": "[custom_message]\nSlots: [slots.current]/[slots.max]",
                "color": 10181046
            }
            types["voicecall-public"] = {
                "default_channel": ctx.channel.id,
                "embed_template": vc_pub_template,
                "tags": {}
            }
            created.append("voicecall-public")

        # Voicecall-Private
        if "voicecall-private" not in types:
            vc_priv_template = {
                "title": "[activity.owner] started a private voice call!",
                "description": "[custom_message]\nSlots: [slots.current]/[slots.max]",
                "color": 10038562
            }
            types["voicecall-private"] = {
                "default_channel": ctx.channel.id,
                "embed_template": vc_priv_template,
                "tags": {}
            }
            created.append("voicecall-private")

        if not created:
            return await ctx.send("All default activity types already exist.")
        await self.config.guild(ctx.guild).activity_types.set(types)
        await ctx.send(
            f"Created default types: {', '.join(created)}.\n"
            "You can customize their templates/tags with `!activitytype embed upload` and `!activitytype tag`."
        )

    # ----------------------------
    # User: activity group
    # ----------------------------
    @commands.group(name="activity", invoke_without_command=True)
    @commands.guild_only()
    async def activity(self, ctx):
        """Create or manage an activity instance."""
        await ctx.send_help(ctx.command)

    @activity.command(name="list")
    async def activity_list(self, ctx):
        """List all activity instances."""
        insts = await self.config.guild(ctx.guild).activity_instances()
        if not insts:
            return await ctx.send("No activities have been started.")
        embed = discord.Embed(title="Activities", color=discord.Color.green())
        for iid, inst in insts.items():
            owner = ctx.guild.get_member(inst["owner_id"])
            embed.add_field(
                name=f"{iid[:8]} ({inst['type_name']})",
                value=(
                    f"Owner: {owner.mention if owner else inst['owner_id']}\n"
                    f"Status: {inst['status']}"
                ),
                inline=False,
            )
        await ctx.send(embed=embed)

    @activity.command(name="info")
    async def activity_info(self, ctx, iid: str):
        """Show detailed info on an activity instance."""
        insts = await self.config.guild(ctx.guild).activity_instances()
        full = self._match_instance_id(insts, iid)
        if full is None:
            return await ctx.send("No such activity.")
        if full == "AMBIG":
            return await ctx.send("That ID is ambiguous; use a longer prefix.")
        inst = insts[full]
        embed = discord.Embed(
            title=f"Activity {full[:8]} Info", color=discord.Color.green()
        )
        owner = ctx.guild.get_member(inst["owner_id"])
        embed.add_field("Type", inst["type_name"], inline=True)
        embed.add_field("Owner", owner.mention if owner else inst["owner_id"], inline=True)
        embed.add_field("Status", inst["status"], inline=True)
        for t, v in inst.get("tag_values", {}).items():
            embed.add_field(name=t, value=v, inline=True)
        parts = []
        for uid in inst.get("participants", {}):
            m = ctx.guild.get_member(int(uid))
            parts.append(m.mention if m else uid)
        embed.add_field("Participants", humanize_list(parts) or "None", inline=False)
        await ctx.send(embed=embed)

    @activity.command(name="start")
    async def activity_start(self, ctx, name: str, channel: discord.TextChannel = None):
        """
        Start a new activity.
        Optional: specify a channel override for the public embed.
        """
        key = name.lower()
        types = await self.config.guild(ctx.guild).activity_types()
        tconf = types.get(key)
        if not tconf:
            return await ctx.send(f"No activity type `{key}` defined.")
        dc = tconf.get("default_channel")
        embed_chan = channel or (ctx.guild.get_channel(dc) if dc else ctx.channel)

        is_pub = key == "voicecall-public"
        is_priv = key == "voicecall-private"
        voice_chan = None
        if is_pub or is_priv:
            if ctx.author.voice and ctx.author.voice.channel:
                voice_chan = ctx.author.voice.channel
            else:
                await ctx.send("You are not in voice. Paste a VC link or ID.")
                try:
                    msg = await self.bot.wait_for(
                        "message",
                        check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                        timeout=120,
                    )
                except asyncio.TimeoutError:
                    return await ctx.send("Timed out.")
                match = re.search(r"/(\d+)$", msg.content)
                cid = match.group(1) if match else msg.content.strip()
                try:
                    cid = int(cid)
                except:
                    return await ctx.send("Invalid channel.")
                voice_chan = ctx.guild.get_channel(cid)
                if not isinstance(voice_chan, discord.VoiceChannel):
                    return await ctx.send("That is not a voice channel.")
            perms = voice_chan.permissions_for(ctx.guild.default_role)
            if is_pub and not (perms.view_channel and perms.connect):
                return await ctx.send("That channel is not public.")
            if is_priv and (perms.view_channel and perms.connect):
                return await ctx.send("That channel is not private.")

        await ctx.send("Send invites via DM only? (yes/no)")
        try:
            dm_reply = await self.bot.wait_for(
                "message",
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                timeout=60,
            )
            use_dm = dm_reply.content.lower().startswith("y")
        except asyncio.TimeoutError:
            return await ctx.send("Timed out.")

        tag_defs = tconf.get("tags", {})
        tag_vals = {}
        for tag, opts in tag_defs.items():
            if opts["mandatory"]:
                while True:
                    await ctx.send(f"Provide **{tag}** ({opts['description']}, max {MAX_TAG_LENGTH} chars):")
                    try:
                        m = await self.bot.wait_for(
                            "message",
                            check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                            timeout=300,
                        )
                    except asyncio.TimeoutError:
                        return await ctx.send("Timed out.")
                    val = m.content.strip()
                    if len(val) > MAX_TAG_LENGTH:
                        await ctx.send(f"Too long ({len(val)}/{MAX_TAG_LENGTH}). Try again.")
                        continue
                    tag_vals[tag] = val
                    break
            else:
                await ctx.send(f"Set **{tag}** now? (yes/no)")
                try:
                    m = await self.bot.wait_for(
                        "message",
                        check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                        timeout=60,
                    )
                except asyncio.TimeoutError:
                    return await ctx.send("Timed out.")
                if m.content.lower().startswith("y"):
                    while True:
                        await ctx.send(f"Provide **{tag}** ({opts['description']}, max {MAX_TAG_LENGTH} chars):")
                        try:
                            v = await self.bot.wait_for(
                                "message",
                                check=lambda v: v.author == ctx.author and v.channel == ctx.channel,
                                timeout=300,
                            )
                        except asyncio.TimeoutError:
                            return await ctx.send("Timed out.")
                        val = v.content.strip()
                        if len(val) > MAX_TAG_LENGTH:
                            await ctx.send(f"Too long ({len(val)}/{MAX_TAG_LENGTH}). Try again.")
                            continue
                        tag_vals[tag] = val
                        break

        await ctx.send("Optional short message (<500 chars), or `skip`:")
        try:
            m = await self.bot.wait_for(
                "message",
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                timeout=300,
            )
            owner_msg = "" if m.content.lower() == "skip" else m.content[:500]
        except asyncio.TimeoutError:
            return await ctx.send("Timed out.")

        dm_targets = []
        if use_dm:
            await ctx.send("Mention users to DM, or `all` to DM channel members:")
            try:
                m = await self.bot.wait_for(
                    "message",
                    check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
                    timeout=300,
                )
            except asyncio.TimeoutError:
                return await ctx.send("Timed out.")
            if m.content.lower() == "all":
                dm_targets = [u.id for u in ctx.channel.members if not u.bot]
            else:
                dm_targets = [u.id for u in m.mentions]
            if not dm_targets:
                await ctx.send("No targets found, proceeding publicly.")
                use_dm = False

        max_slots = voice_chan.user_limit if (is_pub or is_priv) else None
        iid = str(uuid.uuid4())
        inst = {
            "guild_id": ctx.guild.id,
            "type_name": key,
            "owner_id": ctx.author.id,
            "embed_channel_id": None if use_dm else embed_chan.id,
            "voice_channel_id": voice_chan.id if voice_chan else None,
            "embed_message_id": None,
            "status": "OPEN",
            "tag_values": tag_vals,
            "participants": {},
            "max_slots": max_slots,
            "owner_message": owner_msg,
            "use_dm": use_dm,
            "dm_targets": dm_targets,
            "dm_message_ids": {},
            "created_at": time.time(),
        }
        insts = await self.config.guild(ctx.guild).activity_instances()
        insts[iid] = inst
        await self.config.guild(ctx.guild).activity_instances.set(insts)

        if not use_dm:
            e = await self._render_embed(ctx.guild, iid, inst)
            view = ActivityPublicView(self, iid)
            msg = await embed_chan.send(embed=e, view=view)
            inst["embed_message_id"] = msg.id
            insts[iid] = inst
            await self.config.guild(ctx.guild).activity_instances.set(insts)
            await ctx.send(f"Activity posted in {embed_chan.mention} with ID `{iid[:8]}`.")
        else:
            for uid in dm_targets:
                user = self.bot.get_user(uid)
                if not user:
                    continue
                try:
                    dch = await user.create_dm()
                    e = await self._render_embed(ctx.guild, iid, inst)
                    view = ActivityDMView(self, iid, uid)
                    dm_msg = await dch.send(embed=e, view=view)
                    inst["dm_message_ids"][str(uid)] = dm_msg.id
                except Exception:
                    log.exception(f"Could not DM invite to {uid}")
            insts[iid] = inst
            await self.config.guild(ctx.guild).activity_instances.set(insts)
            await ctx.send(f"Sent DM invites for activity `{iid[:8]}`.")

    # ----------------------------
    # Button Callbacks
    # ----------------------------
    async def _handle_join(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message(
                "This must be used in-guild.", ephemeral=True
            )
        insts = await self.config.guild(guild).activity_instances()
        inst = insts.get(iid)
        if not inst:
            return await interaction.response.send_message(
                "Activity not found.", ephemeral=True
            )
        if inst["status"] != "OPEN":
            return await interaction.response.send_message(
                "This activity is not open.", ephemeral=True
            )
        uid = str(interaction.user.id)
        if uid in inst["participants"]:
            return await interaction.response.send_message(
                "You have already joined.", ephemeral=True
            )
        if inst["max_slots"] and len(inst["participants"]) >= inst["max_slots"]:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).activity_instances.set(insts)
            return await interaction.response.send_message(
                "Sorry, it is now full.", ephemeral=True
            )
        inst["participants"][uid] = {"message": None}
        if inst["type_name"] == "voicecall-private":
            vc = guild.get_channel(inst["voice_channel_id"])
            await vc.set_permissions(interaction.user, view_channel=True, connect=True)
        insts[iid] = inst
        await self.config.guild(guild).activity_instances.set(insts)
        ch = guild.get_channel(inst["embed_channel_id"])
        try:
            msg = await ch.fetch_message(inst["embed_message_id"])
            embed = await self._render_embed(guild, iid, inst)
            await msg.edit(embed=embed, view=ActivityPublicView(self, iid))
        except:
            pass
        await interaction.response.send_message("You’ve joined!", ephemeral=True)

    async def _handle_leave(self, interaction: discord.Interaction, iid: str):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message(
                "This must be used in-guild.", ephemeral=True
            )
        insts = await self.config.guild(guild).activity_instances()
        inst = insts.get(iid)
        if not inst:
            return await interaction.response.send_message(
                "Activity not found.", ephemeral=True
            )
        uid = str(interaction.user.id)
        if uid not in inst["participants"]:
            return await interaction.response.send_message(
                "You are not in this activity.", ephemeral=True
            )
        inst["participants"].pop(uid)
        if inst["status"] == "FULL":
            inst["status"] = "OPEN"
        insts[iid] = inst
        await self.config.guild(guild).activity_instances.set(insts)
        ch = guild.get_channel(inst["embed_channel_id"])
        try:
            msg = await ch.fetch_message(inst["embed_message_id"])
            embed = await self._render_embed(guild, iid, inst)
            await msg.edit(embed=embed, view=ActivityPublicView(self, iid))
        except:
            pass
        await interaction.response.send_message("You’ve left.", ephemeral=True)

    async def _handle_accept(
        self, interaction: discord.Interaction, iid: str, target_user_id: int
    ):
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).activity_instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return await interaction.response.send_message(
                "Activity not found.", ephemeral=True
            )
        if inst["status"] != "OPEN":
            return await interaction.response.send_message(
                "This activity is not open.", ephemeral=True
            )
        uid = str(target_user_id)
        if uid in inst["participants"]:
            return await interaction.response.send_message(
                "Already accepted.", ephemeral=True
            )
        if inst["max_slots"] and len(inst["participants"]) >= inst["max_slots"]:
            inst["status"] = "FULL"
            insts[iid] = inst
            await self.config.guild(guild).activity_instances.set(insts)
            return await interaction.response.send_message(
                "It is now full.", ephemeral=True
            )
        inst["participants"][uid] = {"message": None}
        insts[iid] = inst
        await self.config.guild(guild).activity_instances.set(insts)
        owner = self.bot.get_user(inst["owner_id"])
        try:
            await owner.send(f"{interaction.user.mention} accepted your invite for {iid[:8]}.")
        except:
            pass
        await interaction.response.send_message("You accepted!", ephemeral=True)

    async def _handle_decline(
        self, interaction: discord.Interaction, iid: str, target_user_id: int
    ):
        guild = None
        inst = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).activity_instances()
            if iid in insts:
                guild = g
                inst = insts[iid]
                break
        if not inst:
            return await interaction.response.send_message(
                "Activity not found.", ephemeral=True
            )
        owner = self.bot.get_user(inst["owner_id"])
        try:
            await owner.send(f"{interaction.user.mention} declined your invite for {iid[:8]}.")
        except:
            pass
        await interaction.response.send_message("You declined.", ephemeral=True)

    async def _handle_response(
        self, interaction: discord.Interaction, iid: str, target_user_id: int, content: str
    ):
        """
        Forward a custom text response (from Reply modal) to the activity owner.
        """
        inst = None
        owner = None
        for g in self.bot.guilds:
            insts = await self.config.guild(g).activity_instances()
            if iid in insts:
                inst = insts[iid]
                owner = self.bot.get_user(inst["owner_id"])
                break
        if not inst or not owner:
            return
        try:
            await owner.send(
                f"{interaction.user.mention} says about activity `{iid[:8]}`:\n{content}"
            )
        except Exception:
            log.exception("Could not send text response to owner")

    # ----------------------------
    # Owner actions: update/end
    # ----------------------------
    @activity.command(name="update")
    async def activity_update(
        self,
        ctx,
        iid: str,
        status: str = None,
        max_slots: int = None,
    ):
        """
        Update an activity's status (OPEN/FULL/CLOSED/ENDED) or max slots.
        """
        insts = await self.config.guild(ctx.guild).activity_instances()
        full = self._match_instance_id(insts, iid)
        if full is None:
            return await ctx.send("No such activity.")
        if full == "AMBIG":
            return await ctx.send("Ambiguous ID.")
        inst = insts[full]
        if status:
            st = status.upper()
            if st not in ("OPEN", "FULL", "CLOSED", "ENDED"):
                return await ctx.send("Invalid status.")
            inst["status"] = st
        if max_slots is not None:
            inst["max_slots"] = max_slots
        insts[full] = inst
        await self.config.guild(ctx.guild).activity_instances.set(insts)
        chid = inst["embed_channel_id"]
        mid = inst.get("embed_message_id")
        if chid and mid:
            ch = ctx.guild.get_channel(chid)
            try:
                msg = await ch.fetch_message(mid)
                e = await self._render_embed(ctx.guild, full, inst)
                await msg.edit(embed=e, view=ActivityPublicView(self, full))
            except:
                pass
        await ctx.send("Activity updated.")

    @activity.command(name="end")
    async def activity_end(self, ctx, iid: str):
        """End an activity (status=ENDED and disable buttons)."""
        insts = await self.config.guild(ctx.guild).activity_instances()
        full = self._match_instance_id(insts, iid)
        if full is None:
            return await ctx.send("No such activity.")
        if full == "AMBIG":
            return await ctx.send("Ambiguous ID.")
        inst = insts[full]
        inst["status"] = "ENDED"
        insts[full] = inst
        await self.config.guild(ctx.guild).activity_instances.set(insts)
        chid = inst["embed_channel_id"]
        mid = inst.get("embed_message_id")
        if chid and mid:
            ch = ctx.guild.get_channel(chid)
            try:
                msg = await ch.fetch_message(mid)
                e = await self._render_embed(ctx.guild, full, inst)
                await msg.edit(embed=e, view=None)
            except:
                pass
        await ctx.send("Activity ended.")

    # ----------------------------
    # Prune functionality
    # ----------------------------
    async def _prune_guild(self, guild: discord.Guild, status: str = "ENDED", older_than: int = None):
        insts = await self.config.guild(guild).activity_instances()
        now_ts = time.time()
        to_remove = {}
        for iid, inst in insts.items():
            if inst.get("status") != status:
                continue
            if older_than:
                created = inst.get("created_at", now_ts)
                if (now_ts - created) / 86400 < older_than:
                    continue
            to_remove[iid] = inst
        if not to_remove:
            return []
        for iid, inst in to_remove.items():
            ch_id = inst.get("embed_channel_id")
            msg_id = inst.get("embed_message_id")
            if ch_id and msg_id:
                ch = guild.get_channel(ch_id)
                if ch:
                    try:
                        msg = await ch.fetch_message(msg_id)
                        await msg.delete()
                    except:
                        pass
            insts.pop(iid, None)
        await self.config.guild(guild).activity_instances.set(insts)
        return list(to_remove.items())

    async def _prune_scheduler(self):
        await self.bot.wait_until_ready()
        while True:
            now = datetime.utcnow()
            year = now.year + (1 if now.month == 12 else 0)
            month = 1 if now.month == 12 else now.month + 1
            next_run = datetime(year, month, 1, 0, 0, 0)
            delay = (next_run - now).total_seconds()
            await asyncio.sleep(delay)
            for guild in self.bot.guilds:
                chan_id = await self.config.guild(guild).prune_summary_channel()
                pruned = await self._prune_guild(guild, status="ENDED", older_than=None)
                if pruned and chan_id:
                    ch = guild.get_channel(chan_id)
                    if not ch:
                        continue
                    lines = [f"`{iid[:8]}` ({inst['type_name']})" for iid, inst in pruned]
                    text = f"Auto-pruned {len(pruned)} activities:\n" + "\n".join(lines)
                    try:
                        await ch.send(text)
                    except:
                        pass

    @activity.command(name="prune")
    @checks.guildowner()
    async def activity_prune(self, ctx, status: str = "ENDED", older_than: int = None):
        """
        Manually prune activities.
        """
        pruned = await self._prune_guild(ctx.guild, status=status.upper(), older_than=older_than)
        if not pruned:
            return await ctx.send("No matching activities to prune.")
        await ctx.send(f"Pruned {len(pruned)} activities.")

    @activity.group(name="prunechannel", invoke_without_command=True)
    @checks.guildowner()
    async def prunechannel(self, ctx):
        """Configure where monthly prune summaries are posted."""
        await ctx.send_help(ctx.command)

    @prunechannel.command(name="set")
    @checks.guildowner()
    async def prunechannel_set(self, ctx, channel: discord.TextChannel):
        """Set the channel for automatic prune summaries."""
        await self.config.guild(ctx.guild).prune_summary_channel.set(channel.id)
        await ctx.send(f"Prune summary channel set to {channel.mention}.")

    @prunechannel.command(name="clear")
    @checks.guildowner()
    async def prunechannel_clear(self, ctx):
        """Clear the prune summary channel."""
        await self.config.guild(ctx.guild).prune_summary_channel.set(None)
        await ctx.send("Prune summary channel cleared.")


async def setup(bot: Red):
    await bot.add_cog(Activities(bot))
