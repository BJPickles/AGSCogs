# commands/snapshot.py
import json
from datetime import datetime
from typing import Optional

import discord
from redbot.core import commands
from redbot.core.i18n import Translator
from redbot.core.utils.chat_formatting import pagify, text_to_file

from ..models import SnapshotType, Snapshot, GuildConfig
from ..common import generate_snapshot_id, collect_levelup_stats

_ = Translator("AGSProfiles", __file__)

class SnapshotCommands(commands.Cog):
    """Manual snapshot commands for AGSProfiles."""

    @commands.group(name="agsnapshot", aliases=["agsnap", "asnap"])
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def agsnapshot(self, ctx: commands.Context):
        """Manage AGSProfiles snapshots."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agsnapshot.command(name="manual")
    async def snapshot_manual(self, ctx: commands.Context):
        """Take a manual snapshot of current LevelUp stats."""
        guild = ctx.guild
        await ctx.trigger_typing()
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            return await ctx.send(_("No LevelUp data available to snapshot."))
        snap_id = generate_snapshot_id()
        name = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S") + " MANUAL"
        snap = Snapshot(id=snap_id, name=name, created=datetime.utcnow(),
                        type=SnapshotType.MANUAL, data=stats)
        guild_conf: GuildConfig = self.config.guilds.setdefault(guild.id, GuildConfig())
        guild_conf.snapshots.append(snap)
        self.save()
        await ctx.send(_("Manual snapshot '{name}' taken (ID: {id}).").format(name=name, id=snap_id))

    @agsnapshot.command(name="list")
    async def snapshot_list(self, ctx: commands.Context):
        """List all snapshots for this server."""
        guild_conf: GuildConfig = self.config.guilds.get(ctx.guild.id)
        if not guild_conf or not guild_conf.snapshots:
            return await ctx.send(_("No snapshots found for this server."))
        lines = []
        for snap in guild_conf.snapshots:
            lines.append(f"- ID: `{snap.id}` | {snap.name} [{snap.type.value}]")
        for page in pagify("\n".join(lines), delims=["\n"], page_length=2000):
            await ctx.send(page)

    @agsnapshot.command(name="delete")
    async def snapshot_delete(self, ctx: commands.Context, snapshot_id: str):
        """Delete a snapshot by its ID."""
        guild_conf: GuildConfig = self.config.guilds.get(ctx.guild.id)
        if not guild_conf:
            return await ctx.send(_("No snapshots to delete."))
        for i, snap in enumerate(guild_conf.snapshots):
            if snap.id == snapshot_id:
                guild_conf.snapshots.pop(i)
                self.save()
                return await ctx.send(_("Deleted snapshot {id}.").format(id=snapshot_id))
        await ctx.send(_("Snapshot ID {id} not found.").format(id=snapshot_id))

    @agsnapshot.command(name="backup")
    async def snapshot_backup(self, ctx: commands.Context, snapshot_id: Optional[str] = None):
        """
        Backup snapshot data.
        If ID is provided, backs up that snapshot; otherwise backs up all manual snapshots.
        """
        guild_conf: GuildConfig = self.config.guilds.get(ctx.guild.id)
        if not guild_conf or not guild_conf.snapshots:
            return await ctx.send(_("No snapshots to back up."))
        to_backup = []
        if snapshot_id:
            for snap in guild_conf.snapshots:
                if snap.id == snapshot_id:
                    to_backup.append(snap)
                    break
            if not to_backup:
                return await ctx.send(_("Snapshot ID {id} not found.").format(id=snapshot_id))
            filename = f"snapshot-{snapshot_id}.json"
        else:
            # all manual snapshots
            to_backup = [s for s in guild_conf.snapshots if s.type == SnapshotType.MANUAL]
            if not to_backup:
                return await ctx.send(_("No manual snapshots to back up."))
            filename = f"manual-snapshots-{ctx.guild.id}-{datetime.utcnow().strftime('%Y%m%d')}.json"
        # Serialize
        serial = [s.model_dump(exclude_defaults=False) for s in to_backup]
        data = json.dumps(serial, indent=2, default=str)
        file = text_to_file(data, filename=filename)
        await ctx.send(file=file)