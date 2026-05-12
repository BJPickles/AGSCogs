# commands/snapshot.py

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import discord
from redbot.core import commands
from redbot.core.i18n import Translator
from redbot.core.utils.chat_formatting import pagify, text_to_file, warning

from ..common import collect_levelup_stats, generate_snapshot_id, reset_levelup_data, utc_timestamp
from ..models import (
    GuildConfig,
    MasterUserStats,
    Snapshot,
    SnapshotType,
    SnapshotMeta,
    UserYearStats,
    YearData,
)

_ = Translator("AGSProfiles", __file__)


class SnapshotCommands:
    """Manual and year-end snapshot commands for AGSProfiles."""

    @commands.group(name="agsnapshot", aliases=["agsnap", "asnap"])
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def agsnapshot(self, ctx: commands.Context):
        """Manage AGSProfiles snapshots."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agsnapshot.command(name="manual")
    async def snapshot_manual(self, ctx: commands.Context):
        """
        Take a manual backup snapshot of current LevelUp stats.
        """
        guild = ctx.guild
        async with ctx.typing():
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            return await ctx.send(_("No LevelUp data available to snapshot."))

        snap_id = generate_snapshot_id()
        now = utc_timestamp()
        name = now.strftime("%Y-%m-%d_%H-%M-%S_MANUAL")
        snapshot = Snapshot(
            id=snap_id,
            name=name,
            created=now,
            type=SnapshotType.MANUAL,
            data=stats,
        )

        # write full snapshot JSON to disk
        path = self._snapshot_path(guild.id, snapshot.id, snapshot.type.value)
        self._atomic_write_json(path, snapshot.model_dump())

        # compute integrity hash
        sha = self._compute_sha256(path)

        # append only metadata in memory
        meta = SnapshotMeta(
            id=snapshot.id,
            name=snapshot.name,
            created=snapshot.created,
            type=snapshot.type,
            path=path,
            sha256=sha,
        )
        cfg: GuildConfig = self.get_guild_conf(guild.id)
        cfg.snapshots.append(meta)

        # persist metadata/index only
        await self.save()

        await ctx.send(_("Manual snapshot '{name}' taken (ID: {id}).").format(name=name, id=snap_id))

    @agsnapshot.command(name="finalizeyear")
    async def snapshot_finalizeyear(
        self,
        ctx: commands.Context,
        year: Optional[int] = None,
        reset: bool = False,
        force: bool = False,
    ):
        """
        Finalize and archive a calendar year permanently.

        By default only current UTC year may be finalized.
        Use --force to override or re-finalize.
        """
        guild = ctx.guild
        target_year = year or datetime.utcnow().year
        cfg: GuildConfig = self.get_guild_conf(guild.id)

        if year is not None and year != datetime.utcnow().year and not force:
            return await ctx.send(warning(_("Cannot finalize year {yr} without --force.").format(yr=target_year)))

        existing = cfg.years.get(target_year)
        if existing and existing.finalized and not force:
            return await ctx.send(warning(_("Year {yr} already finalized; use --force.").format(yr=target_year)))

        async with ctx.typing():
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            return await ctx.send(_("No LevelUp data available to finalize."))

        now = utc_timestamp()
        snap_id = generate_snapshot_id()
        name = now.strftime("%Y-%m-%d_%H-%M-%S_YEAR_END")
        snapshot = Snapshot(
            id=snap_id,
            name=name,
            created=now,
            type=SnapshotType.YEAR_END,
            data=stats,
        )

        # remove old YEAR_END snapshots for this year
        old_metas = [m for m in cfg.snapshots if m.type == SnapshotType.YEAR_END and m.created.year == target_year]
        for old in old_metas:
            cfg.snapshots.remove(old)
            await self._delete_snapshot_file(guild.id, old)

        # write new snapshot JSON to disk
        path = self._snapshot_path(guild.id, snapshot.id, snapshot.type.value)
        self._atomic_write_json(path, snapshot.model_dump())
        sha = self._compute_sha256(path)

        # append new metadata
        meta = SnapshotMeta(
            id=snapshot.id,
            name=snapshot.name,
            created=snapshot.created,
            type=snapshot.type,
            path=path,
            sha256=sha,
        )
        cfg.snapshots.append(meta)

        # update year archive
        year_data = YearData(
            finalized=True,
            finalized_at=now,
            users={
                uid: UserYearStats(
                    xp=st.xp,
                    level=st.level,
                    messages=st.messages,
                    voicetime=st.voicetime,
                    stars=st.stars,
                    balance=st.balance,
                    rank=st.rank,
                )
                for uid, st in stats.items()
            },
        )
        cfg.years[target_year] = year_data

        # rebuild master aggregates from all finalized years
        cfg.master.clear()
        for y, yd in cfg.years.items():
            if not yd.finalized:
                continue
            for uid, ys in yd.users.items():
                m = cfg.master.setdefault(uid, MasterUserStats())
                m.total_xp        += ys.xp
                m.total_messages  += ys.messages
                m.total_voicetime += ys.voicetime
                m.total_stars     += ys.stars
                m.total_balance   += ys.balance
                if y not in m.years_active:
                    m.years_active.append(y)

        # persist metadata/index and year/master files
        await self.save()

        # optional LevelUp reset
        if reset:
            await reset_levelup_data(self.bot, guild, stats, reset_balances=True)

        reset_msg = _(" LevelUp data reset.") if reset else ""
        await ctx.send(
            _("Finalized year {yr} snapshot '{nm}' (ID: {id}).{rst}").format(
                yr=target_year, nm=name, id=snap_id, rst=reset_msg
            )
        )

    @agsnapshot.command(name="list")
    async def snapshot_list(self, ctx: commands.Context):
        """
        List all snapshots for this guild.
        """
        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)
        if not cfg.snapshots:
            return await ctx.send(_("No snapshots found."))
        snaps = sorted(cfg.snapshots, key=lambda m: m.created, reverse=True)
        lines = [f"{i+1}. `{m.id}` | {m.name} | {m.type.value}" for i, m in enumerate(snaps)]
        for page in pagify("\n".join(lines), page_length=1900):
            await ctx.send(page)

    @agsnapshot.command(name="delete")
    async def snapshot_delete(self, ctx: commands.Context, index: int):
        """
        Delete a snapshot by numeric index.
        """
        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)
        snaps = sorted(cfg.snapshots, key=lambda m: m.created, reverse=True)
        if index < 1 or index > len(snaps):
            return await ctx.send(_("Invalid snapshot index."))
        meta = snaps[index - 1]
        cfg.snapshots.remove(meta)
        await self._delete_snapshot_file(ctx.guild.id, meta)
        await self.save()
        await ctx.send(_("Deleted snapshot '{nm}' (ID: {id}).").format(nm=meta.name, id=meta.id))

    @agsnapshot.command(name="backup")
    async def snapshot_backup(self, ctx: commands.Context, index: Optional[int] = None):
        """
        Export snapshot JSON backup.

        If index is omitted: exports all MANUAL snapshots.
        """
        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)
        if not cfg.snapshots:
            return await ctx.send(_("No snapshots available."))
        snaps = sorted(cfg.snapshots, key=lambda m: m.created, reverse=True)
        if index is not None:
            if index < 1 or index > len(snaps):
                return await ctx.send(_("Invalid snapshot index."))
            selected_metas = [snaps[index - 1]]
        else:
            selected_metas = [m for m in snaps if m.type == SnapshotType.MANUAL]
            if not selected_metas:
                return await ctx.send(_("No manual snapshots found."))
        exports = []
        for meta in selected_metas:
            full = self._read_json(meta.path)
            exports.append(full)
        json_data = json.dumps(exports, indent=2, default=str)
        if len(selected_metas) == 1:
            fn = f"snapshot-{selected_metas[0].id}.json"
        else:
            fn = f"manual-snapshots-{ctx.guild.id}-{datetime.utcnow():%Y%m%d}.json"
        file = text_to_file(json_data, filename=fn)
        await ctx.send(file=file)