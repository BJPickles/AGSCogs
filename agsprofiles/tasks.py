# tasks.py
import asyncio
import logging
from datetime import datetime
from redbot.core import commands, tasks

from .common import generate_snapshot_id, collect_levelup_stats
from .models import SnapshotType, Snapshot, GuildConfig

log = logging.getLogger("red.agsprofiles.tasks")


@tasks.loop(hours=24)
async def daily_snapshot_loop(self):
    """Take a DAILY snapshot and enforce max_auto_daily."""
    now = datetime.utcnow()
    guild_ids = list(self.config.guilds.keys())
    for gid in guild_ids:
        guild = self.bot.get_guild(gid)
        if not guild:
            continue
        cfg: GuildConfig = self.config.guilds[gid]
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            continue
        snap_id = generate_snapshot_id()
        name = now.strftime("%Y-%m-%d") + " DAILY"
        snap = Snapshot(id=snap_id, name=name, created=now, type=SnapshotType.DAILY, data=stats)
        cfg.snapshots.append(snap)
        # purge old DAILY snaps beyond max_auto_daily
        auto_daily = [s for s in cfg.snapshots if s.type == SnapshotType.DAILY]
        if len(auto_daily) > cfg.max_auto_daily:
            auto_daily.sort(key=lambda s: s.created)
            for old in auto_daily[:-cfg.max_auto_daily]:
                cfg.snapshots.remove(old)
    self.save()


@tasks.loop(hours=168)
async def weekly_snapshot_loop(self):
    """Take a WEEKLY snapshot and enforce max_auto_weekly."""
    now = datetime.utcnow()
    guild_ids = list(self.config.guilds.keys())
    for gid in guild_ids:
        guild = self.bot.get_guild(gid)
        if not guild:
            continue
        cfg: GuildConfig = self.config.guilds[gid]
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            continue
        snap_id = generate_snapshot_id()
        name = now.strftime("%Y-%m-%d") + " WEEKLY"
        snap = Snapshot(id=snap_id, name=name, created=now, type=SnapshotType.WEEKLY, data=stats)
        cfg.snapshots.append(snap)
        # purge old WEEKLY snaps beyond max_auto_weekly
        auto_weekly = [s for s in cfg.snapshots if s.type == SnapshotType.WEEKLY]
        if len(auto_weekly) > cfg.max_auto_weekly:
            auto_weekly.sort(key=lambda s: s.created)
            for old in auto_weekly[:-cfg.max_auto_weekly]:
                cfg.snapshots.remove(old)
    self.save()


@daily_snapshot_loop.before_loop
@weekly_snapshot_loop.before_loop
async def before_loops(self):
    await self.bot.wait_until_red_ready()
    await asyncio.sleep(10)
    log.info("Starting AGSProfiles snapshot loops")