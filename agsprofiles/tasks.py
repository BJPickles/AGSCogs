# tasks.py

import asyncio
import logging
from datetime import datetime, timedelta

from redbot.core import tasks

from .common import collect_levelup_stats, generate_snapshot_id
from .models import Snapshot, SnapshotType, SnapshotMeta

log = logging.getLogger("red.agsprofiles.tasks")


async def _sleep_until_next_midnight_utc() -> None:
    """Sleep until the next UTC midnight."""
    now = datetime.utcnow()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    await asyncio.sleep((tomorrow - now).total_seconds())


async def _sleep_until_next_monday_midnight_utc() -> None:
    """Sleep until next Monday 00:00 UTC."""
    now = datetime.utcnow()
    days_ahead = (0 - now.weekday() + 7) % 7
    if days_ahead == 0 and now.hour >= 0:
        days_ahead = 7
    next_run = (now + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
    await asyncio.sleep((next_run - now).total_seconds())


@tasks.loop(hours=24)
async def daily_snapshot_loop(self):
    """
    Take a DAILY snapshot at UTC midnight and prune old DAILY snapshots + files.
    """
    for guild_id, cfg in list(self.config.guilds.items()):
        guild = self.bot.get_guild(guild_id)
        if not guild:
            continue
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            continue

        now = datetime.utcnow()
        snap_id = generate_snapshot_id()
        name = now.strftime("%Y-%m-%d_DAILY")
        snapshot = Snapshot(
            id=snap_id,
            name=name,
            created=now,
            type=SnapshotType.DAILY,
            data=stats,
        )

        # write full snapshot JSON to disk
        path = self._snapshot_path(guild_id, snapshot.id, snapshot.type.value)
        self._atomic_write_json(path, snapshot.model_dump(exclude_defaults=False))

        # compute integrity hash and append only metadata in memory
        sha = self._compute_sha256(path)
        meta = SnapshotMeta(
            id=snapshot.id,
            name=snapshot.name,
            created=snapshot.created,
            type=snapshot.type,
            path=path,
            sha256=sha,
        )
        cfg.snapshots.append(meta)

        # prune old DAILY metas and files
        dailies = [m for m in cfg.snapshots if m.type == SnapshotType.DAILY]
        if len(dailies) > cfg.max_auto_daily:
            dailies.sort(key=lambda m: m.created)
            for old in dailies[:-cfg.max_auto_daily]:
                cfg.snapshots.remove(old)
                await self._delete_snapshot_file(guild_id, old)

    # persist metadata/index only
    await self.save()


@tasks.loop(hours=168)
async def weekly_snapshot_loop(self):
    """
    Take a WEEKLY snapshot at Monday UTC midnight and prune old WEEKLY snapshots + files.
    """
    for guild_id, cfg in list(self.config.guilds.items()):
        guild = self.bot.get_guild(guild_id)
        if not guild:
            continue
        stats = await collect_levelup_stats(self.bot, guild)
        if not stats:
            continue

        now = datetime.utcnow()
        snap_id = generate_snapshot_id()
        name = now.strftime("%Y-%m-%d_WEEKLY")
        snapshot = Snapshot(
            id=snap_id,
            name=name,
            created=now,
            type=SnapshotType.WEEKLY,
            data=stats,
        )

        # write full snapshot JSON to disk
        path = self._snapshot_path(guild_id, snapshot.id, snapshot.type.value)
        self._atomic_write_json(path, snapshot.model_dump(exclude_defaults=False))

        # compute integrity hash and append only metadata in memory
        sha = self._compute_sha256(path)
        meta = SnapshotMeta(
            id=snapshot.id,
            name=snapshot.name,
            created=snapshot.created,
            type=snapshot.type,
            path=path,
            sha256=sha,
        )
        cfg.snapshots.append(meta)

        # prune old WEEKLY metas and files
        weeks = [m for m in cfg.snapshots if m.type == SnapshotType.WEEKLY]
        if len(weeks) > cfg.max_auto_weekly:
            weeks.sort(key=lambda m: m.created)
            for old in weeks[:-cfg.max_auto_weekly]:
                cfg.snapshots.remove(old)
                await self._delete_snapshot_file(guild_id, old)

    # persist metadata/index only
    await self.save()


@daily_snapshot_loop.before_loop
async def before_daily(self):
    await self.bot.wait_until_red_ready()
    log.info("Sleeping until UTC midnight for daily snapshots")
    await _sleep_until_next_midnight_utc()
    log.info("Daily snapshot loop starting")


@weekly_snapshot_loop.before_loop
async def before_weekly(self):
    await self.bot.wait_until_red_ready()
    log.info("Sleeping until next Monday UTC midnight for weekly snapshots")
    await _sleep_until_next_monday_midnight_utc()
    log.info("Weekly snapshot loop starting")