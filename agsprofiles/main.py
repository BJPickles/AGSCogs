import asyncio
import json
import logging
import hashlib
from pathlib import Path
from typing import Any

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

from .models import AGSConfig, GuildConfig, Snapshot, SnapshotMeta
from .common import generate_snapshot_id, utc_timestamp
from .commands import SnapshotCommands, LeaderboardCommands

log = logging.getLogger("red.agsprofiles")


class AGSProfiles(commands.Cog):
    """
    AGSProfiles cog.

    Persistent yearly archival + snapshot system for LevelUp.
    """

    def __init__(self, bot: Red):
        self.bot: Red = bot

        # base directory for all guild data
        self.base_path: Path = cog_data_path(self)
        self.base_path.mkdir(parents=True, exist_ok=True)

        self._save_lock = asyncio.Lock()
        self.config = AGSConfig()

        # attach command modules as composition (NOT inheritance)
        self.snapshots = SnapshotCommands(self)
        self.leaderboard = LeaderboardCommands(self)

        self._load_all_guilds()

    # =========================================================
    # COG LIFECYCLE
    # =========================================================

    async def cog_load(self):
        from .tasks import daily_snapshot_loop, weekly_snapshot_loop

        daily_snapshot_loop.start(self)
        weekly_snapshot_loop.start(self)

        log.info("AGSProfiles loaded; snapshot loops started")

    async def cog_unload(self):
        from .tasks import daily_snapshot_loop, weekly_snapshot_loop

        daily_snapshot_loop.cancel()
        weekly_snapshot_loop.cancel()

        log.info("AGSProfiles unloaded; snapshot loops stopped")

    # =========================================================
    # PATH HELPERS
    # =========================================================

    def _guild_dir(self, guild_id: int) -> Path:
        return self.base_path / f"guild_{guild_id}"

    def _snapshot_path(self, guild_id: int, snapshot_id: str, snapshot_type: str) -> Path:
        subtype = snapshot_type.lower()
        return self._guild_dir(guild_id) / "snapshots" / subtype / f"{snapshot_id}.json"

    # =========================================================
    # HASH / JSON HELPERS
    # =========================================================

    def _compute_sha256(self, path: Path) -> str:
        data = path.read_bytes()
        return hashlib.sha256(data).hexdigest()

    def _atomic_write_json(self, path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        text = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)

    def _read_json(self, path: Path) -> dict:
        try:
            text = path.read_text(encoding="utf-8")
            return json.loads(text)
        except Exception as e:
            log.warning(f"Corrupt JSON in {path}: {e!r}")
            corrupt = path.with_suffix(path.suffix + ".corrupt")
            try:
                path.rename(corrupt)
            except Exception:
                pass
            return {}

    async def _delete_snapshot_file(self, guild_id: int, meta: SnapshotMeta):
        try:
            if meta.path.exists():
                meta.path.unlink()
        except Exception:
            log.warning(f"Failed deleting snapshot {meta.path}")

    # =========================================================
    # CONFIG ACCESS
    # =========================================================

    def get_guild_conf(self, guild_id: int) -> GuildConfig:
        return self.config.guilds.setdefault(guild_id, GuildConfig())

    # =========================================================
    # LOADERS
    # =========================================================

    def _load_all_guilds(self):
        for guild_dir in self.base_path.iterdir():
            if not guild_dir.is_dir() or not guild_dir.name.startswith("guild_"):
                continue
            try:
                gid = int(guild_dir.name.split("_", 1)[1])
            except ValueError:
                continue

            try:
                cfg = self._load_guild_conf(gid)
                self.config.guilds[gid] = cfg
            except Exception:
                log.exception("Failed loading guild %s", gid)

    def _load_guild_conf(self, guild_id: int) -> GuildConfig:
        guild_dir = self._guild_dir(guild_id)
        guild_dir.mkdir(parents=True, exist_ok=True)

        cfg = GuildConfig()

        master_file = guild_dir / "master.json"
        if master_file.exists():
            raw = self._read_json(master_file)
            if raw:
                try:
                    cfg.master = {
                        int(uid): v for uid, v in raw.get("master", {}).items()
                    }
                except Exception:
                    log.exception("master.json load failed")

        years_dir = guild_dir / "years"
        years_dir.mkdir(parents=True, exist_ok=True)

        for f in years_dir.glob("*.json"):
            raw = self._read_json(f)
            if raw:
                try:
                    loaded = GuildConfig.model_validate(raw)
                    cfg.years.update(loaded.years)
                except Exception:
                    log.exception("year load failed %s", f)

        snaps_dir = guild_dir / "snapshots"

        for sub in ("manual", "daily", "weekly", "year_end"):
            part = snaps_dir / sub
            part.mkdir(parents=True, exist_ok=True)

            for f in part.glob("*.json"):
                raw = self._read_json(f)
                if not raw:
                    continue

                try:
                    full = Snapshot.model_validate(raw)
                    sha = self._compute_sha256(f)

                    meta = SnapshotMeta(
                        id=full.id,
                        name=full.name,
                        created=full.created,
                        type=full.type,
                        path=f,
                        sha256=sha,
                    )
                    cfg.snapshots.append(meta)

                except Exception:
                    log.exception("snapshot load failed %s", f)

        return cfg

    # =========================================================
    # SAVING
    # =========================================================

    async def save(self):
        async with self._save_lock:
            for gid, cfg in self.config.guilds.items():
                try:
                    await self._save_guild_conf(gid, cfg)
                except Exception:
                    log.exception("save failed %s", gid)

    async def _save_guild_conf(self, guild_id: int, cfg: GuildConfig):
        guild_dir = self._guild_dir(guild_id)
        guild_dir.mkdir(parents=True, exist_ok=True)

        self._atomic_write_json(
            guild_dir / "master.json",
            {"master": {str(k): v.model_dump() for k, v in cfg.master.items()}},
        )

        yd = guild_dir / "years"
        yd.mkdir(parents=True, exist_ok=True)

        for year, data in cfg.years.items():
            self._atomic_write_json(
                yd / f"{year}.json",
                {"years": {str(year): data.model_dump()}},
            )

        self._atomic_write_json(
            guild_dir / "icons.json",
            {str(k): v for k, v in cfg.icons.items()},
        )

    # =========================================================
    # EVENTS
    # =========================================================

    @commands.Cog.listener()
    async def on_guild_remove(self, guild: discord.Guild):
        self.config.guilds.pop(guild.id, None)
        log.info("Unloaded guild %s", guild.id)

    # =========================================================
    # STATUS COMMAND
    # =========================================================

    @commands.command(name="agsprofilestatus")
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def agsprofilestatus(self, ctx: commands.Context):
        cfg = self.get_guild_conf(ctx.guild.id)

        counts = {"MANUAL": 0, "DAILY": 0, "WEEKLY": 0, "YEAR_END": 0}
        for m in cfg.snapshots:
            counts[m.type.value] += 1

        embed = discord.Embed(
            title="AGSProfiles Status",
            color=await self.bot.get_embed_color(ctx),
        )

        embed.add_field(
            name="Snapshots",
            value="\n".join(f"{k}: {v}" for k, v in counts.items()),
            inline=False,
        )
        embed.add_field(name="Years Archived", value=str(len(cfg.years)))
        embed.add_field(name="Master Users", value=str(len(cfg.master)))

        await ctx.send(embed=embed)