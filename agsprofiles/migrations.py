# migrations.py

import logging
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

from .models import SnapshotMeta, SnapshotType

log = logging.getLogger("red.agsprofiles.migrations")

# bump this when you add new migrations
LATEST_SCHEMA_VERSION = 2

def migrate_v1(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migration for schema_version → 1:
    Ensure all guild partitions have the new fields and defaults.
    """
    guilds = cfg.setdefault("guilds", {})
    for gid_str, guild in guilds.items():
        guild.setdefault("master", {})
        guild.setdefault("years", {})
        guild.setdefault("snapshots", [])
        guild.setdefault("icons", {})
        guild.setdefault("max_auto_daily", 15)
        guild.setdefault("max_auto_weekly", 15)
        guild.setdefault("schema_version", 1)
    return cfg

def migrate_v2(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migration for schema_version → 2:
    - Extract any full Snapshot entries from 'snapshots' into per-guild files.
    - Replace in-memory list entries with SnapshotMeta-only dicts.
    """
    from .common import cog_data_path  # to locate base path
    base = Path(cog_data_path(None)).parent / "AGSProfiles"  # hack to find cog data root
    guilds = cfg.get("guilds", {})
    for gid_str, guild in guilds.items():
        guild_id = int(gid_str)
        snaps = guild.get("snapshots", [])
        new_metas = []
        for entry in snaps:
            # detect full snapshot by presence of "data" key
            if "data" in entry:
                # write full snapshot JSON to disk
                subtype = entry["type"].lower()
                snap_dir = base / f"guild_{guild_id}" / "snapshots" / subtype
                snap_dir.mkdir(parents=True, exist_ok=True)
                path = snap_dir / f"{entry['id']}.json"
                # write raw entry dict
                path.write_text(json.dumps(entry, indent=2, default=str), encoding="utf-8")
                # compute hash
                sha = hashlib.sha256(path.read_bytes()).hexdigest()
                # build meta
                meta = {
                    "id": entry["id"],
                    "name": entry.get("name", entry["id"]),
                    "created": entry.get("created"),
                    "type": entry.get("type"),
                    "path": str(path),
                    "sha256": sha,
                }
                new_metas.append(meta)
            else:
                # already meta-only, ensure all fields present
                path = entry.get("path", "")
                sha = entry.get("sha256", "")
                meta = {
                    "id": entry["id"],
                    "name": entry.get("name", entry["id"]),
                    "created": entry.get("created"),
                    "type": entry.get("type"),
                    "path": path,
                    "sha256": sha,
                }
                new_metas.append(meta)
        guild["snapshots"] = new_metas
        guild["schema_version"] = 2
    return cfg

# mapping: version → migration function
MIGRATIONS = {
    1: migrate_v1,
    2: migrate_v2,
}

def migrate(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply missing migrations to raw AGSConfig dict in place.
    """
    current = cfg.get("schema_version", 0)
    if current > LATEST_SCHEMA_VERSION:
        log.warning(f"Config version {current} > latest {LATEST_SCHEMA_VERSION}")
    for version in range(current + 1, LATEST_SCHEMA_VERSION + 1):
        fn = MIGRATIONS.get(version)
        if not fn:
            continue
        try:
            log.info(f"Migrating AGSConfig to version {version}")
            cfg = fn(cfg)
        except Exception:
            log.exception(f"Error migrating to version {version}")
    cfg["schema_version"] = LATEST_SCHEMA_VERSION
    return cfg