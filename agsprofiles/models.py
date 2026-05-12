# models.py

import typing as t
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel, Field

class SnapshotType(StrEnum):
    MANUAL   = "MANUAL"
    DAILY    = "DAILY"
    WEEKLY   = "WEEKLY"
    YEAR_END = "YEAR_END"

class Stats(BaseModel):
    xp:        float = Field(..., description="Total XP at snapshot")
    level:     int   = Field(..., description="Level at snapshot")
    messages:  int   = Field(..., description="Messages sent")
    voicetime: float = Field(..., description="Voice time (hours)")
    stars:     int   = Field(..., description="Stars received")
    balance:   float = Field(..., description="Economy balance")
    rank:      int   = Field(..., description="Leaderboard rank")

class SnapshotMeta(BaseModel):
    """
    Lightweight index record for a snapshot file.
    Metadata only; full data lives on disk and is lazy–loaded.
    """
    id:      str          = Field(..., description="Unique snapshot ID")
    name:    str          = Field(..., description="Human-readable snapshot name")
    created: datetime     = Field(..., description="When this snapshot was taken")
    type:    SnapshotType = Field(..., description="MANUAL / DAILY / WEEKLY / YEAR_END")
    path:    Path         = Field(..., description="Filesystem path to the snapshot JSON")
    sha256:  str          = Field(..., description="SHA256 hash of the snapshot JSON file")

class Snapshot(BaseModel):
    """
    Full snapshot record, loaded on demand from its JSON file.
    """
    id:      str                = Field(..., description="Unique snapshot ID")
    name:    str                = Field(..., description="Human-readable snapshot name")
    created: datetime           = Field(..., description="When this snapshot was taken")
    type:    SnapshotType       = Field(..., description="MANUAL / DAILY / WEEKLY / YEAR_END")
    data:    t.Dict[int, Stats] = Field(..., description="user_id → Stats at snapshot")

class UserYearStats(BaseModel):
    xp:        float = Field(0.0, description="Year-end total XP")
    level:     int   = Field(0,   description="Year-end level")
    messages:  int   = Field(0,   description="Year-end messages")
    voicetime: float = Field(0.0, description="Year-end voice time (hours)")
    stars:     int   = Field(0,   description="Year-end stars")
    balance:   float = Field(0.0, description="Year-end economy balance")
    rank:      int   = Field(0,   description="Year-end rank")

class YearData(BaseModel):
    finalized:    bool                        = Field(False,    description="Has this year been finalized?")
    finalized_at: datetime | None             = Field(None,     description="UTC timestamp when finalized")
    users:        t.Dict[int, UserYearStats]  = Field(default_factory=dict, description="user_id → YearStats")

class MasterUserStats(BaseModel):
    total_xp:        float   = Field(0.0, description="All-time aggregate XP")
    total_messages:  int     = Field(0,   description="All-time aggregate messages")
    total_voicetime: float   = Field(0.0, description="All-time aggregate voice hours")
    total_stars:     int     = Field(0,   description="All-time aggregate stars")
    total_balance:   float   = Field(0.0, description="All-time aggregate balance")
    years_active:    t.List[int] = Field(default_factory=list, description="Calendar years active")

class GuildConfig(BaseModel):
    schema_version:   int                        = Field(1, description="GuildConfig schema version")
    master:           t.Dict[int, MasterUserStats]= Field(default_factory=dict, description="All-time aggregates")
    years:            t.Dict[int, YearData]      = Field(default_factory=dict, description="Per-year archives")
    snapshots:        t.List[SnapshotMeta]       = Field(default_factory=list, description="Index of snapshots (metadata only)")
    icons:            t.Dict[int, str]           = Field(default_factory=dict, description="Role→icon filename map")
    max_auto_daily:   int                        = Field(15, description="Max rolling daily snapshots to keep")
    max_auto_weekly:  int                        = Field(15, description="Max rolling weekly snapshots to keep")

class AGSConfig(BaseModel):
    schema_version: int                       = Field(1, description="AGSConfig schema version")
    guilds:         t.Dict[int, GuildConfig]  = Field(default_factory=dict, description="Per-guild partitions")