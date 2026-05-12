# models.py
import typing as t
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field


class SnapshotType(str, Enum):
    MANUAL = "MANUAL"
    DAILY  = "DAILY"
    WEEKLY = "WEEKLY"


class Stats(BaseModel):
    xp:        float = Field(..., description="Total XP at snapshot")
    level:     int   = Field(..., description="Level at snapshot")
    messages:  int   = Field(..., description="Messages sent")
    voicetime: float = Field(..., description="Voice time (seconds)")
    stars:     int   = Field(..., description="Stars received")
    balance:   float = Field(..., description="Economy balance")
    rank:      int   = Field(..., description="Leaderboard rank")


class Snapshot(BaseModel):
    id:      str                       = Field(..., description="Unique snapshot ID")
    name:    str                       = Field(..., description="Timestamped name")
    created: datetime                 = Field(default_factory=datetime.utcnow)
    type:    SnapshotType             = Field(..., description="MANUAL/DAILY/WEEKLY")
    data:    t.Dict[int, Stats]       = Field(..., description="user_id → Stats")


class GuildConfig(BaseModel):
    snapshots:         t.List[Snapshot]       = Field(default_factory=list)
    max_auto_daily:    int                    = Field(15, description="Max stored daily snaps")
    max_auto_weekly:   int                    = Field(15, description="Max stored weekly snaps")
    icons:             t.Dict[int, str]       = Field(default_factory=dict, description="role_id → icon filename")


class AGSConfig(BaseModel):
    guilds: t.Dict[int, GuildConfig] = Field(default_factory=dict)