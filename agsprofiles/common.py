# common.py
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional

import discord
from redbot.core.bot import Red
from redbot.core import bank
from levelup.common.formatter import get_user_position
from .models import Stats


def generate_snapshot_id() -> str:
    """
    Generate a unique snapshot ID.
    Combines UTC timestamp (YYYYMMDDHHMMSS) with an 8-char UUID fragment.
    """
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{ts}-{uid}"


def format_timedelta_seconds(seconds: float) -> float:
    """
    Convert seconds (float) to hours rounded to two decimals.
    Used for storing voice time in hours.
    """
    return round(seconds / 3600, 2)


async def collect_levelup_stats(bot: Red, guild: discord.Guild) -> Dict[int, Stats]:
    """
    Gather current LevelUp statistics for all members in a guild.

    Returns a mapping user_id → Stats.
    """
    stats_map: Dict[int, Stats] = {}
    lvl = bot.get_cog("LevelUp")
    if not lvl:
        return stats_map
    conf = lvl.db.get_conf(guild)
    # Precompute total XP leaderboard positions
    for user_id, profile in conf.users.items():
        member = guild.get_member(user_id)
        if not member:
            continue
        # Fetch position in XP leaderboard
        posinfo = await asyncio.to_thread(
            get_user_position, guild, conf, "lb", user_id, "xp"
        )
        rank = posinfo.get("position", 0)
        # Fetch balance safely
        balance = 0.0
        try:
            balance = float(await bank.get_balance(member))
        except Exception:
            balance = 0.0
        stats_map[user_id] = Stats(
            xp=profile.xp,
            level=profile.level,
            messages=profile.messages,
            voicetime=format_timedelta_seconds(profile.voice),
            stars=profile.stars,
            balance=balance,
            rank=rank,
        )
    return stats_map


async def subtract_levelup_data(bot: Red, guild: discord.Guild, snapshot_data: Dict[int, Stats]) -> None:
    """
    Subtract the snapshot_data from LevelUp so you may reset.
    Intended to be called after taking a snapshot and before resetting LevelUp.
    """
    lvl = bot.get_cog("LevelUp")
    if not lvl:
        return
    conf = lvl.db.get_conf(guild)
    for user_id, snap in snapshot_data.items():
        profile = conf.get_profile(user_id)
        # Subtract exact xp, messages, voice time (in seconds), stars, and balance
        profile.xp = max(0, profile.xp - snap.xp)
        profile.messages = max(0, profile.messages - snap.messages)
        # Convert hours back to seconds to subtract
        profile.voice = max(0.0, profile.voice - snap.voicetime * 3600)
        profile.stars = max(0, profile.stars - snap.stars)
        # No direct LevelUp tracking of balance; user must be reset via bank commands
    lvl.save()