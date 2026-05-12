# common.py

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Any

import discord
from PIL import Image
from redbot.core import bank
from redbot.core.bot import Red

from levelup.common.formatter import get_user_position
from .models import Stats

# =========================================================
# SNAPSHOT HELPERS
# =========================================================

def generate_snapshot_id() -> str:
    """
    Generate a unique snapshot ID.

    Format: YYYYMMDDHHMMSS-XXXXXXXX
    """
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    frag = uuid.uuid4().hex[:8]
    return f"{ts}-{frag}"

def utc_timestamp() -> datetime:
    """Return current UTC datetime."""
    return datetime.utcnow()

def snapshot_display_name(snapshot_type: str) -> str:
    """
    Generate standardized snapshot display name.

    Example: 2026-05-12_19-44-10_MANUAL
    """
    return datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S") + "_" + snapshot_type.upper()

# =========================================================
# LEVELUP ADAPTER
# =========================================================

class LevelUpAdapter:
    """
    Abstraction layer for interacting with the LevelUp cog.
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.lvl = bot.get_cog("LevelUp")
        if not self.lvl:
            raise RuntimeError("LevelUp cog is not loaded")

    def get_conf(self, guild: discord.Guild):
        """Get LevelUp guild config."""
        return self.lvl.db.get_conf(guild)

    async def save(self) -> None:
        """Persist LevelUp database."""
        try:
            # newer Red
            self.lvl.save(force=True)
        except TypeError:
            # fallback
            self.lvl.save()

    async def get_balance(self, member: discord.Member) -> float:
        """Safely fetch a user's economy balance."""
        try:
            return float(await bank.get_balance(member))
        except Exception:
            return 0.0

    async def reset_balance(self, member: discord.Member) -> None:
        """Reset a user's economy balance to zero."""
        try:
            await bank.set_balance(member, 0)
        except Exception:
            pass

# =========================================================
# STAT COLLECTION
# =========================================================

async def collect_levelup_stats(
    bot: Red,
    guild: discord.Guild,
) -> Dict[int, Stats]:
    """
    Collect live LevelUp statistics for all valid guild members.

    Returns dict mapping user_id → Stats.
    """
    try:
        adapter = LevelUpAdapter(bot)
    except RuntimeError:
        return {}
    conf = adapter.get_conf(guild)
    stats_map: Dict[int, Stats] = {}
    for user_id, profile in conf.users.items():
        member = guild.get_member(user_id)
        if not member or member.bot:
            continue
        try:
            pos = await asyncio.to_thread(
                get_user_position,
                bot=bot,
                guild=guild,
                db=bot.get_cog("LevelUp").db,
                stat="xp",
                lbtype="lb",
                is_global=False,
                member=member,
                use_displayname=False,
                color=None,
                # fallback minimal args
            )
            rank = pos.get("position", 0)
        except Exception:
            rank = 0
        balance = await adapter.get_balance(member)
        voice_hours = round(profile.voice / 3600, 2)
        stats_map[user_id] = Stats(
            xp=float(profile.xp),
            level=profile.level,
            messages=profile.messages,
            voicetime=voice_hours,
            stars=profile.stars,
            balance=balance,
            rank=rank,
        )
    return stats_map

# =========================================================
# LEVELUP RESET / REZERO
# =========================================================

async def subtract_levelup_data(
    bot: Red,
    guild: discord.Guild,
    snapshot_data: Dict[int, Stats] | Iterable[int],
    reset_balances: bool = False,
) -> None:
    """
    Reset live LevelUp profiles to zero rather than subtract.

    snapshot_data can be a dict of user_id→Stats or an iterable of user_ids.
    All profile XP, level, messages, voice, and stars are set to zero.
    Optionally resets economy balances.
    """
    # Determine user IDs
    if isinstance(snapshot_data, dict):
        user_ids = list(snapshot_data.keys())
    else:
        user_ids = list(snapshot_data)

    try:
        adapter = LevelUpAdapter(bot)
    except RuntimeError:
        return
    conf = adapter.get_conf(guild)
    for user_id in user_ids:
        profile = conf.get_profile(user_id)
        profile.xp = 0.0
        profile.level = 0
        profile.messages = 0
        profile.voice = 0.0
        profile.stars = 0
        if reset_balances:
            member = guild.get_member(user_id)
            if member:
                await adapter.reset_balance(member)
    await adapter.save()

# Alias deprecated subtraction to hard reset
reset_levelup_data = subtract_levelup_data

# =========================================================
# YEAR HELPERS
# =========================================================

def get_current_year() -> int:
    """Return current UTC calendar year."""
    return datetime.utcnow().year

def get_previous_year() -> int:
    """Return previous UTC calendar year."""
    return datetime.utcnow().year - 1

# =========================================================
# ICON HELPERS
# =========================================================

def get_user_icon_paths(
    guild_icon_dir: Path,
    role_icon_map: Dict[int, str],
    member: discord.Member,
) -> list[Path]:
    """
    Resolve all PNG icon paths for a member based on role→filename mappings.
    """
    resolved: list[Path] = []
    member_roles = {r.id for r in member.roles}
    for role_id, filename in role_icon_map.items():
        if role_id not in member_roles:
            continue
        p = guild_icon_dir / filename
        if p.is_file() and p.suffix.lower() == ".png":
            resolved.append(p)
    return resolved

async def apply_role_icons_to_profile(
    profile_image_path: Path,
    output_path: Path,
    icon_paths: Iterable[Path],
) -> Path:
    """
    Overlay role icons beneath a LevelUp-generated profile image.

    Icons are horizontally centered and evenly spaced.
    """
    if not icon_paths:
        return profile_image_path
    base = Image.open(profile_image_path).convert("RGBA")
    w, h = base.size
    icon_size = 42
    spacing = 12
    total_width = len(icon_paths) * icon_size + (len(icon_paths) - 1) * spacing
    start_x = (w - total_width) // 2
    y = int(h * 0.73)
    for idx, path in enumerate(icon_paths):
        try:
            icon = Image.open(path).convert("RGBA").resize((icon_size, icon_size), Image.LANCZOS)
        except Exception:
            continue
        x = start_x + idx * (icon_size + spacing)
        base.alpha_composite(icon, (x, y))
    base.save(output_path)
    return output_path