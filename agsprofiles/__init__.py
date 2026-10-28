# __init__.py
from inspect import signature
from redbot.core.bot import Red
from redbot.core.utils import get_end_user_data_statement

from .main import AGSProfiles

__red_end_user_data_statement__ = get_end_user_data_statement(__file__)

async def setup(bot: Red):
    """Load only when Vertyco LevelUp and the expected formatter API exist."""
    if bot.get_cog("LevelUp") is None:
        raise RuntimeError(
            "AGSProfiles requires Vertyco's LevelUp cog to be loaded first. "
            "Install it from https://github.com/vertyco/vrt-cogs and run "
            "'[p]load levelup' before loading AGSProfiles."
        )

    try:
        from levelup.common.formatter import get_user_position
    except ImportError as exc:
        raise RuntimeError(
            "AGSProfiles could not import the LevelUp formatter. "
            "Update/reinstall Vertyco's LevelUp cog before loading AGSProfiles."
        ) from exc

    try:
        # Verify the API without making any network calls or mutating LevelUp.
        signature(get_user_position).bind(
            guild=None, conf=None, lbtype="lb", target_user=0, key="xp"
        )
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            "AGSProfiles requires a compatible LevelUp formatter API "
            "(guild, conf, lbtype, target_user, key). "
            "Update LevelUp from https://github.com/vertyco/vrt-cogs."
        ) from exc

    await bot.add_cog(AGSProfiles(bot))
