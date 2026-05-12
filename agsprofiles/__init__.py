# __init__.py
from redbot.core.bot import Red
from redbot.core.utils import get_end_user_data_statement

from .main import AGSProfiles

__red_end_user_data_statement__ = get_end_user_data_statement(__file__)

async def setup(bot: Red):
    """Load the AGSProfiles cog."""
    await bot.add_cog(AGSProfiles(bot))
