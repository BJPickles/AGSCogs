from redbot.core.utils import get_end_user_data_statement_or_raise

from .rightmove import RightmoveCog

__red_end_user_data_statement__ = get_end_user_data_statement_or_raise(__file__)


async def setup(bot):
    """Called by Red when the cog is loaded."""
    await bot.add_cog(RightmoveCog(bot))
