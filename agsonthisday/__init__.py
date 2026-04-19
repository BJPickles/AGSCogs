# __init__.py

from .agsonthisday import AGSOnThisDay

async def setup(bot):
    """Called by Red on cog load."""
    await bot.add_cog(AGSOnThisDay(bot))