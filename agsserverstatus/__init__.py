from redbot.core.bot import Red
from .agsserverstatus import AGSServerStatus  # Import the class correctly

async def setup(bot: Red):
    cog = AGSServerStatus(bot)
    await bot.add_cog(cog)
