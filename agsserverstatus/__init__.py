from redbot.core.bot import Red
from .agsserverstatus import AGSServerStatus  # Ensure correct import

async def setup(bot: Red):
    cog = AGSServerStatus(bot)  # Ensure it's an instance of commands.Cog
    await bot.add_cog(cog)
