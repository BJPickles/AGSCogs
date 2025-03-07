from redbot.core.bot import Red
from .agsserverstatus import AGSServerStatus

async def setup(bot: Red):
    await bot.add_cog(AGSServerStatus(bot))
