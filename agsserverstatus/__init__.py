from redbot.core import commands
from .agsserverstatus import AGSServerStatus

async def setup(bot: commands.Bot):
    await bot.add_cog(AGSServerStatus(bot))