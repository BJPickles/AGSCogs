from redbot.core import commands
from .agsactivities import Activities

async def setup(bot: commands.Bot):
    await bot.add_cog(Activities(bot))
