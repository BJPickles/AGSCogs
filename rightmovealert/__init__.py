from .rightmovealert import RightmoveAlert

async def setup(bot):
    cog = RightmoveAlert(bot)
    await bot.add_cog(cog)