from .fridaygamenight import FridayGameNight

async def setup(bot):
    await bot.add_cog(FridayGameNight(bot))
