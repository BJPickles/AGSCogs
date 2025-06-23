from .rightmove import RightmoveCog

async def setup(bot):
    """This is awaited by Red on cog load."""
    await bot.add_cog(RightmoveCog(bot))