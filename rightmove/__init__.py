from .rightmove import RightmoveCog

async def setup(bot):
    """Called by Red on cog load."""
    await bot.add_cog(RightmoveCog(bot))