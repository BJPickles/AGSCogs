from redbot.core import commands

from .agsvoicesoundboard import AGSVoiceSoundboard


async def setup(bot: commands.Bot):
    """Required setup for Red to load the AGSVoiceSoundboard cog."""
    await bot.add_cog(AGSVoiceSoundboard(bot))
