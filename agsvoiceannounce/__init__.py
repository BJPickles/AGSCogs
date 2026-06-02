from redbot.core import commands

from .agsvoiceannounce import AGSVoiceAnnounce


async def setup(bot: commands.Bot):
    """Required setup for Red to load the AGSVoiceAnnounce cog."""
    await bot.add_cog(AGSVoiceAnnounce(bot))
