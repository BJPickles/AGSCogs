from redbot.core import commands
from .daily_dadjokes import DailyDadJokes

async def setup(bot: commands.Bot):
    """Required setup for Red to load the DailyDadJokes cog."""
    await bot.add_cog(DailyDadJokes(bot))