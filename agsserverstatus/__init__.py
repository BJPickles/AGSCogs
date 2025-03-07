from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import warning
import asyncio

from .agsserverstatus import agsserverstatus

async def setup(bot: Red):
    cog = AGSServerStatus(bot)
    await bot.add_cog(cog)
