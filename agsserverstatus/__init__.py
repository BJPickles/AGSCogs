from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import warning
import asyncio

from .agsserverstatus import AGSServerStatus

async def setup(bot: Red):
    cog = AGSServerStatus(bot)
    await bot.add_cog(cog)
