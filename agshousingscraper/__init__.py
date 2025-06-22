# cogs/agshousingscraper/__init__.py

from .agshousingscraper import AGSHousingScraper

async def setup(bot):
    """This is awaited by Red when you do [p]load agshousingscraper."""
    await bot.add_cog(AGSHousingScraper(bot))