# cogs/agshousingscraper/__init__.py

from .agshousingscraper import AGSHousingScraper

def setup(bot):
    bot.add_cog(AGSHousingScraper(bot))