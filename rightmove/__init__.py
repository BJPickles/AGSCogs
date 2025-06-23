# cogs/rightmove/__init__.py

from .rightmove import RightmoveCog

def setup(bot):
    bot.add_cog(RightmoveCog(bot))