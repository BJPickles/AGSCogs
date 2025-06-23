from .rightmovealert import RightmoveAlert

def setup(bot):
    bot.add_cog(RightmoveAlert(bot))