from .agsvrolekick import AGSVRoleKick


async def setup(bot):
    await bot.add_cog(AGSVRoleKick(bot))