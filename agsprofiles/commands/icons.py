# commands/icons.py
import discord
from pathlib import Path
from redbot.core import commands
from redbot.core.i18n import Translator

from ..models import GuildConfig

_ = Translator("AGSProfiles", __file__)


class IconCommands(commands.Cog):
    """Manage role→icon mappings for AGSProfiles."""

    @commands.group(name="agsicons", aliases=["agsicon"])
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def agsicons(self, ctx: commands.Context):
        """Manage role→icon mappings for AGSProfiles."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agsicons.command(name="add")
    async def icon_add(self, ctx: commands.Context, role: discord.Role, filename: str):
        """Associate a PNG icon with a role."""
        icons_dir = Path(self.cog_path) / "icons"
        file_path = icons_dir / filename
        if not file_path.exists():
            return await ctx.send(_("Icon file `{fn}` not found.").format(fn=filename))
        guild_conf: GuildConfig = self.config.guilds.setdefault(ctx.guild.id, GuildConfig())
        guild_conf.icons[role.id] = filename
        self.save()
        await ctx.send(_("Mapped role {role} to icon `{fn}`.").format(role=role.mention, fn=filename))

    @agsicons.command(name="remove")
    async def icon_remove(self, ctx: commands.Context, role: discord.Role):
        """Remove the icon mapping for a role."""
        guild_conf: GuildConfig = self.config.guilds.get(ctx.guild.id)
        if not guild_conf or role.id not in guild_conf.icons:
            return await ctx.send(_("No icon mapping for role {role}.").format(role=role.mention))
        guild_conf.icons.pop(role.id)
        self.save()
        await ctx.send(_("Removed icon mapping for role {role}.").format(role=role.mention))

    @agsicons.command(name="list")
    async def icon_list(self, ctx: commands.Context):
        """List all role→icon mappings."""
        guild_conf: GuildConfig = self.config.guilds.get(ctx.guild.id)
        if not guild_conf or not guild_conf.icons:
            return await ctx.send(_("No role→icon mappings configured."))
        lines = []
        for role_id, fn in guild_conf.icons.items():
            role = ctx.guild.get_role(role_id)
            mention = role.mention if role else str(role_id)
            lines.append(f"{mention} → `{fn}`")
        await ctx.send("\n".join(lines))