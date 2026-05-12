# commands/leaderboard.py

import math
from datetime import datetime
from typing import Dict, List, Tuple

import discord
from redbot.core import commands
from redbot.core.i18n import Translator
from redbot.core.utils.chat_formatting import humanize_number
from redbot.core.utils.menus import DEFAULT_CONTROLS, menu

from ..common import collect_levelup_stats
from ..models import (
    GuildConfig,
    MasterUserStats,
    SnapshotType,
    UserYearStats,
)

_ = Translator("AGSProfiles", __file__)

# Valid stats for current and yearly leaderboards
VALID_STATS = {"xp", "level", "messages", "voicetime", "stars", "balance", "rank"}

# Mapping for global all-time aggregates
GLOBAL_STAT_MAP = {
    "xp": "total_xp",
    "messages": "total_messages",
    "voicetime": "total_voicetime",
    "stars": "total_stars",
    "balance": "total_balance",
}


class LeaderboardCommands(commands.Cog):
    """Year-of-review leaderboard commands."""

    @commands.group(name="agsleaderboard", aliases=["agslb", "profileleaderboard"])
    @commands.guild_only()
    async def agsleaderboard(self, ctx: commands.Context):
        """View AGSProfiles leaderboards."""
        if not ctx.invoked_subcommand:
            await ctx.invoke(self.leaderboard_current)

    @agsleaderboard.command(name="current")
    async def leaderboard_current(self, ctx: commands.Context, stat: str = "xp"):
        """
        Show leaderboard for the current active year.

        Valid stats: xp | level | messages | voicetime | stars | balance | rank
        """
        stat = stat.lower()
        if stat not in VALID_STATS:
            return await ctx.send(_("Invalid stat `{stat}`.").format(stat=stat))

        await ctx.trigger_typing()
        data = await collect_levelup_stats(self.bot, ctx.guild)
        if not data:
            return await ctx.send(_("No LevelUp data available."))

        # Sort by requested stat descending
        items = sorted(
            data.items(),
            key=lambda x: getattr(x[1], stat),
            reverse=True,
        )

        embeds = await self._build_stat_pages(
            ctx=ctx,
            guild=ctx.guild,
            items=items,
            stat=stat,
            title=_("Current Year Leaderboard ({year})").format(year=datetime.utcnow().year),
        )
        await self._send_pages(ctx, embeds)

    @agsleaderboard.command(name="year")
    async def leaderboard_year(self, ctx: commands.Context, year: int, stat: str = "xp"):
        """
        Show leaderboard for a finalized archived year.

        Example: [p]agsleaderboard year 2025 xp
        """
        stat = stat.lower()
        if stat not in VALID_STATS:
            return await ctx.send(_("Invalid stat `{stat}`.").format(stat=stat))

        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)
        year_data = cfg.years.get(year)
        if not year_data or not year_data.finalized:
            return await ctx.send(_("No archived data exists for year `{year}`.").format(year=year))
        if not year_data.users:
            return await ctx.send(_("Year `{year}` contains no user data.").format(year=year))

        # Sort archived users by the stored year-end stat
        items = sorted(
            year_data.users.items(),
            key=lambda x: getattr(x[1], stat),
            reverse=True,
        )

        embeds = await self._build_stat_pages(
            ctx=ctx,
            guild=ctx.guild,
            items=items,
            stat=stat,
            title=_("Archived Leaderboard ({year})").format(year=year),
        )
        await self._send_pages(ctx, embeds)

    @agsleaderboard.command(name="global")
    async def leaderboard_global(self, ctx: commands.Context, stat: str = "xp"):
        """
        Show global all-time leaderboard across all archived years.

        Valid stats: xp | messages | voicetime | stars | balance
        """
        stat = stat.lower()
        if stat not in GLOBAL_STAT_MAP:
            return await ctx.send(
                _("Global leaderboard supports: xp, messages, voicetime, stars, balance")
            )

        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)
        if not cfg.master:
            return await ctx.send(_("No archived master data exists yet."))

        field = GLOBAL_STAT_MAP[stat]
        items = sorted(
            cfg.master.items(),
            key=lambda x: getattr(x[1], field),
            reverse=True,
        )

        # Build paginated embeds, 10 entries per page
        page_size = 10
        total_pages = math.ceil(len(items) / page_size)
        embeds: List[discord.Embed] = []

        for page_index in range(total_pages):
            start = page_index * page_size
            end = start + page_size
            embed = discord.Embed(title=_("Global All-Time Leaderboard"), color=await self.bot.get_embed_color(ctx))
            for pos, (user_id, data) in enumerate(items[start:end], start=start + 1):
                member = ctx.guild.get_member(user_id)
                name = member.display_name if member else f"Unknown User ({user_id})"
                val = getattr(data, field)
                disp = humanize_number(round(val)) if stat in {"xp", "balance"} else (
                    f"{round(val,2)}h" if stat == "voicetime" else humanize_number(val)
                )
                years = ", ".join(str(y) for y in sorted(data.years_active)) or _("None")
                embed.add_field(
                    name=f"#{pos} • {name}",
                    value=_( "{Stat}: `{Value}`\nYears Active: `{Years}`" ).format(
                        Stat=stat.title(), Value=disp, Years=years
                    ),
                    inline=False,
                )
            embed.set_footer(text=_("Page {p}/{t}").format(p=page_index + 1, t=total_pages))
            embeds.append(embed)

        await self._send_pages(ctx, embeds)

    @commands.command(name="agprofile")
    @commands.guild_only()
    async def agprofile(self, ctx: commands.Context, member: discord.Member = None, year: int = None):
        """
        Show archived AGSProfiles data for a user.

        Examples:
        [p]agprofile
        [p]agprofile @User
        [p]agprofile @User 2025
        """
        member = member or ctx.author
        cfg: GuildConfig = self.get_guild_conf(ctx.guild.id)

        # specific year
        if year is not None:
            yd = cfg.years.get(year)
            if not yd or not yd.finalized:
                return await ctx.send(_("No archived data for {user} in {year}.").format(
                    user=member.display_name, year=year
                ))
            us = yd.users.get(member.id)
            if not us:
                return await ctx.send(_("No stats for {user} in {year}.").format(
                    user=member.display_name, year=year
                ))
            embed = discord.Embed(title=f"{member.display_name} • {year} Archive", color=member.color)
            embed.set_thumbnail(url=member.display_avatar.url)
            embed.add_field(name="XP", value=humanize_number(round(us.xp)))
            embed.add_field(name="Level", value=humanize_number(us.level))
            embed.add_field(name="Messages", value=humanize_number(us.messages))
            embed.add_field(name="Voice Hours", value=f"{round(us.voicetime,2)}h")
            embed.add_field(name="Stars", value=humanize_number(us.stars))
            embed.add_field(name="Balance", value=humanize_number(round(us.balance)))
            embed.add_field(name="Rank", value=f"#{us.rank}")
            if yd.finalized_at:
                embed.set_footer(text=_("Finalized {dt} UTC").format(
                    dt=yd.finalized_at.strftime("%Y-%m-%d %H:%M")
                ))
            return await ctx.send(embed=embed)

        # master overview
        ms = cfg.master.get(member.id)
        if not ms:
            return await ctx.send(_("No archived AGSProfiles data exists for this user."))

        embed = discord.Embed(title=f"{member.display_name} • AGSProfiles Archive", color=member.color)
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Total XP",        value=humanize_number(round(ms.total_xp)),    inline=True)
        embed.add_field(name="Total Messages",  value=humanize_number(ms.total_messages),      inline=True)
        embed.add_field(name="Total Voice Hours", value=f"{round(ms.total_voicetime,2)}h",     inline=True)
        embed.add_field(name="Total Stars",     value=humanize_number(ms.total_stars),         inline=True)
        embed.add_field(name="Total Balance",   value=humanize_number(round(ms.total_balance)), inline=True)
        years = ", ".join(str(y) for y in sorted(ms.years_active)) or _("None")
        embed.add_field(name="Years Active", value=years, inline=False)
        return await ctx.send(embed=embed)

    async def _build_stat_pages(
        self,
        ctx: commands.Context,
        guild: discord.Guild,
        items: List[Tuple[int, Any]],
        stat: str,
        title: str,
    ) -> List[discord.Embed]:
        """
        Helper to build paginated embeds given sorted (user_id, data) and stat key.
        """
        page_size = 10
        total_pages = math.ceil(len(items) / page_size)
        embeds: List[discord.Embed] = []

        for idx in range(total_pages):
            start = idx * page_size
            end = start + page_size
            embed = discord.Embed(title=title, color=await self.bot.get_embed_color(ctx))
            for pos, (uid, data) in enumerate(items[start:end], start=start + 1):
                member = guild.get_member(uid)
                name = member.display_name if member else f"Unknown User ({uid})"
                val = getattr(data, stat)
                disp = humanize_number(round(val)) if stat in {"xp", "balance"} else (
                    f"{round(val,2)}h" if stat == "voicetime" else humanize_number(val)
                )
                lvl = getattr(data, "level", 0)
                embed.add_field(
                    name=f"#{pos} • {name}",
                    value=f"{stat.title()}: `{disp}`\nLevel: `{lvl}`",
                    inline=False,
                )
            embed.set_footer(text=_("Page {p}/{t}").format(p=idx + 1, t=total_pages))
            embeds.append(embed)

        return embeds

    async def _send_pages(self, ctx: commands.Context, pages: List[discord.Embed]):
        """
        Send a list of embeds as a paginated menu, or directly if only one page.
        """
        if not pages:
            return await ctx.send(_("Nothing to display."))
        if len(pages) == 1:
            return await ctx.send(embed=pages[0])
        await menu(ctx, pages, DEFAULT_CONTROLS)
