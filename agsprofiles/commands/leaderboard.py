# commands/leaderboard.py
import math
from datetime import datetime
from io import StringIO

import discord
from redbot.core import commands
from redbot.core.i18n import Translator
from redbot.core.utils.chat_formatting import humanize_number

from ..models import SnapshotType, Stats, Snapshot, GuildConfig
from ..common import collect_levelup_stats

_ = Translator("AGSProfiles", __file__)


class LeaderboardCommands(commands.Cog):
    """Year-in-review leaderboard commands."""

    @commands.group(name="agsleaderboard", aliases=["agslb"])
    @commands.guild_only()
    async def agsleaderboard(self, ctx: commands.Context):
        """View AGSProfiles leaderboards."""
        if not ctx.invoked_subcommand:
            await ctx.send_help()

    @agsleaderboard.command(name="year")
    async def leaderboard_year(
        self,
        ctx: commands.Context,
        stat: str = "xp",
        year: int = None,
    ):
        """
        Show leaderboard for a specific year.
        stat: xp|level|messages|voicetime|stars|balance
        year: calendar year (defaults to current year)
        """
        guild = ctx.guild
        cfg: GuildConfig = self.config.guilds.get(guild.id)
        target_year = year or datetime.utcnow().year

        # find manual snapshot for that year
        snap = None
        if cfg:
            for s in cfg.snapshots:
                if s.type == SnapshotType.MANUAL and s.created.year == target_year:
                    snap = s
                    break

        if snap:
            data = snap.data
        else:
            if target_year == datetime.utcnow().year:
                await ctx.trigger_typing()
                data = await collect_levelup_stats(self.bot, guild)
            else:
                return await ctx.send(_("No snapshot found for year {year}.").format(year=target_year))

        if not data:
            return await ctx.send(_("No data for year {year}.").format(year=target_year))

        stat = stat.lower()
        if stat not in Stats.model_fields:
            return await ctx.send(_("Invalid stat: {s}.").format(s=stat))

        items = list(data.items())
        items.sort(key=lambda x: getattr(x[1], stat), reverse=True)

        embeds = []
        pages = math.ceil(len(items) / 10)
        for p in range(pages):
            start = p * 10
            end = start + 10
            embed = discord.Embed(
                title=_("Year {yr} Leaderboard").format(yr=target_year),
                color=await self.bot.get_embed_color(ctx),
            )
            for idx, (uid, st) in enumerate(items[start:end], start=start + 1):
                member = guild.get_member(uid)
                name = member.display_name if member else f"<@{uid}>"
                val = getattr(st, stat)
                if stat in ("xp", "balance"):
                    disp = humanize_number(val)
                elif stat == "voicetime":
                    disp = f"{val}h"
                else:
                    disp = humanize_number(val)
                embed.add_field(
                    name=f"#{idx}. {name}",
                    value=f"{stat.title()}: `{disp}` • Level: `{st.level}`",
                    inline=False,
                )
            embed.set_footer(text=_("Page {p}/{tot}").format(p=p + 1, tot=pages))
            embeds.append(embed)

        await self._send_pages(ctx, embeds)

    @agsleaderboard.command(name="global")
    async def leaderboard_global(
        self,
        ctx: commands.Context,
        stat: str = "xp",
    ):
        """
        Show global multi-year leaderboard (sum across all manual snapshots).
        stat: xp|level|messages|voicetime|stars|balance
        """
        guild = ctx.guild
        cfg: GuildConfig = self.config.guilds.get(guild.id)
        if not cfg or not cfg.snapshots:
            return await ctx.send(_("No snapshots to aggregate."))

        # aggregate over all MANUAL snapshots
        agg: dict[int, Stats] = {}
        for snap in cfg.snapshots:
            if snap.type != SnapshotType.MANUAL:
                continue
            for uid, st in snap.data.items():
                if uid not in agg:
                    agg[uid] = Stats(
                        xp=0.0, level=0, messages=0, voicetime=0.0, stars=0, balance=0.0, rank=0
                    )
                a = agg[uid]
                a.xp += st.xp
                a.level += st.level
                a.messages += st.messages
                a.voicetime += st.voicetime
                a.stars += st.stars
                a.balance += st.balance

        if not agg:
            return await ctx.send(_("No manual snapshots to aggregate."))

        stat = stat.lower()
        if stat not in Stats.model_fields:
            return await ctx.send(_("Invalid stat: {s}.").format(s=stat))

        items = list(agg.items())
        items.sort(key=lambda x: getattr(x[1], stat), reverse=True)

        embeds = []
        pages = math.ceil(len(items) / 10)
        for p in range(pages):
            start = p * 10
            end = start + 10
            embed = discord.Embed(
                title=_("Global Leaderboard (all years)"),
                color=await self.bot.get_embed_color(ctx),
            )
            for idx, (uid, st) in enumerate(items[start:end], start=start + 1):
                member = guild.get_member(uid)
                name = member.display_name if member else f"<@{uid}>"
                val = getattr(st, stat)
                if stat in ("xp", "balance"):
                    disp = humanize_number(val)
                elif stat == "voicetime":
                    disp = f"{val}h"
                else:
                    disp = humanize_number(val)
                embed.add_field(
                    name=f"#{idx}. {name}",
                    value=f"{stat.title()}: `{disp}` • Level: `{st.level}`",
                    inline=False,
                )
            embed.set_footer(text=_("Page {p}/{tot}").format(p=p + 1, tot=pages))
            embeds.append(embed)

        await self._send_pages(ctx, embeds)

    async def _send_pages(self, ctx: commands.Context, pages: list[discord.Embed]):
        """Send or paginate a list of Embed pages."""
        if len(pages) == 1:
            await ctx.send(embed=pages[0])
            return
        # use Red's menu pagination controls
        from redbot.core.utils.menus import menu, DEFAULT_CONTROLS

        await menu(ctx, pages, DEFAULT_CONTROLS)