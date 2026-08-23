from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional, Tuple

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.agsvoicesoundboard")

# A short grace period after Discord accepts the Soundboard request before
# restoring Red Audio's usual self-deafened state.
REDEAF_GRACE_SECONDS = 0.75
VOICE_STATE_TIMEOUT_SECONDS = 2.0
SOUNDS_PER_EMBED = 15


class AGSVoiceSoundboard(commands.Cog):
    """Play a configured guild Soundboard sound whenever Red joins or moves voice channels."""

    __author__ = "AEGIS Game Studios"
    __version__ = "1.0.0"

    def __init__(self, bot: Red):
        self.bot = bot

        # Do not change this identifier after release; it owns this cog's persisted Config.
        self.config = Config.get_conf(self, identifier=0xA6515B001, force_registration=True)
        self.config.register_guild(sound_id=None)

        # Serialises our own self-deafen manipulation per guild so rapid voice moves
        # cannot cause two sound routines to fight over Red's voice state.
        self._guild_locks: Dict[int, asyncio.Lock] = {}

    async def red_delete_data_for_user(self, *, requester, user_id: int):
        """This cog stores no user data."""
        return

    def _guild_lock(self, guild_id: int) -> asyncio.Lock:
        lock = self._guild_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._guild_locks[guild_id] = lock
        return lock

    # ---------------------------------------------------------------------
    # Voice event
    # ---------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        # We only care about Red itself.
        if self.bot.user is None or member.id != self.bot.user.id:
            return

        # Mute/deafen/stream/etc. changes do not count. Only an actual channel change.
        if before.channel == after.channel:
            return

        # Leaving voice does not play anything.
        if after.channel is None:
            return

        # Native Soundboard sending is exposed on normal VoiceChannel objects.
        if not isinstance(after.channel, discord.VoiceChannel):
            return

        guild = member.guild

        if await self.bot.cog_disabled_in_guild(self, guild):
            return

        # Don't do any voice-state manipulation unless this guild is configured.
        if await self.config.guild(guild).sound_id() is None:
            return

        ok, message = await self._play_configured_sound(guild, after.channel)
        if not ok:
            log.warning(
                "AGSVoiceSoundboard could not play in guild %s (%s), channel %s (%s): %s",
                guild.name,
                guild.id,
                after.channel.name,
                after.channel.id,
                message,
            )

    # ---------------------------------------------------------------------
    # Soundboard helpers
    # ---------------------------------------------------------------------

    async def _get_configured_sound(
        self, guild: discord.Guild
    ) -> Tuple[Optional[discord.SoundboardSound], Optional[str]]:
        sound_id = await self.config.guild(guild).sound_id()
        if sound_id is None:
            return None, "No Soundboard sound is configured for this server."

        sound_id = int(sound_id)
        sound = guild.get_soundboard_sound(sound_id)

        if sound is None:
            try:
                sound = await guild.fetch_soundboard_sound(sound_id)
            except discord.NotFound:
                return None, f"The configured Soundboard sound `{sound_id}` no longer exists in this server."
            except discord.HTTPException as exc:
                return None, f"Discord could not retrieve Soundboard sound `{sound_id}`: {exc}"

        if not getattr(sound, "available", True):
            return None, f"The configured Soundboard sound **{sound.name}** (`{sound.id}`) is currently unavailable."

        return sound, None

    async def _wait_for_self_deaf_state(
        self,
        guild: discord.Guild,
        channel_id: int,
        desired_self_deaf: bool,
        *,
        timeout: float = VOICE_STATE_TIMEOUT_SECONDS,
    ) -> bool:
        """Wait for Discord's cached bot voice state to reflect the requested self-deaf value."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            me = guild.me
            voice = me.voice if me is not None else None

            if voice is None or voice.channel is None:
                return False

            if voice.channel.id != channel_id:
                return False

            if voice.self_deaf == desired_self_deaf:
                return True

            await asyncio.sleep(0.05)

        return False

    async def _restore_self_deaf(self, guild: discord.Guild) -> None:
        """Restore self-deaf without ever moving the bot back to an old channel."""
        me = guild.me
        voice = me.voice if me is not None else None

        if voice is None or voice.channel is None or voice.self_deaf:
            return

        try:
            await guild.change_voice_state(
                channel=voice.channel,
                self_mute=voice.self_mute,
                self_deaf=True,
            )
            await self._wait_for_self_deaf_state(
                guild,
                voice.channel.id,
                True,
                timeout=VOICE_STATE_TIMEOUT_SECONDS,
            )
        except Exception:
            log.exception("Failed to restore self-deaf in guild %s (%s)", guild.name, guild.id)

    async def _play_configured_sound(
        self,
        guild: discord.Guild,
        channel: discord.VoiceChannel,
    ) -> Tuple[bool, str]:
        """Play the configured guild sound, temporarily undeafening Red if necessary."""
        async with self._guild_lock(guild.id):
            me = guild.me
            voice = me.voice if me is not None else None

            # If Red moved/disconnected while this event was being handled, don't send
            # a sound into a channel it is no longer actually in.
            if voice is None or voice.channel is None:
                return False, "Red is no longer connected to voice."

            if voice.channel.id != channel.id:
                return False, "Red moved to a different voice channel before the sound could be played."

            sound, error = await self._get_configured_sound(guild)
            if sound is None:
                return False, error or "The configured Soundboard sound could not be resolved."

            permissions = channel.permissions_for(me)
            if not permissions.speak:
                return False, "Red does not have the Speak permission in this voice channel."
            if not permissions.use_soundboard:
                return False, "Red does not have the Use Soundboard permission in this voice channel."

            # Discord's Soundboard endpoint rejects server-deafened, server-muted,
            # self-deafened, and suppressed voice states. We only change self_deaf;
            # moderator/server states are left alone.
            if voice.deaf:
                return False, "Red is server-deafened and cannot use Soundboard."
            if voice.mute:
                return False, "Red is server-muted and cannot use Soundboard."
            if voice.suppress:
                return False, "Red is suppressed and cannot use Soundboard."

            was_self_deaf = voice.self_deaf
            temporarily_undeafened = False

            try:
                if was_self_deaf:
                    try:
                        await guild.change_voice_state(
                            channel=channel,
                            self_mute=voice.self_mute,
                            self_deaf=False,
                        )
                    except Exception as exc:
                        return False, f"Could not temporarily self-undeafen Red: {exc}"

                    temporarily_undeafened = True

                    if not await self._wait_for_self_deaf_state(guild, channel.id, False):
                        return False, "Discord did not confirm that Red became undeafened in time."

                # Refresh voice state after the change and make sure the bot did not move.
                me = guild.me
                voice = me.voice if me is not None else None
                if voice is None or voice.channel is None or voice.channel.id != channel.id:
                    return False, "Red changed voice channels before the Soundboard request was sent."

                if voice.deaf or voice.self_deaf or voice.mute or voice.suppress:
                    return False, "Red's voice state still blocks Soundboard playback."

                try:
                    await channel.send_sound(sound)
                except discord.Forbidden:
                    return False, "Discord denied the Soundboard request. Check Speak and Use Soundboard permissions."
                except discord.HTTPException as exc:
                    return False, f"Discord rejected the Soundboard request: {exc}"

                return True, f"Played **{sound.name}** (`{sound.id}`)."

            finally:
                if was_self_deaf and temporarily_undeafened:
                    # Once Discord has accepted the native Soundboard event it no longer
                    # depends on Red streaming the clip, so we can restore Audio's normal
                    # self-deafened state shortly afterwards.
                    await asyncio.sleep(REDEAF_GRACE_SECONDS)
                    await self._restore_self_deaf(guild)

    # ---------------------------------------------------------------------
    # Commands
    # ---------------------------------------------------------------------

    @commands.group(
        name="agsvoicesoundboard",
        aliases=["agsvsb", "voicesoundboard"],
        invoke_without_command=True,
    )
    @commands.guild_only()
    async def agsvoicesoundboard(self, ctx: commands.Context):
        """Configure the Soundboard sound Red plays whenever it joins or moves voice channels."""
        await ctx.send_help(ctx.command)

    @agsvoicesoundboard.command(name="list", aliases=["sounds"])
    @commands.is_owner()
    @commands.guild_only()
    async def list_sounds(self, ctx: commands.Context):
        """List this server's Soundboard sounds and their IDs."""
        try:
            sounds = await ctx.guild.fetch_soundboard_sounds()
        except discord.HTTPException as exc:
            return await ctx.send(f"❌ I couldn't fetch this server's Soundboard sounds: {exc}")

        colour = await ctx.embed_colour()
        selected_id = await self.config.guild(ctx.guild).sound_id()
        selected_id = int(selected_id) if selected_id is not None else None

        sounds = sorted(sounds, key=lambda sound: (sound.name.casefold(), sound.id))

        if not sounds:
            embed = discord.Embed(
                title=f"{ctx.guild.name} Soundboard",
                description=(
                    "This server has no custom Soundboard sounds.\n\n"
                    "Add sounds in **Server Settings → Soundboard**, then run this command again."
                ),
                colour=colour,
            )
            return await ctx.send(embed=embed)

        selected_present = any(sound.id == selected_id for sound in sounds)
        total_pages = (len(sounds) + SOUNDS_PER_EMBED - 1) // SOUNDS_PER_EMBED

        for page_index in range(total_pages):
            start = page_index * SOUNDS_PER_EMBED
            page_sounds = sounds[start : start + SOUNDS_PER_EMBED]

            description = (
                "**✅ SELECTED** = the sound this cog will play whenever Red joins or moves voice channels.\n"
                f"Set a sound with `{ctx.clean_prefix}agsvoicesoundboard set <sound ID>`."
            )

            if selected_id is None:
                description += "\n\n⚠️ **No sound is currently selected.**"
            elif not selected_present:
                description += (
                    f"\n\n⚠️ Configured sound ID `{selected_id}` is not currently present in this server's Soundboard."
                )

            embed = discord.Embed(
                title=f"{ctx.guild.name} Soundboard",
                description=description,
                colour=colour,
            )

            for sound in page_sounds:
                is_selected = sound.id == selected_id
                marker = "✅ SELECTED" if is_selected else "▫️"
                availability = "Available" if getattr(sound, "available", True) else "Unavailable"
                safe_name = discord.utils.escape_markdown(sound.name)

                embed.add_field(
                    name=f"{marker} • {safe_name}",
                    value=(
                        f"**ID:** `{sound.id}`\n"
                        f"**Status:** {availability}\n"
                        f"**Set:** `{ctx.clean_prefix}agsvoicesoundboard set {sound.id}`"
                    ),
                    inline=False,
                )

            embed.set_footer(text=f"Page {page_index + 1}/{total_pages} • {len(sounds)} guild sound(s)")
            await ctx.send(embed=embed)

    @agsvoicesoundboard.command(name="set")
    @commands.is_owner()
    @commands.guild_only()
    async def set_sound(self, ctx: commands.Context, sound_id: int):
        """Set the guild Soundboard sound by ID."""
        if sound_id <= 0:
            return await ctx.send("❌ Sound ID must be a positive Discord snowflake ID.")

        try:
            sound = await ctx.guild.fetch_soundboard_sound(sound_id)
        except discord.NotFound:
            return await ctx.send(
                "❌ That Soundboard sound ID does not exist in this server. "
                f"Run `{ctx.clean_prefix}agsvoicesoundboard list` to see valid IDs."
            )
        except discord.HTTPException as exc:
            return await ctx.send(f"❌ I couldn't retrieve that Soundboard sound: {exc}")

        if not getattr(sound, "available", True):
            return await ctx.send(
                f"❌ **{sound.name}** (`{sound.id}`) exists, but Discord currently marks it unavailable."
            )

        await self.config.guild(ctx.guild).sound_id.set(sound.id)

        embed = discord.Embed(
            title="Voice join Soundboard sound set",
            description=f"✅ **{discord.utils.escape_markdown(sound.name)}** is now selected.",
            colour=await ctx.embed_colour(),
        )
        embed.add_field(name="Sound ID", value=f"`{sound.id}`", inline=False)
        embed.add_field(
            name="Trigger",
            value="It will play whenever Red joins a voice channel **or moves from one voice channel to another**.",
            inline=False,
        )
        await ctx.send(embed=embed)

    @agsvoicesoundboard.command(name="clear")
    @commands.is_owner()
    @commands.guild_only()
    async def clear_sound(self, ctx: commands.Context):
        """Clear the configured Soundboard sound."""
        await self.config.guild(ctx.guild).sound_id.set(None)
        await ctx.send("✅ Cleared the configured voice join Soundboard sound.")

    @agsvoicesoundboard.command(name="settings", aliases=["show"])
    @commands.is_owner()
    @commands.guild_only()
    async def show_settings(self, ctx: commands.Context):
        """Show the current Soundboard configuration and Red's voice state."""
        sound_id = await self.config.guild(ctx.guild).sound_id()
        sound = None
        sound_error = None

        if sound_id is not None:
            sound, sound_error = await self._get_configured_sound(ctx.guild)

        me = ctx.guild.me
        voice = me.voice if me is not None else None

        if sound is not None:
            sound_text = f"**{discord.utils.escape_markdown(sound.name)}**\n`{sound.id}`"
        elif sound_id is not None:
            sound_text = f"`{sound_id}`\n⚠️ {sound_error or 'Could not resolve this sound.'}"
        else:
            sound_text = "Not configured"

        if voice is None or voice.channel is None:
            voice_text = "Not connected"
        else:
            voice_text = (
                f"{voice.channel.mention}\n"
                f"Self-deafened: **{'Yes' if voice.self_deaf else 'No'}**\n"
                f"Server-deafened: **{'Yes' if voice.deaf else 'No'}**\n"
                f"Server-muted: **{'Yes' if voice.mute else 'No'}**"
            )

        embed = discord.Embed(
            title="AGS Voice Soundboard Settings",
            colour=await ctx.embed_colour(),
        )
        embed.add_field(name="Selected sound", value=sound_text, inline=False)
        embed.add_field(name="Current voice state", value=voice_text, inline=False)
        embed.add_field(
            name="Behaviour",
            value=(
                "On every voice **join or move**, the cog temporarily self-undeafens Red if needed, "
                "triggers the native guild Soundboard sound, then restores self-deaf."
            ),
            inline=False,
        )
        await ctx.send(embed=embed)

    @agsvoicesoundboard.command(name="test")
    @commands.is_owner()
    @commands.guild_only()
    async def test_sound(self, ctx: commands.Context):
        """Play the configured sound in Red's current voice channel."""
        me = ctx.guild.me
        voice = me.voice if me is not None else None

        if voice is None or voice.channel is None:
            return await ctx.send("❌ Red is not currently connected to a voice channel.")

        if not isinstance(voice.channel, discord.VoiceChannel):
            return await ctx.send("❌ Red is not currently in a normal voice channel.")

        ok, message = await self._play_configured_sound(ctx.guild, voice.channel)

        embed = discord.Embed(
            title="Soundboard test passed" if ok else "Soundboard test failed",
            description=("✅ " if ok else "❌ ") + message,
            colour=await ctx.embed_colour(),
        )
        await ctx.send(embed=embed)
