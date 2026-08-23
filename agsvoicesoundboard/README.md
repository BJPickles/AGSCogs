# AGSVoiceSoundboard

A Red Discord Bot cog for AEGIS Game Studios that plays a configured **native Discord Soundboard sound** whenever Red itself joins a voice channel or moves from one voice channel to another.

This cog **does not connect Red to voice**. Red's built-in Audio cog (or another voice system) remains responsible for joining, moving, and leaving voice channels.

## Behaviour

The Soundboard sound is triggered when Red's own voice channel changes to a normal voice channel:

- Disconnected → Voice A: plays
- Voice A → Voice B: plays
- Voice B → Voice C: plays
- Voice C → disconnected: does not play
- Self mute/deafen changes in the same channel: does not play

If Red is self-deafened (for example by Audio's autodeafen setting), the cog temporarily self-undeafens Red, sends the native Discord Soundboard event, then restores self-deaf.

The cog never attempts to override server/moderator deafening, muting, or Stage suppression.

## Setup

List all custom Soundboard sounds in the current server:

`[p]agsvoicesoundboard list`

The list includes every sound's Discord ID and marks the currently configured sound with **✅ SELECTED**.

Select a sound using its ID:

`[p]agsvoicesoundboard set <sound ID>`

Example:

`[p]agsvoicesoundboard set 123456789012345678`

## Commands

- `[p]agsvoicesoundboard list` — list guild sounds, IDs, availability, and the selected sound
- `[p]agsvoicesoundboard set <sound ID>` — select the sound to play
- `[p]agsvoicesoundboard clear` — clear the configured sound
- `[p]agsvoicesoundboard settings` — show the configured sound and current Red voice state
- `[p]agsvoicesoundboard test` — play the configured sound in Red's current voice channel

Aliases for the command group: `[p]agsvsb` and `[p]voicesoundboard`.

Configuration-changing/testing commands are bot-owner-only.

## Required Discord permissions

In the destination voice channel Red needs:

- Connect (normally already required by Audio)
- Speak
- Use Soundboard

The configured sound is always fetched from the current guild, so **Use External Sounds** is not required.

Discord will reject Soundboard playback while Red is server-deafened, self-deafened, server-muted, or suppressed. This cog handles only the self-deafened case automatically.

## Notes

- The configured value is the Discord Soundboard sound ID, not a filename or URL.
- If the configured sound is deleted or becomes unavailable, automatic playback fails safely and writes a warning to Red's log.
- The cog does not interfere with Lavalink playback or manage Red Audio's queue/player state.
