# AGSVoiceAnnounce

A Red Discord Bot cog for AEGIS Game Studios that announces qualifying AutoRoom voice sessions.

## What it does

- Announces once per qualifying public or invite-only voice room/session.
- Never announces private voice rooms, based on whether the configured ping role can view the voice channel.
- Ignores bots.
- Uses a configured opt-out role; users with that role never trigger announcements.
- Supports manual opt-in/opt-out commands as a backup to role-button systems.
- True-pings a configured role using controlled allowed mentions.
- Supports blacklisted voice channels.
- Supports a configurable delay before posting.
- Supports a rejoin grace period so bot-held rooms are not re-announced after accidental disconnects.
- Treats deleted AutoRoom channels as new announcement opportunities when recreated.
- Supports persistent random message cycling without immediate repeats across cycles.

## Setup

Load the cog, then configure:

```text
[p]agsvoiceannounce channel #announcements
[p]agsvoiceannounce pingrole @Voice Ping
[p]agsvoiceannounce optrole @Voice Announce Opt Out
```

Optional settings:

```text
[p]agsvoiceannounce delay 15
[p]agsvoiceannounce rejoingrace 300
[p]agsvoiceannounce blacklist add <voice channel>
[p]agsvoiceannounce message add {role} {user.mention} is in {channel.name}. Come join in!
[p]agsvoiceannounce settings
[p]agsvoiceannounce test
```

## User commands

```text
[p]agsvoiceannounce optout
[p]agsvoiceannounce optin
```

The bot owner may also use:

```text
[p]agsvoiceannounce optout <member or user id>
[p]agsvoiceannounce optin <member or user id>
[p]agsvoiceannounce status
```

## Placeholders

```text
{role}
{role.mention}
{user}
{user.name}
{user.display_name}
{user.mention}
{user.id}
{channel}
{channel.name}
{channel.mention}
{channel.id}
{guild}
{guild.name}
```

## Important permission notes

The bot needs to view/send in the announcement channel. To true-ping a role, either the role must be mentionable or the bot must have Mention Everyone permission in that channel. To use opt-in/out role commands, the bot needs Manage Roles and its top role must be above the configured opt-out role.
