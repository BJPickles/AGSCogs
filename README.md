# AEGIS Game Studios — Aria Cogs

A collection of custom cogs for [Red-DiscordBot](https://github.com/Cog-Creators/Red-DiscordBot) 3.5, built for the AEGIS Game Studios Discord community. The cogs cover community activities, scheduled posts, voice chat, moderation, game server monitoring, and property alerts.

## Rolling cog audit — mandatory PR policy

The repository maintains an ongoing, historical [Cog Audit History](AUDIT_HISTORY.md) with dated findings, severity, evidence, and follow-up status for **all 11 cogs**.

**Every future pull request MUST update `AUDIT_HISTORY.md` in the same PR**, including documentation-only or housekeeping changes. Append a dated entry covering affected cogs, risks, finding IDs (or an explicit no-findings-change statement), and what was or was not tested. Preserve earlier records: fix PRs should link back to the original finding and record its resolution, not delete history.

The [PR template](.github/pull_request_template.md) requires a checklist and the [audit-log workflow](.github/workflows/require-audit-history.yml) fails when a PR omits a change to the log. Reviewers must check entry quality; to block merging on failure, maintainers must make that status check required in GitHub branch protection/rulesets.

## Available cogs

| Cog | What it does | Main command |
| --- | --- | --- |
| [AGS Activity](agsactivity/) | Creates and manages public, private, and scheduled activities or hangouts, with reusable templates, interactive join/leave buttons, RSVPs, DM invites, and activity tracking. | `[p]activity` |
| [AGS Daily Dad Joke](agsdailydadjoke/) | Fetches dad jokes on demand and can post one automatically every day, with a configurable channel, posting time, timezone, and rotating introductions. | `[p]dadjoke` |
| [AGS Housing Scraper](agshousingscraper/) | Watches a configurable social-housing webpage for new posts, matching keywords, or page changes, then sends alerts. Supports different detection modes, scheduled checks, and optional screenshots. | `[p]agshousingscraper` |
| [AGS On This Day](agsonthisday/) | Shares daily historical events and trivia, including themed facts and article links. Supports scheduled posts, timezone settings, and an optional role ping. | `[p]agsonthisday` |
| [AGS Profiles](agsprofiles/) | Saves periodic and year-end snapshots of LevelUp statistics so members can look back at profiles and historical leaderboards. Requires LevelUp data to produce snapshots. | `[p]agsnapshot`, `[p]agprofile`, `[p]agsleaderboard` |
| [AGS Role Kick](agsrolekick/) | Bot-owner-only moderation tool for kicking members who hold a specified role, with user/role blacklists, optional advance DMs, reports, and ModLog integration. | `[p]agsrolekick` |
| [AGS Server Status](agsserverstatus/) | Monitors configured game server realms and announces when they come online or go offline, with customizable status messages and a designated update channel. | `[p]serverstatus` |
| [AGS Voice Announce](agsvoiceannounce/) | Announces eligible AutoRoom voice sessions with configurable role pings, opt-out roles, channel exclusions, delays, and protection against repeat announcements. | `[p]agsvoiceannounce` |
| [AGS Voice Soundboard](agsvoicesoundboard/) | Plays a selected native Discord Soundboard sound whenever the bot joins or moves to a voice channel. It does **not** join voice channels itself; Red's Audio cog (or another voice system) handles that. | `[p]agsvoicesoundboard` |
| [Friday Game Night](fridaygamenight/) | Automatically posts weekly game-night reminders and start-time announcements, using configurable schedules, messages, timezones, and Discord timestamps. | `[p]gamenight` |
| [Rightmove](rightmove/) | Monitors Rightmove property searches for each server, with drawn-area searches, filters, pricing rules, scheduled refreshes, property embeds, and updates to tracked listings. | `[p]rm` |

## Installation

With Red's **Downloader** cog loaded, add this repository and install the cog you want:

```text
[p]repo add AGSCogs https://github.com/BJPickles/AGSCogs
[p]cog install AGSCogs <cog-folder>
[p]load <cog-folder>
```

Replace `<cog-folder>` with a folder name from the links above (for example, `agsvoicesoundboard`). Replace `[p]` with your bot's command prefix.

After loading a cog, use `[p]help <main-command>` to discover its available commands and configuration options.

## Notes

- These cogs target **Red 3.5.x**. Individual cogs may have additional requirements; check their `info.json` files. In particular, **AGS Voice Soundboard requires Red 3.5.17 or newer**.
- Some functionality depends on other bot features or cogs: **AGS Profiles** uses LevelUp statistics, **AGS Voice Announce** is designed around AutoRoom voice channels, and **AGS Voice Soundboard** reacts to voice joins/moves rather than initiating them.
- Configuration and moderation commands may require server administrator or bot-owner permissions. **AGS Role Kick** and **Rightmove** expose their primary management commands to the bot owner.
- For more detailed setup and behaviour, see the dedicated guides for [AGS Voice Announce](agsvoiceannounce/README.md) and [AGS Voice Soundboard](agsvoicesoundboard/README.md).
