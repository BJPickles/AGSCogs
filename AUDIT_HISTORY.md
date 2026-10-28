# Rolling Cog Audit History

> **MANDATORY FOR EVERY FUTURE PULL REQUEST:** Update this file in the *same PR*, including documentation-only, dependency, configuration, and maintenance PRs. Append a dated entry describing the cogs/files reviewed, findings addressed or introduced, risk impact, and validation performed. If no finding changes, explicitly record **"No new findings; existing findings unchanged"** with the reason. Do **not** remove earlier audit entries. This is a permanent, rolling historical audit, **not** a one-time report.
>
> The pull request template requires this declaration, and the repository workflow checks that this file was changed. To make the CI check a *merge requirement*, repository maintainers must require the **Require rolling audit log / audit-history-updated** status check in GitHub branch protection or rulesets.

## How to read and maintain this log

- **IDs are permanent.** Keep the initial observation and its source links intact. When a PR resolves a finding, append a dated resolution entry and update its status in the register; link the fixing PR. Do not silently rewrite or delete the history.
- **Severity:** High = credible security exposure, load failure, or major operational breakage; Medium = material reliability or maintenance problem; Low = smaller packaging or usability issue. Severity is a triage recommendation, not a production incident determination.
- **Evidence:** File links below are pinned to the audited base commit (f2d04f12bc69), so later edits cannot move the cited line numbers.
- **Validation labels:** *Source-confirmed* means the relevant condition was verified in repository code; *needs runtime verification* means the failure/impact has not been reproduced against a running Red bot.
- **PR entry format:** Date and PR link; scope; findings opened/changed/closed (or explicit no-change statement); security/data/compatibility impact; tests performed and tests omitted; outstanding follow-ups. For a fix, reference the original finding ID.

## Current finding dispositions (updated as PRs land)

The baseline observations below remain historically unchanged. This table records
the **latest** state; historical `Status: Open` labels in the original entries
describe the state at the time of the 2026-10-28 baseline review.

| ID | Current state | Follow-up |
| --- | --- | --- |
| **AUD-001** | **Fixed in source in [PR #3](https://github.com/BJPickles/AGSCogs/pull/3); pending live Red/Discord verification** | Bot-owner-only commands, per-target private access opt-in, validated and pinned destinations, redirect suppression, and regression tests. See dated entry below. |
| **AUD-002** | **Fixed in source in [PR #4](https://github.com/BJPickles/AGSCogs/pull/4); isolated CI passed; pending real Red/Discord verification** | All command groups are registered on a single inherited cog; no separate command-class instances. See dated entry below. |
| **AUD-003–AUD-017** | Open (unchanged) | AUD-005 (Python 3.9/3.10 `StrEnum` incompatibility) and AUD-006 (undeclared LevelUp dependency) may still prevent AGSProfiles from loading even after AUD-002. AUD-007 remains separately open. |

## 2026-10-28 — Baseline audit (all 11 cogs)

**Baseline:** [main @ f2d04f12bc69](https://github.com/BJPickles/AGSCogs/commit/f2d04f12bc69ac1e7be781caa75978eba167f3a6). **Method:** manual static review of repository tree, cog entry points, metadata, source commands, scheduled tasks, HTTP/Discord I/O, persistence, and destructive operations. **Not performed:** installing Red, running Discord integrations, live service calls, dynamic security testing, automated test execution, or independent vulnerability exploitation. Findings below are not a claim that every runtime issue has been found.

### Coverage by cog

| Cog | Baseline outcome / tracking |
| --- | --- |
| [agsactivity](agsactivity/) | Background-task cleanup and oversized list embeds: AUD-011, AUD-012. |
| [agsdailydadjoke](agsdailydadjoke/) | Failed sends advance the daily-post marker: AUD-010. |
| [agshousingscraper](agshousingscraper/) | HTTP timeout context, blocking Selenium, interval validation and attachment reuse: AUD-003, AUD-004, AUD-008, AUD-009. |
| [agsonthisday](agsonthisday/) | Missing installer metadata: AUD-016. Daily scheduling and parsing still need integration tests. |
| [agsprofiles](agsprofiles/) | Cog construction, Python compatibility and external dependencies: AUD-002, AUD-005, AUD-006. |
| [agsrolekick](agsrolekick/) | No separate code defect established in this pass; bot-owner gating, confirmation, blacklists and ModLog paths were reviewed. Test the destructive bulk-kick path before changes (AUD-014). |
| [agsserverstatus](agsserverstatus/) | Unrestricted mutations/probing and bot-wide configuration: AUD-001, AUD-007. |
| [agsvoiceannounce](agsvoiceannounce/) | Potential pending-task bookkeeping race: AUD-013; verify voice churn/reload scenarios. |
| [agsvoicesoundboard](agsvoicesoundboard/) | No discrete defect established in static review. Permissions and self-deafen restoration are guarded, but real Discord move/undeafen tests remain outstanding (AUD-014). |
| [fridaygamenight](fridaygamenight/) | Server-specific default role mention: AUD-017; scheduling needs DST/restart tests. |
| [rightmove](rightmove/) | Scraping is offloaded using asyncio.to_thread, and missing-listing deletion has completeness/consecutive-miss safeguards. Destructive reconciliation still needs regression tests (AUD-015). |

### High-priority findings

**AUD-001 — High — agsserverstatus: configuration mutations and HTTP probes have no authorization gate.** **Status: Open; source-confirmed; runtime impact untested.** The command group and subcommands (`add`, `remove`, `togglerealm`, `setchannel`, `setmessage`, `toggle`) have no administrator/owner checks. Any member with command access can alter monitoring, then cause the bot to request `http://<host>:<port>/api/health`. This creates a credible internal network-probing/SSRF risk as well as a moderation/configuration risk. **Action:** restrict all state-changing subcommands to appropriate trusted roles, enforce host/port allowlisting or safe network boundaries, and add negative-permission tests. Evidence: [command entry and add](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsserverstatus/agsserverstatus.py#L173-L195), [health-check URL](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsserverstatus/agsserverstatus.py#L82-L94), [toggle and channel mutation](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsserverstatus/agsserverstatus.py#L233-L304).

**AUD-002 — High — agsprofiles: cog construction and command wiring appear broken.** **Status: Open; source-confirmed construction mismatch; needs runtime verification.** `AGSProfiles.__init__` calls `SnapshotCommands(self)` and `LeaderboardCommands(self)`, although these command classes do not define a constructor accepting the parent instance. The commands are also not inherited/registered on the loaded `AGSProfiles` cog, and `IconCommands` is omitted. This is expected to fail on cog load or leave command groups unavailable. **Action:** adopt a tested command mixin/inheritance or explicit registration architecture; add a clean-bot load test and assert all command groups exist. Evidence: [construction](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsprofiles/main.py#L20-L41), [snapshot command class](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsprofiles/commands/snapshot.py#L26-L34), [command aggregate](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsprofiles/commands/__init__.py#L1-L16).

**AUD-003 — High — agshousingscraper: asynchronous timeout used as a synchronous context manager.** **Status: Open; source-confirmed syntax; needs runtime verification.** The request path uses `with async_timeout.timeout(30):` inside an async function. The modern `async_timeout.timeout` API is an *async* context manager, so the fetch is expected to fail instead of monitoring the page. **Action:** use `async with` or `aiohttp.ClientTimeout`, enforce response status handling, declare required packages, and test an actual fetch/timeout. Evidence: [HTTP fetch](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agshousingscraper/agshousingscraper.py#L149-L164).

**AUD-004 — High — agshousingscraper: Selenium blocks the event loop and lacks guaranteed browser cleanup.** **Status: Open; source-confirmed; performance impact untested.** `capture_screenshot` is declared async but performs synchronous `webdriver.Chrome`, `driver.get`, and `save_screenshot` operations directly. While Chrome waits, Red's event loop can stall; if a step raises before `driver.quit`, browser processes can be left running. **Action:** offload Selenium to `asyncio.to_thread` or an executor; use `try/finally` to quit Chrome and delete temp files; bound timeouts and concurrency. Evidence: [screenshot function](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agshousingscraper/agshousingscraper.py#L280-L298).

**AUD-005 — High — agsprofiles: advertised Python minimum conflicts with `enum.StrEnum`.** **Status: Open; source-confirmed.** `models.py` imports `StrEnum`, which was introduced in Python 3.11, while `info.json` declares Python 3.9. On Python 3.9/3.10, importing the module will fail. **Action:** use a compatible `str, Enum` definition or raise the supported Python minimum; exercise the advertised environment in CI. Evidence: [models import](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsprofiles/models.py#L1-L11), [metadata](agsprofiles/info.json).

### Medium-priority findings

**AUD-006 — Medium — agsprofiles: undeclared hard dependencies.** **Status: Open; source-confirmed.** `common.py` imports `levelup.common.formatter` and Pillow at module import time; `models.py` imports Pydantic, but `info.json` does not declare `requirements` or `required_cogs`. Loading without a compatible LevelUp package can therefore fail before any helpful command is available. **Action:** document/declare compatible dependencies, verify their public APIs, and handle a missing cog gracefully. Evidence: [imports](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsprofiles/common.py#L9-L15), [metadata](agsprofiles/info.json).

**AUD-007 — Medium — agsserverstatus: monitoring state is global rather than per guild.** **Status: Open; source-confirmed.** The cog registers `servers`, `status_channel`, messages and activation under `register_global`. Changing configuration in one guild affects the bot's shared monitor and channel. **Action:** decide whether this is intentionally bot-owner/global; if not, migrate to per-guild configuration and isolate commands/updates. Even if global is intended, AUD-001 must be fixed. Evidence: [global config](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsserverstatus/agsserverstatus.py#L24-L48).

**AUD-008 — Medium — agshousingscraper: no lower bound on polling interval.** **Status: Open; source-confirmed.** The admin `interval` command accepts zero or negative integers and passes them through to `asyncio.sleep` in the monitor loop, allowing a rapid fetch/log loop and avoidable load. **Action:** enforce a positive minimum interval with bounded retry/backoff. Evidence: [interval command](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agshousingscraper/agshousingscraper.py#L368-L382), [monitor loop](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agshousingscraper/agshousingscraper.py#L115-L145).

**AUD-009 — Medium — agshousingscraper: screenshot attachment reused across sends.** **Status: Open; needs runtime verification.** `dispatch_alert` creates a single `discord.File` and passes the same instance to channel send and user DM. Discord upload handlers may close/consume it on the first send; the DM failure is swallowed by a bare `except`. **Action:** create independent file objects per destination, close them safely, and report DM failures. Evidence: [dispatch](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agshousingscraper/agshousingscraper.py#L300-L319).

**AUD-010 — Medium — agsdailydadjoke: failed sends are marked as posted.** **Status: Open; source-confirmed.** `last_posted_unix` advances after handling `Forbidden` or other send exceptions, preventing a retry within the day's posting window. **Action:** set the marker only after a successful send and add a bounded retry/logging policy. Evidence: [post handler](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsdailydadjoke/agsdailydadjoke.py#L163-L186).

**AUD-011 — Medium — agsactivity: created background tasks are not managed on unload.** **Status: Open; source-confirmed.** Startup, monthly prune, scheduled starts and auto-end tasks are created without a retained lifecycle/cancellation set; cog reloads can leave older tasks alive and duplicate scheduled work. **Action:** track and cancel tasks in `cog_unload`; test repeated load/unload and restart recovery. Evidence: [startup scheduling](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsactivity/agsactivities.py#L276-L345), [auto-end tasks](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsactivity/agsactivities.py#L453-L455).

**AUD-012 — Medium — agsactivity: activity list can exceed Discord embed limits.** **Status: Open; source-confirmed.** `activity list` adds one embed field for every persisted activity without paging or limiting to Discord's 25 fields per embed. **Action:** paginate/cap fields and test long lists and large titles. Evidence: [list command](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsactivity/agsactivities.py#L783-L807).

**AUD-013 — Medium — agsvoiceannounce: pending-task map may lose a replacement task.** **Status: Open; concurrency risk; needs runtime verification.** When a new task replaces a cancelled task for a channel, the old task's unconditional `finally: pop(key)` can remove the newer task from the map. This can undermine later cancellation/tracking. **Action:** only pop when the map still points at the current task; add rapid leave/rejoin and unload tests. Evidence: [replace task](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsvoiceannounce/agsvoiceannounce.py#L192-L197), [unconditional cleanup](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsvoiceannounce/agsvoiceannounce.py#L237-L246).

**AUD-014 — Medium — repository-wide: no automated tests or CI were present at baseline.** **Status: Open; source-confirmed from repository tree.** There are no test directories or workflows to exercise Red loading, Discord permissions, scheduling/DST, bot reloads, external HTTP failure handling, or the mass-kick and voice-state paths. **Action:** introduce lightweight import/command tests first, then mocked service and Discord integration tests; prioritize AUD-001 to AUD-005 and destructive operations. Evidence: [baseline tree](https://github.com/BJPickles/AGSCogs/tree/f2d04f12bc69ac1e7be781caa75978eba167f3a6).

**AUD-015 — Medium — rightmove: destructive reconciliation lacks a regression safety net.** **Status: Open; test-coverage risk, not a confirmed deletion bug.** Code protects missing-listing deletes behind `scrape.complete` and repeated missing confirmations; however, it ultimately deletes Discord property channels, and no test harness covers partial results, upstream layout changes or interrupted runs. **Action:** build fixture-based scrape/reconciliation tests and mock channel deletion; add dry-run/backup guidance before production changes. Evidence: [complete-scrape checks and deletion](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/rightmove/rightmove.py#L2536-L2564).

### Low-priority findings

**AUD-016 — Low — agsonthisday: no `info.json` exists.** **Status: Open; source-confirmed.** Unlike the other 10 cog directories, this cog has no metadata file to declare Python/Red requirements, third-party dependencies, or end-user-data behaviour to Downloader. **Action:** add a metadata file matching actual source imports and supported runtime. Evidence: [cog directory](https://github.com/BJPickles/AGSCogs/tree/f2d04f12bc69ac1e7be781caa75978eba167f3a6/agsonthisday).

**AUD-017 — Low — fridaygamenight: default message embeds a fixed server role ID.** **Status: Open; source-confirmed.** The default template includes a guild-specific Discord role mention, which is confusing or ineffective when installed elsewhere. **Action:** make the ping role configurable or use an unopinionated default template. Evidence: [default message](https://github.com/BJPickles/AGSCogs/blob/f2d04f12bc69ac1e7be781caa75978eba167f3a6/fridaygamenight/fridaygamenight.py#L10-L19).

### Recommended fix order

1. **Security and loading:** AUD-001, AUD-002, AUD-005, then AUD-003.
2. **Operational stability:** AUD-004, AUD-007, AUD-008, AUD-010, AUD-011.
3. **Remaining correctness:** AUD-006, AUD-009, AUD-012, AUD-013, AUD-016, AUD-017.
4. **Regression protection:** AUD-014 and AUD-015 alongside the fixes above.

## Dated rolling updates

### 2026-10-28 — [PR #2](https://github.com/BJPickles/AGSCogs/pull/2): Baseline audit documentation (initial entry)

- **Scope:** All 11 cogs and repository delivery practices, at pinned baseline commit f2d04f12bc69.
- **Findings:** AUD-001 through AUD-017 opened; none resolved by this documentation PR.
- **Risk impact:** No runtime cog code changed. Adds a repository policy, PR checklist, and workflow check requiring future updates to this historical log.
- **Validation:** Static source inspection, metadata/tree review, and source-linked finding descriptions. No Red/Discord runtime or automated tests were executed.
- **Outstanding:** Every finding remains open until independently tested and addressed in a follow-up PR. The rolling log must be updated again in that PR.

### 2026-10-28 — [PR #3](https://github.com/BJPickles/AGSCogs/pull/3): AUD-001 Server Status security fix

- **Scope:** `agsserverstatus/agsserverstatus.py`, new `agsserverstatus/target_security.py`, `agsserverstatus/README.md`, `tests/test_serverstatus_security.py`, and test workflow. Original audited source: [AUD-001](#high-priority-findings).
- **Status:** AUD-001 fixed in source; await CI result and a real Red/Discord validation before claiming production verification. AUD-007 is **not** resolved: configuration remains global across guilds. Other findings are unchanged.
- **Security changes:** Bot-owner checks on the parent command group and each mutating command; default-deny private/non-global resolved destinations, with an explicit per-server `allow_private=True` owner opt-in; validate stored as well as new hosts and ports; reject URL injection, redirects and mixed private/public DNS answers; connect to the vetted IP rather than re-resolving the hostname.
- **Behaviour and migration:** Reading/listing monitoring settings is now also owner-only because targets may disclose infrastructure IPs. Existing stored public targets continue to work. Old private/LAN entries remain saved but cannot be polled until the bot owner re-adds each target with the final `True` argument. This opt-in is persisted per realm and visible in list/view. Re-add resets last-known status. Scheduled monitors are not automatically deleted or migrated.
- **Validation:** Added stdlib-only unit tests for permission decorator coverage, host and port validation, mixed DNS rejection, IPv4/IPv6 URL construction, direct-IP pinning, blocked legacy private targets, and disabled redirect following. Added a GitHub Actions runner for those tests. No live service, Red/Discord load, or production migration test was executed as part of this review; run the suite and stage before deployment.
- **Residual risks / next steps:** Bot owners can intentionally opt into private network probing; this power is explicitly trusted and should not be delegated. Consider isolating configuration by guild under AUD-007 and adding integration tests around loaded Red commands, DNS resolution and real Discord permissions. Review existing saved targets before re-enabling private ones.

### 2026-10-28 — [PR #4](https://github.com/BJPickles/AGSCogs/pull/4): AUD-002 AGSProfiles command registration fix

- **Scope:** `agsprofiles/main.py`, the `commands` aggregator and leaderboard/icons command mixins; new `tests/test_agsprofiles_registration.py` and its GitHub Actions workflow.
- **Finding status:** AUD-002 fixed in source; the isolated discord.py CI registration suite passed; real Red integration verification pending. The original source-linked finding remains intact. **No additional finding resolved:** AUD-005 and AUD-006 remain open and can still block module imports on an unsupported Python installation or without a compatible LevelUp package.
- **Fix details:** Replace invalid manual construction of `SnapshotCommands(self)` and `LeaderboardCommands(self)` with one `AGSProfiles(Commands, commands.Cog)` instance; convert the leaderboard and icon classes into plain command mixins; include the missing icon commands and retain the translator on the loaded cog. Preserve existing command names, aliases, permission decorators, data paths, task lifecycle and data files.
- **Compatibility and risk:** No intentional changes to saved snapshot data, the LevelUp adapter, or scheduling. Inherited commands become available only when the cog imports and Red loads; separate pre-existing import/dependency problems must still be addressed in follow-up fixes. Watch for duplicate commands on reload and ensure Red's command permission checks remain effective.
- **Validation:** Added a dependency-isolated regression suite that executes the repository's real class definitions using discord.py's actual Cog metaclass/decorators, constructs the cog, and checks top-level commands, nested command groups, aliases and listener registration. The isolated suite stubs Red-only permission decorators and external model imports, so it is **not** a full Red runtime or permissions integration test. The dedicated GitHub Actions Python 3.11/discord.py registration job passed ([run](https://github.com/BJPickles/AGSCogs/actions/runs/40746898565)); the separately existing Server Status and rolling-audit checks also passed on the same revision. No live bot or LevelUp integration run was performed.
- **Next steps:** Run CI; install and reload on a staging Red bot with compatible Python/LevelUp versions; verify `[p]help agsnapshot`, `[p]agsleaderboard current`, `[p]agprofile`, `[p]agsicons list`, and `[p]agsprofilestatus`. Follow up on AUD-005 and AUD-006.

<!-- APPEND NEW DATED PR AUDIT ENTRIES BELOW; NEVER DELETE OR REPLACE THE INITIAL BASELINE. -->
