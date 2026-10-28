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

### 2026-10-28 — Baseline audit documentation PR (initial entry)

- **Scope:** All 11 cogs and repository delivery practices, at pinned baseline commit f2d04f12bc69.
- **Findings:** AUD-001 through AUD-017 opened; none resolved by this documentation PR.
- **Risk impact:** No runtime cog code changed. Adds a repository policy, PR checklist, and workflow check requiring future updates to this historical log.
- **Validation:** Static source inspection, metadata/tree review, and source-linked finding descriptions. No Red/Discord runtime or automated tests were executed.
- **Outstanding:** Every finding remains open until independently tested and addressed in a follow-up PR. The rolling log must be updated again in that PR.

<!-- APPEND NEW DATED PR AUDIT ENTRIES BELOW; NEVER DELETE OR REPLACE THE INITIAL BASELINE. -->
