# Rightmove property-channel deletion safety

The Rightmove cog maintains Discord property channels and may delete them after
a listing is missing from multiple **complete** scrapes. Deleting a Discord
channel is destructive and **cannot be undone by restoring the cog's cache**.

## Before enabling or changing monitoring

1. Run `[p]rm stop` to disable scheduled monitoring and cancel an active
   reconciliation. Wait until `[p]rm status` shows no run in progress.
2. Make a **verified backup of Red's entire data directory**, including
   its Config data, using your normal bot-host backup procedure. Stop the bot
   for a consistent offline snapshot if your backup system cannot take one
   safely while Red is running. Keep the backup outside the live data folder.
3. Inspect `[p]rm status` for the tracked count, last result and scrape
   completeness. Check Rightmove in a browser for expected search results,
   search URL and filters; upstream layout changes can create false absences.
4. Run `[p]rm missingpreview`. This command **only reads stored state**.
   It reports which records have enough existing missing confirmations that
   they *could* be removed if they are also absent on the next safe, complete
   scrape. It does **not** run a new scrape, inspect live listings, predict
   deletions with certainty, modify counters or delete anything.
5. If your property channels or search results look wrong, keep monitoring
   stopped and investigate. Consider temporarily removing the bot's
   **Manage Channels** permission while diagnosing the problem.

## Automatic deletion safeguards

- An incomplete scrape, failed request or incomplete property-card parse does
  not advance missing counters or delete absent property channels.
- A scraper result that claims completeness but contains **zero properties**
  while a guild still has cached properties is considered **unsafe for
  removals**. This also leaves the last-success marker unchanged, so the
  scheduler can retry. A genuinely empty search must be reviewed manually;
  this deliberately prioritises safety over automatic deletion of all records.
- A listing must be absent for the configured number of complete scrapes
  (`[p]rm setmissing <1-30>`; default: **3**) before its channel is removed.
- The cog checks that a cached channel's name or Rightmove topic marker
  identifies **the same property ID** before issuing `channel.delete()`.
  A mismatched cached channel ID is **not** deleted and its record remains in
  the cache for investigation. Already-missing channels can still have stale
  cache records pruned after confirmations.
- A failed Discord deletion retains the record for a future retry; each
  completed missing-record decision is persisted separately to reduce
  interruption recovery gaps.
- Manual destructive commands such as `[p]rm purgeover ... confirm` and
  `[p]rm cleanup` are separate operations, **not a part of the automatic
  missing-listing safety gate**. Read their previews and requirements carefully.

## Recovery and verification

If monitoring begins to remove unexpected listings, run `[p]rm stop`
immediately and preserve logs. Investigate the configured search URL, result
counts, parser warnings, and the last recorded summary before restarting.

If channels have already been deleted, restoring Red's configuration alone
**will not restore Discord channel history or content**. Recreate channels
manually as needed, using your backup and logs to identify lost mappings.
Do not restore old configuration over live data without a planned recovery
procedure.

For validation before deployment, use a test Discord server and fixture-based
tests in `tests/test_rightmove_reconciliation.py`. They verify partial/empty
scrapes, confirmation thresholds, missing and mismatched channels, failed
Discord deletions, interruption persistence and read-only preview behaviour.
They do **not** replace real Red/Discord testing, nor guarantee an external
Rightmove layout will remain unchanged.
