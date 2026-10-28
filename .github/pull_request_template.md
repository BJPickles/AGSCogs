## Summary

Describe the change and which cogs or repository files it affects.

## Rolling audit history — required for **every** PR

- [ ] I updated [AUDIT_HISTORY.md](../../AUDIT_HISTORY.md) in **this PR** with a dated entry, impacted cogs, risk assessment, validation, and finding IDs. If nothing changed, I explicitly recorded "No new findings; existing findings unchanged" and why.
- [ ] I preserved the historical audit entries; I did not erase old findings.
- [ ] I updated the status of any resolved finding and linked it to this PR in the dated audit entry.

**Audit entry / affected finding IDs:** <!-- AUD-001, or "no change" with reason -->

## Validation

Explain tests run (or why none were run), including permissions, data changes, restart/reload scenarios, and impact on Red/Discord, where relevant.

## Risk and follow-up

What could break? Any unresolved findings or actions after merge?

> **Policy:** A PR that does not update `AUDIT_HISTORY.md` is incomplete. The workflow checks for a file change; reviewers must also check that the entry is meaningful and accurate.
