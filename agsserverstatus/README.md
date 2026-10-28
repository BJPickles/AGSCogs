# AGS Server Status — secure monitoring

AGS Server Status monitors HTTP health endpoints at `/api/health`. Because its
configuration is **global to the Red bot**, **all `[p]serverstatus` commands
are bot-owner-only** (including viewing the stored server IPs and messages).
This prevents ordinary guild users from reconfiguring monitoring or using
the bot as an HTTP probe.

## Add a public server

```text
[p]serverstatus add PublicRealm status.example.org 5757
[p]serverstatus add AnotherRealm 8.8.8.8 5757
[p]serverstatus setchannel #server-status
[p]serverstatus list
```

The host must be an IP literal or hostname (not a URL); the port must be
between 1 and 65535. The server must answer HTTP **200** at the fixed
`/api/health` path. HTTP redirects are not followed. Hostnames are resolved
on each poll; all returned addresses are validated, and the connection uses
the **validated IP** directly to prevent DNS rebinding between checking and
connecting. The original host is retained in the HTTP Host header.

## Deliberate private/LAN monitoring (bot owner only)

Non-public destinations (such as `192.168.x.x`, `10.x.x.x`, loopback,
link-local, and DNS names resolving to those networks) are **blocked by
default**. When an owner intentionally monitors a private service, they may
pass the optional **`True`** flag when adding that *specific* realm:

```text
[p]serverstatus add LANRealm 192.168.1.2 5757 True
```

This opt-in is persisted **per server**, is displayed in `list` and `view`,
and is subject to the same host/port validation and redirect restrictions.
Only grant this to endpoints that you operate and trust. The bot owner
already has broad control over Red, so this explicit exception is a trusted
operator choice, not a permission delegated to Discord members.

**Existing installations:** previously saved realms remain configured, but
existing private/LAN targets (including hostnames that resolve privately)
will be blocked until the owner explicitly re-adds that realm with `True`.
Re-adding a realm resets its last-known status. Check `[p]serverstatus list`
after upgrading and review any pre-existing entries. Existing public
targets do not require changes.

## Additional safety notes

- All monitoring settings are shared across the bot, not isolated per guild
  (tracked separately as AUD-007 in the [audit history](../AUDIT_HISTORY.md)).
- HTTP probing uses no arbitrary URL paths, user-supplied query strings,
  redirects, or URL credentials.
- Network validation is applied on **every poll**, not just when adding a
  target; existing saved configuration cannot bypass the default policy.
- Regression checks can run without Red or Discord:
  `python -m unittest discover -s tests -v`.
