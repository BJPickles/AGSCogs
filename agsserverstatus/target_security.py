"""Validation for outbound Server Status health checks.

This module intentionally uses only the standard library so its security policy
can be tested without loading Red or making network connections.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Iterable

_HOST_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")


def normalize_host(value: str) -> str:
    """Return a safe IP literal or ASCII DNS hostname, never a URL.

    Reject URL components and IPv6 zone identifiers. This also validates
    targets already in Red's persisted configuration, not just new commands.
    """
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("Host must be a non-empty IP address or hostname.")
    if any(c in value for c in "/\\@?#%") or any(c.isspace() for c in value):
        raise ValueError("Specify a host only, without a URL, credentials, or path.")

    host = value
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    elif "[" in host or "]" in host:
        raise ValueError("Invalid bracketed IP address.")

    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass

    if ":" in host or len(host) > 253:
        raise ValueError("Invalid hostname or IP address.")
    host = host.rstrip(".")
    if not host or any(not _HOST_LABEL.fullmatch(label) for label in host.split(".")):
        raise ValueError("Invalid hostname or IP address.")
    # Prevent ambiguous numeric hosts being reinterpreted by getaddrinfo.
    if all(c in "0123456789." for c in host):
        raise ValueError("Invalid IP address.")
    return host.lower()


def validate_port(port: int) -> int:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("Port must be between 1 and 65535.")
    return port


def select_address(addresses: Iterable[str], *, allow_private: bool = False) -> str:
    """Validate *every* resolved answer, then pick a pinned connection address.

    A hostname that resolves to both public and private addresses is denied by
    default. The caller must connect to the selected IP (not resolve the name
    again) to avoid a DNS-rebinding gap between validation and connection.
    """
    result = set()
    for raw in addresses:
        if "%" in raw:
            # Scoped IPv6 literals must not reach URL construction.
            raise ValueError("Scoped DNS addresses are not allowed.")
        try:
            addr = ipaddress.ip_address(raw)
        except ValueError as exc:
            raise ValueError("DNS returned an invalid IP address.") from exc
        if addr.is_unspecified or addr.is_multicast:
            raise ValueError("Unspecified and multicast destinations are not allowed.")
        if not addr.is_global and not allow_private:
            raise ValueError(
                "Private, local, link-local and other non-public destinations are blocked. "
                "Only the bot owner can explicitly opt in for a specific server."
            )
        result.add(addr)

    if not result:
        raise ValueError("The hostname resolved to no addresses.")

    # Prefer IPv4 for compatibility; deterministic across DNS answer ordering.
    return str(sorted(result, key=lambda addr: (addr.version, int(addr)))[0])


def health_request_target(host: str, address: str, port: int) -> tuple[str, str]:
    """Build a fixed health endpoint using the validated, pinned IP address."""
    host = normalize_host(host)
    port = validate_port(port)
    addr = ipaddress.ip_address(address)
    url_host = f"[{addr}]" if addr.version == 6 else str(addr)
    header_host = f"[{host}]" if ":" in host else host
    return f"http://{url_host}:{port}/api/health", f"{header_host}:{port}"
