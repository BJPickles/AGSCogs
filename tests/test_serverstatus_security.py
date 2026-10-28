"""Dependency-free regression tests for AUD-001 (run with unittest).

The Red cog's two network methods are extracted from the source AST so their
real implementations can be tested with a fake HTTP session, without Red or
Discord running or making outbound requests.
"""

import ast
import asyncio
import importlib.util
import ipaddress
import logging
from pathlib import Path
import socket
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
SECURITY_PATH = ROOT / "agsserverstatus" / "target_security.py"
COG_PATH = ROOT / "agsserverstatus" / "agsserverstatus.py"

spec = importlib.util.spec_from_file_location("target_security", SECURITY_PATH)
security = importlib.util.module_from_spec(spec)
spec.loader.exec_module(security)

source_tree = ast.parse(COG_PATH.read_text(encoding="utf-8"))
cog_class = next(
    node for node in source_tree.body
    if isinstance(node, ast.ClassDef) and node.name == "AGSServerStatus"
)
network_methods = [
    node for node in cog_class.body
    if isinstance(node, ast.AsyncFunctionDef)
    and node.name in {"_resolve_address", "is_server_online"}
]
assert len(network_methods) == 2

isolation_tree = ast.fix_missing_locations(ast.Module(
    body=[ast.ClassDef(
        name="HealthProbeHarness", bases=[], keywords=[], decorator_list=[],
        body=network_methods,
    )],
    type_ignores=[],
))
ns = {
    "asyncio": asyncio,
    "socket": socket,
    "ipaddress": ipaddress,
    "normalize_host": security.normalize_host,
    "validate_port": security.validate_port,
    "select_address": security.select_address,
    "health_request_target": security.health_request_target,
    "logger": logging.getLogger("test-serverstatus"),
    "aiohttp": SimpleNamespace(ClientError=type("HTTPClientError", (Exception,), {})),
}
exec(compile(isolation_tree, str(COG_PATH), "exec"), ns)
Harness = ns["HealthProbeHarness"]


class FakeResponse:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeSession:
    def __init__(self, status=200):
        self.status = status
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.status)


class TargetSecurityTests(unittest.TestCase):
    def test_valid_hosts(self):
        self.assertEqual(security.normalize_host("EXAMPLE.COM"), "example.com")
        self.assertEqual(security.normalize_host("test.example.org."), "test.example.org")
        self.assertEqual(security.normalize_host("[2001:4860:4860::8888]"), "2001:4860:4860::8888")

    def test_rejects_url_injection(self):
        invalid = [
            "http://example.com", "example.com/path", "example.com@127.0.0.1",
            "example.com#fragment", "example.com?x=1", "example.com\\evil",
            "fe80::1%eth0", "[::1", "999.999.999.999", "example.com\r\nBad: 1",
            "a..b", "-bad.example", "", "   ",
        ]
        for item in invalid:
            with self.subTest(item=item), self.assertRaises(ValueError):
                security.normalize_host(item)

    def test_port_bounds(self):
        for value in [0, -1, 65536, True, "80"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                security.validate_port(value)
        self.assertEqual(security.validate_port(65535), 65535)

    def test_blocks_nonpublic_and_mixed_dns(self):
        for ip in ["127.0.0.1", "10.2.3.4", "192.168.1.2", "169.254.169.254", "::1", "fe80::1", "0.0.0.0", "224.0.0.1"]:
            with self.subTest(ip=ip), self.assertRaises(ValueError):
                security.select_address([ip])
        with self.assertRaises(ValueError):
            security.select_address(["8.8.8.8", "127.0.0.1"])
        with self.assertRaises(ValueError):
            security.select_address([])

    def test_opt_in_only_for_specific_private_targets(self):
        self.assertEqual(security.select_address(["192.168.1.2"], allow_private=True), "192.168.1.2")
        self.assertEqual(security.select_address(["8.8.8.8", "127.0.0.1"], allow_private=True), "8.8.8.8")
        with self.assertRaises(ValueError):
            security.select_address(["0.0.0.0"], allow_private=True)

    def test_pinned_ipv4_and_ipv6_health_request(self):
        self.assertEqual(
            security.health_request_target("example.com", "8.8.8.8", 1234),
            ("http://8.8.8.8:1234/api/health", "example.com:1234"),
        )
        self.assertEqual(
            security.health_request_target("::1", "::1", 80),
            ("http://[::1]:80/api/health", "[::1]:80"),
        )

    def test_owner_gates_all_mutating_commands(self):
        mutation_names = {
            "serverstatus", "add", "remove", "togglerealm", "setchannel",
            "setmessage", "setmessage_online", "setmessage_offline", "toggle", "reset",
        }
        for node in cog_class.body:
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name in mutation_names:
                decorators = [ast.unparse(d) for d in node.decorator_list]
                self.assertIn("commands.is_owner()", decorators, node.name)
                mutation_names.remove(node.name)
        self.assertFalse(mutation_names, f"Missing command paths: {mutation_names}")


class HealthRequestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.harness = Harness()
        self.harness.session = FakeSession()

    async def test_public_hostname_connects_to_pinned_ip_and_never_redirects(self):
        self.harness._resolve_address = AsyncMock(return_value="8.8.8.8")
        self.assertTrue(await self.harness.is_server_online("example.com", 8080))
        self.harness._resolve_address.assert_awaited_once_with("example.com", 8080, allow_private=False)
        self.assertEqual(
            self.harness.session.calls,
            [("http://8.8.8.8:8080/api/health", {
                "timeout": 5, "headers": {"Host": "example.com:8080"},
                "allow_redirects": False,
            })],
        )

    async def test_redirect_does_not_count_as_online(self):
        self.harness.session = FakeSession(status=302)
        self.harness._resolve_address = AsyncMock(return_value="8.8.8.8")
        self.assertFalse(await self.harness.is_server_online("example.com", 80))
        self.assertFalse(self.harness.session.calls[0][1]["allow_redirects"])

    async def test_saved_private_ip_blocked_without_opt_in(self):
        self.assertFalse(await self.harness.is_server_online("127.0.0.1", 8000))
        self.assertEqual(self.harness.session.calls, [])
        self.assertTrue(await self.harness.is_server_online("127.0.0.1", 8000, allow_private=True))
        self.assertEqual(self.harness.session.calls[0][0], "http://127.0.0.1:8000/api/health")

    async def test_dns_answer_mixing_private_public_is_rejected(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 80)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 80)),
        ]
        with patch.object(asyncio.get_running_loop(), "getaddrinfo", new=AsyncMock(return_value=answers)):
            with self.assertRaises(ValueError):
                await self.harness._resolve_address("example.com", 80)
        self.assertEqual(self.harness.session.calls, [])


if __name__ == "__main__":
    unittest.main()
