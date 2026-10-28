"""AUD-010 Daily Dad Jokes posting and retry semantics.

Executes the actual production scheduler/prefix methods from the source AST
with stdlib-only Red/Discord fakes. No network or running bot is required.
"""
import ast
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "agsdailydadjoke" / "agsdailydadjoke.py"
MODULE = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
COG = next(n for n in MODULE.body if isinstance(n, ast.ClassDef) and n.name == "DailyDadJokes")
TOP_LEVEL = {"compute_last_daily", "choose_next_prefix"}
METHODS = {"_handle_guild", "_get_next_prefix"}
selected = [
    n for n in MODULE.body if isinstance(n, ast.FunctionDef) and n.name in TOP_LEVEL
] + [
    n for n in COG.body if isinstance(n, ast.AsyncFunctionDef) and n.name in METHODS
]
assert len(selected) == 4
source = ast.fix_missing_locations(ast.Module(body=[
    ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
    *selected,
], type_ignores=[]))


class Forbidden(Exception):
    pass


NS = {
    "asyncio": asyncio, "datetime": datetime, "timedelta": timedelta,
    "timezone": timezone, "ZoneInfo": ZoneInfo, "random": random,
    "DEFAULT_PRE_MESSAGES": ["First prefix", "Second prefix"],
    "FAILED_SEND_RETRY_SECONDS": 60,
    "discord": SimpleNamespace(Forbidden=Forbidden),
}
exec(compile(source, str(SOURCE), "exec"), NS)
choose_next_prefix = NS["choose_next_prefix"]
handle_guild = NS["_handle_guild"]
get_next_prefix = NS["_get_next_prefix"]
NOW = datetime(2026, 10, 28, 12, 1, tzinfo=timezone.utc)
POST_TS = int(datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc).timestamp())


class FakeSettings:
    def __init__(self, **updates):
        self.values = {
            "enabled": True, "channel_id": 42, "timezone": "UTC",
            "post_hour": 12, "post_minute": 0, "last_posted_unix": 0,
            "pre_messages": ["First prefix", "Second prefix"],
            "prefix_order": [0, 1], "prefix_index": 0,
        }
        self.values.update(updates)
        self.all = AsyncMock(side_effect=self.read)
        self.last_posted_unix = SimpleNamespace(set=AsyncMock(side_effect=self._set_marker))
        self.prefix_order = SimpleNamespace(set=AsyncMock(side_effect=self._set_order))
        self.prefix_index = SimpleNamespace(set=AsyncMock(side_effect=self._set_index))

    async def read(self):
        return dict(self.values)

    async def _set_marker(self, value):
        self.values["last_posted_unix"] = value

    async def _set_order(self, value):
        self.values["prefix_order"] = value

    async def _set_index(self, value):
        self.values["prefix_index"] = value


class FakeChannel:
    def __init__(self, errors=None):
        self.errors = list(errors or [])
        self.sent = []

    async def send(self, message):
        if self.errors:
            error = self.errors.pop(0)
            if error is not None:
                raise error
        self.sent.append(message)
        return object()


class ScheduledPostingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = FakeSettings()
        self.channel = FakeChannel()
        self.guild = SimpleNamespace(id=987, get_channel=lambda id: self.channel if id == 42 else None)
        self.logger = Mock()
        self.cog = SimpleNamespace(
            config=SimpleNamespace(guild=Mock(return_value=self.settings)),
            _lock=asyncio.Lock(),
            _failed_post_retry_at={},
            bot=SimpleNamespace(logger=self.logger),
            _fetch_joke=AsyncMock(return_value="Test joke"),
        )

    async def run_check(self, now=NOW):
        await handle_guild(self.cog, self.guild, now)

    async def test_success_commits_timestamp_and_prefix_only_once(self):
        await self.run_check()
        self.assertEqual(self.channel.sent, ["First prefix\n`Test joke`"])
        self.assertEqual(self.settings.values["last_posted_unix"], POST_TS)
        self.assertEqual(self.settings.values["prefix_index"], 1)
        self.settings.last_posted_unix.set.assert_awaited_once_with(POST_TS)
        await self.run_check(NOW + timedelta(seconds=30))
        self.assertEqual(len(self.channel.sent), 1)
        self.cog._fetch_joke.assert_awaited_once()

    async def test_forbidden_send_not_recorded_and_retry_is_throttled(self):
        self.channel.errors = [Forbidden("missing permissions")]
        await self.run_check()
        self.assertEqual(self.settings.values["last_posted_unix"], 0)
        self.assertEqual(self.settings.values["prefix_index"], 0)
        self.settings.last_posted_unix.set.assert_not_awaited()
        self.settings.prefix_index.set.assert_not_awaited()
        self.assertEqual(
            self.cog._failed_post_retry_at[self.guild.id], NOW + timedelta(seconds=60)
        )
        await self.run_check(NOW + timedelta(seconds=30))
        self.assertEqual(self.cog._fetch_joke.await_count, 1)
        await self.run_check(NOW + timedelta(seconds=60))
        self.assertEqual(self.channel.sent, ["First prefix\n`Test joke`"])
        self.assertEqual(self.settings.values["last_posted_unix"], POST_TS)
        self.assertEqual(self.settings.values["prefix_index"], 1)
        self.assertNotIn(self.guild.id, self.cog._failed_post_retry_at)
        self.logger.warning.assert_called_once()

    async def test_generic_send_failure_logs_and_can_retry(self):
        self.channel.errors = [RuntimeError("discord unavailable")]
        await self.run_check()
        self.logger.exception.assert_called_once()
        self.assertEqual(self.settings.values["last_posted_unix"], 0)
        await self.run_check(NOW + timedelta(seconds=60))
        self.assertEqual(len(self.channel.sent), 1)
        self.settings.last_posted_unix.set.assert_awaited_once_with(POST_TS)

    async def test_retries_stop_after_five_minute_window(self):
        self.channel.errors = [Forbidden("blocked")]
        await self.run_check(NOW + timedelta(minutes=4, seconds=45))
        await self.run_check(NOW + timedelta(minutes=5, seconds=45))
        self.cog._fetch_joke.assert_awaited_once()
        self.assertEqual(self.settings.values["last_posted_unix"], 0)

    async def test_no_joke_leaves_timestamp_and_prefix_unmodified(self):
        self.cog._fetch_joke.return_value = None
        await self.run_check()
        self.settings.last_posted_unix.set.assert_not_awaited()
        self.settings.prefix_index.set.assert_not_awaited()
        self.assertEqual(self.channel.sent, [])

    async def test_disabled_or_missing_channel_not_posted(self):
        self.settings.values["enabled"] = False
        await self.run_check()
        self.settings.values["enabled"] = True
        self.settings.values["channel_id"] = None
        await self.run_check()
        self.cog._fetch_joke.assert_not_awaited()

    async def test_failed_prefix_persistence_after_send_does_not_duplicate(self):
        self.settings.prefix_index.set.side_effect = RuntimeError("storage failure")
        with self.assertRaisesRegex(RuntimeError, "storage failure"):
            await self.run_check()
        self.assertEqual(len(self.channel.sent), 1)
        self.assertEqual(self.settings.values["last_posted_unix"], POST_TS)
        await self.run_check(NOW + timedelta(seconds=30))
        self.assertEqual(len(self.channel.sent), 1)

    async def test_repeated_concurrent_checks_send_only_once(self):
        await asyncio.gather(self.run_check(), self.run_check())
        self.assertEqual(len(self.channel.sent), 1)
        self.settings.last_posted_unix.set.assert_awaited_once()

    async def test_manual_prefix_rotation_still_advances(self):
        first = await get_next_prefix(self.cog, self.guild, await self.settings.read())
        second = await get_next_prefix(self.cog, self.guild, await self.settings.read())
        self.assertEqual((first, second), ("First prefix", "Second prefix"))
        self.assertEqual(self.settings.values["prefix_index"], 2)


class PrefixSelectionTests(unittest.TestCase):
    def test_does_not_mutate_caller_state(self):
        data = {"pre_messages": ["A", "B"], "prefix_order": [0, 1], "prefix_index": 0}
        result = choose_next_prefix(data)
        self.assertEqual(result, ("A", [0, 1], 1))
        self.assertEqual(data["prefix_index"], 0)

    def test_no_prefixes_and_empty_prefix(self):
        self.assertEqual(
            choose_next_prefix({"pre_messages": []}), ("", [], 0)
        )
        self.assertEqual(
            choose_next_prefix({"pre_messages": [""], "prefix_order": [0], "prefix_index": 0}),
            ("", [0], 1),
        )


if __name__ == "__main__":
    unittest.main()
