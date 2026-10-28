"""AUD-008: tests for safe Housing Scraper polling intervals and legacy state.

Execute the actual helper and command/monitor methods from their source AST.
No Red, Discord, Selenium, browser or network installation is required.
"""

import ast
import asyncio
import datetime
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "agshousingscraper" / "agshousingscraper.py"
tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"), filename=str(SOURCE_PATH))
cog_class = next(
    n for n in tree.body if isinstance(n, ast.ClassDef)
    and n.name == "AGSHousingScraper"
)
helper = next(
    n for n in tree.body if isinstance(n, ast.FunctionDef)
    and n.name == "safe_poll_interval"
)
methods = [
    n for n in cog_class.body
    if isinstance(n, ast.AsyncFunctionDef) and n.name in {"_monitor_loop", "interval"}
]
assert len(methods) == 2

# Remove only Red's command decorator for the method harness, preserving the
# real production function bodies. This tests the command's behavior, not
# Discord permission checks, which belong to Red integration testing.
for method in methods:
    method.decorator_list = []
module = ast.fix_missing_locations(ast.Module(body=[helper, *methods], type_ignores=[]))
ns = {
    "asyncio": asyncio, "datetime": datetime, "log": Mock(),
    "DEFAULT_POLL_INTERVAL_SECONDS": 300,
    "MIN_POLL_INTERVAL_SECONDS": 60,
    "MAX_POLL_INTERVAL_SECONDS": 86400,
}
exec(compile(module, str(SOURCE_PATH), "exec"), ns)
safe_poll_interval = ns["safe_poll_interval"]
monitor_loop = ns["_monitor_loop"]
interval_command = ns["interval"]


class SafeIntervalTests(unittest.TestCase):
    def test_expected_boundaries_and_defaults(self):
        self.assertEqual(safe_poll_interval(60), 60)
        self.assertEqual(safe_poll_interval(300), 300)
        self.assertEqual(safe_poll_interval(86400), 86400)
        self.assertEqual(safe_poll_interval(0), 60)
        self.assertEqual(safe_poll_interval(-500), 60)
        self.assertEqual(safe_poll_interval(1), 60)
        self.assertEqual(safe_poll_interval(86401), 86400)
        self.assertEqual(safe_poll_interval(999999999), 86400)

    def test_malformed_legacy_values_use_safe_default(self):
        for bad in (None, False, True, "0", "60", 30.0, [], {}, object()):
            with self.subTest(value=bad):
                self.assertEqual(safe_poll_interval(bad), 300)


class MockSettings:
    def __init__(self, interval):
        self.url = AsyncMock(return_value="https://example.invalid")
        self.section_selector = AsyncMock(return_value="section")
        self.full_selector = AsyncMock(return_value="html")
        self.detection_modes = AsyncMock(return_value=[])
        self.keywords = AsyncMock(return_value=[])
        self.seen_posts = AsyncMock(return_value=[])
        self.seen_keywords = AsyncMock(return_value=[])
        self.poll_interval = AsyncMock(return_value=interval)
        self.poll_interval.set = AsyncMock()


class IntervalCommandTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = MockSettings(300)
        self.guild = SimpleNamespace(id=123)
        self.ctx = SimpleNamespace(guild=self.guild, send=AsyncMock(), tick=AsyncMock())
        self.monitor = Mock()
        self.loop = SimpleNamespace(create_task=Mock(return_value=object()))
        self.scraper = SimpleNamespace(
            config=SimpleNamespace(guild=Mock(return_value=self.settings)),
            monitor_tasks={123: self.monitor},
            bot=SimpleNamespace(loop=self.loop),
            _monitor_loop=Mock(return_value=object()),
        )

    async def test_invalid_commands_do_not_change_config_or_restart_tasks(self):
        for seconds in (-10, 0, 1, 59, 86401, 999999):
            with self.subTest(seconds=seconds):
                await interval_command(self.scraper, self.ctx, seconds)
        self.assertEqual(self.ctx.send.await_count, 6)
        self.settings.poll_interval.set.assert_not_awaited()
        self.ctx.tick.assert_not_awaited()
        self.monitor.cancel.assert_not_called()
        self.loop.create_task.assert_not_called()
        self.assertIs(self.scraper.monitor_tasks[123], self.monitor)

    async def test_valid_bounds_persist_and_restart_monitor(self):
        for seconds in (60, 300, 86400):
            with self.subTest(seconds=seconds):
                self.scraper.monitor_tasks[123] = self.monitor
                await interval_command(self.scraper, self.ctx, seconds)
                self.settings.poll_interval.set.assert_awaited_with(seconds)
                self.assertTrue(self.monitor.cancel.called)
                self.assertEqual(self.ctx.tick.await_count, len(self.settings.poll_interval.set.await_args_list))
        self.assertEqual(self.loop.create_task.call_count, 3)
        self.ctx.send.assert_not_awaited()

    async def test_valid_interval_does_not_start_stopped_monitor(self):
        self.scraper.monitor_tasks.clear()
        await interval_command(self.scraper, self.ctx, 300)
        self.settings.poll_interval.set.assert_awaited_once_with(300)
        self.loop.create_task.assert_not_called()
        self.ctx.tick.assert_awaited_once()


class StopAfterDelay(Exception):
    pass


class MonitorDelayTests(unittest.IsolatedAsyncioTestCase):
    async def run_one_monitor_iteration(self, stored):
        settings = MockSettings(stored)
        guild = SimpleNamespace(id=123)
        scraper = SimpleNamespace(
            config=SimpleNamespace(guild=Mock(return_value=settings)),
            bot=SimpleNamespace(wait_until_ready=AsyncMock()),
            check_site=AsyncMock(),
            _log_to_channel=AsyncMock(),
        )
        sleep = AsyncMock(side_effect=StopAfterDelay)
        with patch.object(asyncio, "sleep", sleep):
            with self.assertRaises(StopAfterDelay):
                await monitor_loop(scraper, guild)
        scraper.check_site.assert_awaited_once()
        scraper._log_to_channel.assert_awaited_once()
        return settings, sleep

    async def test_saved_zero_or_negative_never_triggers_zero_sleep(self):
        for value in (0, -1, -300):
            with self.subTest(saved=value):
                settings, sleep = await self.run_one_monitor_iteration(value)
                sleep.assert_awaited_once_with(60)
                settings.poll_interval.set.assert_awaited_once_with(60)

    async def test_saved_oversized_delay_is_bounded(self):
        settings, sleep = await self.run_one_monitor_iteration(999999)
        sleep.assert_awaited_once_with(86400)
        settings.poll_interval.set.assert_awaited_once_with(86400)

    async def test_saved_malformed_value_is_repaired_to_default(self):
        settings, sleep = await self.run_one_monitor_iteration("0")
        sleep.assert_awaited_once_with(300)
        settings.poll_interval.set.assert_awaited_once_with(300)

    async def test_valid_saved_interval_is_not_rewritten(self):
        for value in (60, 600, 86400):
            with self.subTest(saved=value):
                settings, sleep = await self.run_one_monitor_iteration(value)
                sleep.assert_awaited_once_with(value)
                settings.poll_interval.set.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
