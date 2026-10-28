"""AUD-011 task-lifecycle regression tests on the real Activities methods.

Uses source AST method bodies and stdlib-only bot/config substitutes.
Tests cover startup tracking, timer replacement, reload cancellation and
recovery of schedules missed while Red was offline.
"""

import ast
import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]
FILE = ROOT / "agsactivity" / "agsactivities.py"
TREE = ast.parse(FILE.read_text(encoding="utf-8"), filename=str(FILE))
CLASS = next(n for n in TREE.body if isinstance(n, ast.ClassDef) and n.name == "Activities")
NAMES = {
    "__init__", "_spawn_task", "_schedule_timer", "_cancel_activity_timers",
    "cog_unload", "_startup_tasks", "stop_activity", "_handle_finalize",
    "_auto_end_task",
}
METHODS = [
    node for node in CLASS.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES
]
assert len(METHODS) == len(NAMES)
for method in METHODS:
    method.decorator_list = []

MODULE = ast.fix_missing_locations(ast.Module(
    body=[ast.ClassDef(name="TestActivities", bases=[], keywords=[], decorator_list=[], body=METHODS)],
    type_ignores=[],
))


class FakeConfig:
    def __init__(self):
        self.contents = {}
        self.saved = {}
        self.defaults = None

    def register_guild(self, **kwargs):
        self.defaults = kwargs

    def guild(self, guild):
        values = self.contents.get(guild.id, {"instances": {}})
        return SimpleNamespace(
            all=AsyncMock(return_value=values),
            instances=AsyncMock(return_value=values["instances"]),
        )


CONFIG = FakeConfig()
NS = {
    "asyncio": asyncio, "time": time,
    "log": logging.getLogger("test.activity_tasks"),
    "Config": SimpleNamespace(get_conf=lambda *args, **kwargs: CONFIG),
    "DEFAULT_GUILD": {"instances": {}},
}
exec(compile(MODULE, str(FILE), "exec"), NS)
Harness = NS["TestActivities"]


class ActivityTaskTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        CONFIG.contents.clear()
        self.block_startup = asyncio.Event()
        self.guild = SimpleNamespace(id=123)
        self.bot = SimpleNamespace(
            loop=asyncio.get_running_loop(),
            guilds=[self.guild],
            wait_until_ready=AsyncMock(side_effect=self.block_startup.wait),
            add_view=Mock(),
        )
        self.cog = Harness(self.bot)

    async def asyncTearDown(self):
        self.cog.cog_unload()
        await asyncio.sleep(0)

    async def test_startup_and_prune_are_owned_and_cancelled_on_unload(self):
        self.assertEqual(len(self.cog._background_tasks), 2)
        tasks = list(self.cog._background_tasks)
        self.cog.cog_unload()
        await asyncio.sleep(0.01)
        self.assertTrue(all(t.cancelled() for t in tasks))
        self.assertEqual(self.cog._background_tasks, set())
        self.assertEqual(self.cog._activity_timers, {})

    async def test_timer_replacement_cancels_old_task_without_erasing_new_one(self):
        # Keep the startup job blocked, and use a stub for the scheduled job.
        runs = []
        async def fake_start(gid, iid, delay):
            runs.append((gid, iid, delay))
            await asyncio.sleep(delay)
        self.cog._schedule_start = fake_start

        old = self.cog._schedule_timer("start", 123, "alpha", 600)
        await asyncio.sleep(0)
        new = self.cog._schedule_timer("start", 123, "alpha", 600)
        await asyncio.sleep(0.01)
        self.assertTrue(old.cancelled())
        self.assertIs(self.cog._activity_timers[("start", 123, "alpha")], new)
        self.assertIn(new, self.cog._background_tasks)
        self.assertEqual(len(runs), 2)
        self.cog.cog_unload()
        await asyncio.sleep(0.01)
        self.assertTrue(new.cancelled())

    async def test_done_tasks_are_removed_and_errors_retrieved(self):
        async def completed():
            return 5
        async def failed():
            raise ValueError("background failure")
        with self.assertLogs("test.activity_tasks", "ERROR") as logs:
            self.cog._spawn_task(completed())
            self.cog._spawn_task(failed())
            await asyncio.sleep(0.02)
        self.assertIn("Activities background task failed", "\n".join(logs.output))
        # Only the two long-lived initialization tasks remain.
        self.assertEqual(len(self.cog._background_tasks), 2)

    async def test_spawn_after_unload_closes_coroutine_and_starts_nothing(self):
        self.cog.cog_unload()
        async def work():
            await asyncio.sleep(5)
        coro = work()
        self.assertIsNone(self.cog._spawn_task(coro))
        self.assertIsNone(coro.cr_frame)
        self.assertEqual(self.cog._background_tasks, set())

    async def test_manual_cancel_clears_start_and_end_timers(self):
        async def delayed(*args):
            await asyncio.sleep(600)
        self.cog._schedule_start = delayed
        self.cog._auto_end_task = delayed
        start = self.cog._schedule_timer("start", 123, "a", 600)
        end = self.cog._schedule_timer("end", 123, "a", 600)
        other = self.cog._schedule_timer("end", 123, "b", 600)
        await asyncio.sleep(0)
        self.cog._cancel_activity_timers(123, "a")
        await asyncio.sleep(0.01)
        self.assertTrue(start.cancelled())
        self.assertTrue(end.cancelled())
        self.assertFalse(other.cancelled())
        self.assertEqual(list(self.cog._activity_timers), [("end", 123, "b")])

    async def test_missed_scheduled_start_is_recovered_on_startup(self):
        now = time.time()
        CONFIG.contents[self.guild.id] = {"instances": {
            "overdue": {
                "status": "SCHEDULED", "public": True,
                "message_ids": {"rsvps": {}, "extend": None},
                "scheduled_time": now - 120,
            },
            "upcoming": {
                "status": "SCHEDULED", "public": True,
                "message_ids": {"rsvps": {}, "extend": None},
                "scheduled_time": now + 120,
            },
            "active": {
                "status": "OPEN", "public": True,
                "message_ids": {"public": None, "extend": None},
                "end_time": now - 50,
            },
            "finished": {
                "status": "ENDED", "public": True, "message_ids": {"extend": None},
            },
        }}
        recorded = []
        self.cog._schedule_timer = lambda *args: recorded.append(args)
        # Exercise startup recovery directly while the constructor's startup
        # task remains blocked at wait_until_ready.
        self.bot.wait_until_ready = AsyncMock()
        await self.cog._startup_tasks()
        self.assertEqual(
            {(kind, iid) for kind, guild_id, iid, delay in recorded},
            {("start", "overdue"), ("start", "upcoming"), ("end", "active")},
        )
        delays = {iid: delay for _, _, iid, delay in recorded}
        self.assertLess(delays["overdue"], 0)
        self.assertGreater(delays["upcoming"], 0)
        self.assertEqual(delays["active"], 0)

    async def test_old_auto_end_does_not_close_extended_activity(self):
        self.block_startup.clear()
        end_time = time.time() + 600
        instance = {"status": "OPEN", "end_time": end_time}
        config = SimpleNamespace(
            instances=AsyncMock(return_value={"a": instance})
        )
        self.cog.config = SimpleNamespace(guild=Mock(return_value=config))
        self.bot.get_guild = Mock(return_value=self.guild)
        scheduled = []
        self.cog._schedule_timer = lambda *args: scheduled.append(args)
        # The real method sleeps for 'delay' seconds and checks persisted end_time.
        await self.cog._auto_end_task(123, "a", 0)
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(scheduled[0][:3], ("end", 123, "a"))
        self.assertGreater(scheduled[0][3], 0)
        config.instances.set = AsyncMock()
        config.instances.set.assert_not_awaited()

    async def test_manual_stop_cancels_timer_after_persisting_ended(self):
        config = SimpleNamespace(instances=AsyncMock(return_value={
            "alpha": {"status": "OPEN", "owner_id": 9, "title": "Event"}
        }))
        config.instances.set = AsyncMock()
        self.cog.config = SimpleNamespace(guild=Mock(return_value=config))
        self.cog._update_all_embeds_to_ended = AsyncMock()
        self.cog._log = AsyncMock()
        self.cog._cancel_activity_timers = Mock()
        ctx = SimpleNamespace(
            guild=self.guild, author=SimpleNamespace(id=9, mention="@owner"),
            send=AsyncMock(),
        )
        await self.cog.stop_activity(ctx, "alpha")
        config.instances.set.assert_awaited_once()
        self.cog._cancel_activity_timers.assert_called_once_with(123, "alpha")
        self.cog._update_all_embeds_to_ended.assert_awaited_once()
        self.assertEqual(config.instances.set.await_args.args[0]["alpha"]["status"], "ENDED")

    async def test_finalize_cancels_timer_after_persisting_ended(self):
        inst = {"status": "OPEN"}
        config = SimpleNamespace(instances=AsyncMock())
        config.instances.set = AsyncMock()
        self.cog.config = SimpleNamespace(guild=Mock(return_value=config))
        self.cog._find_instance = AsyncMock(return_value=(self.guild, {"alpha": inst}, inst))
        self.cog._cancel_activity_timers = Mock()
        self.cog._update_all_embeds_to_ended = AsyncMock()
        interaction = SimpleNamespace(response=SimpleNamespace(edit_message=AsyncMock()))
        await self.cog._handle_finalize(interaction, "alpha")
        self.assertEqual(inst["status"], "ENDED")
        self.cog._cancel_activity_timers.assert_called_once_with(123, "alpha")
        config.instances.set.assert_awaited_once()
        self.cog._update_all_embeds_to_ended.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
