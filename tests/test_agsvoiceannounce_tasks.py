"""AUD-013 regression tests for AGS Voice Announce pending-task ownership.

Exercise production voice-state, delayed-announcement, channel-deletion and
unload methods extracted from their source AST. No Red or Discord installation
and no network access are required.
"""

import ast
import asyncio
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock, Mock


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "agsvoiceannounce" / "agsvoiceannounce.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
COG = next(node for node in TREE.body if isinstance(node, ast.ClassDef)
           and node.name == "AGSVoiceAnnounce")

METHOD_NAMES = {
    "cog_unload",
    "on_voice_state_update",
    "on_guild_channel_delete",
    "_refresh_channel_after_departure",
    "_consider_voice_join",
    "_delayed_announce",
}
METHODS = [
    node for node in COG.body
    if isinstance(node, ast.AsyncFunctionDef) and node.name in METHOD_NAMES
]
assert len(METHODS) == len(METHOD_NAMES)
for method in METHODS:
    method.decorator_list = []

MODULE = ast.fix_missing_locations(ast.Module(
    body=[
        ast.ImportFrom(module="__future__",
                       names=[ast.alias(name="annotations")], level=0),
        ast.ClassDef(
            name="VoiceHarness", bases=[], keywords=[],
            decorator_list=[], body=METHODS,
        ),
    ],
    type_ignores=[],
))


class VoiceChannel:
    def __init__(self, channel_id, guild):
        self.id = channel_id
        self.guild = guild
        self.members = []


class TextChannel:
    def __init__(self):
        self.send = AsyncMock()


class Forbidden(Exception):
    pass


NAMESPACE = {
    "asyncio": asyncio,
    "time": time,
    "discord": SimpleNamespace(
        VoiceChannel=VoiceChannel,
        TextChannel=TextChannel,
        Forbidden=Forbidden,
        AllowedMentions=lambda **kwargs: kwargs,
    ),
    "log": Mock(),
}
exec(compile(MODULE, str(SOURCE), "exec"), NAMESPACE)
VoiceHarness = NAMESPACE["VoiceHarness"]


class VoiceTaskTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.text_channel = TextChannel()
        self.role = SimpleNamespace(id=72)
        self.channels = {}
        self.members = {}
        self.guild = SimpleNamespace(id=64)
        self.guild.get_channel = lambda cid: (
            self.text_channel if cid == 99 else self.channels.get(cid)
        )
        self.guild.get_role = lambda rid: self.role if rid == 72 else None
        self.guild.get_member = lambda mid: self.members.get(mid)
        self.settings = SimpleNamespace(
            all=AsyncMock(return_value={
                "announcement_channel_id": 99,
                "ping_role_id": 72,
            }),
            delay_seconds=AsyncMock(return_value=3600),
            rejoin_grace_seconds=AsyncMock(return_value=0),
        )
        self.cog = VoiceHarness()
        self.cog.bot = SimpleNamespace(
            loop=asyncio.get_running_loop(),
            get_guild=lambda gid: self.guild if gid == 64 else None,
        )
        self.cog.config = SimpleNamespace(
            guild=Mock(return_value=self.settings)
        )
        self.cog._sessions = defaultdict(dict)
        self.cog._pending_tasks = {}
        self.cog._lock = asyncio.Lock()
        self.cog._ready_task = None
        self.cog._human_members = lambda ch: [m for m in ch.members if not m.bot]
        self.cog._is_announcement_candidate = AsyncMock(
            return_value=(True, "ok")
        )
        self.cog._render_next_message = AsyncMock(
            return_value="Join us!"
        )
        self.cog._notify_owners = AsyncMock()
        self.addCleanup(self._cleanup)

    async def _cleanup(self):
        await self.cog.cog_unload()
        await asyncio.sleep(0)

    def new_channel(self, cid, member_id):
        channel = VoiceChannel(cid, self.guild)
        member = SimpleNamespace(
            id=member_id, bot=False, guild=self.guild,
            voice=SimpleNamespace(channel=channel),
        )
        channel.members.append(member)
        self.channels[cid] = channel
        self.members[member_id] = member
        return channel, member

    async def join(self, channel, member):
        member.voice = SimpleNamespace(channel=channel)
        if member not in channel.members:
            channel.members.append(member)
        await self.cog.on_voice_state_update(
            member, SimpleNamespace(channel=None),
            SimpleNamespace(channel=channel),
        )

    async def leave(self, channel, member):
        channel.members.remove(member)
        member.voice = None
        await self.cog.on_voice_state_update(
            member, SimpleNamespace(channel=channel),
            SimpleNamespace(channel=None),
        )

    async def wait_for_worker_start(self, count):
        # Let the actual coroutine reach its configured delay.
        for _ in range(10):
            if self.settings.delay_seconds.await_count >= count:
                return
            await asyncio.sleep(0)
        self.fail("delayed worker did not start")

    async def test_rapid_leave_rejoin_preserves_newer_task(self):
        channel, member = self.new_channel(111, 222)
        key = (64, 111)
        await self.join(channel, member)
        first = self.cog._pending_tasks[key]
        await self.wait_for_worker_start(1)

        await self.leave(channel, member)
        await self.join(channel, member)
        second = self.cog._pending_tasks[key]
        self.assertIsNot(first, second)
        self.assertFalse(second.done())

        # The first task runs its cancellation finally *after* the second
        # was inserted. It must not pop the second from the map.
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertIs(self.cog._pending_tasks.get(key), second)
        self.assertFalse(second.cancelled())
        self.text_channel.send.assert_not_awaited()

        await self.cog.cog_unload()
        with self.assertRaises(asyncio.CancelledError):
            await second
        self.assertNotIn(key, self.cog._pending_tasks)

    async def test_replacement_can_complete_and_cleans_only_its_own_entry(self):
        channel, member = self.new_channel(111, 222)
        key = (64, 111)
        # First announcement waits; replacement has zero delay.
        self.settings.delay_seconds.side_effect = [3600, 0]
        await self.join(channel, member)
        first = self.cog._pending_tasks[key]
        await self.wait_for_worker_start(1)

        await self.leave(channel, member)
        await self.join(channel, member)
        second = self.cog._pending_tasks[key]
        await asyncio.wait_for(second, timeout=1)
        await asyncio.gather(first, return_exceptions=True)

        self.text_channel.send.assert_awaited_once()
        self.assertEqual(
            self.text_channel.send.await_args.args[0], "Join us!"
        )
        self.assertTrue(self.cog._sessions[64][111]["announced"])
        self.assertNotIn(key, self.cog._pending_tasks)
        self.cog._notify_owners.assert_not_awaited()

    async def test_unload_cancels_pending_jobs_for_multiple_channels(self):
        first_channel, first_member = self.new_channel(111, 222)
        second_channel, second_member = self.new_channel(112, 223)
        await self.join(first_channel, first_member)
        await self.join(second_channel, second_member)
        await self.wait_for_worker_start(2)
        tasks = list(self.cog._pending_tasks.values())
        self.assertEqual(len(tasks), 2)
        await self.cog.cog_unload()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.assertTrue(all(task.cancelled() for task in tasks))
        self.assertEqual(self.cog._pending_tasks, {})
        self.text_channel.send.assert_not_awaited()

    async def test_channel_deletion_cancels_only_its_task(self):
        first_channel, first_member = self.new_channel(111, 222)
        second_channel, second_member = self.new_channel(112, 223)
        await self.join(first_channel, first_member)
        await self.join(second_channel, second_member)
        await self.wait_for_worker_start(2)
        first = self.cog._pending_tasks[(64, 111)]
        second = self.cog._pending_tasks[(64, 112)]
        await self.cog.on_guild_channel_delete(first_channel)
        await asyncio.gather(first, return_exceptions=True)
        self.assertTrue(first.cancelled())
        self.assertNotIn((64, 111), self.cog._pending_tasks)
        self.assertIs(self.cog._pending_tasks[(64, 112)], second)
        self.assertFalse(second.cancelled())
        self.assertNotIn(111, self.cog._sessions[64])

    async def test_one_channel_replacement_does_not_affect_other_guild_key(self):
        first_channel, first_member = self.new_channel(111, 222)
        second_channel, second_member = self.new_channel(112, 223)
        await self.join(first_channel, first_member)
        await self.join(second_channel, second_member)
        await self.wait_for_worker_start(2)
        untouched = self.cog._pending_tasks[(64, 112)]
        old = self.cog._pending_tasks[(64, 111)]
        await self.leave(first_channel, first_member)
        await self.join(first_channel, first_member)
        await asyncio.gather(old, return_exceptions=True)
        self.assertIs(self.cog._pending_tasks[(64, 112)], untouched)
        self.assertFalse(untouched.cancelled())
        self.assertIn((64, 111), self.cog._pending_tasks)

    async def test_missing_guild_exits_and_removes_own_entry(self):
        channel, member = self.new_channel(111, 222)
        self.cog.bot.get_guild = lambda gid: None
        await self.join(channel, member)
        task = self.cog._pending_tasks[(64, 111)]
        await asyncio.wait_for(task, timeout=1)
        self.assertNotIn((64, 111), self.cog._pending_tasks)
        self.text_channel.send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
