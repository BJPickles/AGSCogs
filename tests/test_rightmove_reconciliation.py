"""AUD-015: Rightmove destructive reconciliation and scrape safety tests.

Use fixture-based scraper results and the actual reconciliation, scrape
orchestration and read-only preview methods extracted from production source.
No Red installation, requests, Rightmove network access or live Discord calls.
"""

import ast
import asyncio
import copy
import json
import logging
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "rightmove" / "rightmove.py"
FIXTURES = json.loads(
    (ROOT / "tests" / "fixtures" / "rightmove_missing_scrapes.json")
    .read_text(encoding="utf-8")
)
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
COG = next(
    node for node in TREE.body
    if isinstance(node, ast.ClassDef) and node.name == "RightmoveCog"
)
METHODS = {
    "_reconcile_missing_properties", "_run_scrape", "rm_missingpreview",
}
FUNCTIONS = {
    "missing_removal_guard", "preview_missing_candidates",
    "_channel_property_id", "_safe_int",
}
actual_methods = [
    node for node in COG.body
    if isinstance(node, ast.AsyncFunctionDef) and node.name in METHODS
]
assert len(actual_methods) == len(METHODS)
for method in actual_methods:
    method.decorator_list = []
actual_functions = [
    node for node in TREE.body
    if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS
]
assert len(actual_functions) == len(FUNCTIONS)


class Forbidden(Exception):
    pass


class HTTPException(Exception):
    pass


class TextChannel:
    def __init__(self, channel_id, name, *, topic="", error=None):
        self.id = channel_id
        self.name = name
        self.topic = topic
        self.category = None
        self.error = error
        self.deleted = False
        self.delete_calls = 0

    async def delete(self, *, reason):
        self.delete_calls += 1
        if self.error:
            raise self.error
        self.deleted = True


class Value:
    def __init__(self, initial):
        self.data = copy.deepcopy(initial)
        self.snapshots = []

    async def __call__(self):
        return copy.deepcopy(self.data)

    async def set(self, value):
        self.data = copy.deepcopy(value)
        self.snapshots.append(copy.deepcopy(value))


class GuildConfig:
    def __init__(self, *, cache=None, missing=3):
        self.settings = Value({
            "search_url": "https://www.rightmove.co.uk/property-for-sale/find.html",
            "anchor_channel_id": 1,
            "missing_confirmations": missing,
            "last_attempt_ts": None,
            "last_success_ts": None,
            "last_summary": "",
            "consecutive_failures": 0,
            "profile_name": "Fixture Rightmove",
        })
        self.properties = Value(cache or {})
        self.ignored_properties = Value({})


class Guild:
    def __init__(self):
        self.id = 100
        self.channels = {1: TextChannel(1, "anchor")}

    def get_channel(self, channel_id):
        return self.channels.get(channel_id)

    def add(self, channel):
        self.channels[channel.id] = channel


NS = {
    "asyncio": asyncio,
    "copy": copy,
    "json": json,
    "re": re,
    "math": __import__("math"),
    "log": logging.getLogger("rightmove.safety.tests"),
    "discord": SimpleNamespace(
        TextChannel=TextChannel, Forbidden=Forbidden, HTTPException=HTTPException,
    ),
    "TOPIC_MARKER_RE": re.compile(r"RIGHTMOVE_PROPERTY_ID=(\d+)", re.I),
    "PROPERTY_CHANNEL_RE": re.compile(r"^prop-(\d+)", re.I),
    "_utc_ts": lambda: 1234567,
    "_validate_rightmove_url": lambda url: (True, "ok"),
    "_encode_summary": lambda data: json.dumps(data),
    "RightmoveClient": SimpleNamespace(scrape_search=Mock()),
}
MODULE = ast.fix_missing_locations(ast.Module(
    body=[
        ast.ImportFrom(
            module="__future__", names=[ast.alias(name="annotations")], level=0,
        ),
        *actual_functions,
        ast.ClassDef(
            name="RightmoveHarness", bases=[], keywords=[],
            decorator_list=[], body=actual_methods,
        ),
    ],
    type_ignores=[],
))
exec(compile(MODULE, str(SOURCE), "exec"), NS)
Harness = NS["RightmoveHarness"]
guard = NS["missing_removal_guard"]
preview = NS["preview_missing_candidates"]


def scrape(name):
    data = FIXTURES[name]
    return SimpleNamespace(**copy.deepcopy(data))


def state(channel_id, *, count=0):
    return {"channel_id": channel_id, "missing_count": count, "active": True}


class SafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = Guild()
        self.store = GuildConfig(cache={
            "100": state(1000), "200": state(2000)
        })
        self.channel100 = TextChannel(1000, "prop-100")
        self.channel200 = TextChannel(2000, "prop-200")
        self.guild.add(self.channel100)
        self.guild.add(self.channel200)
        self.bot = SimpleNamespace()
        self.cog = Harness()
        self.cog.config = SimpleNamespace(guild=Mock(return_value=self.store))
        self.cog._active_runs = {}
        self.cog._lock_for = lambda guild_id: asyncio.Lock()
        self.cog._migrate_legacy_global = AsyncMock()
        self.cog._normalise_cached_state = lambda pid, raw: dict(raw, id=pid)
        self.cog._discover_channels = Mock(return_value={})
        self.cog._filter_reason = Mock(return_value="fixture excluded")
        self.cog._log = AsyncMock()
        self.cog._reorder_channels = AsyncMock(return_value=(0, []))
        self.cog.bot = self.bot

    async def do_reconcile(self, *, accepted=(), threshold=3):
        cache = await self.store.properties()
        result = await self.cog._reconcile_missing_properties(
            self.guild, self.store, cache,
            set(accepted), {"missing_confirmations": threshold},
        )
        return result, await self.store.properties()

    async def run_scrape(self, fixture_name):
        with patch.object(
            asyncio, "to_thread", new=AsyncMock(return_value=scrape(fixture_name))
        ):
            return await self.cog._run_scrape(
                self.guild, source="manual", force_refresh=False
            )

    async def test_partial_layout_change_never_deletes_or_ages_cache(self):
        result = await self.run_scrape("partial_layout_change")
        self.assertTrue(result["ok"])
        self.assertFalse(self.channel100.deleted)
        self.assertFalse(self.channel200.deleted)
        self.assertEqual((await self.store.properties())["100"]["missing_count"], 0)
        self.assertEqual(len(self.store.properties.snapshots), 1)
        self.assertIsNone((await self.store.settings())["last_success_ts"])
        summary = json.loads((await self.store.settings())["last_summary"])
        self.assertFalse(summary["complete"])
        self.assertEqual(summary["removed"], 0)

    async def test_nominally_complete_empty_layout_is_held_for_review(self):
        result = await self.run_scrape("complete_empty_search")
        self.assertTrue(result["ok"])
        self.assertIn("zero properties", result["message"])
        self.assertFalse(self.channel100.deleted)
        self.assertFalse(self.channel200.deleted)
        self.assertEqual((await self.store.properties())["100"]["missing_count"], 0)
        self.assertIsNone((await self.store.settings())["last_success_ts"])
        self.assertFalse(json.loads((await self.store.settings())["last_summary"])["complete"])

    async def test_failed_request_never_deletes_anything(self):
        result = await self.run_scrape("failed_request")
        self.assertFalse(result["ok"])
        self.assertFalse(self.channel100.deleted)
        self.assertEqual(len(self.store.properties.snapshots), 0)
        self.assertEqual((await self.store.properties())["100"]["missing_count"], 0)

    async def test_complete_nonempty_scrape_still_allows_confirmed_removals(self):
        self.store.settings.data["missing_confirmations"] = 1
        result = await self.run_scrape("complete_with_listings")
        self.assertTrue(result["ok"])
        self.assertTrue(self.channel100.deleted)
        self.assertTrue(self.channel200.deleted)
        self.assertNotIn("100", await self.store.properties())
        self.assertNotIn("200", await self.store.properties())
        self.assertIsNotNone((await self.store.settings())["last_success_ts"])

    async def test_three_confirmations_then_delete_matching_channels(self):
        result1, cache1 = await self.do_reconcile()
        self.assertEqual(result1, (0, 2, []))
        self.assertEqual(cache1["100"]["missing_count"], 1)
        self.assertFalse(self.channel100.deleted)
        result2, cache2 = await self.do_reconcile()
        self.assertEqual(result2, (0, 2, []))
        self.assertEqual(cache2["100"]["missing_count"], 2)
        self.assertFalse(self.channel100.deleted)
        result3, cache3 = await self.do_reconcile()
        self.assertEqual(result3, (2, 2, []))
        self.assertTrue(self.channel100.deleted)
        self.assertEqual(cache3, {})

    async def test_returned_listing_not_counted_missing(self):
        result, cache = await self.do_reconcile(accepted={"100"})
        self.assertEqual(result[0], 0)
        self.assertEqual(cache["100"]["missing_count"], 0)
        self.assertEqual(cache["200"]["missing_count"], 1)

    async def test_mismatched_cached_channel_id_never_deletes_unrelated_channel(self):
        self.store.properties.data = {"100": state(1000, count=2)}
        self.channel100.name = "general-chat"
        result, cache = await self.do_reconcile()
        self.assertEqual(result[:2], (0, 1))
        self.assertIn("does not match", result[2][0])
        self.assertFalse(self.channel100.deleted)
        self.assertEqual(cache["100"]["missing_count"], 3)

    async def test_matching_topic_marker_allows_deletion(self):
        self.store.properties.data = {"100": state(1000, count=2)}
        self.channel100.name = "anything"
        self.channel100.topic = "RIGHTMOVE_PROPERTY_ID=100"
        result, cache = await self.do_reconcile()
        self.assertEqual(result[0], 1)
        self.assertTrue(self.channel100.deleted)
        self.assertEqual(cache, {})

    async def test_discord_forbidden_preserves_cache_for_retry(self):
        self.store.properties.data = {"100": state(1000, count=2)}
        self.channel100.error = Forbidden("no Manage Channels")
        result, cache = await self.do_reconcile()
        self.assertEqual(result[0], 0)
        self.assertIn("Could not delete", result[2][0])
        self.assertEqual(cache["100"]["missing_count"], 3)
        self.assertFalse(self.channel100.deleted)
        self.channel100.error = None
        second, saved = await self.do_reconcile()
        self.assertEqual(second[0], 1)
        self.assertEqual(saved, {})

    async def test_missing_cached_channel_removes_record_without_any_discord_delete(self):
        self.store.properties.data = {"100": state(98765, count=2)}
        result, cache = await self.do_reconcile()
        self.assertEqual(result[0], 1)
        self.assertEqual(cache, {})
        self.assertEqual(self.channel100.delete_calls, 0)

    async def test_interrupted_deletion_persists_prior_decisions(self):
        self.store.properties.data = {
            "100": state(1000, count=2), "200": state(2000, count=2)
        }
        self.channel200.error = asyncio.CancelledError()
        cache = await self.store.properties()
        with self.assertRaises(asyncio.CancelledError):
            await self.cog._reconcile_missing_properties(
                self.guild, self.store, cache, set(),
                {"missing_confirmations": 3},
            )
        saved = await self.store.properties()
        self.assertTrue(self.channel100.deleted)
        self.assertNotIn("100", saved)
        self.assertIn("200", saved)
        self.assertEqual(saved["200"]["missing_count"], 2)

    async def test_read_only_preview_does_not_modify_cache_or_send_deletions(self):
        self.store.properties.data = {
            "100": state(1000, count=1),
            "200": state(2000, count=2),
            "300": state(None, count=0),
        }
        ctx = SimpleNamespace(guild=self.guild, send=AsyncMock())
        await self.cog.rm_missingpreview(ctx)
        ctx.send.assert_awaited_once()
        message = ctx.send.await_args.args[0]
        self.assertIn("read-only", message)
        self.assertIn("1** record", message)
        self.assertIn("prop-200", message)
        self.assertNotIn("prop-100", message)
        self.assertEqual(self.store.properties.snapshots, [])
        self.assertEqual(self.store.settings.snapshots, [])
        self.assertEqual(self.channel100.delete_calls, 0)

    async def test_guard_reports_scrape_completeness_and_empty_cache_exception(self):
        self.assertEqual(guard(scrape("complete_with_listings"), 5), (True, None))
        self.assertFalse(guard(scrape("partial_layout_change"), 5)[0])
        self.assertFalse(guard(scrape("complete_empty_search"), 5)[0])
        self.assertEqual(guard(scrape("complete_empty_search"), 0), (True, None))

    async def test_preview_normalizes_malformed_legacy_counters(self):
        self.assertEqual(
            preview({
                "1": {"missing_count": "not a number"},
                "2": {"missing_count": "2"},
                "3": {"missing_count": -6},
            }, 3),
            ["2"],
        )


if __name__ == "__main__":
    unittest.main()
