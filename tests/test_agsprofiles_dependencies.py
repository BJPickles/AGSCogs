"""AUD-006 dependency metadata, actionable setup errors and LevelUp adapter tests.

The setup and collection functions are extracted from their real source with
Python AST, then exercised with controlled fakes. No Red installation, Discord
API calls or running LevelUp database is needed for this focused contract test.
"""

import ast
import asyncio
import importlib
import inspect
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
COG_DIR = ROOT / "agsprofiles"


def load_function(path, name, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(
        n for n in tree.body
        if isinstance(n, ast.AsyncFunctionDef) and n.name == name
    )
    module = ast.fix_missing_locations(ast.Module(
        body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node],
        type_ignores=[],
    ))
    exec(compile(module, str(path), "exec"), namespace)
    return namespace[name]


def fake_formatter(position_func):
    levelup = types.ModuleType("levelup")
    levelup.__path__ = []
    common = types.ModuleType("levelup.common")
    common.__path__ = []
    formatter = types.ModuleType("levelup.common.formatter")
    formatter.get_user_position = position_func
    return {
        "levelup": levelup,
        "levelup.common": common,
        "levelup.common.formatter": formatter,
    }


class DependencyMetadataTests(unittest.TestCase):
    def test_downloader_declares_runtime_dependencies(self):
        meta = json.loads((COG_DIR / "info.json").read_text(encoding="utf-8"))
        self.assertEqual(meta["required_cogs"]["LevelUp"], "https://github.com/vertyco/vrt-cogs")
        reqs = meta["requirements"]
        self.assertIn("pydantic>=2.11,<2.12", reqs)
        self.assertIn("Pillow>=9.5", reqs)
        self.assertIn("load levelup", meta["install_msg"])
        self.assertEqual(meta["min_python_version"][:2], [3, 9])

    def test_common_has_no_eager_levelup_dependency(self):
        tree = ast.parse((COG_DIR / "common.py").read_text(encoding="utf-8"))
        imports = [
            n for n in tree.body if isinstance(n, (ast.ImportFrom, ast.Import))
        ]
        self.assertFalse(
            any("levelup" in (n.module or "") for n in imports if isinstance(n, ast.ImportFrom))
        )


class SetupGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.cog = object()
        self.namespace = {
            "signature": inspect.signature,
            "AGSProfiles": lambda bot: self.cog,
        }
        self.setup = load_function(COG_DIR / "__init__.py", "setup", self.namespace)
        self.bot = types.SimpleNamespace(
            get_cog=lambda name: object() if name == "LevelUp" else None,
            add_cog=AsyncMock(),
        )

    async def test_missing_levelup_gives_actionable_error_and_no_partial_load(self):
        self.bot.get_cog = lambda name: None
        with self.assertRaisesRegex(RuntimeError, "requires Vertyco's LevelUp"):
            await self.setup(self.bot)
        self.bot.add_cog.assert_not_awaited()

    async def test_missing_formatter_gives_actionable_error(self):
        with patch.dict(sys.modules, fake_formatter(lambda: 1)):
            del sys.modules["levelup.common.formatter"].get_user_position
            with self.assertRaisesRegex(RuntimeError, "could not import the LevelUp formatter"):
                await self.setup(self.bot)
        self.bot.add_cog.assert_not_awaited()

    async def test_wrong_formatter_signature_fails_before_loading(self):
        def wrong_formatter(bot, db):
            return {"position": 1}

        with patch.dict(sys.modules, fake_formatter(wrong_formatter)):
            with self.assertRaisesRegex(RuntimeError, "compatible LevelUp formatter API"):
                await self.setup(self.bot)
        self.bot.add_cog.assert_not_awaited()

    async def test_compatible_formatter_registers_cog_once(self):
        def get_user_position(guild, conf, lbtype, target_user, key):
            return {"position": 2}

        with patch.dict(sys.modules, fake_formatter(get_user_position)):
            await self.setup(self.bot)
        self.bot.add_cog.assert_awaited_once_with(self.cog)


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_rank_uses_verified_formatter_signature(self):
        calls = []

        def get_user_position(guild, conf, lbtype, target_user, key):
            calls.append((guild, conf, lbtype, target_user, key))
            return {"position": 7}

        profile = types.SimpleNamespace(
            xp=250, level=3, messages=40, voice=7200, stars=5
        )
        conf = types.SimpleNamespace(users={42: profile})
        member = types.SimpleNamespace(id=42, bot=False)

        class FakeAdapter:
            def __init__(self, bot):
                pass

            def get_conf(self, guild):
                return conf

            async def get_balance(self, user):
                return 12.5

        class FakeStats:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        guild = types.SimpleNamespace(id=123, get_member=lambda uid: member if uid == 42 else None)
        bot = object()
        ns = {
            "LevelUpAdapter": FakeAdapter, "Stats": FakeStats, "asyncio": asyncio,
            "log": __import__("logging").getLogger("test.dependencies"),
        }
        collect = load_function(COG_DIR / "common.py", "collect_levelup_stats", ns)

        with patch.dict(sys.modules, fake_formatter(get_user_position)):
            stats = await collect(bot, guild)

        self.assertEqual(calls, [(guild, conf, "lb", 42, "xp")])
        self.assertEqual(stats[42].rank, 7)
        self.assertEqual(stats[42].voicetime, 2.0)
        self.assertEqual(stats[42].balance, 12.5)

    async def test_missing_levelup_returns_empty_stats(self):
        class MissingAdapter:
            def __init__(self, bot):
                raise RuntimeError("LevelUp cog is not loaded")

        ns = {
            "LevelUpAdapter": MissingAdapter,
            "Stats": object,
            "asyncio": asyncio,
            "log": __import__("logging").getLogger("test.dependencies"),
        }
        collect = load_function(COG_DIR / "common.py", "collect_levelup_stats", ns)
        self.assertEqual(await collect(object(), object()), {})


if __name__ == "__main__":
    unittest.main()
