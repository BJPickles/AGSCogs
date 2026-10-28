"""AUD-002: test AGSProfiles' real command registration without Red or LevelUp.

The production command class definitions are executed using discord.py's
CogMeta and decorators. Runtime business logic and external services are not
invoked; the production Red integration must still be checked on a live bot.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

try:
    from discord.ext import commands as discord_commands
except ImportError:  # The unrelated Server Status suite has no discord.py dependency.
    discord_commands = None


ROOT = Path(__file__).resolve().parents[1]


def load_class(namespace: dict, relative_path: str, name: str) -> None:
    path = ROOT / relative_path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    cls = next(
        (node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name),
        None,
    )
    if cls is None:
        raise AssertionError(f"{name} not found in {relative_path}")
    # Deferred annotations let the genuine decorators/metaclass execute without
    # needing to import unrelated LevelUp, Pydantic or Discord runtime models.
    module = ast.Module(
        body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), cls],
        type_ignores=[],
    )
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)


@unittest.skipIf(discord_commands is None, "discord.py is not installed (see dedicated CI workflow)")
class AGSProfilesRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        class FakeAGSConfig:
            def __init__(self):
                self.guilds = {}

        # Red's admin_or_permissions is not part of vanilla discord.py. The
        # actual command-group construction and registration use discord.py.
        command_api = SimpleNamespace(
            Cog=discord_commands.Cog,
            command=discord_commands.command,
            group=discord_commands.group,
            guild_only=discord_commands.guild_only,
            admin_or_permissions=lambda **kwargs: lambda fn: fn,
        )
        namespace = {
            "__name__": "agsprofiles_wiring_test",
            "commands": command_api,
            "asyncio": asyncio,
            "Path": Path,
            "cog_data_path": lambda cog: Path(self.tempdir.name),
            "AGSConfig": FakeAGSConfig,
            "_": lambda message: message,
            "cog_i18n": lambda translator: lambda cls: cls,
        }

        for path, cls in (
            ("agsprofiles/commands/snapshot.py", "SnapshotCommands"),
            ("agsprofiles/commands/leaderboard.py", "LeaderboardCommands"),
            ("agsprofiles/commands/icons.py", "IconCommands"),
            ("agsprofiles/commands/__init__.py", "Commands"),
            ("agsprofiles/main.py", "AGSProfiles"),
        ):
            load_class(namespace, path, cls)

        self.namespace = namespace
        self.cog = namespace["AGSProfiles"](object())

    def test_cog_constructs_with_one_registered_instance(self):
        self.assertIsInstance(self.cog, discord_commands.Cog)
        self.assertFalse(hasattr(self.cog, "snapshots"))
        self.assertFalse(hasattr(self.cog, "leaderboard"))
        self.assertEqual(self.cog.config.guilds, {})

    def test_all_top_level_commands_are_registered(self):
        names = {c.name for c in self.cog.get_commands()}
        self.assertEqual(
            names,
            {"agsnapshot", "agsleaderboard", "agsicons", "agprofile", "agsprofilestatus"},
        )

    def test_subcommands_are_registered_under_correct_groups(self):
        registered = {c.qualified_name for c in self.cog.walk_commands()}
        expected = {
            "agsnapshot manual",
            "agsnapshot finalizeyear",
            "agsnapshot list",
            "agsnapshot delete",
            "agsnapshot backup",
            "agsleaderboard current",
            "agsleaderboard year",
            "agsleaderboard global",
            "agsicons add",
            "agsicons remove",
            "agsicons list",
        }
        self.assertTrue(expected.issubset(registered), expected - registered)

    def test_aliases_and_status_listener(self):
        groups = {c.name: c for c in self.cog.get_commands()}
        self.assertIn("agsnap", groups["agsnapshot"].aliases)
        self.assertIn("agslb", groups["agsleaderboard"].aliases)
        self.assertIn("agsicon", groups["agsicons"].aliases)
        self.assertIn("on_guild_remove", dict(self.cog.get_listeners()))


if __name__ == "__main__":
    unittest.main()
