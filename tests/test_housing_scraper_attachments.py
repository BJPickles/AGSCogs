"""AUD-009: independent Discord uploads and screenshot cleanup regression tests.

Execute the real Housing Scraper send-method AST with lightweight Discord/Red
fakes. No Discord connection, Selenium or Chrome installation is needed.
"""

import ast
import asyncio
import datetime
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


SOURCE_PATH = Path(__file__).resolve().parents[1] / "agshousingscraper" / "agshousingscraper.py"
tree = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"), filename=str(SOURCE_PATH))
cog = next(
    n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "AGSHousingScraper"
)
method_names = {"_send_with_screenshot", "dispatch_alert", "force", "_weekly_snapshot_loop"}
methods = [
    node for node in cog.body
    if isinstance(node, ast.AsyncFunctionDef) and node.name in method_names
]
assert len(methods) == len(method_names)
for method in methods:
    method.decorator_list = []

cleanup = next(
    n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "delete_screenshot"
)
source = ast.fix_missing_locations(ast.Module(
    body=[cleanup, ast.ClassDef(
        name="ScreenshotSendHarness",
        bases=[], keywords=[], decorator_list=[], body=methods,
    )],
    type_ignores=[],
))


class FakeFile:
    opened = []

    def __init__(self, path, *, filename):
        self.path = path
        self.filename = filename
        self.stream = open(path, "rb")
        self.closed = False
        self.__class__.opened.append(self)

    def close(self):
        self.closed = True
        self.stream.close()


class FakeEmbed:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.image_url = None

    def set_image(self, *, url):
        self.image_url = url


class FakeColors:
    @staticmethod
    def green():
        return 1

    @staticmethod
    def red():
        return 2

    @staticmethod
    def gold():
        return 3


logger = logging.getLogger("tests.housing_attachments")
namespace = {
    "os": __import__("os"),
    "log": logger,
    "discord": SimpleNamespace(File=FakeFile, Embed=FakeEmbed, Color=FakeColors),
    "datetime": datetime,
    "asyncio": asyncio,
}
exec(compile(source, str(SOURCE_PATH), "exec"), namespace)
Harness = namespace["ScreenshotSendHarness"]


def new_screenshot():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as output:
        output.write(b"fake screenshot")
        return output.name


class FakeDestination:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    async def send(self, **kwargs):
        file = kwargs.get("file")
        if file:
            self.calls.append({
                "file": file, "closed_while_sending": file.closed,
                "payload": file.stream.read(), "embed": kwargs.get("embed"),
                "content": kwargs.get("content"),
            })
        else:
            self.calls.append({"file": None, "content": kwargs.get("content"),
                               "embed": kwargs.get("embed")})
        if self.error:
            raise self.error
        return object()


class AttachmentsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeFile.opened.clear()
        self.scraper = Harness()
        self.channel = FakeDestination()
        self.user = FakeDestination()
        self.guild = SimpleNamespace(id=900, get_channel=lambda cid: self.channel)
        self.config_values = {
            "channel_id": 101, "dm_user_id": 202, "role_id": 303,
        }
        self.settings = SimpleNamespace(all=AsyncMock(side_effect=lambda: self.config_values))
        self.scraper.config = SimpleNamespace(guild=Mock(return_value=self.settings))
        self.scraper.bot = SimpleNamespace(get_user=lambda uid: self.user)

    async def test_channel_and_dm_use_separate_open_uploads(self):
        path = new_screenshot()
        embed = FakeEmbed()
        await self.scraper.dispatch_alert(self.guild, None, embed, path)
        self.assertEqual(len(self.channel.calls), 1)
        self.assertEqual(len(self.user.calls), 1)
        a, b = self.channel.calls[0], self.user.calls[0]
        self.assertIsNot(a["file"], b["file"])
        self.assertEqual(a["payload"], b"fake screenshot")
        self.assertEqual(b["payload"], b"fake screenshot")
        self.assertEqual(a["content"], "<@&303>")
        self.assertIsNone(b["content"])
        self.assertFalse(a["closed_while_sending"])
        self.assertFalse(b["closed_while_sending"])
        self.assertTrue(all(f.closed for f in FakeFile.opened))
        self.assertFalse(Path(path).exists())
        self.assertEqual(embed.image_url, "attachment://snapshot.png")

    async def test_channel_failure_still_tries_dm_and_cleans_everything(self):
        self.channel.error = RuntimeError("channel send failed")
        path = new_screenshot()
        with self.assertLogs(logger, "ERROR"):
            with self.assertRaisesRegex(RuntimeError, "channel send failed"):
                await self.scraper.dispatch_alert(self.guild, None, FakeEmbed(), path)
        self.assertEqual(len(self.user.calls), 1)
        self.assertIsNot(self.channel.calls[0]["file"], self.user.calls[0]["file"])
        self.assertTrue(all(f.closed for f in FakeFile.opened))
        self.assertFalse(Path(path).exists())

    async def test_dm_failure_is_logged_without_losing_channel_alert(self):
        self.user.error = RuntimeError("DM forbidden")
        path = new_screenshot()
        with self.assertLogs(logger, "ERROR") as messages:
            await self.scraper.dispatch_alert(self.guild, None, FakeEmbed(), path)
        self.assertIn("DM alert failed", "\n".join(messages.output))
        self.assertEqual(len(self.channel.calls), 1)
        self.assertTrue(all(f.closed for f in FakeFile.opened))
        self.assertFalse(Path(path).exists())

    async def test_missing_destinations_still_delete_generated_png(self):
        self.config_values["channel_id"] = None
        self.config_values["dm_user_id"] = None
        path = new_screenshot()
        await self.scraper.dispatch_alert(self.guild, None, FakeEmbed(), path)
        self.assertFalse(Path(path).exists())
        self.assertEqual(FakeFile.opened, [])

    async def test_config_error_still_deletes_generated_png(self):
        self.settings.all = AsyncMock(side_effect=RuntimeError("Config unavailable"))
        path = new_screenshot()
        with self.assertRaisesRegex(RuntimeError, "Config unavailable"):
            await self.scraper.dispatch_alert(self.guild, None, FakeEmbed(), path)
        self.assertFalse(Path(path).exists())

    async def test_cancellation_closes_upload_and_removes_png(self):
        self.channel.error = asyncio.CancelledError()
        path = new_screenshot()
        with self.assertRaises(asyncio.CancelledError):
            await self.scraper.dispatch_alert(self.guild, None, FakeEmbed(), path)
        self.assertEqual(len(self.user.calls), 0)
        self.assertTrue(all(f.closed for f in FakeFile.opened))
        self.assertFalse(Path(path).exists())

    async def test_no_screenshot_passes_no_attachment(self):
        await self.scraper.dispatch_alert(self.guild, "alert", FakeEmbed())
        self.assertEqual(len(FakeFile.opened), 0)
        self.assertEqual(len(self.channel.calls), 1)
        self.assertEqual(len(self.user.calls), 1)
        self.assertIsNone(self.channel.calls[0]["file"])
        self.assertIsNone(self.user.calls[0]["file"])

    async def test_force_check_always_closes_and_deletes_generated_png(self):
        path = new_screenshot()
        ctx = FakeDestination()
        ctx.guild = self.guild
        ctx.channel = SimpleNamespace(trigger_typing=AsyncMock())
        ctx.tick = AsyncMock()
        self.settings.url = AsyncMock(return_value="https://example.invalid")
        self.settings.section_selector = AsyncMock(return_value="section")
        self.settings.full_selector = AsyncMock(return_value="html")
        self.settings.detection_modes = AsyncMock(return_value=[])
        self.settings.keywords = AsyncMock(return_value=[])
        self.settings.use_screenshot = AsyncMock(return_value=True)
        self.scraper.check_site = AsyncMock()
        self.scraper.capture_screenshot = AsyncMock(return_value=path)

        await self.scraper.force(ctx)
        self.assertTrue(FakeFile.opened[0].closed)
        self.assertFalse(Path(path).exists())
        ctx.tick.assert_awaited_once()

    async def test_force_send_failure_still_deletes_png(self):
        path = new_screenshot()
        ctx = FakeDestination(error=RuntimeError("force failed"))
        ctx.guild = self.guild
        ctx.channel = SimpleNamespace(trigger_typing=AsyncMock())
        ctx.tick = AsyncMock()
        self.settings.url = AsyncMock(return_value="https://example.invalid")
        self.settings.section_selector = AsyncMock(return_value="section")
        self.settings.full_selector = AsyncMock(return_value="html")
        self.settings.detection_modes = AsyncMock(return_value=[])
        self.settings.keywords = AsyncMock(return_value=[])
        self.settings.use_screenshot = AsyncMock(return_value=True)
        self.scraper.check_site = AsyncMock()
        self.scraper.capture_screenshot = AsyncMock(return_value=path)
        with self.assertRaisesRegex(RuntimeError, "force failed"):
            await self.scraper.force(ctx)
        self.assertTrue(FakeFile.opened[0].closed)
        self.assertFalse(Path(path).exists())
        ctx.tick.assert_not_awaited()

    async def test_weekly_snapshot_cleans_up_when_channel_send_fails(self):
        path = new_screenshot()
        class FirstSendFails(FakeDestination):
            async def send(self, **kwargs):
                if kwargs.get("file"):
                    self.error = RuntimeError("weekly upload failed")
                else:
                    self.error = None
                return await super().send(**kwargs)

        channel = FirstSendFails()
        guild = SimpleNamespace(id=123, get_channel=lambda cid: channel)
        settings = SimpleNamespace(all=AsyncMock(return_value={
            "log_channel_id": 11, "url": "https://example.invalid",
            "section_selector": "section",
        }))
        scraper = Harness()
        scraper.bot = SimpleNamespace(
            wait_until_ready=AsyncMock(), guilds=[guild],
        )
        scraper.config = SimpleNamespace(guild=Mock(return_value=settings))
        scraper.capture_screenshot = AsyncMock(return_value=path)

        class AfterFirstWeek(Exception):
            pass

        sleep = AsyncMock(side_effect=[None, AfterFirstWeek()])
        with patch.object(asyncio, "sleep", sleep):
            with self.assertRaises(AfterFirstWeek):
                await scraper._weekly_snapshot_loop()
        self.assertEqual(len(channel.calls), 2)
        self.assertIsNotNone(channel.calls[0]["file"])
        self.assertIsNone(channel.calls[1]["file"])
        self.assertTrue(FakeFile.opened[0].closed)
        self.assertFalse(Path(path).exists())


if __name__ == "__main__":
    unittest.main()
