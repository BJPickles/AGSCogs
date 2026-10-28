"""AUD-004: mocked Selenium regression tests for the actual scraper methods.

Uses real production method bodies from source AST and a fake Chrome driver.
No Red bot, Discord API, Chrome binary or external website is required.
"""

import ast
import asyncio
import logging
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "agshousingscraper" / "agshousingscraper.py"
TREE = ast.parse(SOURCE_PATH.read_text(encoding="utf-8"), filename=str(SOURCE_PATH))
COG = next(
    node for node in TREE.body
    if isinstance(node, ast.ClassDef) and node.name == "AGSHousingScraper"
)
NAMES = {"_capture_screenshot_sync", "capture_screenshot"}
METHODS = [
    node for node in COG.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES
]
assert len(METHODS) == len(NAMES)
namespace = {
    "asyncio": asyncio,
    "tempfile": tempfile,
    "os": os,
    "webdriver": webdriver,
    "Options": Options,
    "log": logging.getLogger("test.housing_selenium"),
    "SCREENSHOT_PAGE_LOAD_TIMEOUT_SECONDS": 30,
    "SCREENSHOT_SCRIPT_TIMEOUT_SECONDS": 15,
}
harness = ast.fix_missing_locations(ast.Module(
    body=[ast.ClassDef(
        name="HousingScreenshotHarness", bases=[], keywords=[], decorator_list=[],
        body=METHODS,
    )],
    type_ignores=[],
))
exec(compile(harness, str(SOURCE_PATH), "exec"), namespace)
Harness = namespace["HousingScreenshotHarness"]


class FakeDriver:
    def __init__(self, *, wait=None, started=None, error_at=None, return_value=True):
        self.wait = wait
        self.started = started
        self.error_at = error_at
        self.return_value = return_value
        self.quit_count = 0
        self.page_timeout = None
        self.script_timeout = None
        self.page_url = None
        self.screenshot_path = None
        self.thread_id = None
        self.scrolled = False

    def set_page_load_timeout(self, seconds):
        self.page_timeout = seconds

    def set_script_timeout(self, seconds):
        self.script_timeout = seconds

    def get(self, url):
        self.page_url = url
        self.thread_id = threading.get_ident()
        if self.started:
            self.started.set()
        if self.wait:
            if not self.wait.wait(timeout=3):
                raise TimeoutError("Test's fake Selenium operation did not unblock")
        if self.error_at == "navigate":
            raise RuntimeError("Navigation failed")

    def find_element(self, by, selector):
        if self.error_at == "selector":
            raise RuntimeError("Selector unavailable")
        return object()

    def execute_script(self, source, element):
        self.scrolled = True

    def save_screenshot(self, path):
        self.screenshot_path = path
        Path(path).write_bytes(b"fake png bytes")
        if self.error_at == "save":
            raise RuntimeError("Screenshot write interrupted")
        return self.return_value

    def quit(self):
        self.quit_count += 1
        if self.error_at == "quit":
            raise RuntimeError("Failed to quit Chrome")


class ScreenshotSynchronousTests(unittest.TestCase):
    def setUp(self):
        self.scraper = Harness()

    def test_success_closes_browser_and_file_descriptor(self):
        driver = FakeDriver()
        with patch.object(webdriver, "Chrome", return_value=driver):
            path = self.scraper._capture_screenshot_sync("https://example.invalid", "article")
        try:
            self.assertEqual(Path(path).read_bytes(), b"fake png bytes")
            self.assertEqual(driver.quit_count, 1)
            self.assertEqual(driver.page_timeout, 30)
            self.assertEqual(driver.script_timeout, 15)
            self.assertTrue(driver.scrolled)
        finally:
            Path(path).unlink(missing_ok=True)

    def test_navigation_error_always_quits(self):
        driver = FakeDriver(error_at="navigate")
        with patch.object(webdriver, "Chrome", return_value=driver):
            with self.assertRaisesRegex(RuntimeError, "Navigation failed"):
                self.scraper._capture_screenshot_sync("https://example.invalid")
        self.assertEqual(driver.quit_count, 1)
        self.assertIsNone(driver.screenshot_path)

    def test_failure_to_save_removes_partial_file(self):
        for error, returns in [("save", True), (None, False)]:
            with self.subTest(error=error, returns=returns):
                driver = FakeDriver(error_at=error, return_value=returns)
                with patch.object(webdriver, "Chrome", return_value=driver):
                    with self.assertRaises(RuntimeError):
                        self.scraper._capture_screenshot_sync("https://example.invalid")
                self.assertEqual(driver.quit_count, 1)
                self.assertFalse(Path(driver.screenshot_path).exists())

    def test_selector_failure_keeps_full_page_screenshot(self):
        driver = FakeDriver(error_at="selector")
        with patch.object(webdriver, "Chrome", return_value=driver):
            path = self.scraper._capture_screenshot_sync("https://example.invalid", ".missing")
        try:
            self.assertTrue(Path(path).exists())
            self.assertEqual(driver.quit_count, 1)
        finally:
            Path(path).unlink(missing_ok=True)

    def test_chrome_creation_failure_propagates(self):
        with patch.object(webdriver, "Chrome", side_effect=RuntimeError("Driver not available")):
            with self.assertRaisesRegex(RuntimeError, "Driver not available"):
                self.scraper._capture_screenshot_sync("https://example.invalid")


class ScreenshotAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.scraper = Harness()
        self.scraper._screenshot_lock = asyncio.Lock()

    async def test_selenium_runs_off_the_event_loop(self):
        event_loop_thread = threading.get_ident()
        driver = FakeDriver()
        with patch.object(webdriver, "Chrome", return_value=driver):
            path = await self.scraper.capture_screenshot("https://example.invalid")
        try:
            self.assertNotEqual(driver.thread_id, event_loop_thread)
            self.assertEqual(driver.quit_count, 1)
        finally:
            Path(path).unlink(missing_ok=True)

    async def test_parallel_jobs_are_serialized(self):
        started = threading.Event()
        release = threading.Event()
        drivers = [FakeDriver(wait=release, started=started), FakeDriver()]
        with patch.object(webdriver, "Chrome", side_effect=drivers) as factory:
            first = asyncio.create_task(self.scraper.capture_screenshot("https://first.invalid"))
            second = None
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                second = asyncio.create_task(
                    self.scraper.capture_screenshot("https://second.invalid")
                )
                await asyncio.sleep(0.04)
                self.assertEqual(factory.call_count, 1)
                release.set()
                first_path, second_path = await asyncio.wait_for(
                    asyncio.gather(first, second), timeout=3
                )
                try:
                    self.assertEqual(factory.call_count, 2)
                    self.assertEqual(drivers[0].quit_count, 1)
                    self.assertEqual(drivers[1].quit_count, 1)
                finally:
                    Path(first_path).unlink(missing_ok=True)
                    Path(second_path).unlink(missing_ok=True)
            finally:
                release.set()

    async def test_cancelled_job_closes_browser_and_removes_output(self):
        started = threading.Event()
        release = threading.Event()
        driver = FakeDriver(wait=release, started=started)
        with patch.object(webdriver, "Chrome", return_value=driver):
            job = asyncio.create_task(self.scraper.capture_screenshot("https://example.invalid"))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                job.cancel()
                await asyncio.sleep(0.01)
                # Cancellation must not terminate Selenium's already-running thread.
                self.assertEqual(driver.quit_count, 0)
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(job, timeout=3)
                await asyncio.sleep(0.02)  # allow done callback to discard output
                self.assertEqual(driver.quit_count, 1)
                self.assertFalse(Path(driver.screenshot_path).exists())
            finally:
                release.set()

    async def test_error_from_worker_propagates_without_leak(self):
        driver = FakeDriver(error_at="save")
        with patch.object(webdriver, "Chrome", return_value=driver):
            with self.assertRaisesRegex(RuntimeError, "Screenshot write interrupted"):
                await self.scraper.capture_screenshot("https://example.invalid")
        self.assertEqual(driver.quit_count, 1)
        self.assertFalse(Path(driver.screenshot_path).exists())


if __name__ == "__main__":
    unittest.main()
