"""AUD-003 regression tests for the actual Housing Scraper HTTP fetch pipeline.

Execute the production check_site method in isolation with a fake Red config,
using real aiohttp/BeautifulSoup and a local aiohttp.web test server. No Discord,
Red, Selenium, browser, or external website is required.
"""

import ast
import asyncio
from contextlib import suppress
from pathlib import Path
import unittest

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
COG_FILE = ROOT / "agshousingscraper" / "agshousingscraper.py"
SOURCE = COG_FILE.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE, filename=str(COG_FILE))
COG = next(
    node for node in TREE.body
    if isinstance(node, ast.ClassDef) and node.name == "AGSHousingScraper"
)
METHOD = next(
    node for node in COG.body
    if isinstance(node, ast.AsyncFunctionDef) and node.name == "check_site"
)
TIMEOUT = next(
    node.value.value for node in TREE.body
    if isinstance(node, ast.Assign)
    and any(isinstance(target, ast.Name) and target.id == "FETCH_TIMEOUT_SECONDS"
            for target in node.targets)
)

# Execute the same production method from the source AST. This avoids importing
# Red/Selenium just to verify HTTP responses, but it is not a full cog-load test.
isolated = ast.fix_missing_locations(ast.Module(body=[METHOD], type_ignores=[]))
GLOBALS = {"aiohttp": aiohttp, "BeautifulSoup": BeautifulSoup,
           "FETCH_TIMEOUT_SECONDS": TIMEOUT}
exec(compile(isolated, str(COG_FILE), "exec"), GLOBALS)
check_site = GLOBALS["check_site"]


class FakeGuildSettings:
    async def all(self):
        return {"use_screenshot": False}


class FakeConfig:
    def guild(self, guild):
        return FakeGuildSettings()


class FakeScraper:
    config = FakeConfig()


class HousingFetchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = web.Application()
        self.app.router.add_get("/ok", lambda request: web.Response(
            text="<html><body>Available housing</body></html>", content_type="text/html"))
        self.app.router.add_get("/error", lambda request: web.Response(
            status=503, text="<html><body>Maintenance page</body></html>"))
        self.app.router.add_get("/slow-header", self.slow_header)
        self.app.router.add_get("/slow-body", self.slow_body)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        self.scraper = FakeScraper()
        self.guild = object()

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def slow_header(self, request):
        await asyncio.sleep(0.2)
        return web.Response(text="<html>Delayed header</html>")

    async def slow_body(self, request):
        response = web.StreamResponse(status=200)
        response.content_type = "text/html"
        await response.prepare(request)
        await asyncio.sleep(0.2)
        with suppress(ConnectionResetError):
            await response.write(b"<html>Delayed body</html>")
        with suppress(ConnectionResetError):
            await response.write_eof()
        return response

    def stats(self, url):
        return {
            "url": url,
            "section_selector": "section",
            "full_selector": "html",
            "modes": [],
            "keywords": [],
        }

    async def test_successful_html_fetch(self):
        await check_site(self.scraper, self.guild, self.stats(self.base_url + "/ok"))

    async def test_http_error_is_not_treated_as_successful_fetch(self):
        with self.assertRaises(aiohttp.ClientResponseError) as err:
            await check_site(self.scraper, self.guild, self.stats(self.base_url + "/error"))
        self.assertEqual(err.exception.status, 503)

    async def test_timeout_while_waiting_for_response_headers(self):
        GLOBALS["FETCH_TIMEOUT_SECONDS"] = 0.05
        try:
            with self.assertRaises(asyncio.TimeoutError):
                await check_site(
                    self.scraper, self.guild, self.stats(self.base_url + "/slow-header")
                )
        finally:
            GLOBALS["FETCH_TIMEOUT_SECONDS"] = TIMEOUT

    async def test_timeout_while_reading_response_body(self):
        GLOBALS["FETCH_TIMEOUT_SECONDS"] = 0.05
        try:
            with self.assertRaises(asyncio.TimeoutError):
                await check_site(
                    self.scraper, self.guild, self.stats(self.base_url + "/slow-body")
                )
        finally:
            GLOBALS["FETCH_TIMEOUT_SECONDS"] = TIMEOUT

    def test_timeout_default_and_source_api(self):
        self.assertEqual(TIMEOUT, 30)
        self.assertNotIn("import async_timeout", SOURCE)
        self.assertIn("response.raise_for_status()", SOURCE)
        self.assertIn("async with session.get(", SOURCE)


if __name__ == "__main__":
    unittest.main()
