import asyncio
import random
from playwright.async_api import async_playwright, Playwright, BrowserContext

class CaptchaError(Exception):
    pass

class RightmoveScraper:
    """Minimal Playwright scraper that navigates your exact Rightmove URL."""
    def __init__(self):
        self.playwright: Playwright = None
        self.context: BrowserContext = None

    async def _init(self):
        if not self.playwright:
            self.playwright = await async_playwright().start()
        if not self.context:
            # headless Chromium
            self.context = await self.playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )

    async def scrape_url(self, url: str) -> list:
        """Load the given URL and return a list of propertyCard dicts."""
        await self._init()
        page = await self.context.new_page()
        try:
            print("Scraping URL:", url)
            await page.goto(url)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(random.uniform(2, 4))

            # dismiss cookie banner if present
            try:
                await page.click("#onetrust-reject-all-handler", timeout=3000)
                await asyncio.sleep(1)
            except:
                pass

            # some random scrolls to lazy-load more cards
            for _ in range(random.randint(1, 3)):
                height = await page.evaluate("() => document.body.scrollHeight")
                y = random.randint(0, height)
                await page.evaluate(f"window.scrollTo(0, {y})")
                await asyncio.sleep(random.uniform(0.5, 1.5))

            # wait for at least one card
            try:
                await page.wait_for_selector(".propertyCard", timeout=5000)
            except:
                return []

            cards = await page.query_selector_all(".propertyCard")
            results = []
            for card in cards:
                try:
                    lid = await card.get_attribute("data-listing-id") or ""
                    # title
                    t = await card.query_selector(".propertyCard-title")
                    title = (await t.inner_text()).strip() if t else ""
                    # href
                    a = await card.query_selector("a.propertyCard-link")
                    href = await a.get_attribute("href") if a else ""
                    url_full = href if href.startswith("http") else f"https://www.rightmove.co.uk{href}"
                    # price
                    p = await card.query_selector(".propertyCard-priceValue")
                    pt = await p.text_content() if p else ""
                    price = int("".join(filter(str.isdigit, pt))) if pt else 0
                    # beds
                    beds = 0
                    for li in await card.query_selector_all(".propertyCard-details li"):
                        txt = (await li.text_content() or "").lower()
                        if "bed" in txt:
                            parts = txt.split()
                            if parts and parts[0].isdigit():
                                beds = int(parts[0])
                            break
                    # location
                    l = await card.query_selector(".propertyCard-address")
                    location = (await l.text_content()).strip() if l else ""

                    results.append({
                        "id": lid,
                        "title": title,
                        "url": url_full,
                        "price": price,
                        "beds": beds,
                        "location": location
                    })
                except:
                    continue
            return results
        finally:
            await page.close()

    async def close(self):
        if self.context:
            await self.context.close()
        if self.playwright:
            await self.playwright.stop()