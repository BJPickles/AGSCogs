import asyncio
import random
import datetime
from pathlib import Path
from urllib.parse import urlencode, quote
from playwright.async_api import async_playwright, Playwright, BrowserContext

class CaptchaError(Exception):
    pass

class RightmoveScraper:
    def __init__(self):
        self.playwright: Playwright = None
        self.context: BrowserContext = None
        self.backoff_count = 0
        # raw default region code for Hampshire
        self._default_region = "REGION^61303"

    async def _init(self):
        if not self.playwright:
            self.playwright = await async_playwright().start()
        if not self.context:
            data_dir = Path(__file__).parent / "userdata"
            data_dir.mkdir(exist_ok=True)
            today = datetime.datetime.now().strftime("%Y%m%d")
            dir_path = data_dir / today
            dir_path.mkdir(exist_ok=True)
            ua_list = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/15.1 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
            ]
            ua = random.choice(ua_list)
            w = random.randint(1200, 1920)
            h = random.randint(700, 1080)
            self.context = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=str(dir_path),
                headless=True,
                args=["--no-sandbox"],
                user_agent=ua,
                viewport={"width": w, "height": h},
                locale="en-GB",
                timezone_id="Europe/London",
            )
            await self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => false});
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-GB','en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
                Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
                const orig = navigator.permissions.query;
                navigator.permissions.query = p =>
                  p.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : orig(p);
            """)

    async def scrape_area(
        self,
        area: str,
        max_price: int = None,
        min_beds: int = None,
        region_code: str = None
    ) -> list:
        """Scrape Rightmove using your exact URL pattern."""
        await self._init()
        page = await self.context.new_page()
        try:
            # occasional human-like detour
            if random.random() < 0.3:
                for extra in ("/news","/why-buy","/help","/offers-for-sellers","/guides","/overseas"):
                    await page.goto(f"https://www.rightmove.co.uk{extra}")
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(random.uniform(1,2))
                await page.goto("https://www.rightmove.co.uk")
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(1,2))

            # select region identifier
            identifier = region_code or self._default_region
            # percent-encode caret and others
            encoded_region = quote(identifier, safe="")

            # build query params
            params = {
                "searchLocation": area,
                "useLocationIdentifier": "true",
                "locationIdentifier": encoded_region,
                "radius": 0.5,
                "_includeSSTC": "on",
                "includeSSTC": "true",
                "sortType": 2,
                "viewType": "LIST",
                "channel": "BUY",
                "index": 0,
                "propertyTypes": "detached,semi-detached,terraced"
            }
            if max_price is not None:
                params["maxPrice"] = max_price
            if min_beds is not None:
                params["minBedrooms"] = min_beds

            search_url = "https://www.rightmove.co.uk/property-for-sale/find.html?" + urlencode(params)

            # debug
            print("Scraping URL:", search_url)

            # navigate
            await page.goto(search_url)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(random.uniform(2,4))

            # dismiss cookie banner
            try:
                await page.click("#onetrust-reject-all-handler", timeout=5000)
                await asyncio.sleep(1)
            except:
                pass

            # human-like scrolling
            for _ in range(random.randint(2,5)):
                h = await page.evaluate("document.body.scrollHeight")
                await page.evaluate(f"window.scrollTo(0, {random.randint(0, h)})")
                await asyncio.sleep(random.uniform(0.5,1.5))

            # wait for listings
            try:
                await page.wait_for_selector(".propertyCard", timeout=5000)
            except:
                # no cards
                return []

            cards = await page.query_selector_all(".propertyCard")
            results = []
            for card in cards:
                try:
                    cid = (await card.get_attribute("data-listing-id")) or (await card.get_attribute("id")) or ""
                    listing_id = cid.split("-")[-1]
                    title_el = await card.query_selector(".propertyCard-title")
                    title = await title_el.inner_text() if title_el else ""
                    anchor = await card.query_selector("a.propertyCard-link")
                    href = await anchor.get_attribute("href") if anchor else ""
                    url = f"https://www.rightmove.co.uk{href}" if href else ""
                    price_el = await card.query_selector(".propertyCard-priceValue")
                    price_text = await price_el.text_content() if price_el else ""
                    price = int("".join(filter(str.isdigit, price_text))) if price_text else 0
                    beds = 0
                    for li in await card.query_selector_all(".propertyCard-details li"):
                        txt = await li.text_content() or ""
                        if "bed" in txt.lower():
                            beds = int(txt.split()[0]) if txt.split()[0].isdigit() else 0
                            break
                    loc_el = await card.query_selector(".propertyCard-address")
                    location = await loc_el.text_content() if loc_el else ""
                    desc_el = await card.query_selector(".propertyCard-description")
                    description = await desc_el.text_content() if desc_el else ""
                    results.append({
                        "id": listing_id,
                        "title": title,
                        "url": url,
                        "price": price,
                        "beds": beds,
                        "location": location,
                        "description": description
                    })
                except:
                    continue

            return results
        finally:
            await page.close()

    async def close(self):
        """Close Playwright."""
        if self.context:
            await self.context.close()
        if self.playwright:
            await self.playwright.stop()