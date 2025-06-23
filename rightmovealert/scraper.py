import asyncio
import random
import datetime
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright, Playwright, BrowserContext, TimeoutError as PlaywrightTimeoutError

class CaptchaError(Exception):
    pass

class RightmoveScraper:
    def __init__(self):
        self.playwright: Playwright = None
        self.context: BrowserContext = None
        self.backoff_count = 0
        # default polygon from your example URL (percent-encoded)
        self._default_encoded = (
            "USERDEFINEDAREA%5E%7B%22polylines%22%3A%22"
            "sh%7CtHhu%7BE%7D%7CDr_Nf%7BAnjZxvLz%7Dm%40reAllgA%7Bab"
            "%40fg%60%40kyu%40s_Ncq_%40crl%40uvO%7Dc%7C%40jTozbAlvMadq"
            "%40fu%5BasZpmi%40%7BeMjgf%40jdEhpJt%7BZ_%60Jlpz%40%22%7D"
        )

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
            user_agent = random.choice(ua_list)
            width = random.randint(1200, 1920)
            height = random.randint(700, 1080)
            self.context = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=str(dir_path),
                headless=True,
                args=["--no-sandbox"],
                user_agent=user_agent,
                viewport={"width": width, "height": height},
                locale="en-GB",
                timezone_id="Europe/London"
            )
            await self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => false});
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-GB','en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
                Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) =>
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters);
            """)

    async def scrape_area(self, area: str, max_price: int = None) -> list:
        await self._init()
        page = await self.context.new_page()
        try:
            # occasional detour to simulate browsing
            if random.random() < 0.3:
                for extra in ["/news","/why-buy","/help","/offers-for-sellers","/guides","/overseas"]:
                    await page.goto(f"https://www.rightmove.co.uk{extra}")
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(random.uniform(1,2))
                await page.goto("https://www.rightmove.co.uk")
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(1,2))

            # get locationIdentifier via autocomplete API
            identifier = None
            try:
                resp = await page.request.get(
                    f"https://www.rightmove.co.uk/api/_autocomplete?"
                    f"index=search_location&term={quote(area)}",
                    timeout=5000
                )
                data = await resp.json()
                if isinstance(data, list) and data and data[0].get("locationIdentifier"):
                    identifier = data[0]["locationIdentifier"]
            except:
                identifier = None

            # fallback to default polygon
            if not identifier:
                identifier = self._default_encoded

            # build URL with your example parameters
            price_q = max_price if max_price is not None else ""
            search_url = (
                "https://www.rightmove.co.uk/property-for-sale/find.html?"
                f"sortType=2&viewType=LIST&channel=BUY&index=0"
                f"&maxPrice={price_q}&radius=0.0"
                f"&locationIdentifier={quote(identifier, safe='')}"
                f"&tenureTypes=FREEHOLD&transactionType=BUY"
                f"&displayLocationIdentifier=undefined"
                f"&mustHave=parking"
                f"&dontShow=newHome,retirement,sharedOwnership,auction"
            )

            # navigate and wait
            await page.goto(search_url)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(random.uniform(2,4))

            # human-like scrolls
            for _ in range(random.randint(2,5)):
                scroll_height = await page.evaluate("document.body.scrollHeight")
                await page.evaluate(f"window.scrollTo(0, {random.randint(0, scroll_height)})")
                await asyncio.sleep(random.uniform(0.5,1.5))

            # gather cards via multiple selectors
            cards = []
            cards += await page.query_selector_all(".propertyCard")
            cards += await page.query_selector_all("li.component_property-card")
            cards += await page.query_selector_all("[data-testid='property-card']")

            results = []
            for card in cards:
                try:
                    cid = (await card.get_attribute("data-listing-id")) or (await card.get_attribute("id")) or ""
                    listing_id = cid.split("-")[-1]
                    title_el = (await card.query_selector(".propertyCard-title")) or (await card.query_selector("[data-testid='listing-title']"))
                    title = await title_el.inner_text() if title_el else ""
                    anchor = (await card.query_selector("a.propertyCard-link")) or (await card.query_selector("a"))
                    href = await anchor.get_attribute("href") if anchor else ""
                    url = f"https://www.rightmove.co.uk{href}" if href else ""
                    price_el = (await card.query_selector(".propertyCard-priceValue")) or (await card.query_selector("[data-testid='listing-price']"))
                    price_text = await price_el.text_content() if price_el else ""
                    price = int("".join(filter(str.isdigit, price_text))) if price_text else 0
                    beds = 0
                    for li in await card.query_selector_all(".propertyCard-details li"):
                        txt = await li.text_content() or ""
                        if "bed" in txt.lower():
                            try:
                                beds = int(txt.strip().split()[0])
                            except:
                                beds = 0
                            break
                    loc_el = (await card.query_selector(".propertyCard-address")) or (await card.query_selector("[data-testid='listing-address']"))
                    location = await loc_el.text_content() if loc_el else ""
                    desc_el = await card.query_selector(".propertyCard-description")
                    description = await desc_el.text_content() if desc_el else ""
                    screenshot = await card.screenshot(type="png")
                    results.append({
                        "id": listing_id,
                        "title": title,
                        "url": url,
                        "price": price,
                        "beds": beds,
                        "location": location,
                        "description": description,
                        "screenshot": screenshot
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