import asyncio
import random
from pathlib import Path
from playwright.async_api import async_playwright, Playwright, BrowserContext

class CaptchaError(Exception):
    pass

class RightmoveScraper:
    def __init__(self):
        self.playwright: Playwright = None
        self.context: BrowserContext = None
        self.backoff_count = 0

    async def _init(self):
        if not self.playwright:
            self.playwright = await async_playwright().start()
        if not self.context:
            data_dir = Path(__file__).parent / "userdata"
            data_dir.mkdir(exist_ok=True)
            # rotate user_data_dir daily
            today = datetime.datetime.now().strftime("%Y%m%d")
            dir_path = data_dir / today
            dir_path.mkdir(exist_ok=True)
            ua_list = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
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
                window.navigator.permissions.query = (parameters) => parameters.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : originalQuery(parameters);
            """)

    async def scrape_area(self, area: str) -> list:
        await self._init()
        page = await self.context.new_page()
        try:
            # random full-page extra navigation
            if random.random() < 0.3:
                extras = ["/news","/why-buy","/help","/offers-for-sellers","/guides","/overseas"]
                await page.goto(f"https://www.rightmove.co.uk{random.choice(extras)}")
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(2,4))
                await page.goto("https://www.rightmove.co.uk")
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(1,2))
            await page.goto("https://www.rightmove.co.uk")
            await asyncio.sleep(random.uniform(2,5))
            if "captcha" in page.url.lower():
                raise CaptchaError("Captcha page detected")
            # random mouse movements
            viewport = await page.viewport_size()
            for _ in range(random.randint(5,10)):
                x = random.randint(0, viewport["width"])
                y = random.randint(0, viewport["height"])
                await page.mouse.move(x, y, steps=random.randint(5,20))
                await asyncio.sleep(random.uniform(0.1,0.5))
            # random scrolling
            scroll_height = await page.evaluate("document.body.scrollHeight")
            for _ in range(random.randint(2,5)):
                pos = random.randint(0, scroll_height)
                await page.evaluate(f"window.scrollTo(0, {pos})")
                await asyncio.sleep(random.uniform(0.5,1.5))
            # set random localStorage to simulate returning user
            await page.evaluate("localStorage.setItem('visit_time', Date.now().toString())")
            # search
            try:
                await page.click('input[id="searchLocation"]')
                await page.fill('input[id="searchLocation"]', area)
            except:
                await page.fill('input[name="searchLocation"]', area)
            await asyncio.sleep(random.uniform(1,3))
            try:
                await page.click('button:has-text(\"Find properties\")')
            except:
                await page.click('button[type=\"submit\"]')
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(random.uniform(2,4))
            # more scrolling
            scroll_height = await page.evaluate("document.body.scrollHeight")
            await page.evaluate(f"window.scrollTo(0, {random.randint(0, scroll_height)})")
            await asyncio.sleep(random.uniform(1,2))
            cards = await page.query_selector_all(".propertyCard")
            results = []
            for card in cards:
                try:
                    cid = await card.get_attribute("id")
                    if not cid:
                        continue
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
                    details = await card.query_selector_all(".propertyCard-details li")
                    for li in details:
                        text = await li.text_content() or ""
                        if "bed" in text.lower():
                            try:
                                beds = int(text.strip().split()[0])
                            except:
                                beds = 0
                            break
                    loc_el = await card.query_selector(".propertyCard-address")
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