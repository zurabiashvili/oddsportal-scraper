"""
Helper script to explore OddsPortal page structure.
Run: python explore_page.py
Opens a results page, waits for load, saves HTML for inspection.
"""

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright


async def main():
    url = "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Collect API responses that might contain match data
        api_responses = []

        async def handle_response(response):
            if "oddsportal" in response.url and ("dat" in response.url or "feed" in response.url or "ajax" in response.url):
                try:
                    body = await response.text()
                    if len(body) < 50000 and ("score" in body.lower() or "match" in body.lower() or "event" in body.lower()):
                        api_responses.append((response.url, body[:3000]))
                except Exception:
                    pass

        page.on("response", handle_response)
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        # Accept cookies if banner appears
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=5000)
            await asyncio.sleep(2)
        except Exception:
            pass
        await asyncio.sleep(12)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(3)
        html = await page.content()

        # Save API responses for inspection
        if api_responses:
            api_file = Path(__file__).parent / "debug_api_samples.txt"
            with open(api_file, "w", encoding="utf-8") as f:
                for url, body in api_responses[:20]:
                    f.write(f"=== {url} ===\n{body}\n\n")
            print(f"Saved {len(api_responses)} API samples to {api_file.name}")

        await browser.close()

        out = Path(__file__).parent / "debug_results_page.html"
        out.write_text(html, encoding="utf-8")
        print(f"Saved HTML to {out}")
    print(f"Size: {len(html)} chars")
    print("\nSearch for: group, event, xeid, match, odds, Over, 1.5, Betfair")


if __name__ == "__main__":
    asyncio.run(main())
