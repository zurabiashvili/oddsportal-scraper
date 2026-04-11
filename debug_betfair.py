"""
Debug script: Open a match page, navigate to Over/Under, save content to inspect why Betfair isn't found.
Run: python debug_betfair.py
With login: set ODDSPORTAL_USER and ODDSPORTAL_PASS in .env - Betfair may only show when logged in.
"""
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from playwright.async_api import async_playwright


async def main():
    url = "https://www.oddsportal.com/football/england/league-one/luton-afc-wimbledon-0dpjEhy9/"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()
        
        # Login first - Betfair may only appear when logged in
        user, pw = os.getenv("ODDSPORTAL_USER", ""), os.getenv("ODDSPORTAL_PASS", "")
        if user and pw:
            try:
                await page.goto("https://www.oddsportal.com/login/", wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(3)
                await page.fill('#login-username-sign', user)
                await page.fill('#login-password-sign', pw)
                await page.click('button[name="login-submit"], button:has-text("Sign in")')
                await asyncio.sleep(5)
                print("Logged in, now loading match...")
            except Exception as e:
                print(f"Login skipped: {e}")
        
        await page.goto(url + "#/over-under", wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(3)
        # Switch to Decimal odds
        try:
            await page.locator("[data-testid='header-odds-formats-selector']").click(timeout=3000)
            await asyncio.sleep(1)
            await page.get_by_text("Decimal", exact=True).first.click(timeout=3000)
            await asyncio.sleep(3)
        except Exception:
            print("Could not switch to Decimal odds")
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
            await asyncio.sleep(1)
        except Exception:
            pass
        
        for tab in ["Over/Under", "Goals"]:
            try:
                await page.get_by_text(tab, exact=False).first.click(timeout=2000)
                await asyncio.sleep(2)
                break
            except Exception:
                pass
        
        for label in ["Over/Under +1.5", "+1.5"]:
            try:
                await page.get_by_text(label, exact=False).first.scroll_into_view_if_needed(timeout=3000)
                await page.get_by_text(label, exact=False).first.click(timeout=4000)
                await asyncio.sleep(3)
                break
            except Exception:
                pass
        
        for _ in range(8):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
        await asyncio.sleep(5)
        
        text = await page.evaluate("() => document.body.innerText")
        html = await page.content()
        
        out_dir = Path(__file__).parent
        (out_dir / "debug_page_text.txt").write_text(text, encoding="utf-8")
        (out_dir / "debug_match_for_betfair.html").write_text(html, encoding="utf-8")
        
        bf_pos = text.lower().find("betfair")
        lay_pos = text.lower().find("lay")
        print(f"Page text length: {len(text)} chars")
        print(f"'Betfair' found at index: {bf_pos}")
        print(f"'Lay' found at index: {lay_pos}")
        
        if bf_pos >= 0:
            excerpt = text[max(0, bf_pos - 50) : bf_pos + 500]
            print(f"\nExcerpt around Betfair:\n{excerpt}")
            (out_dir / "debug_betfair_excerpt.txt").write_text(excerpt, encoding="utf-8")
        
        if lay_pos >= 0 and bf_pos >= 0:
            chunk = text[lay_pos : lay_pos + 300]
            print(f"\nExcerpt from Lay onwards:\n{chunk}")
        
        await asyncio.sleep(3)
        await browser.close()
    
    print("\nSaved: debug_page_text.txt, debug_match_for_betfair.html, debug_betfair_excerpt.txt")


if __name__ == "__main__":
    asyncio.run(main())
