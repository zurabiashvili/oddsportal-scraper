"""
Debug script: Open a match page, navigate to Over/Under, save content to inspect why Betfair isn't found.

Uses the SAME browser session as the main scraper (storage_state + login_oddsportal).

Run:
  python debug_betfair.py
  python debug_betfair.py "https://www.oddsportal.com/football/h2h/..."
  # https:// can be omitted — "oddsportal.com/football/..." is accepted.
  # Optional: paste the full URL from the address bar including #... — opens that exact market.

Optional env (same idea as ScraperConfig):
  DEBUG_MARKET=ht|ft   (default ft)
  DEBUG_LINE=0.5|1.5|2.5  (default 1.5)

Credentials: ODDSPORTAL_USER / ODDSPORTAL_PASS in .env — Betfair often only appears when logged in.
"""
import asyncio
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

from playwright.async_api import async_playwright

import config
from scraper import (
    _click_oddsportal_over_under_market_tab,
    _extract_betfair_lay_from_tables,
    _get_betting_exchanges_subtree_text,
    _normalize_oddsportal_netloc,
    _parse_betfair_exchange_lay_odds,
    login_oddsportal,
)


def _ensure_https_scheme(url: str) -> str:
    """Playwright requires a full URL; users often paste host/path without https://."""
    u = url.strip()
    if u.startswith("//"):
        return "https:" + u
    if not re.match(r"^https?://", u, re.I):
        return "https://" + u.lstrip("/")
    return u


async def main():
    raw_arg = (sys.argv[1] if len(sys.argv) > 1 else "").strip()
    if raw_arg:
        _prev_arg = raw_arg
        raw_arg = _ensure_https_scheme(raw_arg)
        raw_arg = _normalize_oddsportal_netloc(raw_arg)
        if _prev_arg != raw_arg:
            print(f"Normalized URL:\n  was: {_prev_arg}\n  now: {raw_arg}\n", flush=True)
    if not raw_arg:
        base = "https://www.oddsportal.com/football/england/league-one/luton-afc-wimbledon-0dpjEhy9/"
        nav_url = f"{base}#/over-under"
    else:
        # Drop query string; keep #fragment — OddsPortal uses hashes like #ppKoQ2Ga:over-under;3;0.50;0
        raw = raw_arg.split("?")[0].strip()
        path_only = raw.split("#")[0].rstrip("/")
        if "#" in raw and raw.split("#", 1)[1].strip():
            nav_url = raw
        else:
            nav_url = f"{path_only}#/over-under"
        base = path_only

    if raw_arg:
        low = raw_arg.lower()
        if "paste" in low and ("url" in low or "full" in low or "hash" in low):
            print(
                "\nError: Replace the placeholder with a real OddsPortal link from your browser.\n"
                "Example:\n"
                '  python debug_betfair.py "https://www.oddsportal.com/football/h2h/bolton-Or1bBrWD/cardiff-hO8wh6aP/"\n',
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)
        nav_url = _ensure_https_scheme(nav_url)
        parsed = urlparse(nav_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            print(
                f"\nError: Not a valid http(s) URL: {nav_url!r}\n",
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)
        if "oddsportal.com" not in (parsed.netloc or "").lower():
            print(
                f"\nWarning: Expected an oddsportal.com URL (got host: {parsed.netloc}).\n",
                flush=True,
            )

    market = (os.getenv("DEBUG_MARKET", "ft") or "ft").lower().strip()
    if market not in ("ft", "ht"):
        market = "ft"
    line_s = (os.getenv("DEBUG_LINE", "1.5") or "1.5").strip()
    try:
        line = float(line_s)
    except ValueError:
        line = 1.5

    out_dir = Path(__file__).parent
    auth_rel = getattr(config, "AUTH_STATE_FILE", "output/.auth/oddsportal_state.json")
    auth_path = out_dir / auth_rel
    auth_path.parent.mkdir(parents=True, exist_ok=True)

    # Always load #/over-under — browser copy-paste hashes (e.g. #hxEBot8d) often leave you on 1X2 + 1st Half.
    ou_route = f"{base}#/over-under"
    print(
        f"Will open Over/Under market route:\n  {ou_route}\n"
        f"(base path: {base})\n"
        f"DEBUG_MARKET={market} DEBUG_LINE={line}",
        flush=True,
    )
    print(f"Session file: {auth_path} ({'found' if auth_path.exists() else 'MISSING — run main scraper login once'})", flush=True)
    print(f"Credentials in .env: {'yes' if getattr(config, 'ODDSPORTAL_USER', '') else 'no'}\n", flush=True)

    async with async_playwright() as p:
        headless = getattr(config, "HEADLESS", False)
        launch_kw = dict(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            browser = await p.chromium.launch(**launch_kw, channel="chrome")
        except Exception:
            browser = await p.chromium.launch(**launch_kw)

        ctx_kw = dict(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1365, "height": 900},
            locale="en-GB",
        )
        if auth_path.exists():
            ctx_kw["storage_state"] = str(auth_path)
            print("Loaded storage_state (cookies) like main scraper.", flush=True)

        context = await browser.new_context(**ctx_kw)
        page = await context.new_page()

        await login_oddsportal(
            page,
            getattr(config, "ODDSPORTAL_USER", ""),
            getattr(config, "ODDSPORTAL_PASS", ""),
        )

        await page.goto(ou_route, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(10)

        try:
            await page.click("#onetrust-accept-btn-handler", timeout=4000)
            await asyncio.sleep(1)
        except Exception:
            pass

        try:
            fmt_sel = page.locator("[data-testid='header-odds-formats-selector']")
            await fmt_sel.click(timeout=3000)
            await asyncio.sleep(1)
            await page.get_by_text("Decimal", exact=True).first.click(timeout=3000)
            await asyncio.sleep(3)
        except Exception:
            print("Could not switch to Decimal odds", flush=True)

        ou_ok = await _click_oddsportal_over_under_market_tab(page)
        if not ou_ok:
            try:
                await page.get_by_text("Goals", exact=False).first.click(timeout=3000)
                await asyncio.sleep(2)
            except Exception:
                pass
            print("Warning: Over/Under market tab may not be active — check debug_betfair.png", flush=True)
        else:
            print("Clicked Over/Under market tab.", flush=True)

        if market == "ft":
            for tab_label in ["Full Time"]:
                try:
                    await page.get_by_text(tab_label, exact=True).first.click(timeout=2000)
                    await asyncio.sleep(2)
                    break
                except Exception:
                    pass
            try:
                await page.get_by_role("tab", name=re.compile(r"^\s*Full\s+Time\s*$", re.I)).first.click(
                    timeout=3000
                )
                await asyncio.sleep(2)
            except Exception:
                pass
        else:
            for tab_label in ["1st Half", "Half Time"]:
                try:
                    await page.get_by_text(tab_label, exact=True).first.click(timeout=2000)
                    await asyncio.sleep(2)
                    break
                except Exception:
                    pass

        for label in [
            f"Over/Under +{line}",
            f"Over/Under +{line} Goals",
            f"+{line}",
            f"+{line} Goals",
        ]:
            try:
                el = page.get_by_text(label, exact=False).first
                await el.scroll_into_view_if_needed(timeout=3000)
                await el.click(timeout=4000)
                await asyncio.sleep(5)
                break
            except Exception:
                pass

        try:
            await page.get_by_text("Betting Exchanges", exact=False).first.scroll_into_view_if_needed(
                timeout=5000
            )
            await asyncio.sleep(2)
        except Exception:
            pass

        for _ in range(8):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
        await asyncio.sleep(5)

        text = await page.evaluate("() => document.body.innerText")
        html = await page.content()

        bf_subtree = await _get_betting_exchanges_subtree_text(page)
        lo_tb, lu_tb = await _extract_betfair_lay_from_tables(page)

        (out_dir / "debug_page_text.txt").write_text(text, encoding="utf-8")
        (out_dir / "debug_match_for_betfair.html").write_text(html, encoding="utf-8")

        low = text.lower()
        bf_pos = low.find("betfair")
        be_pos = low.rfind("betting exchanges")
        lay_count = len(re.findall(r"\b[Ll]ay\b", text))
        lo_body, lu_body = _parse_betfair_exchange_lay_odds(text)
        lo_sub, lu_sub = _parse_betfair_exchange_lay_odds(bf_subtree or "")

        lines = [
            "=== OddsPortal Betfair debug report ===",
            f"opened_url={ou_route}",
            f"market={market} line={line}",
            f"body.innerText length={len(text)}",
            f"'betfair' in page (first index)={bf_pos}",
            f"'betting exchanges' last index={be_pos}",
            f"word Lay count ~={lay_count}",
            "",
            "Parse _parse_betfair_exchange_lay_odds(body):",
            f"  betfair_lay_over={lo_body!r} betfair_lay_under={lu_body!r}",
            "",
            "Parse from betting-exchanges subtree scan:",
            f"  subtree_len={len(bf_subtree or '')}",
            f"  betfair_lay_over={lo_sub!r} betfair_lay_under={lu_sub!r}",
            "",
            "_extract_betfair_lay_from_tables:",
            f"  betfair_lay_over={lo_tb!r} betfair_lay_under={lu_tb!r}",
            "",
            "Interpretation:",
        ]
        if len(text) < 1500:
            lines.append("- VERY LITTLE TEXT: bot block, CAPTCHA, or page did not render. Try HEADLESS=False in config.py.")
        if bf_pos < 0:
            lines.append("- NO 'betfair' in visible text: often NOT LOGGED IN or region blocks exchange odds.")
        elif lo_body is None and lu_body is None and lo_tb is None and lu_tb is None:
            lines.append("- Betfair mentioned but parser got no Lay prices: send debug_page_text.txt (or HTML) for regex update.")
        else:
            lines.append("- Parser produced at least one Lay value — if full scrape still empty, compare wait times / headless mode.")

        report = "\n".join(lines) + "\n"
        (out_dir / "debug_report.txt").write_text(report, encoding="utf-8")

        print(report, flush=True)

        if bf_pos >= 0:
            excerpt = text[max(0, bf_pos - 50) : bf_pos + 800]
            print(f"\n--- Excerpt around first 'Betfair' ---\n{excerpt}\n", flush=True)
            (out_dir / "debug_betfair_excerpt.txt").write_text(excerpt, encoding="utf-8")

        try:
            await page.screenshot(path=str(out_dir / "debug_betfair.png"), full_page=True)
            print(f"Screenshot: {out_dir / 'debug_betfair.png'}", flush=True)
        except Exception as ex:
            print(f"Screenshot failed: {ex}", flush=True)

        await browser.close()

    print(
        "\nFiles written in project folder:",
        "debug_report.txt (read this first), debug_page_text.txt, debug_match_for_betfair.html, "
        "debug_betfair_excerpt.txt, debug_betfair.png",
        sep="\n",
    )


if __name__ == "__main__":
    asyncio.run(main())
