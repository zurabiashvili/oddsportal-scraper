"""
OddsPortal Historical Matches Scraper

Scrapes archived football matches from OddsPortal:
- Date, teams, full-time and half-time scores
- Betfair Lay odds for Over/Under 1.5 goals market

Uses page scraping (JS + HTML parsing) to extract scores and Betfair Lay odds.
"""

import asyncio
import csv
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

import config
from scraper_config import ScraperConfig

# Stop flag - set by GUI when Stop button clicked
_stop_requested = False


def request_stop() -> None:
    """Signal the scraper to stop after current match/page."""
    global _stop_requested
    _stop_requested = True


def _check_stop() -> bool:
    """Return True if stop was requested."""
    return _stop_requested


@dataclass
class MatchData:
    """Single match record. Odds columns are for the selected market (FT/HT) and line (0.5, 1.5, 2.5)."""
    date: str
    home_team: str
    away_team: str
    full_time_home: str
    full_time_away: str
    half_time_home: str
    half_time_away: str
    half_time_total: str
    over_odds: str | None
    under_odds: str | None
    betfair_lay_over: str | None
    betfair_lay_under: str | None
    match_url: str
    league: str


def _date_sort_key(m: MatchData) -> tuple:
    """Return sort key for chronological order (oldest first). Unparseable dates go last."""
    s = re.sub(r"\s+", " ", (m.date or "").strip().replace("\n", " "))[:50]
    for fmt in ("%d %b %Y, %H:%M", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return (datetime.strptime(s.strip(), fmt),)
        except (ValueError, TypeError):
            continue
    return (datetime.max,)  # Unparseable: put at end


def _sort_results_chronological(results: list) -> None:
    """Sort match results chronologically (oldest match first)."""
    results.sort(key=_date_sort_key)


def _dict_match_date_sort_key(m: dict) -> datetime:
    """Sort key for match dicts from results page (uses date string). Unparseable → oldest."""
    s = re.sub(r"\s+", " ", (m.get("date") or "").strip().replace("\n", " "))[:80]
    for fmt in ("%d %b %Y, %H:%M", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except (ValueError, TypeError):
            continue
    return datetime.min


def _parse_betfair_exchange_lay_odds(page_text: str) -> tuple[str | None, str | None]:
    """Lay Over/Under from the Betting Exchanges → Betfair table only (after Back row, not main bookmakers)."""
    if not page_text:
        return None, None
    low = page_text.lower()
    idx = low.find("betting exchanges")
    if idx < 0:
        idx = low.find("betfair exchange")
    if idx < 0:
        return None, None
    block = page_text[idx : idx + 6500]
    # Use the last 'Lay' in this block so we don't pick bookmaker rows above the exchange widget.
    lay_pos = block.rfind("Lay")
    if lay_pos < 0:
        return None, None
    tail = block[lay_pos : lay_pos + 900]

    def _parse_o_u(o_raw: str, u_raw: str) -> tuple[str | None, str | None]:
        o_raw, u_raw = o_raw.strip(), u_raw.strip()
        lo = None
        if o_raw and o_raw not in ("-", "–", "−", "—") and re.match(r"\d", o_raw):
            try:
                if 1.01 <= float(o_raw) <= 50:
                    lo = o_raw
            except ValueError:
                pass
        lu = None
        try:
            if u_raw and 1.01 <= float(u_raw) <= 50:
                lu = u_raw
        except ValueError:
            pass
        return lo, lu

    for pat in (
        r"Lay\s+Over\s*([-–−]|\d+\.\d+)\s+Under\s*(\d+\.\d+)",
        r"Lay\s+Over\s*([-–−]|\d+\.\d+)\s*\n\s*Under\s*(\d+\.\d+)",
        r"Lay\s*\n\s*Over\s*\n\s*([-–−]|\d+\.\d+)\s*\n\s*Under\s*\n\s*(\d+\.\d+)",
    ):
        m = re.search(pat, tail, re.I | re.DOTALL)
        if m:
            return _parse_o_u(m.group(1), m.group(2))
    lay_m = re.search(r"(?:^|\n)\s*Lay\s+([^\n]+)", tail, re.I | re.MULTILINE)
    if not lay_m:
        return None, None
    row = lay_m.group(1).strip()
    pair = re.match(r"^\s*([\d.\-–−]+)\s+([\d.\-–−]+)\s*$", row)
    if pair:
        a, b = pair.group(1).strip(), pair.group(2).strip()

        def _cell(x: str) -> str | None:
            if x in ("-", "–", "−", "—"):
                return None
            try:
                v = float(x)
                return x if 1.01 <= v <= 50 else None
            except ValueError:
                return None

        return _cell(a), _cell(b)
    nums = re.findall(r"\d+\.\d+", row)
    if not nums:
        return None, None
    if len(nums) == 1:
        if re.match(r"^[\s\-–−—]", row) or re.search(r"[-–−—]", row.split(nums[0])[0]):
            return None, nums[0]
        return nums[0], None
    if len(nums) >= 2:
        try:
            fo, fu = float(nums[0]), float(nums[1])
            if 1.01 <= fo <= 50 and 1.01 <= fu <= 50:
                return str(nums[0]), str(nums[1])
        except ValueError:
            pass
    return None, None


def _fill_scores_and_date_from_page_text(page_text: str, result: dict) -> None:
    """Fill date / FT / HT scores when innerText regex in evaluate missed (spacing, locale)."""
    if not page_text:
        return
    if not (result.get("date") or "").strip():
        for pat in (
            r"\b(\d{1,2}\s+[A-Za-z]{3}\s+20\d{2}(?:\s*,\s*\d{1,2}:\d{2})?)\b",
            r"\b(\d{1,2}\s+[A-Za-z]{3}\s+20\d{2})\b",
        ):
            m = re.search(pat, page_text)
            if m:
                result["date"] = m.group(1).strip()
                break
    if not (result.get("score_ft") or "").strip():
        fm = re.search(
            r"Final\s+result\s*(\d{1,2})\s*:\s*(\d{1,2})\s*\(\s*(\d{1,2})\s*:\s*(\d{1,2})\s*,\s*(\d{1,2})\s*:\s*(\d{1,2})\s*\)",
            page_text,
            re.I,
        )
        if fm:
            result["score_ft"] = f"{fm.group(1)}:{fm.group(2)}"
            if not (result.get("score_ht") or "").strip():
                result["score_ht"] = f"{fm.group(3)}:{fm.group(4)}"
        else:
            fm2 = re.search(r"Final\s+result\s*(\d{1,2})\s*:\s*(\d{1,2})", page_text, re.I)
            if fm2:
                result["score_ft"] = f"{fm2.group(1)}:{fm2.group(2)}"
    if not (result.get("score_ht") or "").strip():
        hm = re.search(
            r"\(\s*(\d{1,2})\s*:\s*(\d{1,2})\s*,\s*\d{1,2}\s*:\s*\d{1,2}\s*\)",
            page_text,
        )
        if hm:
            result["score_ht"] = f"{hm.group(1)}:{hm.group(2)}"


def _fill_scores_and_date_from_html(html: str, result: dict) -> None:
    """Parse scores/date from raw HTML when visible text is empty or delayed (SPA)."""
    if not html:
        return
    t = html.replace("&nbsp;", " ").replace("&#x3A;", ":")
    if not (result.get("score_ft") or "").strip():
        fm = re.search(
            r"Final\s+result\s*(\d{1,2})\s*:\s*(\d{1,2})\s*\(\s*(\d{1,2})\s*:\s*(\d{1,2})\s*,\s*(\d{1,2})\s*:\s*(\d{1,2})\s*\)",
            t,
            re.I,
        )
        if fm:
            result["score_ft"] = f"{fm.group(1)}:{fm.group(2)}"
            if not (result.get("score_ht") or "").strip():
                result["score_ht"] = f"{fm.group(3)}:{fm.group(4)}"
        else:
            fm2 = re.search(r"Final\s+result\s*(\d{1,2})\s*:\s*(\d{1,2})", t, re.I)
            if fm2:
                result["score_ft"] = f"{fm2.group(1)}:{fm2.group(2)}"
    if not (result.get("score_ht") or "").strip():
        hm = re.search(
            r"\(\s*(\d{1,2})\s*:\s*(\d{1,2})\s*,\s*\d{1,2}\s*:\s*\d{1,2}\s*\)",
            t,
        )
        if hm:
            result["score_ht"] = f"{hm.group(1)}:{hm.group(2)}"
    if not (result.get("date") or "").strip():
        m = re.search(
            r"<title>[^<]*(\d{1,2}\s+[A-Za-z]{3}\s+20\d{2})[^<]*</title>",
            t,
            re.I,
        )
        if m:
            result["date"] = m.group(1).strip()


def _parse_score(score: str) -> tuple[str, str]:
    """Parse '2:0' into (home, away). Returns ('', '') if invalid."""
    if not score:
        return "", ""
    parts = re.split(r"[\-:]", str(score).strip())
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return "", ""


def _half_time_total_goals(ht_home: str, ht_away: str) -> str:
    """Sum of first-half goals for export column. Empty string if either side missing."""
    try:
        if not (str(ht_home).strip() and str(ht_away).strip()):
            return ""
        return str(int(ht_home) + int(ht_away))
    except (ValueError, TypeError):
        return ""


def _is_match_page_url(href: str) -> bool:
    """True only for single-match OddsPortal URLs (not league hub, outrights, standings)."""
    if not href or not str(href).strip():
        return False
    try:
        p = urlparse(str(href).strip())
        path = (p.path or "").strip("/")
        segments = [s for s in path.split("/") if s]
        # Hub: /football/england/league-one → 3 segments. Match adds /slug-with-id → 4+.
        if len(segments) < 4:
            return False
        if segments[0].lower() not in ("football", "soccer"):
            return False
        low = path.lower()
        if "/outrights" in low or "/standings" in low or "/draw/" in low:
            return False
        last = segments[-1].lower()
        if last in ("results", "outrights", "standings", "fixtures", "draw"):
            return False
        parts = last.split("-")
        if len(parts) < 3:
            return False
        id_part = parts[-1]
        if not re.match(r"^[a-z0-9]{4,}$", id_part, re.I):
            return False
        return True
    except Exception:
        return False


# Display tweaks for teams that don't round-trip from URL slugs
_TEAM_DISPLAY = {
    "g a eagles": "G.A. Eagles",
    "az alkmaar": "AZ Alkmaar",
    "nac breda": "NAC Breda",
    "fc volendam": "FC Volendam",
    "willem ii": "Willem II",
    "afc wimbledon": "AFC Wimbledon",
    "bradford city": "Bradford City",
    "lincoln city": "Lincoln City",
    "port vale": "Port Vale",
    "stockport county": "Stockport County",
    "milton keynes dons": "Milton Keynes Dons",
    "manchester city": "Manchester City",
    "manchester utd": "Manchester Utd",
    "newcastle utd": "Newcastle Utd",
}


def _slug_segment_to_team_name(seg: str) -> str:
    """One URL segment is 'name-parts-teamId'; strip trailing OddsPortal id token."""
    seg = (seg or "").strip()
    if not seg:
        return "?"
    parts = seg.split("-")
    if len(parts) >= 2:
        last = parts[-1]
        if re.match(r"^[a-zA-Z0-9]{4,}$", last):
            return "-".join(parts[:-1]).replace("-", " ").title()
    return seg.replace("-", " ").title()


def _apply_team_display(home: str, away: str) -> tuple[str, str]:
    for k, v in _TEAM_DISPLAY.items():
        if home.lower() == k:
            home = v
        if away.lower() == k:
            away = v
    return home, away


def _split_legacy_combined_slug(team_slug: str) -> tuple[str, str]:
    """Legacy URLs: one slug 'home-away-...' without /h2h/. Uses compound list + midpoint split."""
    team_slug = team_slug.strip()
    if not team_slug:
        return "?", "?"
    compound_teams = [
        "g-a-eagles", "az-alkmaar", "sparta-rotterdam", "nac-breda",
        "fc-volendam", "almere-city", "willem-ii", "psv-eindhoven",
        "port-vale", "stockport-county", "lincoln-city", "afc-wimbledon",
        "milton-keynes-dons", "bradford-city",
    ]
    home, away = "?", "?"
    for ct in compound_teams:
        if team_slug.startswith(ct + "-"):
            rest = team_slug[len(ct) + 1 :]
            home = ct.replace("-", " ").title()
            away = rest.replace("-", " ").title() if rest else "?"
            break
        if team_slug.endswith("-" + ct):
            rest = team_slug[: -(len(ct) + 1)]
            home = rest.replace("-", " ").title() if rest else "?"
            away = ct.replace("-", " ").title()
            break
    if home == "?":
        teams = team_slug.replace("-", " ").split()
        home = " ".join(teams[: len(teams) // 2]).title() if teams else "?"
        away = " ".join(teams[len(teams) // 2 :]).title() if teams else "?"
    return _apply_team_display(home, away)


def teams_from_match_url(href: str) -> tuple[str, str]:
    """Home/away display names from OddsPortal match URL (h2h or legacy league folder)."""
    href = (href or "").strip().split("?")[0].split("#")[0]
    try:
        path = urlparse(href).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if len(parts) < 3:
            return "?", "?"
        if parts[0].lower() not in ("football", "soccer"):
            return "?", "?"
        if len(parts) >= 4 and parts[1].lower() == "h2h":
            home = _slug_segment_to_team_name(parts[2])
            away = _slug_segment_to_team_name(parts[3])
            return _apply_team_display(home, away)
        last = parts[-1]
        segs = last.split("-")
        if len(segs) >= 2 and re.match(r"^[a-zA-Z0-9]{4,}$", segs[-1], re.I):
            team_slug = "-".join(segs[:-1])
        else:
            team_slug = last
        return _split_legacy_combined_slug(team_slug)
    except Exception:
        return "?", "?"


async def _dismiss_blocking_modals(page) -> None:
    """Close cookie/bookmaker overlays that intercept clicks (e.g. overlay-bookie-modal)."""
    for _ in range(3):
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.25)
    for sel in (
        ".overlay-bookie-modal button",
        ".overlay-bookie-modal [class*='close']",
        # Close is often an SVG in a span, not a <button>
        ".overlay-bookie-modal svg.cursor-pointer",
        ".overlay-bookie-modal span:has(svg.cursor-pointer)",
        "[class*='bookie-modal'] button",
        "button:has-text('Continue')",
        "button:has-text('Not now')",
        "button:has-text('Close')",
    ):
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=800):
                await loc.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass
    try:
        await page.locator(".overlay-bookie-modal").first.wait_for(state="hidden", timeout=5000)
    except Exception:
        pass


async def _strip_blocking_overlays_js(page) -> None:
    """Last resort: remove overlay DOM nodes that keep inputs non-visible (promo modal, dimmers)."""
    try:
        await page.evaluate(
            """() => {
            document.querySelectorAll(
              '[class*="overlay-bookie"],[class*="bookie-modal"],.onetrust-pc-dark-filter,[id*="modal-overlay"],[class*="modal-mask"]'
            ).forEach((el) => { try { el.remove(); } catch (e) {} });
        }"""
        )
    except Exception:
        pass


def _is_login_page_url(url: str) -> bool:
    """True if URL path is the login screen (saved session often redirects away)."""
    try:
        return "/login" in (urlparse(url).path or "").lower()
    except Exception:
        return False


async def login_oddsportal(page, username: str, password: str) -> bool:
    """Log in to OddsPortal. Returns True on success."""
    if not username or not password:
        print("  Skipping login (no credentials in .env)")
        return False
    try:
        await page.goto("https://www.oddsportal.com/login/", wait_until="domcontentloaded", timeout=45_000)
        await asyncio.sleep(3)
        # Valid cookies: OddsPortal often redirects off /login/ — avoid waiting on a hidden form.
        if not _is_login_page_url(page.url):
            print("  Already logged in (saved session — skipped login form).")
            return True
        # Accept cookie banner (may block the form)
        for _ in range(3):
            try:
                btn = page.locator(
                    "#onetrust-accept-btn-handler, button:has-text('Accept'), button:has-text('Allow'), [id*='accept']"
                ).first
                await btn.click(timeout=5000)
                await asyncio.sleep(2)
                break
            except Exception:
                await asyncio.sleep(1)
        await _dismiss_blocking_modals(page)
        await _strip_blocking_overlays_js(page)
        await asyncio.sleep(0.5)
        await _dismiss_blocking_modals(page)
        await _strip_blocking_overlays_js(page)
        # Wait for login form — retry dismiss if overlays still block visibility
        user_loc = page.locator('input[name="username"], #login-username-sign').first
        for attempt in range(4):
            try:
                await user_loc.wait_for(state="visible", timeout=15_000)
                break
            except Exception:
                print(
                    f"  Login form not visible yet (attempt {attempt + 1}/4) — closing overlays...",
                    flush=True,
                )
                await _dismiss_blocking_modals(page)
                await _strip_blocking_overlays_js(page)
                try:
                    await page.evaluate("window.scrollTo(0, 0)")
                except Exception:
                    pass
                await asyncio.sleep(1.5)
        else:
            await user_loc.wait_for(state="visible", timeout=5_000)

        await asyncio.sleep(0.5)
        await page.fill('input[name="username"], #login-username-sign', username)
        await page.fill('input[name="password"], #login-password-sign', password)
        await _dismiss_blocking_modals(page)
        submit = page.locator('input[name="login-submit"]').first
        await submit.scroll_into_view_if_needed(timeout=10_000)
        await asyncio.sleep(0.5)
        try:
            await submit.click(timeout=20_000)
        except Exception:
            await _dismiss_blocking_modals(page)
            await submit.click(timeout=15_000, force=True)
        await asyncio.sleep(5)
        url = page.url
        if not _is_login_page_url(url):
            print("  Logged in successfully.")
            return True
        print("  Login submitted (may need verification).")
        return True
    except Exception as e:
        print(f"  Login failed: {e}")
        return False


def _is_likely_score(val: str) -> bool:
    """Exclude kickoff times (e.g. 16:00, 14:30) from scores."""
    try:
        parts = re.split(r"[:\-]", val)
        if len(parts) != 2:
            return False
        a, b = int(parts[0]), int(parts[1])
        if a > 5 or b > 5:
            return False  # Scores rarely exceed 5 each
        if b in (15, 30, 45):
            return False  # Minutes in time
        if a > 12 and b == 0:
            return False  # e.g. 16:00
        return True
    except (ValueError, IndexError):
        return False


def parse_results_page_html(html: str, base_url: str, league_name: str) -> list[dict]:
    """Parse results page and extract match links with date, teams, scores."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    matches = []
    seen_urls = set()
    # Optional ?query breaks a strict $ anchor; IDs are usually 6–8 chars
    match_link_re = re.compile(
        r"/(?:football|soccer)/[^/]+/[^/]+/[a-z0-9]+(?:-[a-z0-9]+)+-[a-zA-Z0-9]{3,}/?"
    )

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip().split("?")[0].split("#")[0]
        if not href or not match_link_re.search(href):
            continue
        if not href.startswith("http"):
            href = f"https://www.oddsportal.com{href}" if href.startswith("/") else href
        if href in seen_urls:
            continue
        if not _is_match_page_url(href):
            continue
        seen_urls.add(href)

        home, away = teams_from_match_url(href)

        parent = a.parent
        date_val, score_ft, score_ht = "", "", ""
        for _ in range(10):
            if parent is None:
                break
            text = parent.get_text(separator="\n").strip()
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            for line in lines:
                if re.match(r"[\d]{2}[/.-][\d]{2}[/.-][\d]{2,4}|[\d]{1,2}\s+[A-Za-z]+\s+[\d]{4}", line):
                    date_val = line
                elif re.match(r"^\(\d+[:\-]\d+\)", line):
                    score_ht = line.strip("()").replace("-", ":")
                elif re.match(r"^\d{1,2}[:\-]\d{1,2}$", line) and _is_likely_score(line):
                    score_ft = line.replace("-", ":")
            if date_val or score_ft or len(lines) >= 2:
                break
            parent = getattr(parent, "parent", None)

        # Override with link text if it has " - " (team names)
        link_text = a.get_text(strip=True)
        if " - " in link_text and len(link_text) < 80:
            parts = link_text.split(" - ", 1)
            if len(parts) == 2:
                home, away = parts[0].strip(), parts[1].strip()

        matches.append({
            "date": date_val,
            "home_team": home or "?",
            "away_team": away or "?",
            "score_ft": score_ft,
            "score_ht": score_ht,
            "match_url": href,
            "league": league_name,
        })

    return matches


def _merge_match_rows_by_url(rows: list[dict], extra: list[dict]) -> list[dict]:
    seen = {m["match_url"] for m in rows}
    for m in extra:
        u = m.get("match_url", "")
        if u and u not in seen:
            seen.add(u)
            rows.append(m)
    return rows


def extract_matches_from_embedded_urls(html: str, league_url: str, league_name: str) -> list[dict]:
    """OddsPortal embeds match URLs in JSON/scripts; they may not appear as <a href> in the DOM."""
    if not html:
        return []
    variants = [
        html,
        html.replace("\\/", "/"),
        html.replace("\\\\/", "/"),
        html.replace("\\u002f", "/"),
    ]
    raw_urls: set[str] = set()
    for blob in variants:
        for pat in (
            r"https://www\.oddsportal\.com/(?:football|soccer)/[^\s\"'<>]+",
            r"https://oddsportal\.com/(?:football|soccer)/[^\s\"'<>]+",
            r"https://m\.oddsportal\.com/(?:football|soccer)/[^\s\"'<>]+",
            r'"(?:https:)?//www\.oddsportal\.com/(?:football|soccer)/[^"]+',
            r"'(?:https:)?//www\.oddsportal\.com/(?:football|soccer)/[^']+",
        ):
            raw_urls.update(re.findall(pat, blob, flags=re.I))
        # OddsPortal often embeds paths without domain, e.g. "/football/england/league-one-2025-2026/team-team-AbCdEfGh/"
        for m in re.finditer(
            r"/(?:football|soccer)/[^/]+/[^/]+/[a-z0-9]+(?:-[a-z0-9]+)+-[a-zA-Z0-9]{4,}/?",
            blob,
            flags=re.I,
        ):
            raw_urls.add("https://www.oddsportal.com" + m.group(0).rstrip("/"))
    out: list[dict] = []
    seen: set[str] = set()
    for raw in raw_urls:
        href = raw.strip().strip('"').strip("'")
        href = href.split("?")[0].split("#")[0].rstrip(",").rstrip("\\")
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/") and "/football/" in href:
            href = "https://www.oddsportal.com" + href
        if not href.startswith("http"):
            continue
        if league_url and not _match_url_belongs_to_league(href, league_url):
            continue
        if not _is_match_page_url(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        home, away = teams_from_match_url(href)
        out.append(
            {
                "date": "",
                "home_team": home,
                "away_team": away,
                "score_ft": "",
                "score_ht": "",
                "match_url": href,
                "league": league_name,
            }
        )
    if not out and raw_urls:
        samp = list(raw_urls)[:3]
        ok_league = sum(1 for u in raw_urls if _match_url_belongs_to_league(u, league_url))
        ok_match = sum(1 for u in raw_urls if _match_url_belongs_to_league(u, league_url) and _is_match_page_url(u))
        print(
            f"  Debug: {len(raw_urls)} raw URL strings | {ok_league} pass league slug | {ok_match} pass match-page rules. "
            f"Samples: {samp!r}",
            flush=True,
        )
    return out


async def collect_matches_from_dom(page, league_url: str, league_name: str) -> list[dict]:
    """Collect match URLs from live DOM (OddsPortal SPA often omits links from static page.content())."""
    raw = await page.evaluate(
        """() => {
        const seen = new Set();
        const urls = [];
        for (const a of document.querySelectorAll('a[href]')) {
            let h = a.getAttribute('href') || '';
            if (h.startsWith('/')) h = 'https://www.oddsportal.com' + h;
            else if (!h.startsWith('http')) continue;
            h = h.split('?')[0].split('#')[0];
            if (!h.includes('/football/') && !h.includes('/soccer/')) continue;
            const lo = h.toLowerCase();
            if (lo.includes('/outrights') || lo.includes('/standings') || lo.includes('/draw/')) continue;
            if (seen.has(h)) continue;
            seen.add(h);
            urls.push(h);
        }
        return urls;
    }"""
    )
    matches: list[dict] = []
    seen_u: set[str] = set()
    for href in raw or []:
        href = str(href).strip()
        if not href.startswith("http"):
            href = f"https://www.oddsportal.com{href}" if href.startswith("/") else href
        if league_url and not _match_url_belongs_to_league(href, league_url):
            continue
        if not _is_match_page_url(href):
            continue
        if href in seen_u:
            continue
        seen_u.add(href)
        home, away = teams_from_match_url(href)
        matches.append(
            {
                "date": "",
                "home_team": home,
                "away_team": away,
                "score_ft": "",
                "score_ht": "",
                "match_url": href,
                "league": league_name,
            }
        )
    return matches


async def collect_h2h_matches_ordered(page, league_url: str, league_name: str) -> list[dict]:
    """H2h match links in stable document order (same as earlier scraper versions)."""
    raw = await page.evaluate(
        """() => {
        const root = document.getElementById('app') || document.querySelector('main') || document.body;
        const out = [];
        const seen = new Set();
        for (const a of root.querySelectorAll('a[href]')) {
            const href = (a.getAttribute('href') || '').toLowerCase();
            if (!href.includes('/h2h/')) continue;
            let full = a.href.split('?')[0].split('#')[0];
            if (!full.includes('oddsportal.com')) continue;
            if (seen.has(full)) continue;
            seen.add(full);
            out.push(full);
        }
        return out;
    }"""
    )
    out: list[dict] = []
    for href in raw or []:
        href = str(href).strip()
        if league_url and not _match_url_belongs_to_league(href, league_url):
            continue
        if not _is_match_page_url(href):
            continue
        home, away = teams_from_match_url(href)
        out.append(
            {
                "date": "",
                "home_team": home,
                "away_team": away,
                "score_ft": "",
                "score_ht": "",
                "match_url": href,
                "league": league_name,
            }
        )
    return out


async def scrape_match(
    page, url: str,
    market: str = "ft", line: float = 1.5,
    screenshot_path: Path | None = None,
) -> dict:
    """Load match page, extract date & Betfair odds via page scraping (no API)."""
    result = {
        "date": "", "score_ft": "", "score_ht": "",
        "over_odds": None, "under_odds": None,
        "betfair_lay_over": None, "betfair_lay_under": None,
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            await page.goto(url + "#/over-under", wait_until="domcontentloaded", timeout=60_000)
            # SPAs often never reach "networkidle"; a fixed delay is more reliable than waiting on it.
            await asyncio.sleep(10)
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait_sec = (attempt + 1) * 15
                print(f"    Retry in {wait_sec}s ({e})")
                await asyncio.sleep(wait_sec)
            else:
                raise

    line_str = str(line).replace(".", "\\.")  # for regex
    try:
        # Over/Under hash already loaded above; give the tab a moment to paint.
        await asyncio.sleep(3)
        # Switch to Decimal odds (page defaults to Fractional - Betfair/bookmakers need Decimal view)
        try:
            fmt_sel = page.locator("[data-testid='header-odds-formats-selector']")
            await fmt_sel.click(timeout=3000)
            await asyncio.sleep(1)
            await page.get_by_text("Decimal", exact=True).first.click(timeout=3000)
            await asyncio.sleep(3)
        except Exception:
            for fmt in ["Decimal odds", "Decimal"]:
                try:
                    await page.get_by_text(fmt, exact=False).first.click(timeout=2000)
                    await asyncio.sleep(2)
                    break
                except Exception:
                    pass
        for tab_name in ["Over/Under", "Goals"]:
            try:
                tab = page.get_by_text(tab_name, exact=False).first
                await tab.click(timeout=3000)
                await asyncio.sleep(2)
                break
            except Exception:
                pass
        # Only click Full Time or 1st Half (per user selection - faster)
        # OddsPortal uses "1st Half" for half-time Over/Under; must load before extracting
        if market == "ft":
            tab_labels = ["Full Time"]
        else:
            tab_labels = ["1st Half", "Half Time"]  # Try OddsPortal's exact label first
        for tab_label in tab_labels:
            try:
                mt = page.get_by_text(tab_label, exact=True).first
                await mt.click(timeout=2000)
                await asyncio.sleep(2)
                break
            except Exception:
                pass
        if market == "ft":
            try:
                await page.get_by_role("tab", name=re.compile(r"^\s*Full\s+Time\s*$", re.I)).first.click(
                    timeout=3000
                )
                await asyncio.sleep(2)
            except Exception:
                try:
                    await page.get_by_text("Full Time", exact=False).first.click(timeout=2000)
                    await asyncio.sleep(2)
                except Exception:
                    pass
        # Accept cookies if shown (may block content)
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=2000)
            await asyncio.sleep(1)
        except Exception:
            pass
        # Click the selected line row (+0.5, +1.5, or +2.5) - may expand to show Betfair
        for label in [f'Over/Under +{line}', f'+{line}']:
            try:
                el = page.get_by_text(label, exact=False).first
                await el.scroll_into_view_if_needed(timeout=3000)
                await el.click(timeout=4000)
                await asyncio.sleep(5)
                break
            except Exception:
                pass
        # Scroll to load Betfair Exchange (at bottom, lazy-loaded). Scroll to section first.
        try:
            await page.get_by_text("Betting Exchanges", exact=False).first.scroll_into_view_if_needed(timeout=5000)
            await asyncio.sleep(3)
        except Exception:
            pass
        for _ in range(6):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
        await asyncio.sleep(4)
        try:
            await page.get_by_text("Betfair Exchange", exact=False).first.wait_for(state="visible", timeout=15_000)
            await asyncio.sleep(2)
        except Exception:
            pass
        await asyncio.sleep(2)

        # Extract via JS: date, scores, odds for selected line (pure page scraping)
        # For 1st Half (ht), must search within "1st Half" section to avoid Full Time odds
        line_label = f"Over/Under +{line}"
        line_val = float(line)
        try:
            extracted = await page.evaluate("""(params) => {
            const [lineLabel, isHalfTime, lineVal] = params;
            const text = document.body.innerText;
            const out = { date: '', scoreFt: '', scoreHt: '', underOdds: null, betfairLayUnder: null, betfairLayOver: null, overOdds: null };
            const dateMatch = text.match(/(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?,?\\s*([\\d]{1,2}\\s+[A-Za-z]{3}\\s+[\\d]{4}(?:,\\s*[\\d]{1,2}:[\\d]{2})?)/);
            if (dateMatch) out.date = dateMatch[1].trim();
            let finalMatch = text.match(/Final result\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*\\(\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*,\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*\\)/i);
            if (!finalMatch) {
                finalMatch = text.match(/Final result\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*\\(\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*,\\s*\\d{1,2}\\s*:\\s*\\d{1,2}\\s*\\)/i);
            }
            if (finalMatch) {
                out.scoreFt = finalMatch[1] + ':' + finalMatch[2];
                out.scoreHt = finalMatch[3] + ':' + finalMatch[4];
            } else {
                const fr = text.match(/Final result\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})/i);
                if (fr) out.scoreFt = fr[1] + ':' + fr[2];
                const parenMatch = text.match(/\\(\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*,\\s*(\\d{1,2})\\s*:\\s*(\\d{1,2})\\s*\\)/);
                if (parenMatch) out.scoreHt = parenMatch[1] + ':' + parenMatch[2];
                else {
                    const paren2 = text.match(/\\((\\d{1,2}):(\\d{1,2}),\\s*\\d{1,2}:\\d{1,2}\\)/);
                    if (paren2) out.scoreHt = paren2[1] + ':' + paren2[2];
                }
            }
            const excludeLine = (v) => Math.abs(v - lineVal) > 0.15;
            const ouIdx = text.indexOf('Over/Under');
            let searchStart = 0;
            if (isHalfTime) {
                const halfIdx = text.indexOf('1st Half');
                searchStart = halfIdx >= 0 ? halfIdx : text.indexOf('Half Time');
                if (searchStart < 0) searchStart = ouIdx >= 0 ? ouIdx : 0;
            } else {
                searchStart = ouIdx >= 0 ? ouIdx : 0;
                const ftIdx = text.indexOf('Full Time', searchStart);
                if (ftIdx >= 0) searchStart = ftIdx;
            }
            let lineIdx = text.indexOf(lineLabel, searchStart);
            if (lineIdx < 0 && !isHalfTime) {
                const ftIdx = text.indexOf('Full Time');
                if (ftIdx >= 0) lineIdx = text.indexOf(lineLabel, ftIdx);
            }
            if (lineIdx >= 0) {
                const nextHandicap = text.indexOf('Over/Under +', lineIdx + 5);
                const endIdx = nextHandicap >= 0 ? nextHandicap : lineIdx + 3500;
                const chunk = text.substring(lineIdx, endIdx);
                const nums = (chunk.match(/\\d+\\.\\d+/g) || []).map(parseFloat);
                const valid = nums.filter(v => v >= 1.01 && v <= 15 && excludeLine(v));
                // Do not infer Betfair Lay from min/max here — Back + Lay rows mix; '-' has no price.
                // Bookmaker Over/Under (not Lay)
                if (!out.overOdds && !out.underOdds && valid.length >= 2) {
                    const sorted = [...valid].sort((a,b)=>a-b);
                    out.overOdds = String(sorted[0]);
                    out.underOdds = String(sorted[1]);
                } else if (!out.overOdds && !out.underOdds && valid.length === 1) {
                    out.underOdds = String(valid[0]);
                }
            }
            // Betfair Lay parsed in Python from Lay row only (not this text slice).
            // Broader fallback: any odds in line section when we still have nothing
            if (!out.overOdds && !out.underOdds && lineIdx >= 0) {
                const chunk = text.substring(lineIdx, lineIdx + 4000);
                const allNums = (chunk.match(/\\d+\\.\\d+/g) || []).map(parseFloat);
                const allValid = allNums.filter(v => v >= 1.01 && v <= 15 && excludeLine(v));
                if (allValid.length >= 2) {
                    const s = [...allValid].sort((a,b)=>a-b);
                    out.overOdds = String(s[0]);
                    out.underOdds = String(s[1]);
                } else if (allValid.length >= 1) {
                    out.underOdds = String(allValid[0]);
                }
            }
            return out;
        }""", [line_label, market == "ht", line_val])
        except Exception as ex:
            extracted = None
            print(f"    Extract warning: {ex}", flush=True)

        if extracted:
            result["date"] = extracted.get("date", "") or result.get("date", "")
            if extracted.get("scoreFt"):
                result["score_ft"] = str(extracted["scoreFt"])
            if extracted.get("scoreHt"):
                result["score_ht"] = str(extracted["scoreHt"])
            # Betfair Lay comes only from _parse_betfair_exchange_lay_odds (not bookmaker columns).
            # Fallback: any bookmaker's over/under when Betfair not found
            over_v = extracted.get("overOdds")
            under_v = extracted.get("underOdds")
            # Reject wrong/placeholder odds: 1X2 market (1.75/2.25), score-like (2.0/2.0, 2.0/3.0)
            if over_v and under_v:
                try:
                    o, u = float(over_v), float(under_v)
                    if abs(o - 1.75) < 0.01 and abs(u - 2.25) < 0.01:
                        over_v, under_v = None, None
                    elif abs(o - 2.0) < 0.01 and (abs(u - 2.0) < 0.01 or abs(u - 3.0) < 0.01):
                        over_v, under_v = None, None
                except (ValueError, TypeError):
                    pass
            if under_v and not result.get("under_odds"):
                result["under_odds"] = str(under_v)
            if over_v:
                result["over_odds"] = str(over_v)

        # 1b–2. Date/scores + Betfair Lay row (single innerText read; Lay never fills over_odds/under_odds)
        try:
            page_text = await page.evaluate("() => document.body.innerText")
            if len(page_text or "") < 500:
                print(
                    "    Warning: match page has very little text — odds/scores may be missing (CAPTCHA, block, or slow SPA).",
                    flush=True,
                )
            _fill_scores_and_date_from_page_text(page_text or "", result)
            lo, lu = _parse_betfair_exchange_lay_odds(page_text or "")
            result["betfair_lay_over"] = lo
            result["betfair_lay_under"] = lu
        except Exception:
            pass

        # 3. Fallback: bookmaker Over/Under from HTML when still empty
        if not result.get("over_odds") or not result.get("under_odds"):
            html = await page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            page_text = soup.get_text()
            idx = page_text.find(f"Over/Under +{line}")
            if idx < 0:
                idx = page_text.find(f"+{line}")
            if idx >= 0:
                chunk = page_text[idx : idx + 3000]
                nums = re.findall(r"\d+\.\d+", chunk)
                valid = [n for n in nums if 1.01 <= float(n) <= 15 and abs(float(n) - line) > 0.15]
                if len(valid) >= 2:
                    sorted_valid = sorted(valid, key=lambda x: float(x))
                    if not result.get("over_odds"):
                        result["over_odds"] = sorted_valid[0]
                    if not result.get("under_odds"):
                        result["under_odds"] = sorted_valid[1]
                elif len(valid) == 1 and not result.get("under_odds"):
                    result["under_odds"] = valid[0]

        # 5. Score from page
        if not result.get("score_ft") or not result.get("score_ht"):
            html = await page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            page_text = soup.get_text()
            if not result.get("score_ft"):
                for m in re.finditer(r"\b(\d{1,2})\s*[:\-]\s*(\d{1,2})\b", page_text):
                    if _is_likely_score(f"{m.group(1)}:{m.group(2)}"):
                        result["score_ft"] = f"{m.group(1)}:{m.group(2)}"
                        break
            if not result.get("score_ht"):
                ht_match = re.search(r"\(\s*(\d{1,2})\s*:\s*(\d{1,2})\s*,\s*\d{1,2}\s*:\s*\d{1,2}\s*\)", page_text)
                if ht_match:
                    result["score_ht"] = f"{ht_match.group(1)}:{ht_match.group(2)}"

        # 5b. HTML string often contains scores before innerText catches up
        if not result.get("score_ft") or not result.get("date"):
            from bs4 import BeautifulSoup

            html_snap = await page.content()
            _fill_scores_and_date_from_html(html_snap, result)
            if not result.get("date"):
                _fill_scores_and_date_from_page_text(
                    BeautifulSoup(html_snap, "html.parser").get_text("\n"),
                    result,
                )

        # 6. Screenshot
        if screenshot_path:
            try:
                await page.screenshot(path=str(screenshot_path))
            except Exception:
                pass

    except PlaywrightTimeout:
        pass
    except Exception as e:
        print(f"  Error: {e}")

    return result


def league_slug_from_url(url: str) -> str:
    """Extract league-season slug for filename. Use league name when no season hyphen (e.g. eredivisie)."""
    parts = url.rstrip("/").split("/")
    for p in reversed(parts):
        if p and p != "results" and "-" in p:
            return p
    # No season suffix (e.g. /eredivisie/results/) - use league name to avoid "unknown"
    for p in reversed(parts):
        if p and p not in ("results", "football", "soccer"):
            return p
    return "unknown"


def _league_path_from_url(league_url: str) -> str:
    """Extract league path for filtering: /football/england/premier-league-2024-2025/"""
    league_path = re.sub(r"/results.*$", "/", league_url)
    if "oddsportal.com" in league_path:
        league_path = "/" + league_path.split("oddsportal.com/", 1)[-1]
    return league_path


def _league_slug_from_results_url(league_url: str) -> str:
    """Competition folder in the path, e.g. league-one (from .../league-one/results/)."""
    try:
        path = urlparse(league_url).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if not parts:
            return ""
        if parts[-1] == "results":
            return parts[-2] if len(parts) >= 2 else ""
        return parts[-1]
    except Exception:
        return ""


def _match_url_belongs_to_league(match_url: str, league_url: str) -> bool:
    """Same competition as the results URL. league-one matches .../league-one-2024-2025/... (season suffix)."""
    if not league_url or not match_url:
        return True
    slug = _league_slug_from_results_url(league_url)
    if not slug:
        return True
    try:
        path = urlparse(match_url).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if len(parts) < 2:
            return False
        if parts[0] not in ("football", "soccer"):
            return False
        # OddsPortal links many match pages as /football/h2h/team1-id/team2-id/ (no league folder).
        # Those rows are already scoped to the league results page we are scraping.
        if parts[1].lower() == "h2h" and len(parts) >= 4:
            return True
        if len(parts) < 4:
            return False
        league_seg = parts[2]
        return league_seg == slug or league_seg.startswith(slug + "-")
    except Exception:
        return False


def _load_existing_results(out_dir: Path, slug: str, league_url: str = "") -> tuple[list[MatchData], set[str]]:
    """Load existing CSV for league. Only loads rows whose match_url is for this league."""
    csv_path = out_dir / f"{slug}.csv"
    if not csv_path.exists():
        return [], set()
    try:
        import pandas as pd
        df = pd.read_csv(csv_path)
        results = []
        seen = set()
        def _opt(row, col, default=""):
            if col not in df.columns:
                return default
            v = row.get(col)
            if pd.isna(v):
                return default
            s = str(v).strip()
            return default if not s or s.lower() == "nan" else s

        for _, row in df.iterrows():
            url = _opt(row, "match_url", "")
            if not url or url in seen:
                continue
            if league_url and not _match_url_belongs_to_league(url, league_url):
                continue
            seen.add(url)
            o15 = _opt(row, "over_odds", "") or _opt(row, "over_15_odds", "")
            u15 = _opt(row, "under_odds", "") or _opt(row, "under_15_odds", "")
            bfo = _opt(row, "betfair_lay_over", "") or _opt(row, "betfair_lay_over_15", "")
            bfu = _opt(row, "betfair_lay_under", "") or _opt(row, "betfair_lay_under_15", "")
            hth = _opt(row, "half_time_home", "")
            hta = _opt(row, "half_time_away", "")
            htt = _opt(row, "half_time_total", "") or _opt(row, "half_time_total_goals", "")
            if not htt:
                htt = _half_time_total_goals(hth, hta)
            results.append(MatchData(
                date=_opt(row, "date", ""),
                home_team=_opt(row, "home_team", ""),
                away_team=_opt(row, "away_team", ""),
                full_time_home=_opt(row, "full_time_home", ""),
                full_time_away=_opt(row, "full_time_away", ""),
                half_time_home=hth,
                half_time_away=hta,
                half_time_total=htt,
                over_odds=o15 or None,
                under_odds=u15 or None,
                betfair_lay_over=bfo or None,
                betfair_lay_under=bfu or None,
                match_url=url,
                league=_opt(row, "league", ""),
            ))
        return results, seen
    except Exception as e:
        print(f"  Could not load existing CSV: {e}")
        return [], set()


async def _click_next_page(page) -> bool:
    """Click Next in pagination. Returns True on success."""
    for selector in [
        'a:has-text("Next")',
        'xpath=//*[contains(., "Prev") or contains(., "Previous")]//a[contains(text(), "Next")]',
        'xpath=//*[contains(., "Next")]//a[contains(text(), "Next")]',
        '[aria-label="Next"]',
    ]:
        try:
            loc = page.locator(selector).first
            await loc.scroll_into_view_if_needed(timeout=5000)
            await loc.click(timeout=5000)
            await asyncio.sleep(5)
            return True
        except Exception:
            continue
    return False


async def _discover_last_page(
    page, league_url: str, league_name: str,
    max_pages: int, page_load_wait: float,
) -> list[int]:
    """Find the highest page with league matches. Probe 1,2,3,... until a page returns 0 matches."""
    last_ok = 1
    for probe in range(2, max_pages + 1):
        if _check_stop():
            break
        try:
            print(f"  Probing page {probe}...", flush=True)
            await page.goto(league_url, wait_until="domcontentloaded", timeout=45_000)
            await asyncio.sleep(2)
            try:
                await page.click("#onetrust-accept-btn-handler", timeout=2000)
                await asyncio.sleep(1)
            except Exception:
                pass
            ok = await _try_go_to_page(page, league_url, probe)
            if not ok:
                # Pagination may need more time - retry once with fresh load and scroll-to-bottom first
                if probe == 2:
                    print(f"    Page 2 navigation failed, retrying with scroll-first...", flush=True)
                    await page.goto(league_url, wait_until="domcontentloaded", timeout=60_000)
                    await asyncio.sleep(8)
                    for _ in range(15):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(2)
                    ok = await _try_go_to_page(page, league_url, probe)
                if not ok:
                    break
            await asyncio.sleep(min(page_load_wait, 6))
            html = await page.content()
            matches = parse_results_page_html(html, league_url, league_name)
            for _ in range(20):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2.5)
                html = await page.content()
                more = parse_results_page_html(html, league_url, league_name)
                seen = {m["match_url"] for m in matches}
                for m in more:
                    if m["match_url"] not in seen:
                        matches.append(m)
                        seen.add(m["match_url"])
            matches = [m for m in matches if _match_url_belongs_to_league(m.get("match_url", ""), league_url)]
            if matches:
                last_ok = probe
            else:
                break
        except Exception:
            break
    print(f"  Discovered: {league_name} has {last_ok} page(s).", flush=True)
    return [last_ok]


async def _try_go_to_page(page, league_url: str, page_num: int, max_retries: int = 3) -> bool:
    """Advance to results page N. For page 7+, can step from page 6 via Next to avoid SPA navigation issues."""
    if page_num <= 1:
        return True
    # OddsPortal may use hash routing - try direct URL first
    base = league_url.rstrip("/").split("#")[0].rstrip("/")
    try:
        await page.goto(f"{base}#/page/{page_num}", wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(5)
        # Verify URL changed (SPA may ignore unknown hash)
        if f"page/{page_num}" in page.url or f"page%2F{page_num}" in page.url:
            return True
    except Exception:
        pass
    for attempt in range(max_retries):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(3)
            # Try clicking page number - OddsPortal pagination (broader selectors)
            for selector in [
                f'a:text-is("{page_num}")',
                f'[class*="pagination"] a:text-is("{page_num}")',
                f'nav a:text-is("{page_num}")',
                f'xpath=//a[normalize-space(text())="{page_num}"]',
                f'xpath=//*[contains(., "Prev") or contains(., "Previous")]//a[text()="{page_num}"]',
                f'xpath=//*[contains(., "Next")]//a[text()="{page_num}"]',
            ]:
                try:
                    loc = page.locator(selector).first
                    await loc.scroll_into_view_if_needed(timeout=5000)
                    await loc.click(timeout=5000)
                    await asyncio.sleep(5)
                    return True
                except Exception:
                    continue
            # Fallback: Next button (single step)
            if await _click_next_page(page):
                return True
        except Exception as e:
            if attempt < max_retries - 1:
                wait_sec = (attempt + 1) * 5
                print(f"    Pagination retry {attempt + 1}/{max_retries} in {wait_sec}s: {e}", flush=True)
                await asyncio.sleep(wait_sec)
            else:
                pass  # Try step-from-prev strategy below
        # For page 7+: step from page 6 by clicking Next (N-6) times - more reliable than direct jump
        if page_num >= 7:
            try:
                await page.goto(league_url, wait_until="domcontentloaded", timeout=60_000)
                await asyncio.sleep(4)
                if await _try_go_to_page(page, league_url, 6):  # Go to page 6 first
                    await asyncio.sleep(4)
                    for _ in range(page_num - 6):
                        if not await _click_next_page(page):
                            break
                        await asyncio.sleep(4)
                    else:
                        return True
            except Exception:
                pass
    return False


def _report_progress(cb, percent: float | None, message: str, eta_seconds: float | None):
    """Helper to call progress callback if provided."""
    if cb:
        cb(percent, message, eta_seconds)


async def main(run_config: ScraperConfig | None = None, progress_cb=None):
    """Main scraper. Use run_config from GUI, or config.LEAGUES for legacy.
    progress_cb(percent, message, eta_seconds) - percent/eta None when unknown."""
    global _stop_requested
    _stop_requested = False
    _report_progress(progress_cb, None, "Initializing browser...", None)
    print("  Initializing browser...", flush=True)
    out_dir = Path(__file__).parent / getattr(config, "OUTPUT_DIR", "output")
    out_dir.mkdir(exist_ok=True)
    auth_rel = getattr(config, "AUTH_STATE_FILE", "output/.auth/oddsportal_state.json")
    auth_path = Path(__file__).parent / auth_rel
    auth_path.parent.mkdir(parents=True, exist_ok=True)

    leagues = run_config.league_urls if run_config else config.LEAGUES

    async with async_playwright() as p:
        _report_progress(progress_cb, None, "Launching browser...", None)
        print("  Launching browser...", flush=True)
        headless = getattr(config, "HEADLESS", True)
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
            print(f"  Loaded saved session from {auth_path.name}", flush=True)
        context = await browser.new_context(**ctx_kw)
        page = await context.new_page()

        _report_progress(progress_cb, None, "Logging in to OddsPortal...", None)
        print("  Logging in to OddsPortal...", flush=True)
        login_ok = await login_oddsportal(
            page,
            getattr(config, "ODDSPORTAL_USER", ""),
            getattr(config, "ODDSPORTAL_PASS", ""),
        )
        if not login_ok and getattr(config, "ODDSPORTAL_USER", ""):
            print(
                "  WARNING: Login did not complete — set HEADLESS=False, dismiss any promo modal, "
                "or delete output/.auth/oddsportal_state.json and log in manually once.",
                flush=True,
            )

        for league_url in leagues:
            if _check_stop():
                print("  Stop requested. Exiting.", flush=True)
                break
            league_name = league_url.split("/")[-3] or "Unknown"
            slug = league_slug_from_url(league_url)
            if run_config:
                slug = f"{slug}_{run_config.slug_suffix()}"
            if run_config and getattr(run_config, "fresh_run", False):
                league_results, existing_urls = [], set()
                print(f"\n--- League: {league_name} --- (fresh run)", flush=True)
            else:
                league_results, existing_urls = _load_existing_results(out_dir, slug, league_url)
                print(f"\n--- League: {league_name} ---", flush=True)
                if league_results:
                    print(f"  Resuming: {len(league_results)} matches already scraped.", flush=True)

            max_pages = getattr(config, "MAX_PAGINATION_PAGES", 20)
            target_matches = run_config.match_limit if run_config else getattr(config, "TARGET_MATCHES_PER_SEASON", 380)
            oldest_first = (run_config.direction == "oldest") if run_config else False
            market = run_config.market if run_config else "ft"
            line = float(run_config.line) if run_config else 1.5
            # OddsPortal: page 1 = newest (end of season), last page = oldest (first of season). So:
            # oldest_first → [last,...,1]   newest_first → [1,2,...,last]
            skip_pagination_probe = (
                run_config
                and not oldest_first
                and target_matches is not None
                and target_matches <= 200
            )
            if skip_pagination_probe:
                last_page_num = 1
                print(
                    "  Newest-first with match limit: skipping pagination probe (page 1 only).",
                    flush=True,
                )
            else:
                discovered = await _discover_last_page(
                    page, league_url, league_name,
                    max_pages, config.PAGE_LOAD_WAIT_SEC,
                )
                last_page_num = discovered[0] if discovered else 1
            if oldest_first:
                page_order = list(range(last_page_num, 0, -1))  # last,...,1 (oldest first)
            else:
                page_order = list(range(1, last_page_num + 1))  # 1,...,last (newest first)
            _report_progress(progress_cb, 0, f"Found {len(page_order)} pages for {league_name}. Starting scrape...", None)
            page_idx = 0
            total_new_this_run = 0
            total_estimate = target_matches or min(99999, len(page_order) * 55)
            scrape_start_time = None  # set when we start processing first match

            while page_idx < len(page_order) and len(league_results) < (target_matches or 99999):
                if _check_stop():
                    print("  Stop requested. Exiting.", flush=True)
                    break
                page_num = page_order[page_idx]
                print(f"\n  --- Batch: page {page_num} ---", flush=True)
                if page_num == 1:
                    print(
                        "  (Page 1: short wait for links, then parse + scroll — can take several minutes.)",
                        flush=True,
                    )
                try:
                    if page_num == 1:
                        for attempt in range(3):
                            try:
                                await page.goto(league_url, wait_until="domcontentloaded", timeout=60_000)
                                await asyncio.sleep(12)
                                break
                            except Exception as e:
                                if attempt < 2:
                                    await asyncio.sleep((attempt + 1) * 10)
                                else:
                                    raise
                        await asyncio.sleep(3)
                        try:
                            await page.click("#onetrust-accept-btn-handler", timeout=3000)
                            await asyncio.sleep(2)
                        except Exception:
                            pass
                        await _dismiss_blocking_modals(page)
                        # Lazy-loaded rows: scroll before first snapshot so match URLs enter HTML/DOM
                        print("  Pre-scrolling results (15 passes) to load match rows...", flush=True)
                        for _ in range(15):
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(2)
                    else:
                        ok = await _try_go_to_page(page, league_url, page_num)
                        if not ok:
                            # Retry: reload league (page 1) and try pagination again
                            print(f"  Pagination to page {page_num} failed. Retrying from league page...", flush=True)
                            try:
                                await page.goto(league_url, wait_until="domcontentloaded", timeout=60_000)
                                await asyncio.sleep(6)
                                ok = await _try_go_to_page(page, league_url, page_num)
                            except Exception:
                                pass
                        if not ok:
                            print(f"  Pagination to page {page_num} failed after retries. Stopping.", flush=True)
                            break

                    await asyncio.sleep(config.PAGE_LOAD_WAIT_SEC)
                    if page_num == 1:
                        print("  Waiting for first football link in DOM (up to 20s, then we continue anyway)...", flush=True)
                        try:
                            await page.wait_for_selector(
                                'a[href*="/football/"], a[href*="/soccer/"]',
                                timeout=20_000,
                            )
                            print("  OK: football links visible.", flush=True)
                        except Exception:
                            print(
                                "  No football links in 20s — continuing (HTML parse + DOM may still work). "
                                "If results stay empty: set HEADLESS=False in config.py and check CAPTCHA/login.",
                                flush=True,
                            )
                    html = await page.content()
                except Exception as e:
                    print(f"  Failed to load page {page_num}: {e}", flush=True)
                    break

                # Retry when page 2+ returns 0 league matches (pagination may fail or page not fully loaded)
                # More retries for deeper pages (6+) - they often need extra load/scroll time
                empty_retries = 3 if page_num >= 6 else (2 if page_num > 1 else 0)
                matches = []
                # More scroll rounds to load full season (300+ matches) - OddsPortal uses infinite scroll
                scroll_rounds = 100 if page_num >= 6 else (90 if page_num >= 3 else 80)
                scroll_wait = 4
                if target_matches is not None and target_matches <= 30 and page_num == 1:
                    scroll_rounds = min(scroll_rounds, 25)

                for empty_attempt in range(empty_retries + 1):
                    matches = parse_results_page_html(html, league_url, league_name)
                    emb = extract_matches_from_embedded_urls(html, league_url, league_name)
                    if emb:
                        n0 = len(matches)
                        matches = _merge_match_rows_by_url(matches, emb)
                        if len(matches) > n0:
                            print(
                                f"  Embedded URLs in page HTML: +{len(matches) - n0} match links (scripts/JSON).",
                                flush=True,
                            )
                    if not matches and page_num == 1:
                        await asyncio.sleep(2)
                        dom_m = await collect_matches_from_dom(page, league_url, league_name)
                        if dom_m:
                            matches = dom_m
                            print(f"  DOM discovery: {len(dom_m)} match links (HTML parse had none).", flush=True)
                    prev_count = len(matches)

                    # Deep pages: extra initial wait for SPA to hydrate
                    if page_num >= 6 and empty_attempt > 0:
                        await asyncio.sleep(8)

                    # OddsPortal uses scroll-to-load. Keep scrolling while count is 0 (SPA / lazy load).
                    # Do NOT stop when len==0 after one scroll — that was quitting before rows appeared.
                    for si in range(scroll_rounds):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(scroll_wait)
                        html = await page.content()
                        more = parse_results_page_html(html, league_url, league_name)
                        more = _merge_match_rows_by_url(
                            more, extract_matches_from_embedded_urls(html, league_url, league_name)
                        )
                        seen = {m["match_url"] for m in matches}
                        for m in more:
                            if m["match_url"] not in seen:
                                matches.append(m)
                                seen.add(m["match_url"])
                        if len(matches) == prev_count:
                            if prev_count > 0:
                                break
                            if prev_count == 0 and (si + 1) % 10 == 0:
                                print(
                                    f"  Still scrolling (no matches in HTML yet)... {si + 1}/{scroll_rounds}",
                                    flush=True,
                                )
                            continue
                        prev_count = len(matches)
                        print(f"  Loaded {len(matches)} matches on this page...", flush=True)

                    if not matches and page_num == 1:
                        await asyncio.sleep(2)
                        dom_m = await collect_matches_from_dom(page, league_url, league_name)
                        if dom_m:
                            matches = dom_m
                            print(f"  DOM discovery (after scroll): {len(dom_m)} match links.", flush=True)

                    # Keep only matches from THIS league
                    matches = [m for m in matches if _match_url_belongs_to_league(m.get("match_url", ""), league_url)]
                    # Newest-first + limit: optional DOM order on page 1, then always sort by parsed date.
                    if page_num == 1 and not oldest_first and run_config and target_matches:
                        try:
                            dom_order = await collect_h2h_matches_ordered(page, league_url, league_name)
                            if dom_order:
                                by_url = {m["match_url"]: m for m in matches}
                                reordered: list[dict] = []
                                seen_r: set[str] = set()
                                for row in dom_order:
                                    u = row["match_url"]
                                    if u in by_url and u not in seen_r:
                                        reordered.append(by_url[u])
                                        seen_r.add(u)
                                for m in matches:
                                    if m["match_url"] not in seen_r:
                                        reordered.append(m)
                                matches = reordered
                        except Exception:
                            pass
                    if not oldest_first:
                        matches.sort(key=_dict_match_date_sort_key, reverse=True)
                    else:
                        matches.sort(key=_dict_match_date_sort_key)

                    if not matches and page_num == 1 and empty_attempt == 0:
                        try:
                            diag = await page.evaluate(
                                """() => ({
                                href: location.href,
                                title: document.title,
                                footballInBody: (document.body && document.body.innerHTML) ?
                                  document.body.innerHTML.split('oddsportal.com/football').length - 1 : 0
                            })"""
                            )
                            print(
                                f"  Debug: HTML length={len(html)} chars | page url={diag.get('href', '')[:90]} "
                                f"| title={diag.get('title', '')!r} | "
                                f"~{diag.get('footballInBody', 0)}× 'oddsportal.com/football' in body",
                                flush=True,
                            )
                        except Exception:
                            print(f"  Debug: HTML length={len(html)} chars.", flush=True)

                    if matches:
                        break
                    if page_num == 1:
                        break
                    # 0 league matches: always retry before stopping (don't trust URL - pagination URLs can vary)
                    if empty_attempt < empty_retries:
                        print(f"  Page {page_num}: no {league_name} matches yet. Retrying (attempt {empty_attempt + 2}/{empty_retries + 1})...", flush=True)
                        try:
                            await page.goto(league_url, wait_until="domcontentloaded", timeout=60_000)
                            await asyncio.sleep(6)
                            if not await _try_go_to_page(page, league_url, page_num):
                                await asyncio.sleep(3)
                            await asyncio.sleep(config.PAGE_LOAD_WAIT_SEC + (8 if page_num >= 6 else 4))
                            html = await page.content()
                        except Exception as e:
                            print(f"    Retry failed: {e}", flush=True)
                            break
                    else:
                        # Only stop after all retries exhausted
                        current_url = page.url
                        slug = _league_slug_from_results_url(league_url)
                        if not slug or slug in current_url:
                            print(f"  Page {page_num}: no {league_name} matches after {empty_retries + 1} attempts. Stopping.", flush=True)
                        else:
                            print(f"  Page {page_num}: left {league_name} (URL changed). Stopping.", flush=True)
                        break

                if page_num > 1 and not matches:
                    if oldest_first:
                        # Page may not exist (e.g. page 20 for small league) - skip to next in order
                        pass
                    else:
                        break
                # Filter to only matches we haven't scraped yet
                to_scrape = [
                    m for m in matches
                    if m["match_url"] not in existing_urls and _is_match_page_url(m["match_url"])
                ]
                # Oldest first: process bottom-of-page (oldest) first, reverse match order
                if oldest_first:
                    to_scrape = to_scrape[::-1]
                print(f"  Page {page_num}: {len(matches)} visible, {len(to_scrape)} new to scrape.", flush=True)

                if not to_scrape:
                    if page_num == 1 and league_results:
                        print("  All visible matches already scraped. Trying next page...", flush=True)
                    elif page_num == 1:
                        pass  # Will try JS fallback below
                    elif not matches:
                        if oldest_first:
                            print(f"  Page {page_num}: no matches (page may not exist). Skipping.", flush=True)
                        else:
                            print(f"  No {league_name} matches on this page. Stopping.", flush=True)
                            break
                    else:
                        print(f"  Page {page_num}: all {len(matches)} already scraped. Trying next page...", flush=True)

                # JS fallback if no matches at all on page 1
                if not matches and page_num == 1:
                    js_matches = await page.evaluate("""() => {
                        const links = Array.from(document.querySelectorAll(
                            'a[href*="/football/"], a[href*="/soccer/"]'
                        ));
                        const seen = new Set();
                        return links.map(a => {
                            const h = a.href || '';
                            if (h.includes('results') || seen.has(h)) return null;
                            seen.add(h);
                            const row = a.closest('div[class*="flex"]') || a.parentElement?.parentElement;
                            return { url: h, text: row ? row.innerText : '' };
                        }).filter(x => x && x.url);
                    }""")
                    for m in (js_matches or []):
                        url = m.get("url", "")
                        if url and _is_match_page_url(url):
                            text = m.get("text", "")
                            lines = [l.strip() for l in text.split("\n") if l.strip()]
                            score_ft = next((l for l in lines if re.match(r"^\d{1,2}[:\-]\d{1,2}$", l) and _is_likely_score(l)), "")
                            date_val = next((l for l in lines if re.match(r"[\d]{2}[/.-][\d]{2}", l)), "")
                            slug_parts = url.rstrip("/").split("/")[-1].rsplit("-", 1)[0].replace("-", " ").split()
                            home = " ".join(slug_parts[: len(slug_parts) // 2]).title() if slug_parts else "?"
                            away = " ".join(slug_parts[len(slug_parts) // 2 :]).title() if slug_parts else "?"
                            matches.append({
                                "date": date_val,
                                "home_team": home,
                                "away_team": away,
                                "score_ft": score_ft,
                                "score_ht": "",
                                "match_url": url,
                                "league": league_name,
                            })
                    # Filter JS fallback matches by league
                    matches = [m for m in matches if _match_url_belongs_to_league(m.get("match_url", ""), league_url)]
                    to_scrape = [
                        m for m in matches
                        if m["match_url"] not in existing_urls and _is_match_page_url(m["match_url"])
                    ]

                if not to_scrape and page_num == 1 and not matches:
                    print("  No matches found.")
                    break

                # Scrape only new matches in this batch
                screenshot_dir = None
                if getattr(config, "SCREENSHOT_MODE", False):
                    screenshot_dir = out_dir / "screenshots" / slug
                    screenshot_dir.mkdir(parents=True, exist_ok=True)

                screenshot_only = getattr(config, "SCREENSHOT_ONLY_FAST", False)
                limit_run = config.MAX_MATCHES_PER_RUN
                take = len(to_scrape)
                if target_matches:
                    take = min(take, max(0, target_matches - len(league_results)))
                if limit_run:
                    take = min(take, max(0, limit_run - total_new_this_run))
                to_process = to_scrape[:take] if take > 0 else []
                progress_file = out_dir / "progress.txt"
                if to_process and scrape_start_time is None:
                    scrape_start_time = time.time()

                for i, m in enumerate(to_process):
                    if _check_stop():
                        print("  Stop requested. Exiting.", flush=True)
                        break
                    if not _is_match_page_url(m.get("match_url", "")):
                        continue
                    # Skip non-league matches (failsafe before scrape)
                    if not _match_url_belongs_to_league(m.get("match_url", ""), league_url):
                        continue
                    print(f"  [{i+1}/{len(to_process)}] {m.get('home_team')} vs {m.get('away_team')}", flush=True)
                    sp = None
                    if screenshot_dir:
                        match_id = m["match_url"].rstrip("/").split("/")[-1].replace("/", "_")
                        sp = screenshot_dir / f"{i+1:03d}_{match_id[:20]}.png"
                    if screenshot_only and sp:
                        # Fast mode: just open page and screenshot
                        try:
                            await page.goto(m["match_url"], wait_until="domcontentloaded", timeout=25_000)
                            await asyncio.sleep(5)
                            try:
                                tab = page.get_by_text("Over/Under", exact=False).or_(page.get_by_text("Goals", exact=False)).first
                                await tab.click(timeout=2000)
                                await asyncio.sleep(2)
                            except Exception:
                                pass
                            await page.screenshot(path=str(sp))
                        except Exception:
                            pass
                        api_result = {}
                    else:
                        for attempt in range(3):
                            try:
                                api_result = await scrape_match(
                                    page, m["match_url"],
                                    market=market, line=line,
                                    screenshot_path=sp,
                                )
                                break
                            except Exception as err:
                                if attempt < 2:
                                    await asyncio.sleep((attempt + 1) * 15)
                                else:
                                    api_result = {}
                                    print(f"    Failed after retries: {err}")

                    # Guard: only add matches from this league
                    if not _match_url_belongs_to_league(m.get("match_url", ""), league_url):
                        continue

                    # Merge: prefer match page data for date
                    date_val = api_result.get("date") or m.get("date", "")
                    score_ft = api_result.get("score_ft") or m.get("score_ft", "")
                    score_ht = api_result.get("score_ht") or m.get("score_ht", "")
                    ft_home, ft_away = _parse_score(score_ft)
                    ht_home, ht_away = _parse_score(score_ht)

                    fix_home, fix_away = teams_from_match_url(m.get("match_url", ""))
                    home_team = fix_home if fix_home != "?" else m.get("home_team", "")
                    away_team = fix_away if fix_away != "?" else m.get("away_team", "")

                    league_results.append(MatchData(
                        date=date_val,
                        home_team=home_team,
                        away_team=away_team,
                        full_time_home=ft_home,
                        full_time_away=ft_away,
                        half_time_home=ht_home,
                        half_time_away=ht_away,
                        half_time_total=_half_time_total_goals(ht_home, ht_away),
                        over_odds=api_result.get("over_odds"),
                        under_odds=api_result.get("under_odds"),
                        betfair_lay_over=api_result.get("betfair_lay_over"),
                        betfair_lay_under=api_result.get("betfair_lay_under"),
                        match_url=m.get("match_url", ""),
                        league=m.get("league", ""),
                    ))
                    existing_urls.add(m.get("match_url", ""))
                    total_new_this_run += 1
                    # Progress report: percent and ETA
                    done = len(league_results)
                    if total_estimate and done > 0 and scrape_start_time:
                        pct = min(99.9, 100.0 * done / total_estimate)
                        elapsed = time.time() - scrape_start_time
                        rate = done / elapsed if elapsed > 0 else 0
                        eta_sec = (total_estimate - done) / rate if rate > 0.1 else None
                        _report_progress(progress_cb, pct, f"{done}/{total_estimate} — {m.get('home_team','')} vs {m.get('away_team','')}", eta_sec)
                    else:
                        _report_progress(progress_cb, None, f"{done} scraped — {m.get('home_team','')} vs {m.get('away_team','')}", None)
                    # Incremental save every 10 matches + progress file
                    progress_file.write_text(f"{len(league_results)} total - {m.get('home_team', '')} vs {m.get('away_team', '')}\nLast update: {time.strftime('%H:%M:%S')}", encoding="utf-8")
                    if (len(league_results)) % 10 == 0:
                        import pandas as pd
                        _sort_results_chronological(league_results)
                        df = pd.DataFrame([asdict(r) for r in league_results])
                        try:
                            with open(out_dir / f"{slug}.csv", "w", newline="", encoding="utf-8") as f:
                                df.to_csv(f, index=False)
                        except PermissionError:
                            pass
                    time.sleep(config.DELAY_BETWEEN_REQUESTS)

                    # Stop if we hit per-run limit
                    if limit_run and total_new_this_run >= limit_run:
                        break

                page_idx += 1

            # Final export per league (after all batches) - always chronological order
            if league_results:
                import pandas as pd
                _sort_results_chronological(league_results)
                df = pd.DataFrame([asdict(r) for r in league_results])
                csv_path = out_dir / f"{slug}.csv"
                xlsx_path = out_dir / f"{slug}.xlsx"
                for path in (csv_path, xlsx_path):
                    try:
                        if path.suffix == ".csv":
                            with open(path, "w", newline="", encoding="utf-8") as f:
                                df.to_csv(f, index=False)
                        else:
                            df.to_excel(path, index=False, engine="openpyxl")
                        print(f"Exported to {path}")
                    except PermissionError:
                        alt = path.parent / f"{path.stem}_new{path.suffix}"
                        if path.suffix == ".csv":
                            with open(alt, "w", newline="", encoding="utf-8") as f:
                                df.to_csv(f, index=False)
                        else:
                            df.to_excel(alt, index=False, engine="openpyxl")
                        print(f"  (original file locked) Saved to {alt}")
                print(f"\nExported {len(league_results)} matches.")
            else:
                print(f"  No data for {league_name}.")

        try:
            await context.storage_state(path=str(auth_path))
            print(f"  Saved session to {auth_path}", flush=True)
        except Exception as e:
            print(f"  Could not save session: {e}", flush=True)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
