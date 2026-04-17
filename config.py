# OddsPortal Scraper Configuration

# Credentials - load from .env (create .env from .env.example)
# Or set ODDSPORTAL_USER and ODDSPORTAL_PASS in environment
import os
from dotenv import load_dotenv
load_dotenv()
ODDSPORTAL_USER = os.getenv("ODDSPORTAL_USER", "")
ODDSPORTAL_PASS = os.getenv("ODDSPORTAL_PASS", "")
# Target leagues - OddsPortal URL path format
# One CSV file per league/season will be created in OUTPUT_DIR
LEAGUES = [
    # OddsPortal URL formats: (a) league name only = current season (e.g. league-one)
    # (b) league-season = specific year (e.g. premier-league-2024-2025)
    "https://www.oddsportal.com/football/england/league-one/results/",
    # "https://www.oddsportal.com/football/germany/bundesliga/results/",
    # "https://www.oddsportal.com/football/england/premier-league-2024-2025/results/",
]

# Market to extract
MARKET_OVER_UNDER_15 = "Goals Over/Under 1.5"

# Target: Betfair Exchange (peer-to-peer Lay odds). Do NOT use Betfair bookmaker odds.
BETFAIR_EXCHANGE = "Betfair Exchange"

# Scraping behaviour
PAGE_LOAD_WAIT_SEC = 8       # Wait for JS to render
DELAY_BETWEEN_REQUESTS = 1   # Be respectful, avoid rate limits (reduced for speed)
HEADLESS = False             # True often gets 0 matches on OddsPortal; use False to see browser + CAPTCHA
# Saved after login — next runs can reuse cookies (still set HEADLESS=False if blocked)
AUTH_STATE_FILE = "output/.auth/oddsportal_state.json"
MAX_MATCHES_PER_RUN = None   # None = no per-run cap (aside from TARGET_MATCHES_PER_SEASON if set)
MAX_PAGINATION_PAGES = 20    # Max results pages to try (page 2, 3, ... for older matches)
# None = no total row cap — scrape until all pages are exhausted (league size varies: ~300–400+).
# Set an int only if you need a hard stop (e.g. testing). GUI "Limit to N" overrides when set.
TARGET_MATCHES_PER_SEASON = None

# Output - one CSV per league/season in output_dir
OUTPUT_DIR = "output"
# Excel: first row of match data (1-based). Rows 1–(MATCH_DATA_START_ROW - 1) are header / summary (see template).
MATCH_DATA_START_ROW = 10
# If OUTPUT_DIR / EXCEL_TEMPLATE_FILE exists, .xlsx exports copy that workbook (rows 1–4 unchanged) and fill from MATCH_DATA_START_ROW.
EXCEL_TEMPLATE_FILE = "Template_Frame.xlsx"
# Optional: absolute path to the template .xlsx (overrides repo templates/ and OUTPUT_DIR when non-empty).
EXCEL_TEMPLATE_ABSOLUTE = ""

# Save screenshot of each match page (date, odds visible) for later OCR/manual use
# Disable for full-season runs to speed up (380 screenshots = ~15+ min extra)
SCREENSHOT_MODE = False
# Screenshot-only = faster: just teams, URL, screenshot per match (no odds parsing)
SCREENSHOT_ONLY_FAST = False
