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
MAX_MATCHES_PER_RUN = None   # None = no limit; scraper runs until TARGET_MATCHES_PER_SEASON (380). Do not set to 50.
MAX_PAGINATION_PAGES = 20    # Max results pages to try (page 2, 3, ... for older matches)
TARGET_MATCHES_PER_SEASON = 380  # Stop when we have this many (full PL season)

# Output - one CSV per league/season in output_dir
OUTPUT_DIR = "output"

# Save screenshot of each match page (date, odds visible) for later OCR/manual use
# Disable for full-season runs to speed up (380 screenshots = ~15+ min extra)
SCREENSHOT_MODE = False
# Screenshot-only = faster: just teams, URL, screenshot per match (no odds parsing)
SCREENSHOT_ONLY_FAST = False
