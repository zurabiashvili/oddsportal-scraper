# OddsPortal Historical Matches Scraper

Scrapes archived football match data from [OddsPortal](https://www.oddsportal.com/):
- **Date**, **teams**, **full-time** and **half-time scores**
- **Betfair Lay odds** for the **Over/Under 1.5 goals** market

---

## What You Need

### 1. Python 3.10+
Install from [python.org](https://www.python.org/downloads/).

### 2. Dependencies
```bash
pip install -r requirements.txt
```

### 3. Playwright Browsers
OddsPortal is **JavaScript-rendered**; you need Playwright to load pages:
```bash
playwright install chromium
```

### 4. Target Leagues
Edit `config.py` and add league results URLs, e.g.:
- Premier League 2024/25:  
  `https://www.oddsportal.com/football/england/premier-league-2024-2025/results/`

---

## How to Run

```bash
python scraper.py
```

Output: **one CSV per league and season** in the `output/` folder, e.g.:
- `output/premier-league-2024-2025.csv`
- `output/premier-league-2023-2024.csv`

---

## Important Notes

### OddsPortal Structure
- **Results pages**: e.g. `.../premier-league-2024-2025/results/`
- **Match pages**: each row links to a match detail page with tabs (1X2, Goals, etc.)
- **Over/Under 1.5**: usually under the "Goals" tab
- Content is loaded dynamically; plain `requests` will not work

### Betfair Lay Odds
- OddsPortal typically compares **Back** odds across bookmakers.
- **Betfair Exchange** exposes both Back and Lay; OddsPortal may show only Back.
- If Lay odds are missing in output, check the match page manually to see if they are available. Some markets may only show Back odds.
- Alternative: [Betfair's own API](https://docs.developer.betfair.com/) if you need Lay odds directly.

### Rate Limiting
- Use `DELAY_BETWEEN_REQUESTS` in `config.py` (default: 2 seconds).
- Avoid large runs without delays to reduce risk of being blocked.

### robots.txt
Check [OddsPortal's robots.txt](https://www.oddsportal.com/robots.txt) and Terms of Use before large-scale scraping.

---

## Current Status

- **Teams & URLs**: Working — extracted from links and URL slugs
- **Date & scores**: Partially working — OddsPortal loads data dynamically; dates/scores appear when DOM structure matches. Finished matches (e.g. 2023–24 season) may show more.
- **Over/Under 1.5 odds**: Attempted via API interception and Goals tab scrape — OddsPortal’s feed is encrypted; extraction depends on their current layout.
- **Betfair Lay**: OddsPortal mainly shows Back odds; Lay is exchange-specific and may not be available.

**Screenshot mode:** Set `SCREENSHOT_MODE = True` in config to save a PNG of each match page. You can later use OCR or manual entry to get date/odds from the images. Screenshots go to `output/screenshots/{league-slug}/`.

**Tip:** For more complete data, use completed seasons such as `premier-league-2023-2024/results/`.

---

## Project Structure

```
Parsing _ Project/
├── config.py       # Leagues, delays, OUTPUT_DIR
├── scraper.py      # Main scraper (Playwright + BeautifulSoup)
├── requirements.txt
├── README.md
└── output/         # One CSV per league-season
    ├── premier-league-2024-2025.csv
    ├── premier-league-2023-2024.csv
    └── ...
```

---

## Customization

- **Leagues**: add URLs to `LEAGUES` in `config.py` — one CSV per league/season
- **Output folder**: set `OUTPUT_DIR` in `config.py` (default: `output/`)
- **Max matches per run**: `MAX_MATCHES_PER_RUN` in `config.py` (`None` = no cap)
- **Total rows cap** (optional): `TARGET_MATCHES_PER_SEASON` (`None` = no cap; scrape full league). The GUI can set a temporary “Limit to N matches” for testing.

---

## Troubleshooting

1. **No matches found**  
   Page layout may have changed. Inspect the results page in DevTools to update selectors in `parse_results_page_html()`.

2. **Empty odds**  
   Match page structure or market naming may differ. Verify Over/Under 1.5 on the site.

3. **Playwright errors**  
   Run `playwright install chromium` and ensure a supported browser is available.
