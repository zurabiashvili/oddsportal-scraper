"""Regression tests for scraper fixes (no Playwright). Run from repo root: python tests/test_regression.py"""
from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scraper import (
    EXPORT_TEMPLATE_N_IMPLIED_PCT_FORMULA,
    EXPORT_TEMPLATE_P_EDGE_FORMULA,
    EXPORT_TEMPLATE_Q_EV_FORMULA,
    _template_n_avg_odds_formula,
    MatchData,
    _dedupe_league_results,
    _load_existing_results,
    _match_row_incomplete,
    _normalize_results_row_ft_score,
    _parse_match_datetime,
    _write_matches_template_csv,
    merge_listing_and_api_home_away,
    normalize_match_url,
    normalize_match_date_field,
    parse_results_page_html,
)


def test_match_row_incomplete_ft_missing():
    md = MatchData(
        date="12 Apr 2025",
        home_team="A",
        away_team="B",
        full_time_home="",
        full_time_away="",
        half_time_home="",
        half_time_away="",
        half_time_total="",
        over_odds=None,
        under_odds=None,
        betfair_lay_over=None,
        betfair_lay_under=None,
        match_url="https://www.oddsportal.com/x",
        league="L",
    )
    assert _match_row_incomplete(md) is True


def test_match_row_incomplete_fully_done():
    md = MatchData(
        date="12 Apr 2025",
        home_team="A",
        away_team="B",
        full_time_home="2",
        full_time_away="1",
        half_time_home="1",
        half_time_away="0",
        half_time_total="1",
        over_odds="1.9",
        under_odds="1.95",
        betfair_lay_over="2.0",
        betfair_lay_under="1.9",
        match_url="https://www.oddsportal.com/x",
        league="L",
    )
    assert _match_row_incomplete(md) is False


def test_match_row_incomplete_needs_odds():
    md = MatchData(
        date="12 Apr 2025",
        home_team="A",
        away_team="B",
        full_time_home="2",
        full_time_away="1",
        half_time_home="1",
        half_time_away="0",
        half_time_total="1",
        over_odds=None,
        under_odds=None,
        betfair_lay_over=None,
        betfair_lay_under=None,
        match_url="https://www.oddsportal.com/x",
        league="L",
    )
    assert _match_row_incomplete(md) is True


def test_merge_listing_api_direct_and_swap():
    h, a = merge_listing_and_api_home_away("Osasuna", "Betis", "Osasuna", "Betis", "?", "?")
    assert (h, a) == ("Osasuna", "Betis")
    h, a = merge_listing_and_api_home_away("Betis", "Osasuna", "Osasuna", "Betis", "?", "?")
    assert (h, a) == ("Osasuna", "Betis")


def test_normalize_match_url_hosts():
    u1 = "https://wv.oddsportal.com/football/spain/laliga/a-b-CdEfGhIj/"
    u2 = "https://www.oddsportal.com/football/spain/laliga/a-b-CdEfGhIj/"
    assert normalize_match_url(u1) == normalize_match_url(u2)


def test_season_date_inference():
    league = "https://www.oddsportal.com/football/spain/laliga-2025-2026/results/"
    assert _parse_match_datetime("Friday, 15 Aug", league).year == 2025
    assert _parse_match_datetime("Friday, 11 Apr", league).year == 2026
    s = normalize_match_date_field("Friday, 11 Apr", league)
    assert "2026" in s or "Apr" in s


def test_spaced_ft_score_token():
    assert _normalize_results_row_ft_score("3 - 0") == "3:0"
    assert _normalize_results_row_ft_score("3:0") == "3:0"


def test_parse_results_html_spaced_score():
    html = """
<html><body>
<p>Today, 12 Apr</p>
<div>
  <div>Ath Bilbao</div>
  <div>Villarreal</div>
  <a href="/football/spain/laliga-2025-2026/ath-bilbao-villarreal-AbCdEfGh/">x</a>
  <div>2 - 1</div>
</div>
</body></html>
"""
    rows = parse_results_page_html(html, "", "laliga")
    r0 = next(r for r in rows if "ath-bilbao" in r["match_url"])
    assert r0.get("score_ft") == "2:1"


def test_parse_results_plain_date_bar_after_yesterday_bar():
    """OddsPortal often uses 'Yesterday, 19 Apr' then a plain '18 Apr 2026' bar; later rows must not keep yesterday's date."""
    html = """
<html><body>
<div>Yesterday, 19 Apr</div>
<div>
  <a href="/football/england/league-one-2025-2026/port-vale-wigan-AbCdEfGh/">x</a>
  <div>0 - 0</div>
</div>
<div>18 Apr 2026</div>
<div>
  <a href="/football/england/league-one-2025-2026/afc-wimbledon-plymouth-XyZaBcDe/">x</a>
  <div>1 - 3</div>
</div>
</body></html>
"""
    rows = parse_results_page_html(html, "", "League One")
    by_url = {r["match_url"]: r for r in rows}
    u1 = next(k for k in by_url if "port-vale" in k)
    u2 = next(k for k in by_url if "wimbledon" in k)
    assert "Yesterday" in (by_url[u1].get("date") or "") or "19" in (by_url[u1].get("date") or "")
    d2 = (by_url[u2].get("date") or "").strip()
    assert "18" in d2 and "Apr" in d2
    assert "Yesterday" not in d2


def test_template_csv_partial_row_resume():
    tmp = Path(tempfile.mkdtemp())
    slug = "t-league"
    league_url = "https://www.oddsportal.com/football/spain/laliga-2025-2026/results/"
    md = MatchData(
        date="12 Apr 2025",
        home_team="H",
        away_team="A",
        full_time_home="",
        full_time_away="",
        half_time_home="",
        half_time_away="",
        half_time_total="",
        over_odds=None,
        under_odds=None,
        betfair_lay_over=None,
        betfair_lay_under=None,
        match_url="https://www.oddsportal.com/football/spain/laliga-2025-2026/h-a-XyZaBcDe/",
        league="laliga",
    )
    _write_matches_template_csv(tmp / f"{slug}.csv", [md])
    loaded, complete = _load_existing_results(tmp, slug, league_url=league_url)
    assert len(loaded) == 1
    assert _match_row_incomplete(loaded[0]) is True
    assert len(complete) == 0


def test_dedupe_duplicate_urls():
    u = "https://www.oddsportal.com/football/spain/laliga-2025-2026/x-y-AbCdEfGh/"
    a = MatchData(
        date="1 Jan 2025",
        home_team="X",
        away_team="Y",
        full_time_home="1",
        full_time_away="1",
        half_time_home="0",
        half_time_away="0",
        half_time_total="0",
        over_odds=None,
        under_odds=None,
        betfair_lay_over=None,
        betfair_lay_under=None,
        match_url=u,
        league="L",
    )
    b = MatchData(
        date="1 Jan 2025",
        home_team="X",
        away_team="Y",
        full_time_home="1",
        full_time_away="1",
        half_time_home="0",
        half_time_away="0",
        half_time_total="0",
        over_odds="1.5",
        under_odds="2.5",
        betfair_lay_over="1.6",
        betfair_lay_under="2.4",
        match_url="https://wv.oddsportal.com/football/spain/laliga-2025-2026/x-y-AbCdEfGh/",
        league="L",
    )
    out = _dedupe_league_results([a, b])
    assert len(out) == 1
    assert out[0].over_odds == "1.5"


def test_template_csv_average_betfair_lay_under_summary():
    tmp = Path(tempfile.mkdtemp())
    out_csv = tmp / "avg-check.csv"
    rows = [
        MatchData(
            date="1 Jan 2025",
            home_team="A",
            away_team="B",
            full_time_home="1",
            full_time_away="0",
            half_time_home="1",
            half_time_away="0",
            half_time_total="1",
            over_odds="1.7",
            under_odds="2.1",
            betfair_lay_over="1.8",
            betfair_lay_under="3.10",
            match_url="https://www.oddsportal.com/football/x/a-b-AbCdEfGh/",
            league="L",
        ),
        MatchData(
            date="2 Jan 2025",
            home_team="C",
            away_team="D",
            full_time_home="2",
            full_time_away="1",
            half_time_home="1",
            half_time_away="1",
            half_time_total="2",
            over_odds="1.9",
            under_odds="1.9",
            betfair_lay_over="2.0",
            betfair_lay_under="3.30",
            match_url="https://www.oddsportal.com/football/x/c-d-QwErTyUi/",
            league="L",
        ),
        MatchData(
            date="3 Jan 2025",
            home_team="E",
            away_team="F",
            full_time_home="0",
            full_time_away="0",
            half_time_home="0",
            half_time_away="0",
            half_time_total="0",
            over_odds="2.0",
            under_odds="1.8",
            betfair_lay_over=None,
            betfair_lay_under=None,
            match_url="https://www.oddsportal.com/football/x/e-f-ZxCvBnMm/",
            league="L",
        ),
    ]
    _write_matches_template_csv(out_csv, rows)
    with open(out_csv, newline="", encoding="utf-8-sig") as f:
        data = list(csv.reader(f))
    assert data[1][13] == "AVERAGE ODDS"
    assert data[2][13] == _template_n_avg_odds_formula()
    assert data[0][15] == "EDGE"
    assert data[1][15] == EXPORT_TEMPLATE_P_EDGE_FORMULA
    assert data[0][16] == "EV"
    assert data[3][13] == EXPORT_TEMPLATE_N_IMPLIED_PCT_FORMULA
    assert data[3][16] == EXPORT_TEMPLATE_Q_EV_FORMULA


def main() -> None:
    tests = [
        test_match_row_incomplete_ft_missing,
        test_match_row_incomplete_fully_done,
        test_match_row_incomplete_needs_odds,
        test_merge_listing_api_direct_and_swap,
        test_normalize_match_url_hosts,
        test_season_date_inference,
        test_spaced_ft_score_token,
        test_parse_results_html_spaced_score,
        test_template_csv_partial_row_resume,
        test_dedupe_duplicate_urls,
        test_template_csv_average_betfair_lay_under_summary,
    ]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\nAll {len(tests)} regression tests passed.")


if __name__ == "__main__":
    main()
