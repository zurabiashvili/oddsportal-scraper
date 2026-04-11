"""Scraper run configuration - used by GUI and CLI."""

from dataclasses import dataclass
from typing import Literal


@dataclass
class ScraperConfig:
    """Configuration for a single scrape run."""

    league_urls: list[str]  # Up to 12 URLs; processed sequentially (queue)
    market: Literal["ft", "ht"]  # Full Time or Half Time
    line: Literal[0.5, 1.5, 2.5]  # Over/Under goals line
    match_limit: int | None  # None = full season, else first N matches
    direction: Literal["newest", "oldest"]  # Newest first (page 1→) or oldest first (last page→)
    fresh_run: bool = False  # True = start empty and overwrite; False = resume from existing CSV

    def slug_suffix(self) -> str:
        """e.g. 'ft_1.5' or 'ht_2.5' for filename."""
        m = "ft" if self.market == "ft" else "ht"
        return f"{m}_{self.line}"
