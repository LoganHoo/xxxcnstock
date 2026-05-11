"""
Market calendar with holiday awareness.

Identifies trading vs non-trading days and determines whether a task
should run based on its day_type configuration.
"""

import datetime
from typing import Optional

from loguru import logger


class MarketCalendar:
    """Market calendar that knows about trading days and holidays."""

    def __init__(self, holidays: Optional[list[str]] = None):
        """Initialize with optional holiday list.

        Args:
            holidays: List of date strings in YYYY-MM-DD format.
        """
        self._holidays: set[datetime.date] = set()
        if holidays:
            for h in holidays:
                try:
                    self._holidays.add(datetime.date.fromisoformat(h))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid holiday date '{h}': {e}")

    def is_trading_day(self, date: Optional[datetime.date] = None) -> bool:
        """Check if a date is a trading day.

        A trading day is a weekday (Mon-Fri) that is not in the holiday list.
        """
        date = date or datetime.date.today()
        if date in self._holidays:
            return False
        return date.weekday() < 5  # Mon=0 ... Fri=4

    def should_run_task(
        self, day_type: Optional[str], date: Optional[datetime.date] = None
    ) -> bool:
        """Determine if a task should run based on day_type.

        Args:
            day_type: One of "daily", "weekday", "weekend", or None.
                - "daily": Always run.
                - "weekday": Run only on trading days.
                - "weekend": Run only on non-trading days.
                - None: Always run (default).
            date: Date to check (defaults to today).

        Returns:
            True if the task should run.
        """
        if day_type is None or day_type == "daily":
            return True

        trading = self.is_trading_day(date)

        if day_type == "weekday":
            return trading
        elif day_type == "weekend":
            return not trading
        else:
            # Unknown day_type, default to allowing execution
            logger.warning(f"Unknown day_type '{day_type}', allowing execution")
            return True
