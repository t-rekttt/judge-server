"""
Paces calls to the MicroVM API so they stay under its rate limit instead of running into it.

DigitalOcean allows 5,000 requests an hour and 250 a minute per token, and answers every call with
`ratelimit-remaining` and `ratelimit-reset` (a Unix timestamp). Rather than retrying after a 429, which spends a
request to learn nothing, callers wait their turn here first: the budget left in the window is spread evenly over the
time left in it, so a burst of VM creations slows down to a sustainable pace and the hour never ends with the budget
gone and submissions waiting a full reset.

One limiter is shared by every caller in the process and by the grading workers forked from it, so the pace is the
token's, not one caller's.
"""

import multiprocessing
import threading
import time
from typing import Any, Mapping, Optional

# DigitalOcean's limits; the headers correct these as soon as the first call answers.
HOURLY_LIMIT = 5000
MINUTE_LIMIT = 250
# Never spend the window's whole budget: keep this many requests for the hour's end (retries, sweeps, deletions).
HOURLY_FLOOR = 200
# Below this share of the minute budget, calls wait for the next minute rather than compete for the last requests.
MINUTE_FLOOR = 10


class RateLimiter:
    def __init__(self, hourly_limit: int = HOURLY_LIMIT, minute_limit: int = MINUTE_LIMIT) -> None:
        self.hourly_limit = hourly_limit
        self.minute_limit = minute_limit
        self._lock = multiprocessing.Lock()
        # Hour window, from the API's headers: requests left and when the window resets (Unix time).
        self._remaining = multiprocessing.Value('i', hourly_limit)
        self._reset_at = multiprocessing.Value('d', time.time() + 3600)
        # Minute window, counted here: the API doesn't report it.
        self._minute_start = multiprocessing.Value('d', time.time())
        self._minute_used = multiprocessing.Value('i', 0)
        # Earliest time the next call may go, so calls are spaced instead of released together.
        self._next_at = multiprocessing.Value('d', 0.0)

    def wait(self, cancelled: Optional[threading.Event] = None) -> None:
        """
        Blocks until this call fits the pace, then takes its place in both windows. Returns early, without taking a
        place, if `cancelled` is set while waiting: the caller then doesn't make the call.
        """
        while True:
            if cancelled is not None and cancelled.is_set():
                return
            with self._lock:
                now = time.time()
                if now >= self._minute_start.value + 60:
                    self._minute_start.value = now
                    self._minute_used.value = 0
                if now >= self._reset_at.value:
                    # The hour rolled over without a call telling us the new window: assume a full budget.
                    self._remaining.value = self.hourly_limit
                    self._reset_at.value = now + 3600
                delay = self._delay(now)
                if delay <= 0:
                    self._minute_used.value += 1
                    self._remaining.value = max(0, self._remaining.value - 1)
                    self._next_at.value = now + self._spacing(now)
                    return
            if cancelled is not None:
                cancelled.wait(min(delay, 5.0))
            else:
                time.sleep(min(delay, 5.0))

    def _delay(self, now: float) -> float:
        """Seconds until the next call may go, 0 if now."""
        if self._minute_used.value >= self.minute_limit - MINUTE_FLOOR:
            return self._minute_start.value + 60 - now
        if self._remaining.value <= HOURLY_FLOOR:
            return self._reset_at.value - now
        return self._next_at.value - now

    def _spacing(self, now: float) -> float:
        """Seconds between calls that spreads the hour's remaining budget over the time left, within the minute limit."""
        left_in_window = max(1.0, self._reset_at.value - now)
        spendable = max(1, self._remaining.value - HOURLY_FLOOR)
        hourly_pace = left_in_window / spendable
        minute_pace = 60.0 / max(1, self.minute_limit - MINUTE_FLOOR)
        return max(hourly_pace, minute_pace)

    def update(self, headers: Mapping[str, Any]) -> None:
        """Takes the API's view of the hour window from a response's headers."""
        remaining = headers.get('ratelimit-remaining')
        reset = headers.get('ratelimit-reset')
        with self._lock:
            if remaining is not None and str(remaining).isdigit():
                self._remaining.value = int(remaining)
            if reset is not None and str(reset).isdigit():
                self._reset_at.value = float(reset)
            limit = headers.get('ratelimit-limit')
            if limit is not None and str(limit).isdigit() and int(limit) > 0:
                self.hourly_limit = int(limit)

    def remaining(self) -> int:
        with self._lock:
            return self._remaining.value


_shared: Optional[RateLimiter] = None
_shared_lock = threading.Lock()


def shared_limiter() -> RateLimiter:
    """The process's limiter. Create it in the main process before forking workers, so they share it."""
    global _shared
    with _shared_lock:
        if _shared is None:
            _shared = RateLimiter()
        return _shared
