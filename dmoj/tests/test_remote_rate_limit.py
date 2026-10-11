import multiprocessing
import time
import unittest
from unittest import mock

from dmoj.remote import rate_limit
from dmoj.remote.rate_limit import RateLimiter


class RateLimiterTest(unittest.TestCase):
    def setUp(self):
        self.now = 1_000_000.0
        self.slept = []
        mock.patch('dmoj.remote.rate_limit.time.time', side_effect=lambda: self.now).start()
        mock.patch('dmoj.remote.rate_limit.time.sleep', side_effect=self._sleep).start()
        self.addCleanup(mock.patch.stopall)

    def _sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    def test_spreads_the_remaining_budget_over_the_window(self):
        limiter = RateLimiter()
        # 600 requests left for the next 600 seconds: one a second.
        limiter.update({'ratelimit-remaining': '800', 'ratelimit-reset': str(int(self.now + 600))})
        limiter.wait()  # the first goes at once
        self.assertEqual(self.slept, [])
        limiter.wait()
        self.assertAlmostEqual(sum(self.slept), 1.0, places=2)

    def test_full_budget_is_paced_over_the_hour(self):
        limiter = RateLimiter()
        limiter.update({'ratelimit-remaining': '5000', 'ratelimit-reset': str(int(self.now + 3600))})
        limiter.wait()
        limiter.wait()
        # 4,800 spendable requests over 3,600 s: one every 0.75 s, slower than the minute limit's 0.25 s.
        self.assertAlmostEqual(sum(self.slept), 0.75, places=2)

    def test_a_short_window_is_paced_by_the_minute_limit(self):
        limiter = RateLimiter()
        # Plenty left for the 2 minutes left in the hour: the minute limit binds, a quarter second apart.
        limiter.update({'ratelimit-remaining': '5000', 'ratelimit-reset': str(int(self.now + 120))})
        limiter.wait()
        limiter.wait()
        self.assertAlmostEqual(sum(self.slept), 0.25, places=2)

    def test_keeps_a_floor_for_the_rest_of_the_hour(self):
        limiter = RateLimiter()
        limiter.update(
            {'ratelimit-remaining': str(rate_limit.HOURLY_FLOOR), 'ratelimit-reset': str(int(self.now + 120))}
        )
        limiter.wait()
        # Only the floor is left: this call waited for the window to reset.
        self.assertGreaterEqual(sum(self.slept), 120)

    def test_minute_limit_holds_a_burst(self):
        limiter = RateLimiter()
        limiter.update({'ratelimit-remaining': '5000', 'ratelimit-reset': str(int(self.now + 120))})
        for _ in range(rate_limit.MINUTE_LIMIT - rate_limit.MINUTE_FLOOR):
            limiter.wait()
        # 240 calls a quarter second apart fill the minute; the next one waits for it to roll over.
        self.assertLess(sum(self.slept), 60)
        limiter.wait()
        self.assertGreaterEqual(sum(self.slept), 60)

    def test_headers_without_digits_are_ignored(self):
        limiter = RateLimiter()
        limiter.update({'ratelimit-remaining': 'x', 'ratelimit-reset': None})
        self.assertEqual(limiter.remaining(), rate_limit.HOURLY_LIMIT)


class SharedRateLimiterTest(unittest.TestCase):
    def test_forked_workers_share_the_budget(self):
        limiter = RateLimiter()
        limiter.update({'ratelimit-remaining': '1000', 'ratelimit-reset': str(int(time.time() + 3600))})

        def child():
            limiter.wait()
            limiter.wait()

        process = multiprocessing.get_context('fork').Process(target=child)
        process.start()
        process.join(10)
        self.assertEqual(limiter.remaining(), 998)
