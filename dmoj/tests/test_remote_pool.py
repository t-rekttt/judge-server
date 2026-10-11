import multiprocessing
import threading
import unittest

from dmoj.remote.pool import VMPool


class VMPoolTest(unittest.TestCase):
    def test_first_vm_is_always_granted_and_extras_follow_fair_share(self):
        pool = VMPool(4)
        pool.register()
        self.assertTrue(pool.acquire_first())
        # One submission: its share is the whole pool.
        self.assertTrue(pool.acquire_extra(held=1))
        self.assertTrue(pool.acquire_extra(held=2))
        self.assertTrue(pool.acquire_extra(held=3))
        self.assertFalse(pool.acquire_extra(held=4))  # pool exhausted

        # A second submission halves the share: the first one can't grow, even once slots free up.
        pool.register()
        pool.release()
        pool.release()
        self.assertFalse(pool.acquire_extra(held=2))
        self.assertTrue(pool.acquire_first())  # the newcomer gets its first VM
        self.assertTrue(pool.acquire_extra(held=1))
        self.assertFalse(pool.acquire_extra(held=2))  # share is 2 each

        # The first submission finishes: the share grows back to the whole pool.
        pool.unregister()
        pool.release()
        pool.release()
        self.assertTrue(pool.acquire_extra(held=2))

    def test_first_vm_waits_for_a_free_slot_unless_cancelled(self):
        pool = VMPool(1)
        pool.register()
        self.assertTrue(pool.acquire_first())
        cancelled = threading.Event()
        threading.Timer(0.2, cancelled.set).start()
        self.assertFalse(pool.acquire_first(cancelled))

        cancelled = threading.Event()
        threading.Timer(0.2, pool.release).start()
        self.assertTrue(pool.acquire_first(cancelled))

    def test_slots_are_shared_across_forked_processes(self):
        # The judge grades each submission in a forked worker; the pool must count across them.
        pool = VMPool(2)
        pool.register()
        self.assertTrue(pool.acquire_first())

        def child(result):
            result.value = int(pool.acquire_extra(held=1)) + int(pool.acquire_extra(held=2)) * 10

        context = multiprocessing.get_context('fork')
        result = context.Value('i', -1)
        process = context.Process(target=child, args=(result,))
        process.start()
        process.join(10)
        # The child got the last slot (1) but not a third (no +10).
        self.assertEqual(result.value, 1)
        self.assertFalse(pool.acquire_extra(held=1))
