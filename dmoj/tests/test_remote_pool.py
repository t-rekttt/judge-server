import multiprocessing
import threading
import time
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

    def test_ownership_record_is_shared_with_forked_workers(self):
        # Sessions record their VMs from forked workers; the sweeper reads the record in the main process.
        pool = VMPool(3)
        pool.own('vm-main')

        def child():
            pool.own('vm-child')
            pool.disown('vm-main')

        context = multiprocessing.get_context('fork')
        process = context.Process(target=child)
        process.start()
        process.join(10)
        self.assertEqual(pool.owned(), {'vm-child'})
        pool.disown('vm-child')
        self.assertEqual(pool.owned(), set())

    def test_ownership_record_is_bounded_by_capacity(self):
        pool = VMPool(2)
        pool.own('a')
        pool.own('b')
        pool.own('c')  # no slot left: dropped, never raises
        self.assertEqual(pool.owned(), {'a', 'b'})
        pool.own('a')  # already recorded: no duplicate
        self.assertEqual(pool.owned(), {'a', 'b'})

    def test_warm_vms_are_shared_with_forked_workers_and_reaped_by_age(self):
        from dmoj.remote.backend import VM

        pool = VMPool(2)
        self.assertTrue(pool.acquire_first())
        pool.own('vm-a')

        def child():
            pool.park(VM('vm-a', 'http://a', 'token', {'Authorization': 'Bearer x'}))

        context = multiprocessing.get_context('fork')
        process = context.Process(target=child)
        process.start()
        process.join(10)
        self.assertEqual(pool.warm_count(), 1)
        self.assertEqual(pool.reap_warm(3600), [])  # too young
        taken = pool.take_warm()
        self.assertEqual(VM(**taken), VM('vm-a', 'http://a', 'token', {'Authorization': 'Bearer x'}))
        self.assertIsNone(pool.take_warm())
        self.assertTrue(pool.park(VM(**taken)))
        self.assertEqual([record['id'] for record in pool.reap_warm(0)], ['vm-a'])
        self.assertEqual(pool.warm_count(), 0)

    def test_parked_lists_without_taking(self):
        from dmoj.remote.backend import VM

        pool = VMPool(2)
        pool.park(VM('a', 'u', 't'))
        self.assertEqual([record['id'] for record in pool.parked()], ['a'])
        self.assertEqual(pool.warm_count(), 1)

    def test_parking_is_bounded_by_capacity(self):
        from dmoj.remote.backend import VM

        pool = VMPool(1)
        self.assertTrue(pool.park(VM('a', 'u', 't')))
        self.assertFalse(pool.park(VM('b', 'u', 't')))

    def test_reaper_keeps_the_standby_reserve(self):
        from dmoj.remote.backend import VM

        pool = VMPool(4)
        for name in ('old', 'mid', 'new'):
            self.assertTrue(pool.acquire_standby())
            self.assertTrue(pool.park(VM(name, 'u', 't')))
            time.sleep(0.01)
        # Beyond the two newest, only the oldest ages out; the reserve stays whatever its age.
        self.assertEqual([record['id'] for record in pool.reap_warm(0, keep=2)], ['old'])
        self.assertEqual(pool.warm_count(), 2)
        self.assertEqual(pool.reap_warm(0, keep=2), [])
        # Shutdown takes everything.
        self.assertEqual(sorted(record['id'] for record in pool.reap_warm(0)), ['mid', 'new'])
        # Standby VMs never wait for a slot: the fourth slot is free, then the pool is full.
        self.assertTrue(pool.acquire_standby())
        self.assertFalse(pool.acquire_standby())

    def test_over_release_is_logged_not_raised(self):
        pool = VMPool(1)
        with self.assertLogs('dmoj.remote.pool', level='ERROR'):
            pool.release()
        # The pool is still exactly its capacity.
        self.assertTrue(pool.acquire_first())
        self.assertFalse(pool.acquire_extra(held=0))
