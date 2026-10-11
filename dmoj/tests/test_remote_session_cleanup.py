import threading
import time
import unittest
from typing import Set
from unittest import mock

from dmoj.remote.backend import CreationCancelled, VM, VMBackend
from dmoj.remote.session import RemoteGradingSession


class SlowBackend(VMBackend):
    """VM 1 starts at once; later VMs take a while, like MicroVMs being provisioned. Records what is alive."""

    def __init__(self, extra_delay: float = 5) -> None:
        self.lock = threading.Lock()
        self.alive: Set[str] = set()
        self.created = 0
        self.extra_delay = extra_delay

    def create_vm(self, label, cancelled=None):
        with self.lock:
            self.created += 1
            vm_id = f'vm-{self.created}'
            self.alive.add(vm_id)
        if not label.endswith('-1'):
            deadline = time.monotonic() + self.extra_delay
            while time.monotonic() < deadline:
                if cancelled is not None and cancelled.is_set():
                    self.delete_vm(VM(vm_id, '', ''))
                    raise CreationCancelled(vm_id)
                time.sleep(0.01)
        return VM(vm_id, f'http://{vm_id}', 'token')

    def delete_vm(self, vm):
        with self.lock:
            self.alive.discard(vm.id)


def case(position, points=1):
    return {'position': position, 'batch': None, 'points': points, 'cache_key': [f'{position}.in', f'{position}.out']}


def graded(points):
    return {
        'result_flag': 0,
        'execution_time': 0.1,
        'wall_clock_time': 0.1,
        'max_memory': 1,
        'context_switches': [0, 0],
        'runtime_version': '',
        'output': '',
        'feedback': '',
        'extended_feedback': '',
        'points': points,
    }


class RemoteSessionCleanupTest(unittest.TestCase):
    def run_session(self, abort_after=None, warm_idle=0, backend=None, pool=None, healthy=True, setup_delay=0):
        backend = backend or SlowBackend()
        submission = mock.Mock(
            id=7,
            problem_id='p',
            storage_namespace=None,
            language='CPP17',
            source='',
            time_limit=1.0,
            memory_limit=65536,
            short_circuit=False,
            meta={},
        )
        config = mock.Mock(max_vms=4, max_concurrent_vms=8, warm_idle_seconds=warm_idle, storage=mock.Mock())
        with mock.patch('dmoj.remote.session.env', mock.Mock(remote=config)):
            session = RemoteGradingSession(submission, backend, pool)

        def call(runner, path, timeout, json=None, data=None, **kwargs):
            if path in ('/prepare', '/artifact') and setup_delay:
                time.sleep(setup_delay)  # an extra VM still being set up while VM 1 finishes everything
            if path == '/compile':
                manifest = {
                    'pretested': False,
                    'warning': None,
                    'cases': [case(i) for i in range(1, 9)],
                    'batch_dependencies': [],
                }
                return {'manifest': manifest, 'first_result': graded(1)}
            if path == '/grade':
                time.sleep(0.02)
                return graded(1)
            return {'ok': True}

        with mock.patch('dmoj.remote.session.Runner.call', call), mock.patch(
            'dmoj.remote.session.Runner.download_artifact', return_value=b''
        ), mock.patch('dmoj.remote.session.Runner.healthy', return_value=healthy), mock.patch(
            'dmoj.remote.session.problem_urls', return_value={}
        ), mock.patch(
            'dmoj.remote.session.list_problem_objects', return_value=[('init.yml', 10)]
        ):
            messages = []
            alive_at_end = None
            if abort_after is not None:
                threading.Timer(abort_after, session.abort).start()
            for name, _ in session.run():
                messages.append(name.name if hasattr(name, 'name') else name)
                if messages[-1] == 'GRADING_END':
                    alive_at_end = set(backend.alive)
        return backend, messages, alive_at_end, session

    def test_vms_still_starting_when_grading_ends_are_deleted(self):
        # VM 1 grades all 8 cases long before VMs 2-4 are ready.
        backend, messages, alive_at_end, _ = self.run_session()
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.created, 4)
        self.assertEqual(backend.alive, set())
        # The verdict is reported only once every VM is gone: the site gives this judge its next submission as soon
        # as it hears it, and the judge can't take it (or answer pings) while the worker is still deleting VMs.
        self.assertEqual(alive_at_end, set())

    def test_vms_still_starting_on_abort_are_deleted(self):
        backend, messages, _, _ = self.run_session(abort_after=0.05)
        self.assertEqual(messages[-1], 'GRADING_ABORTED')
        self.assertEqual(backend.alive, set())

    def test_clean_end_parks_vms_for_the_next_submission(self):
        from dmoj.remote.pool import VMPool

        pool = VMPool(8)
        backend, messages, alive_at_end, _ = self.run_session(warm_idle=10, pool=pool)
        self.assertEqual(messages[-1], 'GRADING_END')
        # VM 1 graded everything, so it is parked (after a reset); the three VMs still starting are deleted.
        self.assertEqual(backend.alive, {'vm-1'})
        self.assertEqual(pool.warm_count(), 1)

        # The next submission takes the parked VM as its first instead of creating one (its slot travels with it),
        # and creates only its three extras, which are again deleted as they come up too late.
        backend, messages, _, _ = self.run_session(warm_idle=10, backend=backend, pool=pool)
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.created, 7)
        self.assertEqual(backend.alive, {'vm-1'})
        self.assertEqual(pool.warm_count(), 1)

        # After the idle timeout the reaper takes it out of the pool to delete it, and the slot is freed.
        self.assertEqual([record['id'] for record in pool.reap_warm(0)], ['vm-1'])
        self.assertEqual(pool.warm_count(), 0)

    def test_abort_deletes_vms_instead_of_parking(self):
        from dmoj.remote.pool import VMPool

        pool = VMPool(8)
        backend, messages, _, _ = self.run_session(abort_after=0.05, warm_idle=10, pool=pool)
        self.assertEqual(messages[-1], 'GRADING_ABORTED')
        self.assertEqual(backend.alive, set())
        self.assertEqual(pool.warm_count(), 0)

    def test_dead_parked_vm_is_replaced(self):
        from dmoj.remote.backend import VM
        from dmoj.remote.pool import VMPool

        pool = VMPool(8)
        self.assertTrue(pool.acquire_first())
        pool.own('stale')
        self.assertTrue(pool.park(VM('stale', 'http://stale', 'token')))
        backend, messages, _, _ = self.run_session(warm_idle=10, pool=pool, healthy=False)
        # The stale VM was deleted and a fresh one created in its place; grading still succeeded.
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.created, 4)
        self.assertNotIn('stale', pool.owned())

    def test_vm_still_being_set_up_when_grading_ends_is_released(self):
        # Extra VMs come up quickly but take a while to prepare; VM 1 grades every case before they are ready. The
        # session must wait for those workers before letting go of the VMs, or a VM recorded after teardown took
        # the list (or one still in use by its worker) is never parked or deleted.
        from dmoj.remote.pool import VMPool

        pool = VMPool(8)
        backend, messages, alive_at_end, _ = self.run_session(
            backend=SlowBackend(extra_delay=0.05), warm_idle=10, pool=pool, setup_delay=0.3
        )
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.created, 4)
        # Every VM is accounted for: parked (reusable) or deleted, none forgotten.
        self.assertEqual(len(backend.alive), pool.warm_count())
        self.assertEqual(sorted(record['id'] for record in pool.reap_warm(0)), sorted(backend.alive))

    def test_slots_balance_when_a_vm_becomes_ready_after_grading_ends(self):
        # Extra VMs become ready right after VM 1 graded everything: they are deleted, and every pool slot comes back
        # exactly once. A slot released twice over-releases the pool for the reaper to trip on later; one released
        # never shrinks the pool for good.
        from dmoj.remote.pool import VMPool

        pool = VMPool(8)
        backend, messages, _, session = self.run_session(backend=SlowBackend(extra_delay=0.15), pool=pool)
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.alive, set())
        self.assertEqual(session._slots, 0)
        for _ in range(8):
            self.assertTrue(pool.acquire_extra(held=0))
        self.assertFalse(pool.acquire_extra(held=0))
        for _ in range(8):
            pool.release()
        with self.assertLogs('dmoj.remote.pool', level='ERROR'):
            pool.release()  # the pool is full again: no slot was released twice

    def test_vm_recorded_after_cleanup_is_let_go(self):
        # The record closes under the lock when cleanup takes it; a VM arriving afterwards is deleted by its creator
        # and its slot returned, whatever the thread timing.
        from dmoj.remote.pool import VMPool

        pool = VMPool(4)
        backend, messages, _, session = self.run_session(pool=pool)
        self.assertTrue(session._closed)
        # A creation that was already past the abort check when the record closed: the backend returns the VM.
        session._aborted.clear()

        def late_create(label, cancelled=None):
            backend.alive.add('late')
            return VM('late', 'http://late', 't')

        with mock.patch.object(backend, 'create_vm', late_create):
            with self.assertRaises(Exception):
                session._create_runner_with_slot('9', first=True)
        self.assertNotIn('late', backend.alive)
        self.assertEqual(session._slots, 0)
        self.assertNotIn('late', pool.owned())
        # The slot it took is back in the pool: all 4 can be taken again.
        for _ in range(4):
            self.assertTrue(pool.acquire_extra(held=0))
