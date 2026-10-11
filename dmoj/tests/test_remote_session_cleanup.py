import threading
import time
import unittest
from typing import Set
from unittest import mock

from dmoj.remote.backend import CreationCancelled, VM, VMBackend
from dmoj.remote.session import RemoteGradingSession


class SlowBackend(VMBackend):
    """VM 1 starts at once; later VMs take a while, like MicroVMs being provisioned. Records what is alive."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.alive: Set[str] = set()
        self.created = 0

    def create_vm(self, label, cancelled=None):
        with self.lock:
            self.created += 1
            vm_id = f'vm-{self.created}'
            self.alive.add(vm_id)
        if not label.endswith('-1'):
            deadline = time.monotonic() + 5
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
    def run_session(self, abort_after=None):
        backend = SlowBackend()
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
        config = mock.Mock(max_vms=4, storage=mock.Mock())
        with mock.patch('dmoj.remote.session.env', mock.Mock(remote=config)):
            session = RemoteGradingSession(submission, backend)

        def call(runner, path, timeout, json=None, data=None):
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
        ), mock.patch('dmoj.remote.session.problem_urls', return_value={}), mock.patch(
            'dmoj.remote.session.list_problem_objects', return_value=[('init.yml', 10)]
        ):
            messages = []
            if abort_after is not None:
                threading.Timer(abort_after, session.abort).start()
            for name, _ in session.run():
                messages.append(name.name if hasattr(name, 'name') else name)
        return backend, messages

    def test_vms_still_starting_when_grading_ends_are_deleted(self):
        # VM 1 grades all 8 cases long before VMs 2-4 are ready.
        backend, messages = self.run_session()
        self.assertEqual(messages[-1], 'GRADING_END')
        self.assertEqual(backend.created, 4)
        self.assertEqual(backend.alive, set())

    def test_vms_still_starting_on_abort_are_deleted(self):
        backend, messages = self.run_session(abort_after=0.05)
        self.assertEqual(backend.alive, set())
