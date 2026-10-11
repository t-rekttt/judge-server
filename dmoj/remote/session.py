"""
Grades one submission in disposable VMs while the judge only orchestrates.

1. The first VM downloads the problem, compiles the submission and grades case 1. On a compile error, grading stops
   there, without creating any other VM.
2. Other VMs start in parallel, download the problem and take the first VM's compiled programs, so they never compile.
3. Every VM repeatedly takes the next case still worth grading. Results go through the `Emitter`, which reports them
   with the same verdicts and in an order the site handles.
4. All VMs are deleted at the end, on abort and on errors.

`run()` yields the same IPC messages as `JudgeWorker._grade_cases`, so the rest of the judge is unchanged.
"""

import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Generator, List, Optional, TYPE_CHECKING, Tuple

import requests

from dmoj.judgeenv import env
from dmoj.remote.backend import VM, VMBackend, make_backend
from dmoj.remote.emitter import Emitter, UnitKey
from dmoj.remote.protocol import Manifest
from dmoj.remote.storage import list_problem_objects, problem_urls

if TYPE_CHECKING:
    from dmoj.judge import IPC, Submission

logger = logging.getLogger(__name__)

# Generous margins: a case can involve a generator, the submission (up to the wall time limit) and a checker.
REQUEST_MARGIN = 120  # seconds
SETUP_TIMEOUT = 600  # seconds, for downloading the problem and compiling
# How long grading may wait at the end for VM creations to notice they're cancelled: one API call with retries.
CREATE_CANCEL_TIMEOUT = 120  # seconds


class RemoteGradingError(Exception):
    pass


class Runner:
    """HTTP client for the runner in one VM."""

    def __init__(self, vm: VM, session: requests.Session) -> None:
        self.vm = vm
        self.http = session
        self.headers = {**vm.headers, 'X-Runner-Token': vm.token}

    def call(self, path: str, timeout: float, json: Any = None, data: Optional[bytes] = None) -> Any:
        response = self.http.post(
            self.vm.url + path,
            json=json,
            data=data,
            timeout=timeout,
            headers=self.headers,
        )
        if response.status_code != 200:
            raise RemoteGradingError(f'runner {self.vm.id} {path} failed ({response.status_code}): {response.text}')
        return response.json()

    def download_artifact(self) -> bytes:
        response = self.http.get(self.vm.url + '/artifact', timeout=SETUP_TIMEOUT, headers=self.headers)
        response.raise_for_status()
        return response.content


class RemoteGradingSession:
    def __init__(self, submission: 'Submission', backend: Optional[VMBackend] = None) -> None:
        self.submission = submission
        self.config = env.remote
        self.backend = backend or make_backend(self.config, env['id'])
        self.max_vms = int(self.config.max_vms or 4)
        wall_time = self.submission.time_limit * 3  # matches the default `wall_time_factor`
        self.grade_timeout = wall_time + REQUEST_MARGIN

        self._problem_objects: Optional[List[str]] = None
        self._lock = threading.Lock()
        self._vms: List[VM] = []
        self._aborted = threading.Event()
        self._http = requests.Session()
        # Threads inside `backend.create_vm`: their VM isn't recorded yet, so grading must not end under them.
        self._creating = 0
        self._creating_done = threading.Condition(self._lock)

    # VM bookkeeping: every VM created is recorded before use, so `_cleanup` and `abort` can always delete it.
    def _create_runner(self, label: str) -> Runner:
        with self._lock:
            if self._aborted.is_set():
                raise RemoteGradingError('aborted')
            self._creating += 1
        try:
            # Deletes its VM and raises if `_aborted` is set before the VM is ready.
            vm = self.backend.create_vm(f'{self.submission.id}-{label}', self._aborted)
        finally:
            with self._creating_done:
                self._creating -= 1
                self._creating_done.notify_all()
        with self._lock:
            aborted = self._aborted.is_set()
            if not aborted:
                self._vms.append(vm)
        if aborted:
            # Grading ended right as this VM became ready; cleanup may already have run, so delete it here.
            self.backend.delete_vm(vm)
            raise RemoteGradingError('aborted')
        return Runner(vm, self._http)

    def _cleanup(self) -> None:
        with self._lock:
            vms, self._vms = self._vms, []
        if vms:
            with ThreadPoolExecutor(len(vms)) as pool:
                list(pool.map(self.backend.delete_vm, vms))

    def abort(self) -> None:
        """Called from the IPC thread: stop everything. In-flight requests fail, and `run` then reports the abort."""
        self._aborted.set()
        self._cleanup()

    def _problem_urls(self) -> dict:
        """Fresh presigned URLs for the problem's objects. The listing is done once per submission."""
        sub = self.submission
        if self._problem_objects is None:
            objects = list_problem_objects(self.config.storage, sub.problem_id, sub.storage_namespace)
            if not any(name == 'init.yml' for name, _ in objects):
                raise RemoteGradingError(f'problem {sub.problem_id} has no init.yml in storage')
            self._problem_objects = [name for name, _ in objects]
        return problem_urls(self.config.storage, sub.problem_id, sub.storage_namespace, self._problem_objects)

    def _prepare(self, runner: Runner) -> None:
        sub = self.submission
        runner.call(
            '/prepare',
            SETUP_TIMEOUT,
            json={
                'submission': {
                    'id': sub.id,
                    'problem_id': sub.problem_id,
                    'language': sub.language,
                    'source': sub.source,
                    'time_limit': sub.time_limit,
                    'memory_limit': sub.memory_limit,
                    'short_circuit': sub.short_circuit,
                    'meta': sub.meta,
                },
                'problem': self._problem_urls(),
            },
        )

    def run(self) -> Generator[Tuple['IPC', tuple], None, None]:
        from dmoj.judge import IPC

        try:
            yield from self._run(IPC)
        except Exception:
            if self._aborted.is_set():
                yield IPC.GRADING_ABORTED, ()
                return
            raise
        finally:
            # Stop workers grading cases nobody needs anymore, and delete every VM. Workers still creating a VM must
            # finish first: this runs in a forked worker process that exits right after, which would kill them before
            # they record (and so delete) their VM. Creation checks `_aborted`, so they stop promptly.
            self._aborted.set()
            self._cleanup()
            with self._creating_done:
                if not self._creating_done.wait_for(lambda: self._creating == 0, timeout=CREATE_CANCEL_TIMEOUT):
                    logger.error(
                        'VM creation did not stop within %d seconds; a VM may outlive grading', CREATE_CANCEL_TIMEOUT
                    )
            self._cleanup()

    def _run(self, IPC: Any) -> Generator[Tuple['IPC', tuple], None, None]:
        first = self._create_runner('1')
        self._prepare(first)
        compiled = first.call('/compile', SETUP_TIMEOUT + self.grade_timeout, json={'grade_first': True})
        if 'compile_error' in compiled:
            yield IPC.COMPILE_ERROR, (compiled['compile_error'],)
            return

        manifest = Manifest.from_dict(compiled['manifest'])
        if manifest.warning is not None:
            yield IPC.COMPILE_MESSAGE, (manifest.warning,)
        yield IPC.GRADING_BEGIN, (manifest.pretested,)

        emitter = Emitter(manifest, self.submission.short_circuit)
        emitter.add_result(emitter.unit_of[0], compiled['first_result'])
        for name, args in emitter.drain():
            yield getattr(IPC, name), args
        if emitter.finished:
            return

        events: 'queue.Queue[Tuple[str, Any]]' = queue.Queue()
        dispatcher = _Dispatcher(emitter)
        artifact = first.download_artifact()
        extra_vms = min(self.max_vms, len(emitter.units_in_order)) - 1

        def worker(runner_factory: Callable[[], Runner]) -> None:
            unit: Optional[UnitKey] = None
            try:
                runner = runner_factory()
                while not self._aborted.is_set():
                    unit = dispatcher.next_unit()
                    if unit is None:
                        return
                    position = emitter.unit_case(unit).position
                    result = runner.call(
                        '/grade', self.grade_timeout, json={'position': position, 'problem': self._problem_urls()}
                    )
                    events.put(('result', (unit, result)))
                    unit = None
            except Exception as error:
                if unit is not None:
                    dispatcher.requeue(unit)
                events.put(('error', error))
            finally:
                events.put(('done', None))

        def new_runner(label: str) -> Callable[[], Runner]:
            def factory() -> Runner:
                runner = self._create_runner(label)
                self._prepare(runner)
                runner.call('/artifact', SETUP_TIMEOUT, data=artifact)
                # Builds the grader; with the artifact in place, nothing is compiled again.
                runner.call('/compile', SETUP_TIMEOUT, json={'grade_first': False})
                return runner

            return factory

        workers = [threading.Thread(target=worker, args=(lambda: first,), daemon=True)]
        workers += [
            threading.Thread(target=worker, args=(new_runner(str(i + 2)),), daemon=True) for i in range(extra_vms)
        ]
        for thread in workers:
            thread.start()

        running = len(workers)
        errors: List[BaseException] = []
        while not emitter.finished:
            if running == 0:
                if errors:
                    raise RemoteGradingError('all runners failed') from errors[0]
                raise RemoteGradingError('runners finished without grading every case')
            kind, payload = events.get()
            if kind == 'done':
                running -= 1
            elif kind == 'error':
                if self._aborted.is_set():
                    raise RemoteGradingError('aborted') from payload
                # One VM failing is survivable while others are still grading; its unit goes back in the queue.
                logger.warning('Runner failed, its case goes back in the queue: %s', payload)
                errors.append(payload)
            else:
                unit, result = payload
                emitter.add_result(unit, result)
                for name, args in emitter.drain():
                    yield getattr(IPC, name), args


class _Dispatcher:
    """
    Hands out units in case order, skipping units whose cases are already decided. Workers call it concurrently with
    the main thread updating the emitter; a stale `unit_needed` answer only means grading a case nobody needed.
    """

    def __init__(self, emitter: Emitter) -> None:
        self._emitter = emitter
        self._lock = threading.Lock()
        self._pending = [unit for unit in emitter.units_in_order if emitter.unit_needed(unit)]

    def next_unit(self) -> Optional[UnitKey]:
        with self._lock:
            while self._pending:
                unit = self._pending.pop(0)
                if self._emitter.unit_needed(unit):
                    return unit
            return None

    def requeue(self, unit: UnitKey) -> None:
        """Gives a unit from a failed VM to the next free VM."""
        with self._lock:
            self._pending.insert(0, unit)
