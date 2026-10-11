"""
Grading service that runs inside each VM: `python -m dmoj.remote.runner`.

The orchestrating judge drives it over HTTP, one submission per VM:

    POST /prepare   {submission, problem: urls}   download the problem's files except zip archives, which stay remote
    POST /compile   {grade_first}                 compile (a cache hit after POST /artifact); returns a compile error,
                                                  or the case manifest, plus case 1's result if `grade_first`
    GET  /artifact                                compiled programs, for other VMs of the same submission
    POST /artifact  <tar.gz>                      reuse compiled programs from the first VM
    POST /grade     {position, problem: urls}     grade one case, reading only its data from the test archive;
                                                  returns its result. URLs are re-sent so they never expire mid-grading
    POST /abort     {}                            kill whatever is running
    GET  /health                                  readiness probe

Every request except /health must carry `X-Runner-Token: $RUNNER_TOKEN`. (`Authorization` is left to the platform:
DigitalOcean's MicroVM endpoints require a DigitalOcean token there.) The token is generated per VM by the orchestrator,
or per checkpoint when VMs are created from one. The service reuses the regular judge code (`Problem`, executors, graders, checkers), so a case is
graded exactly as a local judge grades it.
"""

import argparse
import hmac
import json
import logging
import os
import queue
import shutil
import tempfile
import threading
import traceback
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Tuple

import yaml

# Only modules that don't import executors may be imported here: executor classes read the runtime configuration
# when they're first imported, so `main` must load it before anything imports them.
from dmoj import judgeenv
from dmoj.remote import artifact
from dmoj.remote.protocol import Manifest, make_case_info, result_to_dict
from dmoj.remote.range_file import HTTPRangeFile, open_remote_zip
from dmoj.utils import builtin_int_patch
from dmoj.utils.unicode import utf8bytes

if TYPE_CHECKING:
    from dmoj.judge import Submission
    from dmoj.problem import Problem, TestCase

logger = logging.getLogger('dmoj.remote.runner')

# Problem data is unpacked to <root>/<problem id>, so the judge's regular problem lookup finds it.
PROBLEMS_ROOT = os.path.join(tempfile.gettempdir(), 'dmoj-problems')
# Problem files other than zip archives: checkers, generators, init.yml, loose test data.
MAX_PROBLEM_FILES = 256 * 1024**2


class RunnerState:
    """
    The submission this VM grades. Its work runs on the main thread, one request at a time: cases never compete for
    the vCPU, and graders may use signals (the output-only grader times itself with SIGALRM), which Python only
    allows on the main thread. HTTP handler threads hand work over through `run_on_main_thread`.
    """

    def __init__(self) -> None:
        self._work: 'queue.Queue[Tuple[Callable[[], Any], queue.Queue]]' = queue.Queue()
        self.submission: Optional['Submission'] = None
        self.problem: Optional['Problem'] = None
        self.grader: Any = None
        self.cases: Dict[int, 'TestCase'] = {}
        self.archive_file: Optional[HTTPRangeFile] = None
        self.archive_name: Optional[str] = None
        # Presigned URL per object of the problem, by name relative to the problem.
        self.urls: Dict[str, str] = {}

    def prepare(self, data: dict) -> None:
        from dmoj.judge import Submission

        sub = data['submission']
        self.submission = Submission(
            id=sub['id'],
            problem_id=sub['problem_id'],
            storage_namespace=None,
            language=sub['language'],
            source=sub['source'],
            time_limit=float(sub['time_limit']),
            memory_limit=int(sub['memory_limit']),
            short_circuit=bool(sub['short_circuit']),
            meta=sub['meta'],
        )
        self.urls = data['problem']
        _download_problem(self.submission.problem_id, self.urls)

    def compile(self, grade_first: bool) -> dict:
        from dmoj.error import CompileError
        from dmoj.problem import Problem

        submission = self.submission
        assert submission is not None, 'prepare first'
        self.problem = Problem(submission.problem_id, submission.time_limit, submission.memory_limit, submission.meta)
        try:
            # Graders keep the judge worker only for custom graders that need it; there is none in a runner.
            self.grader = self.problem.grader_class(
                None, self.problem, submission.language, utf8bytes(submission.source)  # type: ignore[arg-type]
            )
        except CompileError as error:
            return {'compile_error': error.message}

        flattened, batch_dependencies = self.problem.flattened_cases()
        infos = []
        for position, (batch, case) in enumerate(flattened, start=1):
            self.cases[position] = case
            infos.append(make_case_info(position, batch, case))
        warning = getattr(self.grader.binary, 'warning', None)
        manifest = Manifest(
            bool(self.problem.run_pretests_only),
            None if warning is None else utf8bytes(warning).decode('utf-8', 'replace'),
            infos,
            [sorted(deps) for deps in batch_dependencies],
        )
        reply: Dict[str, Any] = {'manifest': manifest.to_dict()}
        if grade_first:
            # Grading case 1 compiles what the grader only compiles on first use (custom checkers, generators), so
            # the artifact other VMs fetch next carries those too. It is usually a sample, so it's fast, and it gives
            # the orchestrator an early result to short-circuit on.
            reply['first_result'] = self.grade(1)
        return reply

    def open_remote_archive(self, problem_id: str, name: str) -> Optional[zipfile.ZipFile]:
        """Opens the problem's test archive from storage; members are fetched with range requests when read."""
        if name not in self.urls:
            return None  # not in storage: the regular local lookup reports it missing
        archive = open_remote_zip(self.urls[name])
        self.archive_file = archive.raw
        self.archive_name = name
        return archive

    def refresh_urls(self, urls: Optional[Dict[str, str]]) -> None:
        if urls:
            self.urls = urls
            if self.archive_file is not None and self.archive_name in urls:
                self.archive_file.url = urls[self.archive_name]

    def grade(self, position: int) -> dict:
        assert self.grader is not None, 'compile first'
        result = self.grader.grade(self.cases[position])
        if self.archive_file is not None:
            logger.info(
                'Case %d graded; test archive: %d of %d bytes fetched in %d requests',
                position,
                self.archive_file.bytes_fetched,
                self.archive_file.size,
                self.archive_file.requests,
            )
        return result_to_dict(result)

    def abort(self) -> None:
        if self.grader is not None:
            self.grader.abort_grading()

    def run_on_main_thread(self, work: Callable[[], Any]) -> Any:
        reply: queue.Queue = queue.Queue(1)
        self._work.put((work, reply))
        ok, value = reply.get()
        if not ok:
            raise value
        return value

    def serve_work_forever(self) -> None:
        while True:
            work, reply = self._work.get()
            try:
                reply.put((True, work()))
            except BaseException as error:
                reply.put((False, error))


def _download_problem(problem_id: str, urls: Dict[str, str]) -> None:
    """Fetches the problem's files into its directory, except zip archives, which are read in place from storage."""
    if '/' in problem_id or problem_id in ('', '.', '..'):
        raise ValueError(f'invalid problem id: {problem_id!r}')
    destination = os.path.realpath(os.path.join(PROBLEMS_ROOT, problem_id))
    shutil.rmtree(destination, ignore_errors=True)
    os.makedirs(destination)

    remaining = MAX_PROBLEM_FILES
    for name, url in urls.items():
        if name.lower().endswith('.zip'):
            continue
        path = os.path.realpath(os.path.join(destination, name))
        if not path.startswith(destination + os.sep):
            raise ValueError(f'unsafe path in problem: {name}')
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read(remaining + 1)
        remaining -= len(data)
        if remaining < 0:
            raise ValueError('problem files too large')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)


class RunnerHandler(BaseHTTPRequestHandler):
    state: RunnerState
    token: str

    def log_message(self, format: str, *args) -> None:
        logger.info('%s %s', self.address_string(), format % args)

    def _reply(self, code: int, body: bytes, content_type: str = 'application/json') -> None:
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, data: Any) -> None:
        self._reply(code, json.dumps(data).encode())

    def _authorized(self) -> bool:
        if hmac.compare_digest(self.headers.get('X-Runner-Token', ''), self.token):
            return True
        self._json(401, {'error': 'unauthorized'})
        return False

    def _body(self) -> bytes:
        return self.rfile.read(int(self.headers.get('Content-Length') or 0))

    def do_GET(self) -> None:
        if self.path == '/health':
            self._json(200, {'ok': True})
        elif not self._authorized():
            return
        elif self.path == '/artifact':
            self._reply(200, self.state.run_on_main_thread(artifact.pack), 'application/gzip')
        else:
            self._json(404, {'error': 'not found'})

    def do_POST(self) -> None:
        if not self._authorized():
            return
        state = self.state
        try:
            if self.path == '/abort':
                # Runs right here, not on the main thread: it must interrupt the grading running there.
                state.abort()
                self._json(200, {'ok': True})
                return
            body = self._body()

            def prepare() -> dict:
                state.prepare(json.loads(body))
                return {'ok': True}

            def compile() -> dict:
                return state.compile(bool(json.loads(body or b'{}').get('grade_first')))

            def unpack_artifact() -> dict:
                artifact.unpack(body)
                return {'ok': True}

            def grade() -> dict:
                request = json.loads(body)
                state.refresh_urls(request.get('problem'))
                return state.grade(int(request['position']))

            routes: Dict[str, Callable[[], dict]] = {
                '/prepare': prepare,
                '/compile': compile,
                '/artifact': unpack_artifact,
                '/grade': grade,
            }
            work = routes.get(self.path)
            if work is None:
                self._json(404, {'error': 'not found'})
            else:
                self._json(200, state.run_on_main_thread(work))
        except Exception:
            message = traceback.format_exc()
            logger.error('request %s failed:\n%s', self.path, message)
            self._json(500, {'error': message})


def main():  # untyped like `dmoj.judge.main`: judgeenv's settings are dynamic
    parser = argparse.ArgumentParser(description='Grades cases of one submission for an orchestrating judge.')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=int(os.environ.get('MICROVM_HTTP_PORT', 8080)))
    args = parser.parse_args()

    token = os.environ.get('RUNNER_TOKEN')
    if not token:
        raise SystemExit('RUNNER_TOKEN must be set')

    builtin_int_patch.apply()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s %(module)s %(message)s')

    state = RunnerState()

    # Problems live in our own scratch directory. Executors were self-tested when the image was built, so skip that.
    os.makedirs(PROBLEMS_ROOT, exist_ok=True)
    with open(os.environ.get('DMOJ_RUNTIME_CONFIG', '/judge-runtime-paths.yml')) as f:
        judgeenv.env.update(yaml.safe_load(f) or {})
    # Set before any executor module is imported: some copy this flag at import time.
    judgeenv.skip_self_test = True
    judgeenv.storage_namespaces[None] = [os.path.join(PROBLEMS_ROOT, '*')]
    judgeenv.problem_globs = judgeenv.storage_namespaces[None]
    cache = judgeenv._storage_namespace_cache[None]
    cache.problem_roots_cache = [PROBLEMS_ROOT]
    judgeenv.env.cache_all_compiled_binaries = True
    if judgeenv.env.compiled_binary_cache_dir is None:
        judgeenv.env.compiled_binary_cache_dir = os.path.join(tempfile.gettempdir(), 'dmoj-compiled')
    os.makedirs(judgeenv.env.compiled_binary_cache_dir, exist_ok=True)

    from dmoj import contrib, executors, problem

    problem.remote_archive_opener = state.open_remote_archive
    executors.load_executors()
    contrib.load_contrib_modules()

    RunnerHandler.state = state
    RunnerHandler.token = token
    server = ThreadingHTTPServer((args.host, args.port), RunnerHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info('Runner listening on %s:%d', args.host, args.port)
    state.serve_work_forever()


if __name__ == '__main__':
    main()
