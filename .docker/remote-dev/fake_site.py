"""
Stands in for the site's judge bridge to test remote grading end to end, with no site changes.

It accepts one judge connection, sends every submission in the testsuite (`<problem>/tests/<case>/test.yml`), and
stores test case packets exactly as the VNOI bridge does (`judge/bridge/judge_handler.py`): a case belongs to the batch
opened by the last `batch-begin`, and batches are numbered by counting `batch-begin`s. It then checks the verdicts and
scores against each test's expectations and reports how long grading took.

Usage: python fake_site.py <testsuite dir> [port] [problem regex]
With JUDGE_COUNT=<n>, it waits for that many judges and grades on all of them at once, one submission per judge at a
time, like the site's bridge. Exits non-zero if any submission's stored result doesn't match its expectations.
With ABORT_AFTER=<seconds>, it instead aborts the first submission after that long, like a user pressing "Abort", and
exits non-zero unless the judge confirms the abort.
"""

import json
import os
import re
import socket
import struct
import sys
import threading
import time
import zlib

import yaml

SIZE = struct.Struct('!I')

# Bridge mapping from status bits to the site's verdict (same order as judge_handler.on_test_case).
STATUS_BITS = [(4, 'TLE'), (8, 'MLE'), (64, 'OLE'), (2, 'RTE'), (16, 'IR'), (1, 'WA'), (32, 'SC')]


def site_status(case: dict) -> str:
    for bit, name in STATUS_BITS:
        if case['status'] & bit:
            return name
    return 'AC'


class Connection:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.input = sock.makefile('rb')
        self._send_lock = threading.Lock()

    def send(self, packet: dict) -> None:
        raw = zlib.compress(json.dumps(packet).encode())
        with self._send_lock:
            self.sock.sendall(SIZE.pack(len(raw)) + raw)

    def drop(self) -> None:
        """Closes the connection from the site's side, like a bridge restart does."""
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    def receive(self) -> dict:
        header = self.input.read(SIZE.size)
        if not header:
            raise ConnectionError('judge disconnected')
        return json.loads(zlib.decompress(self.input.read(SIZE.unpack(header)[0])))


def load_submissions(testsuite: str, problem_regex: str):
    for problem in sorted(os.listdir(testsuite)):
        tests_dir = os.path.join(testsuite, problem, 'tests')
        if not re.match(problem_regex, problem) or not os.path.isdir(tests_dir):
            continue
        for case in sorted(os.listdir(tests_dir)):
            case_dir = os.path.join(tests_dir, case)
            config: dict = {}
            for name in ('test.yml', 'test.posix.yml', 'test.linux.yml'):
                path = os.path.join(case_dir, name)
                if os.path.isfile(path):
                    with open(path) as f:
                        config.update(yaml.safe_load(f) or {})
            if not config or config.get('skip') or ('arch' in config and 'amd64' not in config['arch']):
                continue
            sources = config['source'] if isinstance(config['source'], list) else [config['source']]
            for source in sources:
                with open(os.path.join(case_dir, source)) as f:
                    yield problem, case, config, f.read()


def parse_expect(value, cases, parse):
    """Same parsing as `dmoj.testsuite.Tester.parse_expect`: `cases` is a list (from case 1) or a position map."""
    items = enumerate(cases, 1) if isinstance(cases, list) else cases.items()
    return parse(value), {int(position): parse(expected) for position, expected in items}


def parse_codes(codes):
    return ALL_CODES if codes == '*' else {codes} if isinstance(codes, str) else set(codes)


def parse_score(score):
    if score is None or score == '*':
        return None
    if isinstance(score, float):
        return {score}
    if isinstance(score, (str, int)):
        return {int(score)}
    return set(map(int, score))


ALL_CODES = {'AC', 'IE', 'TLE', 'MLE', 'OLE', 'RTE', 'IR', 'WA', 'CE', 'SC'}


def check(config: dict, stored: dict, outcome: str) -> list:
    """The verdict and score expectations `dmoj.testsuite` checks, applied to what the site stored."""
    codes_all, codes_cases = parse_expect(config.get('expect', 'AC'), config.get('cases', {}), parse_codes)
    score_all, score_cases = parse_expect(config.get('score'), config.get('score_cases', {}), parse_score)
    if outcome == 'compile-error':
        return [] if 'CE' in codes_all else ['unexpected compile error']
    if outcome == 'internal-error':
        return [] if 'IE' in codes_all else ['unexpected internal error']
    errors = []
    for position, case in sorted(stored.items()):
        expected = codes_cases.get(position, codes_all)
        if case['status'] not in expected:
            errors.append(f'case {position}: {case["status"]}, expected {sorted(expected)}')
        expected_score = score_cases.get(position, score_all)
        if expected_score is not None and case['points'] not in expected_score:
            errors.append(f'case {position}: {case["points"]} points, expected {sorted(expected_score)}')
    return errors


def grade(conn: Connection, sub_id: int, problem: str, config: dict, source: str, send_request: bool = True):
    if send_request:
        conn.send(
            {
                'name': 'submission-request',
                'submission-id': sub_id,
                'problem-id': problem,
                'language': config['language'],
                'source': source,
                'time-limit': config['time'],
                'memory-limit': config['memory'],
                'short-circuit': False,
                'meta': config.get('meta', {}),
            }
        )
    stored: dict = {}
    batch_id, in_batch, batches, outcome, details = 0, False, [], 'graded', ''
    while True:
        packet = conn.receive()
        name = packet['name']
        if name in (
            'submission-acknowledged',
            'compile-message',
            'supported-problems',
            'ping-response',
        ):
            continue
        if name == 'grading-begin':
            # Told again from the start (after a dropped connection): forget what was stored before, as the site does.
            stored, batch_id, in_batch, batches = {}, 0, False, []
            continue
        if name == 'batch-begin':
            batch_id += 1
            in_batch = True
            batches.append(batch_id)
        elif name == 'batch-end':
            in_batch = False
        elif name == 'test-case-status':
            for case in packet['cases']:
                stored[case['position']] = {
                    'status': site_status(case),
                    'points': case['points'],
                    'batch': batch_id if in_batch else None,
                    'time': case['time'],
                }
        elif name == 'compile-error':
            return stored, 'compile-error', packet.get('log', '')
        elif name == 'internal-error':
            outcome, details = 'internal-error', packet.get('message', '')
            return stored, outcome, details
        elif name == 'grading-end':
            return stored, outcome, details
        elif name == 'submission-terminated':
            return stored, 'aborted', ''


def connect_judges(server: socket.socket, expected: int) -> list:
    """Accepts `expected` judge connections and completes their handshakes."""
    judges = []
    while len(judges) < expected:
        sock, _ = server.accept()
        conn = Connection(sock)
        handshake = conn.receive()
        assert handshake['name'] == 'handshake', handshake
        conn.send({'name': 'handshake-success'})
        judges.append((handshake['id'], conn, set(handshake['executors'])))
        print(f'Judge {handshake["id"]} connected with {len(handshake["executors"])} executors', flush=True)
    return judges


def main() -> None:
    testsuite = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999
    problem_regex = sys.argv[3] if len(sys.argv) > 3 else '.*'
    # How many judges connect: one process may present itself as several.
    judge_count = int(os.environ.get('JUDGE_COUNT') or 1)
    submissions = list(load_submissions(testsuite, problem_regex))

    server = socket.create_server(('0.0.0.0', port))
    print(f'Waiting for {judge_count} judge(s) on port {port} ({len(submissions)} submissions queued)...', flush=True)
    judges = connect_judges(server, judge_count)

    abort_after = float(os.environ.get('ABORT_AFTER') or 0)
    if abort_after:
        _, conn, _ = judges[0]
        problem, case, config, source = submissions[0]
        abort = threading.Timer(abort_after, lambda: conn.send({'name': 'terminate-submission', 'submission-id': 1}))
        abort.start()
        begin = time.monotonic()
        stored, outcome, _ = grade(conn, 1, problem, config, source)
        print(f'{problem}/{case}: {outcome} after {time.monotonic() - begin:.1f}s, {len(stored)} cases stored')
        for _, each, _ in judges:
            each.send({'name': 'disconnect'})
        sys.exit(0 if outcome == 'aborted' else 1)

    # Every judge takes the next queued submission it can grade, like the site's bridge handing out work.
    queue = list(enumerate(submissions, start=1))
    lock = threading.Lock()
    totals = {'graded': 0, 'failures': 0, 'dropped': 0, 'in_flight': 0}
    started = time.monotonic()
    expected = len(queue)

    # DROP_JUDGE_AFTER=<seconds>: close the last judge's connection while it grades, as a site restart would. The
    # judge must reconnect (counted below) and the others must keep grading. The judge reports the dropped
    # submission again from the start on its new connection, so the reconnected loop first expects that replay and
    # checks its verdict like any other.
    drop_after = float(os.environ.get('DROP_JUDGE_AFTER') or 0)
    reconnected: list = []
    dropped_once = threading.Event()

    replayed: dict = {}  # submission id -> (problem, case, config, source) the dropped judge was grading

    def accept_reconnections() -> None:
        while True:
            for name, conn, executors in connect_judges(server, 1):
                reconnected.append(name)
                print(f'Judge {name} reconnected', flush=True)
                threading.Thread(target=judge_loop, args=(name, conn, executors, True), daemon=True).start()

    if drop_after:
        threading.Thread(target=accept_reconnections, daemon=True).start()

    def ping_forever(conn: Connection) -> None:
        # The bridge pings every few seconds; a judge closes a connection silent for 300 s.
        while True:
            time.sleep(10)
            try:
                conn.send({'name': 'ping', 'when': time.time()})
            except OSError:
                return

    for _, conn, _ in judges:
        threading.Thread(target=ping_forever, args=(conn,), daemon=True).start()

    def judge_loop(name: str, conn: Connection, executors: set, expect_replay: bool = False) -> None:
        if expect_replay:
            # The first thing on a reconnected judge's connection is the dropped submission, told again.
            with lock:
                if not replayed:
                    print(f'FAIL: {name} reconnected but no dropped submission awaits a replay', flush=True)
                    totals['failures'] += 1
                    return
                sub_id, (problem, case, config, source) = next(iter(replayed.items()))
                del replayed[sub_id]
                totals['in_flight'] += 1
            begin = time.monotonic()
            try:
                stored, outcome, details = grade(conn, sub_id, problem, config, source, send_request=False)
            except (ConnectionError, OSError, ValueError, EOFError, zlib.error) as error:
                with lock:
                    totals['failures'] += 1
                    totals['in_flight'] -= 1
                print(
                    f'FAIL: {name} reconnected but the replay of {problem}/{case} never completed: {error}', flush=True
                )
                return
            errors = check(config, stored, outcome)
            status = 'ok' if not errors else 'FAIL'
            with lock:
                totals['graded'] += 1
                totals['in_flight'] -= 1
                totals['failures'] += bool(errors)
                print(
                    f'[{status}] {problem}/{case} ({config["language"]}): {outcome}, {len(stored)} cases, '
                    f'{time.monotonic() - begin:.1f}s after reconnect, REPLAYED [{name}]',
                    flush=True,
                )
                for error in errors:
                    print(f'    {error}', flush=True)
        while True:
            with lock:
                index = next(
                    (
                        i
                        for i, (_, (_, _, config, _)) in enumerate(queue)
                        if {config['language'], *config.get('required_auxiliary_executors', [])} <= executors
                    ),
                    None,
                )
                if index is None:
                    return
                sub_id, (problem, case, config, source) = queue.pop(index)
                totals['in_flight'] += 1
            begin = time.monotonic()
            if drop_after and name == judges[-1][0] and not dropped_once.is_set():
                dropped_once.set()
                threading.Timer(drop_after, conn.drop).start()
            try:
                stored, outcome, details = grade(conn, sub_id, problem, config, source)
            except (ConnectionError, OSError, ValueError, EOFError, zlib.error):
                with lock:
                    totals['dropped'] += 1
                    totals['in_flight'] -= 1
                    replayed[sub_id] = (problem, case, config, source)
                print(
                    f'[dropped] {problem}/{case} on {name}: connection closed mid-grading, awaiting replay', flush=True
                )
                return
            errors = check(config, stored, outcome)
            times = sorted(case['time'] for case in stored.values() if case['status'] != 'SC')
            cpu = f', CPU per case median {times[len(times) // 2]:.2f}s max {times[-1]:.2f}s' if times else ''
            status = 'ok' if not errors else 'FAIL'
            with lock:
                totals['graded'] += 1
                totals['in_flight'] -= 1
                totals['failures'] += bool(errors)
                print(
                    f'[{status}] {problem}/{case} ({config["language"]}): {outcome}, {len(stored)} cases, '
                    f'{time.monotonic() - begin:.1f}s{cpu} [{name}]',
                    flush=True,
                )
                if errors:
                    for error in errors:
                        print(f'    {error}', flush=True)
                    if details:
                        print('    ' + details.strip().replace('\n', '\n    '), flush=True)

    threads = [threading.Thread(target=judge_loop, args=judge, daemon=True) for judge in judges]
    for thread in threads:
        thread.start()
    # The submission a dropped judge was grading is reported again by its reconnected loop, so wait on the counts,
    # not the original threads. A submission skipped by every judge (unsupported language) is never counted as graded.
    while True:
        with lock:
            skipped = len(queue) if not totals['in_flight'] and not any(t.is_alive() for t in threads) else 0
            finished = totals['graded'] + skipped >= expected and not totals['in_flight']
        if finished:
            break
        time.sleep(1)

    print(
        f'{totals["graded"]} submissions graded in {time.monotonic() - started:.0f}s on {len(judges)} judge(s), '
        f'{totals["failures"]} failed, {totals["dropped"]} dropped, {len(reconnected)} reconnected',
        flush=True,
    )
    if drop_after and not reconnected:
        print('FAIL: the dropped judge never reconnected', flush=True)
        totals['failures'] += 1
    for _, conn, _ in judges:
        try:
            conn.send({'name': 'disconnect'})
        except OSError:
            pass
    if drop_after:
        print(f'graded on the reconnected judge: {totals["graded"]}', flush=True)
    sys.exit(1 if totals['failures'] else 0)


if __name__ == '__main__':
    main()
