import itertools
import random
import unittest
from itertools import groupby
from operator import itemgetter

from dmoj.remote.emitter import Emitter
from dmoj.remote.protocol import CaseInfo, Manifest
from dmoj.result import Result


def graded(flag, points):
    return {
        'result_flag': flag,
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


def sequential(manifest, short_circuit, outcome):
    """Same loop as `JudgeWorker._grade_cases`, with `outcome(case)` standing in for `grader.grade(case)`."""
    messages = []
    deps = [set(d) for d in manifest.batch_dependencies]
    is_sc, sc_enabled, passed, cache = False, short_circuit, set(), {}
    flattened = [(case.batch, case) for case in manifest.cases]
    for batch_number, cases in groupby(flattened, key=itemgetter(0)):
        if batch_number:
            messages.append(('BATCH_BEGIN', (batch_number,)))
            if passed & deps[batch_number - 1] != deps[batch_number - 1]:
                is_sc = True
        for _, case in cases:
            if is_sc:
                messages.append(('RESULT', (case.batch, case.position, Result.SC, 0)))
                continue
            key = case.cache_key or (None, None)
            if key in cache:
                flag, points, from_points = cache[key]
                points = case.points * points / from_points
            else:
                flag, points = outcome(case)
                if case.points != 0 and key != (None, None):
                    cache[key] = (flag, points, case.points)
            if flag & Result.WA:
                sc_enabled |= not case.points
                is_sc |= batch_number is not None or sc_enabled
            messages.append(('RESULT', (case.batch, case.position, flag, points)))
        if batch_number:
            if not is_sc:
                passed.add(batch_number)
            messages.append(('BATCH_END', (batch_number,)))
            is_sc &= sc_enabled
    messages.append(('GRADING_END', ()))
    return messages


def parallel(manifest, short_circuit, outcome, order_seed):
    """Feeds the emitter one unit at a time in random order, skipping units it says are no longer needed."""
    emitter = Emitter(manifest, short_circuit)
    rng = random.Random(order_seed)
    messages = []
    pending = list(emitter.units_in_order)
    rng.shuffle(pending)
    graded_units = 0
    while not emitter.finished:
        messages += emitter.drain()
        if emitter.finished:
            break
        while pending and not emitter.unit_needed(pending[0]):
            pending.pop(0)
        assert pending, 'emitter stalled with no work left'
        unit = pending.pop(0)
        flag, points = outcome(emitter.unit_case(unit))
        emitter.add_result(unit, graded(flag, points))
        graded_units += 1
    normalized = []
    for name, args in messages:
        if name == 'RESULT':
            batch, position, result = args
            normalized.append((name, (batch, position, result.result_flag, result.points)))
        else:
            normalized.append((name, args))
    return normalized, graded_units


def site_view(messages):
    """What the VNOI site stores: (position -> (batch number it assigns, flag, points)), and the batches it saw."""
    batch_id, in_batch, stored = 0, False, {}
    for name, args in messages:
        if name == 'BATCH_BEGIN':
            batch_id += 1
            in_batch = True
        elif name == 'BATCH_END':
            in_batch = False
        elif name == 'RESULT':
            _, position, flag, points = args
            stored[position] = (batch_id if in_batch else None, flag, points)
    return stored


def make_manifest(groups, batch_dependencies=None):
    """`groups`: list of either (points, files) for a plain case or [(points, files), ...] for a batch."""
    cases, batch, position = [], 0, 0
    for group in groups:
        members = group if isinstance(group, list) else [group]
        if isinstance(group, list):
            batch += 1
        for points, files in members:
            position += 1
            cases.append(CaseInfo(position, batch if isinstance(group, list) else None, points, files))
    return Manifest(False, None, cases, batch_dependencies or [[] for _ in range(batch)])


class RemoteEmitterTest(unittest.TestCase):
    def check(self, manifest, short_circuit, outcome, seeds=range(30)):
        expected = sequential(manifest, short_circuit, outcome)
        for seed in seeds:
            got, _ = parallel(manifest, short_circuit, outcome, seed)
            self.assertEqual(site_view(got), site_view(expected), f'seed {seed}')
            self.assertEqual(got[-1], ('GRADING_END', ()))

    def test_all_ac_plain_cases(self):
        manifest = make_manifest([(10, (f'{i}.in', f'{i}.out')) for i in range(6)])
        self.check(manifest, False, lambda case: (Result.AC, case.points))

    def test_batch_failure_short_circuits_rest_of_batch(self):
        manifest = make_manifest(
            [[(10, ('a', 'a')), (10, ('b', 'b')), (10, ('c', 'c'))], [(5, ('d', 'd')), (5, ('e', 'e'))]]
        )
        self.check(manifest, False, lambda case: (Result.WA if case.position == 2 else Result.AC, 0))

    def test_failed_zero_point_case_short_circuits_everything_after(self):
        manifest = make_manifest([(0, ('s', 's')), (10, ('a', 'a')), [(5, ('b', 'b')), (5, ('c', 'c'))]])
        self.check(manifest, False, lambda case: (Result.WA if case.position == 1 else Result.AC, case.points))

    def test_site_requested_short_circuit(self):
        manifest = make_manifest([(10, (str(i), str(i))) for i in range(5)])
        self.check(manifest, True, lambda case: (Result.WA if case.position == 3 else Result.AC, 0))

    def test_unmet_batch_dependency(self):
        manifest = make_manifest(
            [[(10, ('a', 'a'))], [(10, ('b', 'b'))], [(10, ('c', 'c'))]], batch_dependencies=[[], [1], [2]]
        )
        self.check(manifest, False, lambda case: (Result.WA if case.position == 1 else Result.AC, 0))

    def test_cached_results_are_reused_with_scaled_points(self):
        manifest = make_manifest([(0, ('x', 'x')), (10, ('x', 'x')), (20, ('x', 'x')), [(5, ('x', 'x'))]])
        self.check(manifest, False, lambda case: (Result.AC, case.points / 2))

    def test_shared_files_graded_once(self):
        manifest = make_manifest([(10, ('x', 'x')), (10, ('x', 'x')), (10, ('y', 'y'))])
        _, graded_units = parallel(manifest, False, lambda case: (Result.AC, case.points), 0)
        self.assertEqual(graded_units, 2)

    def test_plain_cases_are_reported_before_slow_earlier_cases_finish(self):
        manifest = make_manifest([(10, ('a', 'a')), (10, ('b', 'b')), (10, ('c', 'c'))])
        emitter = Emitter(manifest, False)
        emitter.add_result(emitter.unit_of[2], graded(Result.AC, 10))
        self.assertEqual([args[1] for name, args in emitter.drain() if name == 'RESULT'], [3])

    def test_batches_are_reported_one_at_a_time(self):
        manifest = make_manifest([[(5, ('a', 'a')), (5, ('b', 'b'))], [(5, ('c', 'c'))], (10, ('d', 'd'))])
        emitter = Emitter(manifest, False)
        for index in (2, 3):  # batch 2 and the plain case finish first
            emitter.add_result(emitter.unit_of[index], graded(Result.AC, 5))
        self.assertEqual([name for name, _ in emitter.drain()], ['RESULT'])
        emitter.add_result(emitter.unit_of[0], graded(Result.AC, 5))
        self.assertEqual([name for name, _ in emitter.drain()], ['BATCH_BEGIN', 'RESULT'])
        emitter.add_result(emitter.unit_of[1], graded(Result.AC, 5))
        self.assertEqual(
            [name for name, _ in emitter.drain()],
            ['RESULT', 'BATCH_END', 'BATCH_BEGIN', 'RESULT', 'BATCH_END', 'GRADING_END'],
        )

    def test_random_problems_match_sequential_judge(self):
        rng = random.Random(1234)
        flags = [Result.AC, Result.WA, Result.TLE | Result.WA, Result.RTE | Result.WA]
        for _ in range(300):
            groups, batches = [], 0
            for _ in range(rng.randint(1, 6)):
                if rng.random() < 0.5:
                    batches += 1
                    groups.append([(rng.choice([0, 5, 10]), rng.choice('abcd') * 2) for _ in range(rng.randint(1, 3))])
                else:
                    groups.append((rng.choice([0, 0, 10, 20]), rng.choice('abcdef') * 2))
            groups = [g if isinstance(g, list) else (g[0], tuple(g[1])) for g in groups]
            groups = [[(p, tuple(f)) for p, f in g] if isinstance(g, list) else g for g in groups]
            dependencies = [sorted(rng.sample(range(1, b), rng.randint(0, b - 1))) for b in range(1, batches + 1)]
            manifest = make_manifest(groups, dependencies)
            verdict = {files: rng.choice(flags) for files in itertools.product('abcdef', repeat=2)}

            def outcome(case, verdict=verdict):
                return verdict[case.cache_key], 0 if verdict[case.cache_key] else case.points

            self.check(manifest, rng.random() < 0.3, outcome, seeds=range(5))
