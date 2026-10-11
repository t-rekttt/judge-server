"""
Turns per-case results arriving in any order (from parallel VMs) into the exact verdicts the sequential judge would
produce, and decides when each result may be reported to the site.

Verdicts: replayed strictly in case order with the same rules as `JudgeWorker._grade_cases` (batch dependencies,
in-batch short-circuiting, global short-circuiting after a failed 0-point case or when the site asks for it, and reuse
of results for cases with the same input/output files).

Reporting order is constrained by the site: it does not read a batch number from test case packets but assigns every
case it receives to the batch opened by the last `batch-begin`, and numbers batches by counting `batch-begin`s.
So batches are reported one at a time, in order, and non-batched cases only while no batch is open. Non-batched cases
are reported as soon as their verdict is certain, which is immediately unless an earlier case could still trigger a
global short-circuit.
"""

from typing import Dict, Hashable, List, Optional, Set, Tuple

from dmoj.remote.protocol import CaseInfo, Manifest, result_from_dict, sc_result
from dmoj.result import Result

# A unit of work for a VM: one case, or one shared input/output pair whose result several cases reuse.
UnitKey = Hashable
# (message name, args) pairs, matching the IPC messages `JudgeWorker._grade_cases` yields.
Message = Tuple[str, tuple]


def assign_units(cases: List[CaseInfo]) -> List[UnitKey]:
    """
    Mirrors the local result cache: every case looks up results by its input/output files, but only cases worth
    points store their result. So a case reuses the unit of the first earlier point-worth case with the same files.
    """
    units: List[UnitKey] = []
    by_files: Dict[tuple, UnitKey] = {}
    for case in cases:
        if case.cache_key is not None and case.cache_key in by_files:
            units.append(by_files[case.cache_key])
        elif case.cache_key is not None and case.points != 0:
            by_files[case.cache_key] = ('files',) + tuple(case.cache_key)
            units.append(by_files[case.cache_key])
        else:
            units.append(('case', case.position))
    return units


class Emitter:
    def __init__(self, manifest: Manifest, short_circuit: bool) -> None:
        self.cases = manifest.cases
        self._batch_dependencies = [set(deps) for deps in manifest.batch_dependencies]

        # Work units, and the case each unit is graded as: its first occurrence. (Locally, if that occurrence is
        # short-circuited, a later one is graded instead; same files, so only per-case checker overrides could differ.)
        self.unit_of: List[UnitKey] = assign_units(self.cases)
        self.unit_cases: Dict[UnitKey, List[int]] = {}
        for index, unit in enumerate(self.unit_of):
            self.unit_cases.setdefault(unit, []).append(index)
        self.units_in_order: List[UnitKey] = list(self.unit_cases)
        self._unit_results: Dict[UnitKey, dict] = {}

        # Index of the last earlier case whose failure could short-circuit everything after it, per case.
        self._last_trigger: List[int] = []
        last = -1
        for index, case in enumerate(self.cases):
            self._last_trigger.append(last)
            if short_circuit or not case.points:
                last = index

        # Replay state, same variables as the sequential loop.
        self._replay_index = 0
        self._entered_index = -1
        self._is_short_circuiting = False
        self._short_circuit_enabled = short_circuit
        self._passed_batches: Set[int] = set()
        self.final: List[Optional[Result]] = [None] * len(self.cases)

        # Reporting state.
        self._reported = [False] * len(self.cases)
        self._open_batch: Optional[int] = None
        self._next_batch = 1
        self._batch_count = len(self._batch_dependencies)
        self._batch_cases: Dict[int, List[int]] = {}
        for index, case in enumerate(self.cases):
            if case.batch is not None:
                self._batch_cases.setdefault(case.batch, []).append(index)
        self.finished = False

    def unit_case(self, unit: UnitKey) -> CaseInfo:
        return self.cases[self.unit_cases[unit][0]]

    def unit_needed(self, unit: UnitKey) -> bool:
        """Whether grading `unit` can still change anything: not graded yet and some case using it is undecided."""
        return unit not in self._unit_results and any(self.final[i] is None for i in self.unit_cases[unit])

    def add_result(self, unit: UnitKey, data: dict) -> None:
        # The first result for a unit wins, so verdicts already derived from it never change.
        self._unit_results.setdefault(unit, data)

    def _result_for(self, index: int) -> Optional[Result]:
        unit = self.unit_of[index]
        data = self._unit_results.get(unit)
        if data is None:
            return None
        case = self.cases[index]
        graded_case = self.unit_case(unit)
        if graded_case.position == case.position:
            return result_from_dict(data, case)
        # Cache hit: scale the points to this case, as the local judge does.
        return result_from_dict(data, case, points=case.points * data['points'] / graded_case.points)

    def _enter_group(self, index: int) -> None:
        batch = self.cases[index].batch
        previous = self.cases[index - 1].batch if index else None
        if index and batch == previous:
            return
        if previous is not None:
            if not self._is_short_circuiting:
                self._passed_batches.add(previous)
            self._is_short_circuiting &= self._short_circuit_enabled
        if batch is not None:
            dependencies = self._batch_dependencies[batch - 1]
            if self._passed_batches & dependencies != dependencies:
                self._is_short_circuiting = True

    def _replay(self) -> None:
        while self._replay_index < len(self.cases):
            index = self._replay_index
            if self._entered_index < index:
                self._enter_group(index)
                self._entered_index = index

            case = self.cases[index]
            if self._is_short_circuiting:
                result = sc_result(case)
            else:
                graded = self._result_for(index)
                if graded is None:
                    return
                result = graded
                if result.result_flag & Result.WA:
                    self._short_circuit_enabled |= not case.points
                    self._is_short_circuiting |= case.batch is not None or self._short_circuit_enabled

            if self.final[index] is None:
                self.final[index] = result
            self._replay_index += 1

    def _finalize_early(self) -> None:
        # A non-batched case can only be short-circuited by an earlier trigger; once the replay is past all of those,
        # its graded result is final.
        for index, case in enumerate(self.cases):
            if (
                self.final[index] is None
                and case.batch is None
                and self._last_trigger[index] < self._replay_index
                and self.unit_of[index] in self._unit_results
            ):
                self.final[index] = self._result_for(index)

    def drain(self) -> List[Message]:
        """Updates verdicts and returns the messages that may be reported now, in order."""
        if self.finished:
            return []
        self._replay()
        self._finalize_early()

        messages: List[Message] = []
        while True:
            progressed = False
            if self._open_batch is not None:
                batch = self._open_batch
                for index in self._batch_cases[batch]:
                    if self._reported[index]:
                        continue
                    if self.final[index] is None:
                        break
                    messages.append(self._report(index))
                    progressed = True
                if all(self._reported[index] for index in self._batch_cases[batch]):
                    messages.append(('BATCH_END', (batch,)))
                    self._open_batch = None
                    progressed = True
            else:
                for index, case in enumerate(self.cases):
                    if case.batch is None and not self._reported[index] and self.final[index] is not None:
                        messages.append(self._report(index))
                        progressed = True
                if self._next_batch <= self._batch_count:
                    batch = self._next_batch
                    if self.final[self._batch_cases[batch][0]] is not None:
                        messages.append(('BATCH_BEGIN', (batch,)))
                        self._open_batch = batch
                        self._next_batch += 1
                        progressed = True

            if self._open_batch is None and all(self._reported):
                messages.append(('GRADING_END', ()))
                self.finished = True
                return messages
            if not progressed:
                return messages

    def _report(self, index: int) -> Message:
        self._reported[index] = True
        case = self.cases[index]
        result = self.final[index]
        assert result is not None
        return 'RESULT', (case.batch, case.position, result)
