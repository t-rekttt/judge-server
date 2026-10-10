"""
Data exchanged between the judge (orchestrator) and the runner inside each VM.

Everything crosses the wire as JSON. Cases are identified by their position, which is the 1-based case number the
local judge reports to the site, so both sides number cases identically by flattening `Problem.cases()` the same way.
"""

from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from dmoj.result import Result
from dmoj.utils.unicode import utf8bytes

# Key under which the local judge caches results of cases sharing the same input/output files.
CacheKey = Tuple[Optional[str], Optional[str]]


class CaseInfo(NamedTuple):
    position: int
    batch: Optional[int]
    points: float
    # None when the case can't be cached, i.e. both its input and output come from a generator.
    cache_key: Optional[CacheKey]


class RemoteCase:
    """Stands in for `TestCase` on `Result` objects rebuilt from runner output; the judge only reads these fields."""

    # `Result.output` slices `proc_output` with this; the runner already trimmed the output.
    output_prefix_length = None

    def __init__(self, position: int, batch: Optional[int], points: float) -> None:
        self.position = position
        self.batch = batch
        self.points = points


def case_to_dict(case: CaseInfo) -> Dict[str, Any]:
    return {
        'position': case.position,
        'batch': case.batch,
        'points': case.points,
        'cache_key': list(case.cache_key) if case.cache_key is not None else None,
    }


def case_from_dict(data: Dict[str, Any]) -> CaseInfo:
    cache_key = data['cache_key']
    return CaseInfo(data['position'], data['batch'], data['points'], tuple(cache_key) if cache_key else None)


def make_case_info(position: int, batch: Optional[int], case: Any) -> CaseInfo:
    key = (case.config['in'], case.config['out'])
    return CaseInfo(position, batch, case.points, None if key == (None, None) else key)


def result_to_dict(result: Result) -> Dict[str, Any]:
    return {
        'result_flag': result.result_flag,
        'execution_time': result.execution_time,
        'wall_clock_time': result.wall_clock_time,
        'max_memory': result.max_memory,
        'context_switches': list(result.context_switches),
        'runtime_version': result.runtime_version,
        # Trimmed to the problem's output prefix length, exactly what the local judge keeps before reporting.
        'output': result.output,
        'feedback': result.feedback,
        'extended_feedback': result.extended_feedback,
        'points': result.points,
    }


def result_from_dict(data: Dict[str, Any], case: CaseInfo, points: Optional[float] = None) -> Result:
    """Rebuilds a `Result` for `case`; `points` overrides the graded points (used when reusing a cached result)."""
    return Result(
        RemoteCase(case.position, case.batch, case.points),  # type: ignore[arg-type]
        result_flag=data['result_flag'],
        execution_time=data['execution_time'],
        wall_clock_time=data['wall_clock_time'],
        max_memory=data['max_memory'],
        context_switches=tuple(data['context_switches']),  # type: ignore[arg-type]
        runtime_version=data['runtime_version'],
        proc_output=utf8bytes(data['output']),
        feedback=data['feedback'],
        extended_feedback=data['extended_feedback'],
        points=data['points'] if points is None else points,
    )


def sc_result(case: CaseInfo) -> Result:
    return Result(RemoteCase(case.position, case.batch, case.points), result_flag=Result.SC)  # type: ignore[arg-type]


class Manifest(NamedTuple):
    """What a runner reports after compiling: everything the orchestrator needs to plan grading."""

    pretested: bool
    warning: Optional[str]
    cases: List[CaseInfo]
    batch_dependencies: List[List[int]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'pretested': self.pretested,
            'warning': self.warning,
            'cases': [case_to_dict(case) for case in self.cases],
            'batch_dependencies': self.batch_dependencies,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Manifest':
        return cls(
            data['pretested'],
            data['warning'],
            [case_from_dict(case) for case in data['cases']],
            data['batch_dependencies'],
        )
