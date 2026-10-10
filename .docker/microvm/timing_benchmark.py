"""
Measures how stable the judge's reported CPU time is on this machine.

Compiles two C++ workloads with the judge's own CPP17 executor and runs each one repeatedly under cptbox, exactly like
a submission is run, recording the CPU time and wall time cptbox reports. Also samples CPU steal time from /proc/stat,
which shows time the hypervisor gave our vCPU to someone else (noisy neighbours).

Usage: python timing_benchmark.py [runs] > result.json   (progress goes to stderr)
"""

import json
import os
import platform
import statistics
import subprocess
import sys
import time

import yaml

from dmoj.judgeenv import env

env.update(yaml.safe_load(open('/judge-runtime-paths.yml')))

from dmoj.executors import load_executor  # noqa: E402, I100, I202, executors read env.runtime when imported

# Pure integer arithmetic: measures raw vCPU speed.
CPU_BOUND = rb"""
#include <cstdio>
int main() {
    unsigned long long x = 88172645463325252ULL, s = 0;
    for (long i = 0; i < 400000000; i++) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; s += x & 1023; }
    printf("%llu\n", s);
}
"""

# Random reads over 256 MiB: also sensitive to cache and memory bandwidth contention from other tenants.
MEMORY_BOUND = rb"""
#include <cstdio>
#include <vector>
int main() {
    const unsigned n = 1u << 26;
    std::vector<unsigned> a(n);
    for (unsigned i = 0; i < n; i++) a[i] = i * 2654435761u % n;
    unsigned p = 0, s = 0;
    for (int i = 0; i < 20000000; i++) { p = a[p]; s += p; }
    printf("%u\n", s);
}
"""


def read_steal_ticks() -> int:
    with open('/proc/stat') as f:
        fields = f.readline().split()
    return int(fields[8])  # cpu user nice system idle iowait irq softirq steal ...


def summarize(values):
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    ordered = sorted(values)
    return {
        'min': ordered[0],
        'median': statistics.median(ordered),
        'p95': ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))],
        'max': ordered[-1],
        'mean': mean,
        'stdev': stdev,
        'cv_percent': 100 * stdev / mean if mean else 0.0,
        'spread_percent': 100 * (ordered[-1] - ordered[0]) / ordered[0] if ordered[0] else 0.0,
    }


def bench(name: str, source: bytes, runs: int) -> dict:
    executor = load_executor('CPP17').Executor(name, source)
    cpu_times, wall_times = [], []
    steal_before = read_steal_ticks()
    for i in range(runs):
        proc = executor.launch(
            time=30, memory=524288, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, wall_time=60
        )
        proc.communicate()
        if proc.returncode != 0 or proc.is_tle or proc.is_mle or proc.is_rte:
            raise RuntimeError(
                f'{name} run {i} failed: returncode={proc.returncode}, flags tle={proc.is_tle} '
                f'mle={proc.is_mle} rte={proc.is_rte}'
            )
        cpu_times.append(proc.execution_time)
        wall_times.append(proc.wall_clock_time)
        print(
            f'{name} {i + 1}/{runs}: cpu={proc.execution_time:.3f}s wall={proc.wall_clock_time:.3f}s', file=sys.stderr
        )
    steal_ticks = read_steal_ticks() - steal_before
    return {
        'cpu_time': summarize(cpu_times),
        'wall_time': summarize(wall_times),
        'steal_seconds': steal_ticks / os.sysconf('SC_CLK_TCK'),
        'cpu_samples': cpu_times,
        'wall_samples': wall_times,
    }


def main() -> None:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    with open('/proc/cpuinfo') as f:
        cpu_model = next(
            (line.split(':', 1)[1].strip() for line in f if line.startswith('model name')), platform.machine()
        )
    started = time.time()
    result = {
        'host': platform.node(),
        'cpu_model': cpu_model,
        'online_cpus': os.cpu_count(),
        'runs': runs,
        'cpu_bound': bench('cpu_bound', CPU_BOUND, runs),
        'memory_bound': bench('memory_bound', MEMORY_BOUND, runs),
    }
    result['duration_seconds'] = time.time() - started
    json.dump(result, sys.stdout, indent=1)
    print()


if __name__ == '__main__':
    main()
