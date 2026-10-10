"""
Compares timing_benchmark.py results from several machines (e.g. several MicroVMs and one live judge).

The question it answers: if the same submission is judged on any of these machines, how far apart can its reported
CPU time be? Within one machine that is run-to-run noise; across machines it also includes vCPU speed differences.

Usage: python compare_benchmarks.py vm1.json vm2.json ... [--baseline live-judge.json]
"""

import argparse
import json
import statistics

WORKLOADS = ('cpu_bound', 'memory_bound')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('results', nargs='+')
    parser.add_argument('--baseline', help='result from a current live judge, to compare speed against')
    args = parser.parse_args()

    loaded = {path: json.load(open(path)) for path in args.results}
    baseline = json.load(open(args.baseline)) if args.baseline else None

    for workload in WORKLOADS:
        print(f'== {workload} (CPU time reported by cptbox, seconds) ==')
        print(f'{"machine":30} {"median":>8} {"p95":>8} {"max":>8} {"cv%":>6} {"steal s":>8}  cpu')
        medians, all_samples = [], []
        for path, result in loaded.items():
            stats = result[workload]['cpu_time']
            medians.append(stats['median'])
            # Drop the first run: it pays for cold caches and page faults the others don't.
            all_samples += result[workload]['cpu_samples'][1:]
            print(
                f'{path:30} {stats["median"]:8.3f} {stats["p95"]:8.3f} {stats["max"]:8.3f} '
                f'{stats["cv_percent"]:6.1f} {result[workload]["steal_seconds"]:8.2f}  {result["cpu_model"]}'
            )

        overall = statistics.median(all_samples)
        print(f'median of all runs:            {overall:.3f}')
        print(f'slowest/fastest VM median:     {100 * (max(medians) / min(medians) - 1):.1f}% apart')
        print(f'worst run vs overall median:   +{100 * (max(all_samples) / overall - 1):.1f}%')
        if baseline:
            base = baseline[workload]['cpu_time']['median']
            print(
                f'MicroVM vs live judge:         {100 * (overall / base - 1):+.1f}% '
                f'(live judge median {base:.3f}, {baseline["cpu_model"]})'
            )
        print()


if __name__ == '__main__':
    main()
