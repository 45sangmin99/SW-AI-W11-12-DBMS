#!/usr/bin/env python3
"""Measure, without changing durability or batching, SQL insertion bottlenecks."""
import argparse
import json
import pathlib
import platform
import statistics
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser()
p.add_argument('--count', type=int, default=1000000)
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--output', type=pathlib.Path, default=ROOT / 'profile-results.json')
a = p.parse_args()
if not 1 <= a.count <= 2000000 or not 1 <= a.repeats <= 20:
    p.error('count must be 1..2000000 and repeats 1..20')
subprocess.run(['make', 'build/bench'], cwd=ROOT, check=True)
subprocess.run(['make', 'BUILD=build/profile',
                'CPPFLAGS=-Iinclude -D_POSIX_C_SOURCE=200809L -D_FILE_OFFSET_BITS=64 -DMDB_PROFILE',
                'build/profile/bench'], cwd=ROOT, check=True)
runs = []
for repeat in range(a.repeats):
    # Alternate ordering to reduce a systematic first/second-run bias.
    modes = ['baseline', 'profile'] if repeat % 2 == 0 else ['profile', 'baseline']
    for mode in modes:
        with tempfile.TemporaryDirectory(prefix='mdb-profile-') as folder:
            data = pathlib.Path(folder)
            (data / 'records.schema').write_text('id INT AUTO\nname VARCHAR(32)\nk INT\n')
            (data / 'records.csv').write_bytes(b'')
            executable = ROOT / ('build/bench' if mode == 'baseline' else 'build/profile/bench')
            result = subprocess.run([str(executable), folder, str(a.count)],
                                    capture_output=True, text=True, check=True)
            measured = json.loads(result.stdout)
            assert measured['count'] == a.count and measured['transaction_count'] == 1
            assert measured['commit_storage_bytes'] == a.count * 68
            assert measured['update_storage_bytes'] == 68
            if mode == 'profile':
                assert measured['syscalls']['data_pwrite_calls'] == a.count
                assert measured['syscalls']['data_pwrite_bytes'] == a.count * 68
            measured.update(mode=mode, repeat=repeat + 1)
            runs.append(measured)
            print(f"{mode} {repeat+1}/{a.repeats}: insert+commit={measured['insert_phase_wall_s']:.6f}s", flush=True)

def medians(mode):
    values = [r for r in runs if r['mode'] == mode]
    summary = {}
    for key, value in values[0].items():
        if isinstance(value, (int, float)):
            summary[key] = statistics.median(r[key] for r in values)
        elif key in ('stages', 'syscalls'):
            summary[key] = {k: statistics.median(r[key][k] for r in values) for k in value}
    return summary

baseline, profiled = medians('baseline'), medians('profile')
report = {'count': a.count, 'repeats_per_mode': a.repeats,
          'platform': platform.platform(),
          'compiler': subprocess.check_output(['cc', '--version'], text=True).splitlines()[0],
          'conditions': 'fresh directory per run; sequential automatic IDs and unique k; name=bench; one explicit transaction; normal WAL/data/APPLIED fsync; no relaxed durability; normal process/OS cache, not cold cache; no concurrent benchmarks',
          'method': 'CLOCK_MONOTONIC coarse stages in both builds; MDB_PROFILE additionally times and counts actual write/pwrite calls, including retries; syscall times are inclusive elapsed time, not pure kernel CPU or device latency; fine counters include ID reservation during SQL insertion; nested timers must not be added to enclosing stages; query/recovery/standalone BPT phases occur after insertion timing',
          'profiling_overhead': 'paired baseline/profile builds expose instrumentation impact; median difference also includes run-to-run noise',
          'median_insert_overhead_s': profiled['insert_phase_wall_s'] - baseline['insert_phase_wall_s'],
          'baseline_medians': baseline, 'profile_medians': profiled, 'runs': runs,
          'unix_time': time.time()}
a.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({k:v for k,v in report.items() if k != 'runs'}, ensure_ascii=False, indent=2))
