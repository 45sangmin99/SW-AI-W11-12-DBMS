#!/usr/bin/env python3
"""Measure insertion stages and optionally compare WAL batching with equal durability."""
import argparse
import hashlib
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
p.add_argument('--compare-wal', action='store_true')
p.add_argument('--output', type=pathlib.Path)
a = p.parse_args()
if not 1 <= a.count <= 2000000 or not 1 <= a.repeats <= 20:
    p.error('count must be 1..2000000 and repeats 1..20')
if a.output is None:
    a.output = ROOT / ('wal-buffer-profile-results.json' if a.compare_wal else 'profile-results.json')
flags = '-Iinclude -D_POSIX_C_SOURCE=200809L -D_FILE_OFFSET_BITS=64'
configurations = {'baseline': ('build', ''), 'profile': ('build/profile', ' -DMDB_PROFILE')}
if a.compare_wal:
    configurations = {
        'unbuffered': ('build/wal-control', ' -DMDB_WAL_UNBUFFERED'),
        'buffered': ('build', ''),
        'unbuffered_profile': ('build/wal-control-profile', ' -DMDB_WAL_UNBUFFERED -DMDB_PROFILE'),
        'buffered_profile': ('build/profile', ' -DMDB_PROFILE'),
    }
for build, extra in configurations.values():
    subprocess.run(['make', 'BUILD=' + build, 'CPPFLAGS=' + flags + extra,
                    build + '/bench'], cwd=ROOT, check=True)

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()

runs = []
for repeat in range(a.repeats):
    modes = list(configurations)
    # Rotate positions across repeats; no two benchmark processes overlap.
    modes = modes[repeat % len(modes):] + modes[:repeat % len(modes)]
    for mode in modes:
        with tempfile.TemporaryDirectory(prefix='mdb-profile-') as folder:
            data = pathlib.Path(folder)
            (data / 'records.schema').write_text('id INT AUTO\nname VARCHAR(32)\nk INT\n')
            (data / 'records.csv').write_bytes(b'')
            executable = ROOT / configurations[mode][0] / 'bench'
            result = subprocess.run([str(executable), folder, str(a.count)],
                                    capture_output=True, text=True, check=True)
            measured = json.loads(result.stdout)
            assert measured['count'] == a.count and measured['transaction_count'] == 1
            assert measured['commit_storage_bytes'] == a.count * 68
            assert measured['update_storage_bytes'] == 68
            if 'profile' in mode:
                assert measured['syscalls']['data_pwrite_calls'] >= a.count
                assert measured['syscalls']['data_pwrite_bytes'] == a.count * 68
            measured.update(mode=mode, repeat=repeat + 1,
                            wal_sha256=digest(data / '.mdb.wal'),
                            data_sha256=digest(data / 'records.bin'))
            runs.append(measured)
            print(f"{mode} {repeat+1}/{a.repeats}: insert+commit={measured['insert_phase_wall_s']:.6f}s", flush=True)
assert len({r['wal_sha256'] for r in runs}) == 1, 'WAL bytes changed between configurations'
assert len({r['data_sha256'] for r in runs}) == 1, 'Recovered binary data changed between configurations'

def medians(mode):
    values = [r for r in runs if r['mode'] == mode]
    summary = {}
    for key, value in values[0].items():
        if isinstance(value, (int, float)):
            summary[key] = statistics.median(r[key] for r in values)
        elif key in ('stages', 'syscalls'):
            summary[key] = {k: statistics.median(r[key][k] for r in values) for k in value}
    return summary

summaries = {mode: medians(mode) for mode in configurations}
report = {'count': a.count, 'repeats_per_mode': a.repeats,
          'platform': platform.platform(),
          'compiler': subprocess.check_output(['cc', '--version'], text=True).splitlines()[0],
          'conditions': 'fresh directory per run; sequential automatic IDs and unique k; name=bench; one explicit transaction; normal WAL/data/APPLIED fsync; no relaxed durability; normal process/OS cache, not cold cache; no concurrent benchmarks',
          'method': 'CLOCK_MONOTONIC coarse stages in all builds; MDB_PROFILE additionally times and counts actual write/pwrite calls including retries; syscall durations are inclusive elapsed time, not pure kernel CPU or device latency; fine counters include ID reservation during SQL insertion; nested timers must not be added to enclosing stages; query/recovery/standalone BPT phases occur after insertion timing',
          'wal_buffer_bytes': 65536,
          'same_wal_bytes_all_runs': True, 'same_recovered_data_all_runs': True,
          'profiling_overhead': 'compare coarse-only and fine-profile variants separately; their differences include run-to-run noise, not just timer overhead',
          'mode_medians': summaries, 'runs': runs, 'unix_time': time.time()}
if a.compare_wal:
    report['buffered_speedup_vs_unbuffered'] = (
        summaries['unbuffered']['insert_phase_wall_s'] / summaries['buffered']['insert_phase_wall_s'])
else:
    report['baseline_medians'], report['profile_medians'] = summaries['baseline'], summaries['profile']
    report['median_insert_overhead_s'] = summaries['profile']['insert_phase_wall_s'] - summaries['baseline']['insert_phase_wall_s']
a.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({k:v for k,v in report.items() if k != 'runs'}, ensure_ascii=False, indent=2))
