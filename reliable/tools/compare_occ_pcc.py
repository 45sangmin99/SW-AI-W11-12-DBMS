#!/usr/bin/env python3
"""Compare historical OCC engines with current row locks on the same order workload."""
import argparse
import io
import json
from pathlib import Path
import platform
import statistics
import subprocess
import tarfile
import tempfile

REPO = Path(__file__).resolve().parents[2]
HARNESS = Path(__file__).with_name('occ_pcc_bench.c')
BUILD = REPO / 'reliable/build/occ-pcc'
ENGINES = {
    'occ_global': '540ada5',
    'occ_row': 'fc79d0e',
    'pessimistic': 'e69bdf2',
}
WORKLOADS = ('single', 'disjoint', 'hot')


def git(*args):
    return subprocess.check_output(['git', '-C', str(REPO), *args])


def build_engine(source, executable, harness, defines):
    # Compile against each historical engine's own API headers and object list.
    makefile = (source / 'Makefile').read_text()
    core = next(line.split('=', 1)[1].split() for line in makefile.splitlines()
                if line.startswith('CORE = '))
    subprocess.run(['cc', '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', '-pedantic',
                    '-D_POSIX_C_SOURCE=200809L', '-D_FILE_OFFSET_BITS=64',
                    '-Iinclude', *defines, str(harness), *core, '-pthread', '-o', str(executable)],
                   cwd=source, check=True)


def run_case(executable, engine, workload, max_attempts=None, *, hold_ms=None, batch_width=None, detail_count=None):
    with tempfile.TemporaryDirectory(prefix='mdb-occ-pcc-') as directory:
        db = Path(directory)
        (db / 'inventory.schema').write_text('id INT AUTO\nstock INT\n')
        stock = 2 if max_attempts is None and workload not in ('paired', 'batch') else 10000
        (db / 'inventory.csv').write_text(''.join(f'{i},{stock}\n' for i in range(1, (batch_width * 2 if batch_width else 8) + 1)))
        (db / 'orders.schema').write_text('id INT AUTO\ninventory_id INT\n')
        (db / 'orders.csv').write_text('')
        if detail_count is not None:
            (db / 'order_lines.schema').write_text('id INT AUTO\nrequest_id INT\nline_no INT\n')
            (db / 'order_lines.csv').write_text('')
        command = [str(executable), directory, engine, workload]
        argument = batch_width or hold_ms or max_attempts
        if argument is not None:
            command.append(str(argument))
        if detail_count is not None:
            command.append(str(detail_count))
        output = subprocess.check_output(command, timeout=30)
        return json.loads(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeats', type=int, default=5, choices=range(1, 21))
    parser.add_argument('--max-attempts', type=int, default=1, choices=range(1, 2001), metavar='N')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--cancellation', action='store_true', help='measure competing order latency when the first order cancels')
    modes.add_argument('--batch', action='store_true', help='measure preparation and commit of bulk orders on independent rows')
    modes.add_argument('--paired', action='store_true', help='benchmark 2,000 overlapping pairs on independent rows')
    modes.add_argument('--contention', action='store_true', help='time two overlapping orders through successful commit and retry')
    modes.add_argument('--load-details', action='store_true', help='compare independent and shared orders with retries under 8-session load')
    modes.add_argument('--controlled', action='store_true', help='verify when a conflicting transaction waits or fails')
    args = parser.parse_args()
    scheduled = args.controlled or args.cancellation or args.paired or args.batch or args.contention
    harness = HARNESS.with_name('occ_pcc_schedule.c') if scheduled else HARNESS
    prefix = 'controlled-' if scheduled else ''
    BUILD.mkdir(parents=True, exist_ok=True)
    revisions = {name: git('rev-parse', ref).decode().strip() for name, ref in ENGINES.items()}
    if args.cancellation or args.batch or args.contention or args.load_details:
        revisions.pop('occ_global')
    if args.paired:
        revisions['occ_global'] = git('rev-parse', '6d5280b').decode().strip()
    for name, revision in revisions.items():
        with tempfile.TemporaryDirectory(prefix='mdb-revision-') as directory:
            with tarfile.open(fileobj=io.BytesIO(git('archive', revision, 'reliable')), mode='r:') as src:
                src.extractall(directory)
            defines = ['-DMDB_TESTING', '-DBENCH_PESSIMISTIC'] if scheduled and not (args.paired or args.batch) and name == 'pessimistic' else []
            build_engine(Path(directory) / 'reliable', BUILD / (prefix + name), harness, defines)
    if args.load_details:
        sizes = (16, 128, 512)
        workloads = ('disjoint', 'hot')
        runs = []
        for repeat in range(args.repeats):
            engines = list(revisions) if repeat % 2 == 0 else list(reversed(revisions))
            for size in sizes:
                for workload in workloads:
                    for engine in engines:
                        result = run_case(BUILD / engine, engine, workload, max_attempts=2000,
                                          detail_count=size)
                        if workload == 'disjoint':
                            assert not result['conflicts'] and not result['timeouts'], result
                        runs.append(dict(engine=engine, detail_count=size, workload=workload,
                                         repeat=repeat + 1, **result))
        fields = ('orders', 'committed', 'conflicts', 'timeouts', 'elapsed_ms', 'order_p95_ms',
                  'cpu_ms', 'replayed_detail_inserts')
        summary = []
        for size in sizes:
            for workload in workloads:
                for engine in revisions:
                    group = [r for r in runs if (r['detail_count'], r['workload'], r['engine']) ==
                             (size, workload, engine)]
                    summary.append(dict(engine=engine, detail_count=size, workload=workload,
                                        **{key: statistics.median(r[key] for r in group) for key in fields}))
        report = dict(revisions=revisions, environment=platform.platform(), runs=runs, summary=summary)
        (BUILD / 'load-details-results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return
    if args.contention:
        sizes = (1, 128, 2048)
        runs = []
        for repeat in range(args.repeats):
            engines = list(revisions) if repeat % 2 == 0 else list(reversed(revisions))
            for detail_count in sizes:
                for engine in engines:
                    result = run_case(BUILD / (prefix + engine), engine, 'contention',
                                      detail_count=detail_count)
                    runs.append(dict(engine=engine, detail_count=detail_count, repeat=repeat + 1, **result))
        fields = ('pair_ms', 'second_ms', 'cpu_ms', 'committed', 'conflicts', 'replayed_detail_inserts')
        summary = [dict(engine=engine, detail_count=size,
                        **{key: statistics.median(r[key] for r in runs
                                                  if r['engine'] == engine and r['detail_count'] == size)
                           for key in fields}) for size in sizes for engine in revisions]
        report = dict(revisions=revisions, environment=platform.platform(), runs=runs, summary=summary)
        (BUILD / 'contention-results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return
    if args.batch:
        runs = []
        widths = (8, 64, 256)
        for repeat in range(args.repeats):
            engines = list(revisions) if repeat % 2 == 0 else list(reversed(revisions))
            for width in widths:
                for engine in engines:
                    result = run_case(BUILD / (prefix + engine), engine, 'batch', batch_width=width)
                    runs.append(dict(engine=engine, width=width, repeat=repeat + 1, **result))
        fields = ('orders', 'commits', 'prepare_ms', 'commit_ms', 'elapsed_ms')
        summary = [dict(engine=engine, width=width,
                        **{key: statistics.median(r[key] for r in runs
                                                  if r['engine'] == engine and r['width'] == width)
                           for key in fields}) for width in widths for engine in revisions]
        report = dict(revisions=revisions, environment=platform.platform(), runs=runs, summary=summary)
        (BUILD / 'batch-results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return
    if args.paired:
        runs = []
        for repeat in range(args.repeats):
            engines = list(revisions) if repeat % 2 == 0 else list(reversed(revisions))
            for engine in engines:
                result = run_case(BUILD / (prefix + engine), engine, 'paired')
                runs.append(dict(engine=engine, repeat=repeat + 1, **result))
        fields = ('pairs', 'attempts', 'committed', 'conflicts', 'elapsed_ms', 'commits_per_sec')
        summary = [dict(engine=engine, **{key: statistics.median(r[key] for r in runs if r['engine'] == engine)
                                         for key in fields}) for engine in revisions]
        report = dict(revisions=revisions, environment=platform.platform(), runs=runs, summary=summary)
        (BUILD / 'paired-results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return
    if args.cancellation:
        runs = []
        for repeat in range(args.repeats):
            engines = list(revisions) if repeat % 2 == 0 else list(reversed(revisions))
            for workload, hold_ms in [('hot', 10), ('hot', 50), ('disjoint', 50)]:
                for engine in engines:
                    result = run_case(BUILD / (prefix + engine), engine, workload, hold_ms=hold_ms)
                    runs.append(dict(engine=engine, workload=workload, hold_ms=hold_ms,
                                     repeat=repeat + 1, **result))
        summary = []
        for workload, hold_ms in [('hot', 10), ('hot', 50), ('disjoint', 50)]:
            for engine in revisions:
                group = [r for r in runs if (r['workload'], r['hold_ms'], r['engine']) ==
                         (workload, hold_ms, engine)]
                summary.append(dict(engine=engine, workload=workload, hold_ms=hold_ms,
                                    order_ms=statistics.median(r['order_ms'] for r in group),
                                    cancel_after_ms=statistics.median(r['cancel_after_ms'] for r in group),
                                    completed_before_cancel=sum(r['completed_before_cancel'] for r in group)))
        report = dict(revisions=revisions, environment=platform.platform(), runs=runs, summary=summary)
        (BUILD / 'cancellation-results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return
    if args.controlled:
        runs = [dict(engine=engine, workload=workload, repeat=repeat + 1,
                     **run_case(BUILD / (prefix + engine), engine, workload, None))
                for repeat in range(args.repeats) for workload in ('hot', 'disjoint') for engine in ENGINES]
        for run in runs[:6]:
            assert all({k: v for k, v in other.items() if k != 'repeat'} ==
                       {k: v for k, v in run.items() if k != 'repeat'}
                       for other in runs if (other['engine'], other['workload']) == (run['engine'], run['workload']))
            print(json.dumps(run))
        (BUILD / 'controlled-results.json').write_text(json.dumps(dict(revisions=revisions, runs=runs), indent=2) + '\n')
        print(f'PASS {len(runs)} controlled cases')
        return
    runs = []
    for repeat in range(args.repeats):
        for workload in WORKLOADS:
            parity = list(ENGINES) if repeat % 2 == 0 else list(reversed(ENGINES))
            for engine in parity:
                result = run_case(BUILD / engine, engine, workload, args.max_attempts)
                runs.append(dict(engine=engine, workload=workload, repeat=repeat + 1, **result))
                print(workload, engine, repeat + 1, result['committed'], flush=True)
    summary = []
    for workload in WORKLOADS:
        for engine in ENGINES:
            group = [r for r in runs if r['workload'] == workload and r['engine'] == engine]
            fields = [key for key in group[0] if key not in ('engine', 'workload', 'repeat')]
            summary.append(dict(engine=engine, workload=workload,
                                **{key: statistics.median(r[key] for r in group) for key in fields}))
    report = dict(max_attempts=args.max_attempts, revisions=revisions, environment=platform.platform(),
                  compiler=subprocess.check_output(['cc', '--version'], text=True).splitlines()[0],
                  runs=runs, summary=summary)
    output = 'results.json' if args.max_attempts == 1 else f'results-{args.max_attempts}-attempts.json'
    (BUILD / output).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
