#!/usr/bin/env python3
"""Build a benchmark-only S/X strict-2PL variant; production sources unchanged."""
import argparse
import json
import hashlib
import pathlib
import platform
import statistics
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser()
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--output', type=pathlib.Path, default=ROOT/'isolation-comparison.json')
a = p.parse_args()
if not 1 <= a.repeats <= 10:
    p.error('repeats must be 1..10')
build = ROOT/'build/isolation-compare'
build.mkdir(parents=True, exist_ok=True)
s = (ROOT/'src/engine.c').read_text()
s = s.replace('int whole; /* Whole-table', 'int shared;\n    int whole; /* Whole-table', 1)
s = s.replace('int64_t id, int whole, mdb_error *e)', 'int64_t id, int whole, int shared, mdb_error *e)', 1)
start = s.index('        if (whole) {', s.index('static snapshot *lock_key'))
end = s.index('        if (!db->healthy)', start)
s = s[:start] + '''        /* S/S compatible; X conflicts on the same row or overlapping table.
         * id=0,whole=0 is an INSERT intention, not a data row lock. */
        if (whole) {
            for (row_lock *l = db->locks; l; l = l->next) {
                if (l->table != ti) continue;
                if (l->owner == tx) {
                    if (l->whole && (!l->shared || shared)) owned = 1;
                } else if (!shared || !l->shared) blocked = 1;
            }
        } else if (db->lock_capacity) {
            for (int pass = 0; pass < (id ? 2 : 1); pass++) {
                int64_t key = pass ? id : 0;
                size_t b = lock_bucket(ti, key, db->lock_capacity);
                for (row_lock *l = db->lock_buckets[b]; l; l = l->hash_next) {
                    if (l->table != ti || l->id != key) continue;
                    if (l->owner == tx) {
                        if ((l->whole || l->id == id) && (!l->shared || shared)) owned = 1;
                    } else if ((l->whole || (id && l->id == id)) && (!shared || !l->shared)) blocked = 1;
                }
            }
        }
''' + s[end:]
s = s.replace('.owner = tx, .table = ti, .id = id, .whole = whole', '.owner = tx, .table = ti, .id = id, .whole = whole, .shared = shared')
s = s.replace('return lock_key(db, tx, ti, id, whole, e);', 'return lock_key(db, tx, ti, id, whole, st->kind == MDB_SELECT && !st->for_update, e);')
s = s.replace('lock_key(db, tx, ti, id, 0, &out->error)', 'lock_key(db, tx, ti, id, 0, 0, &out->error)')
s = s.replace('if (st->kind != MDB_SELECT || st->for_update) {', 'if (1) { /* benchmark strict 2PL: ordinary SELECT also acquires S locks */')
variant = build/'engine-2pl.c'
variant.write_text(s)
# A third baseline prevents fresh S locks from bypassing queued X requests.
priority = s.replace('struct mdb_db {', 'typedef struct pending_lock {\n    struct pending_lock *next;\n    transaction *owner;\n    size_t table;\n    int64_t id;\n    int whole;\n} pending_lock;\nstruct mdb_db {\n    pending_lock *pending;\n', 1)
priority = priority.replace('    struct timespec deadline;\n    clock_gettime',
                            '    pending_lock request = {0};\n    int queued = 0;\n    struct timespec deadline;\n    clock_gettime', 1)
priority = priority.replace('        if (!db->healthy)', '        if (shared && !owned) {\n            for (pending_lock *p = db->pending; p; p = p->next)\n                if (p->owner != tx && p->table == ti && (p->whole || whole || (id && p->id == id))) blocked = 1;\n        }\n        if (!db->healthy)', 1)
priority = priority.replace('#ifdef MDB_TESTING\n        if (db->lock_hook)', '        if (!shared && !queued) {\n            request = (pending_lock){db->pending, tx, ti, id, whole};\n            db->pending = &request; queued = 1;\n        }\n#ifdef MDB_TESTING\n        if (db->lock_hook)', 1)
remove_pending = '            if (queued) {\n                pending_lock **p = &db->pending;\n                while (*p != &request) p = &(*p)->next;\n                *p = request.next;\n                pthread_cond_broadcast(&db->lock_changed);\n            }\n'
priority = priority.replace('            snapshot *latest = db->current;', remove_pending + '            snapshot *latest = db->current;', 1)
start = priority.index('static snapshot *lock_key')
end = priority.index('static snapshot *lock_statement', start)
piece = priority[start:end].replace('    pthread_mutex_unlock(&db->mutex);\n    return NULL;', remove_pending + '    pthread_mutex_unlock(&db->mutex);\n    return NULL;', 1)
priority = priority[:start] + piece + priority[end:]
priority_variant = build/'engine-2pl-priority.c'
priority_variant.write_text(priority)
MODES = ['snapshot', '2pl', '2pl_priority']
core = ['common', 'sql', 'bpt', 'binary', 'slots', 'group']
for name in MODES:
    engine = ROOT/'src/engine.c' if name == 'snapshot' else priority_variant if name == '2pl_priority' else variant
    cmd = ['cc', '-std=c11', '-O2', '-g', '-Wall', '-Wextra', '-Werror', '-pedantic',
           '-D_POSIX_C_SOURCE=200809L', '-D_FILE_OFFSET_BITS=64', '-DMDB_TESTING',
           '-I'+str(ROOT/'include'), '-I'+str(ROOT/'src'),
           str(ROOT/'tools/isolation_bench.c'), *[str(ROOT/'src'/f'{n}.c') for n in core],
           str(engine), '-pthread', '-o', str(build/name)]
    subprocess.run(cmd, check=True)
results = []
for rep in range(a.repeats):
    for mode in (MODES if rep % 2 == 0 else list(reversed(MODES))):
        with tempfile.TemporaryDirectory(prefix='mdb-isolation-') as tmp:
            path = pathlib.Path(tmp)
            (path/'t.schema').write_text('id INT AUTO\nvalue INT\n')
            (path/'t.csv').write_text(''.join(f'{i},0\n' for i in range(1, 1001)))
            output = subprocess.check_output([str(build/mode), str(path), mode], text=True)
            run = json.loads(output)
            for case in run['cases']:
                case['read_rps'] = case['reads'] * 1000 / case['elapsed_ms']
                case['write_rps'] = case['writes'] * 1000 / case['elapsed_ms']
            run['repeat'] = rep + 1
            results.append(run)
            print(mode, rep+1, json.dumps(run['cases']), flush=True)
summary = []
for mode in MODES:
    selected = [r for r in results if r['mode'] == mode]
    for index, case in enumerate(selected[0]['cases']):
        rows = [r['cases'][index] for r in selected]
        summary.append({'mode':mode, 'case':case['case'], 'threads':case['threads'],
                        **{k:statistics.median(r[k] for r in rows) for k in
                           ['reads','writes','errors','lock_timeouts','elapsed_ms','read_p50_us','read_p95_us','write_p50_us','write_p95_us','read_rps','write_rps']}})
report = {'source_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
          'engine_sha256':hashlib.sha256((ROOT/'src/engine.c').read_bytes()).hexdigest(),
          'environment':{'os':platform.platform(), 'cpu':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip() if platform.system()=='Darwin' else platform.processor(),
                        'compiler':subprocess.check_output(['cc','--version'],text=True).splitlines()[0],
                        'flags':'C11 -O2 MDB_TESTING, engine API, same COW/WAL/fsync', 'repeats':a.repeats,
                        'quantiles':'nearest rank per run; median of run statistics; microseconds'},
          'limitations':['2PL retains the same snapshot/COW storage underneath; this isolates read-lock policy, not a fully redesigned 2PL storage engine.',
                         'Engine API only; HTTP overhead excluded. No power-failure test.'],
          'runs':results, 'summary':summary}
a.output.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
print('Report:', a.output)
