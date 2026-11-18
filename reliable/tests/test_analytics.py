#!/usr/bin/env python3
"""Seeded Python oracle for grouped numeric/string aggregates and UTC months."""
import collections
import datetime
import random
import tempfile
import pathlib
from helpers import run

rng = random.Random(91425)
with tempfile.TemporaryDirectory(prefix='mdb-analytics-') as tmp:
    path = pathlib.Path(tmp)
    (path/'t.schema').write_text('id INT AUTO\ncategory VARCHAR(12)\namount INT\ncreated INT\n')
    data = [(rng.choice(['a', 'b', 'c', '한글']), rng.randrange(-100, 101),
             rng.choice([-1, 0, 1704067200, 1706745600, 1709251200])) for _ in range(200)]
    (path/'t.csv').write_text(''.join(f'{i},{c},{n},{ts}\n' for i, (c,n,ts) in enumerate(data, 1)))
    for lower in range(-100, 101, 10):
        chosen = [r for r in data if r[1] >= lower]
        _, results = run(path, f'SELECT category,SUM(amount) AS s,AVG(amount),MIN(amount),MAX(amount),COUNT(*) FROM t WHERE amount>={lower} GROUP BY category ORDER BY s DESC,category;')
        grouped = collections.defaultdict(list)
        for c,n,_ in chosen:
            grouped[c].append(n)
        expected = sorted(grouped, key=lambda c: (-sum(grouped[c]), c.encode()))
        rows = results[-1]['rows']
        assert len(rows) == len(expected)
        for row, c in zip(rows, expected):
            nums = grouped[c]
            assert row[0] == c and int(row[1]) == sum(nums) and abs(float(row[2]) - sum(nums)/len(nums)) < 1e-12, row
            assert list(map(int,row[3:])) == [min(nums), max(nums), len(nums)], row
    months = collections.defaultdict(list)
    for _,n,ts in data:
        key = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime('%Y-%m')
        months[key].append(n)
    _, results = run(path, 'SELECT MONTH(created) AS m,SUM(amount),COUNT(*) FROM t GROUP BY MONTH(created) ORDER BY m;')
    assert results[-1]['rows'] == [[m,str(sum(ns)),str(len(ns))] for m,ns in sorted(months.items())]
    _, results = run(path, 'SELECT SUM(amount),AVG(amount),MIN(category),MAX(category),COUNT(*) FROM t WHERE id=0;')
    assert results[-1]['rows'] == [[None,None,None,None,'0']]
    # Group limits must still apply before LIMIT/OFFSET.
    proc, results = run(path, 'SELECT category,SUM(amount) FROM t GROUP BY category LIMIT 1 OFFSET 1;',
                        env={'MDB_MAX_GROUPS':'2'}, check=False)
    assert proc.returncode and results[-1]['rows'] == []
    # UTC conversion boundaries are independent of the host timezone.
    _, results = run(path, 'UPDATE t SET created=253402300799;SELECT MONTH(created),COUNT(*) FROM t GROUP BY MONTH(created);')
    assert results[-1]['rows'] == [['9999-12','200']]
    proc, results = run(path, 'UPDATE t SET created=253402300800;SELECT MONTH(created),COUNT(*) FROM t GROUP BY MONTH(created);', check=False)
    assert proc.returncode and not results[-1]['ok']
print('PASS analytics: 21 seeded grouped reference queries, fractional AVG, UTF-8 keys, UTC boundaries, empty aggregates, workspace failure')
