#!/usr/bin/env python3
"""Real HTTP: lock waits, snapshots, latest writes, aggregates and bounded pages."""
import concurrent.futures as futures
import http.client
import json
import os
import pathlib
import signal
import socket
import subprocess
import tempfile
import threading
import time
from helpers import ROOT, fixture, run

SERVER = pathlib.Path(os.environ.get('MDB_SERVER', ROOT / 'build/mini-db-server'))
TOKEN = 'final-feature-tests'


def main():
    with tempfile.TemporaryDirectory(prefix='mdb-final-') as tmp:
        path = pathlib.Path(tmp)
        fixture(path)
        (path / 'characters.csv').write_text('1,a,10\n2,b,20\n')
        (path / 'events.schema').write_text('id INT AUTO\ncreated INT\namount INT\ncategory VARCHAR(20)\n')
        (path / 'events.csv').write_text('1,1704067200,10,x\n2,1705276800,20,x\n3,1706745600,15,y\n')
        (path / 'pages.schema').write_text('id INT AUTO\nvalue INT\n')
        (path / 'pages.csv').write_text(''.join(f'{i},{i}\n' for i in range(1, 100001)))
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        log = open(path / 'server.log', 'w+')
        proc = subprocess.Popen([str(SERVER), '--data-dir', str(path), '--port', str(port), '--threads', '8'],
                                stdout=log, stderr=log, env=dict(os.environ, MINIDB_TOKEN=TOKEN))

        def request(method, route, data=None):
            c = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
            try:
                c.request(method, route, None if data is None else json.dumps(data),
                          {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
                r = c.getresponse()
                return r.status, json.loads(r.read())
            finally:
                c.close()

        def q(sql, session=None, expected=200):
            status, data = request('POST', '/sessions/' + session + '/query' if session else '/query', {'sql': sql})
            assert status == expected, (sql, status, data)
            return data

        def session():
            status, data = request('POST', '/sessions')
            assert status == 201, data
            return data['session']

        def close(s):
            assert request('DELETE', '/sessions/' + s)[0] == 200

        try:
            for _ in range(200):
                if proc.poll() is not None:
                    log.seek(0)
                    raise AssertionError(log.read())
                try:
                    if request('GET', '/health')[0] == 200:
                        break
                except OSError:
                    time.sleep(.025)
            else:
                raise AssertionError('server startup timed out')
            # Different rows, same BEGIN generation: both commit, neither loses the other.
            a, b = session(), session()
            q('BEGIN', a); q('BEGIN', b)
            q('UPDATE characters SET attack=11 WHERE id=1', a)
            q('UPDATE characters SET attack=22 WHERE id=2', b)
            q('COMMIT', a); q('COMMIT', b)
            assert q('SELECT attack FROM characters ORDER BY id')['rows'] == [['11'], ['22']]
            # Ordinary reads remain on the BEGIN snapshot; locking reads use latest.
            q('BEGIN', a)
            q('UPDATE characters SET attack=33 WHERE id=1')
            assert q('SELECT attack FROM characters WHERE id=1', a)['rows'] == [['11']]
            assert q('SELECT attack FROM characters WHERE id=1 FOR UPDATE', a)['rows'] == [['33']]
            q('UPDATE characters SET attack=34 WHERE id=1', a)
            assert q('SELECT attack FROM characters WHERE id=1', a)['rows'] == [['34']]
            q('ROLLBACK', a)
            # Both sides of JOIN retain the same old snapshot after a multi-table commit.
            q("INSERT INTO items VALUES ('seed',33)")
            joined = ('SELECT c.name, i.name FROM characters c JOIN items i '
                      'ON c.attack=i.price ORDER BY c.id,i.id')
            q('BEGIN', a)
            assert q(joined, a)['rows'] == [['a', 'seed']]
            q('BEGIN', b)
            q("UPDATE characters SET name='new name' WHERE id=1", b)
            q("UPDATE items SET name='new item' WHERE price=33", b)
            q('COMMIT', b)
            assert q(joined, a)['rows'] == [['a', 'seed']]
            assert q(joined)['rows'] == [['new name', 'new item']]
            q('ROLLBACK', a)
            # Known holder + actual submitted waiter, no assumption about accepted sockets.
            q('BEGIN', a); q('BEGIN', b)
            q('SELECT attack FROM characters WHERE id=1 FOR UPDATE', a)
            submitted = threading.Event()
            def waiter():
                submitted.set()
                return q('SELECT attack FROM characters WHERE id=1 FOR UPDATE', b)
            with futures.ThreadPoolExecutor() as pool:
                future = pool.submit(waiter)
                assert submitted.wait(2)
                time.sleep(.1)
                assert not future.done(), 'same-row locking read did not wait'
                assert q('SELECT attack FROM characters WHERE id=1')['rows'] == [['33']]
                q('UPDATE characters SET attack=35 WHERE id=1', a)
                q('COMMIT', a)
                assert future.result(timeout=3)['rows'] == [['35']]
            q('ROLLBACK', b)
            # Timeout marks transaction failed, ROLLBACK clears it and releases locks.
            q('BEGIN', a); q('BEGIN', b)
            q('SELECT id FROM characters WHERE id=1 FOR UPDATE', a)
            start = time.monotonic()
            body = q('UPDATE characters SET attack=99 WHERE id=1', b, expected=409)
            assert body['error']['code'] == 'ERR_BUSY', body
            assert .8 < time.monotonic() - start < 4
            q('COMMIT', b, expected=409)
            q('ROLLBACK', b); close(a)
            q('UPDATE characters SET attack=36 WHERE id=1', b)
            close(b)
            # Empty/missing key locks protect a future insert through table-lock conflicts.
            a, b = session(), session()
            q('BEGIN', a); q('BEGIN', b)
            q('SELECT * FROM characters FOR UPDATE', a)
            q("INSERT INTO characters VALUES ('blocked',1)", b, expected=409)
            q('ROLLBACK', b); q('ROLLBACK', a); close(a); close(b)
            assert q('SELECT SUM(amount),AVG(amount),MIN(amount),MAX(amount),COUNT(*) FROM events')['rows'] == [['45', '15', '10', '20', '3']]
            assert q('SELECT MONTH(created) AS month,SUM(amount) AS total,AVG(amount) AS mean,COUNT(*) AS n FROM events GROUP BY MONTH(created) ORDER BY month')['rows'] == [['2024-01', '30', '15', '2'], ['2024-02', '15', '15', '1']]
            assert q('SELECT category,SUM(amount) AS total FROM events WHERE created>=1704067200 AND created<1706745600 GROUP BY category ORDER BY total DESC')['rows'] == [['x', '30']]
            assert q('SELECT MIN(category),MAX(category),AVG(amount) FROM events WHERE id=0')['rows'] == [[None, None, None]]
            assert q('SELECT AVG(amount) FROM events WHERE id<=2')['rows'] == [['15']]
            assert q('SELECT AVG(amount) FROM events WHERE id>=2')['rows'] == [['17.5']]
            assert q('SELECT MIN(category),MAX(category) FROM events')['rows'] == [['x', 'y']]
            assert q('SELECT MONTH(created),COUNT(*) FROM events WHERE created<1706745600 GROUP BY MONTH(created) LIMIT 1 OFFSET 1')['rows'] == []
            q('SELECT COUNT(*) FROM events ORDER BY SUM(amount)', expected=400)
            q("INSERT INTO events VALUES (0,9223372036854775807,'overflow')")
            q('SELECT SUM(amount) FROM events', expected=413)
            q('SELECT SUM(category) FROM events', expected=400)
            q('SELECT MONTH(category),COUNT(*) FROM events GROUP BY MONTH(category)', expected=400)
            q('SELECT SUM(amount) FROM events FOR UPDATE', expected=400)
            q('SELECT * FROM events OFFSET 1', expected=400)
            # Ten bounded responses enumerate 100k records. Same snapshot across pages.
            a = session(); q('BEGIN', a)
            ids = []
            for offset in range(0, 100000, 10000):
                rows = q(f'SELECT id FROM pages ORDER BY id LIMIT 10000 OFFSET {offset}', a)['rows']
                assert len(rows) == 10000
                ids.extend(int(row[0]) for row in rows)
            assert ids == list(range(1, 100001))
            assert q('SELECT id FROM pages LIMIT 3 OFFSET 99998', a)['rows'] == [['99999'], ['100000']]
            assert q('SELECT id FROM pages ORDER BY id LIMIT 5 OFFSET 100000', a)['rows'] == []
            q('ROLLBACK', a); close(a)
            assert q('SELECT category,COUNT(*) FROM events GROUP BY category ORDER BY category LIMIT 1 OFFSET 1')['rows'] == [['x', '2']]
            assert q('SELECT category,SUM(amount) AS total FROM events WHERE amount<100 GROUP BY category ORDER BY total DESC LIMIT 1 OFFSET 1')['rows'] == [['y', '15']]
            assert q('SELECT COUNT(*) FROM events LIMIT 1 OFFSET 1')['rows'] == []
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=15)
            log.close()
        _, recovered = run(path, 'SELECT attack FROM characters ORDER BY id;SELECT COUNT(*) FROM events;')
        assert recovered[0]['rows'] == [['36'], ['22']], recovered
        assert recovered[1]['rows'] == [['4']], recovered
    print('PASS final features: pessimistic locks/latest state, snapshot reads, timeout/release, aggregates/monthly UTC, 100000-row pagination')


if __name__ == '__main__':
    main()
