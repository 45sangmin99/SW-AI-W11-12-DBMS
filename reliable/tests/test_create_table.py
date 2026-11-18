"""Real-engine catalog growth, stable WAL indices, and external SIGKILL recovery."""
import os
from pathlib import Path
import select
import signal
import struct
import subprocess
import tempfile

from helpers import CLI, fixture, run


CREATE = 'CREATE TABLE aaa (id INT AUTO, event_id VARCHAR(64), amount INT);'


def kill_at(path, point):
    reader, writer = os.pipe()
    process = subprocess.Popen([str(CLI), '--data-dir', str(path), '-e', CREATE],
        env=dict(os.environ, MDB_FAILPOINT=point, MDB_FAILPOINT_ACTION='pause',
                 MDB_FAILPOINT_READY_FD=str(writer)), pass_fds=(writer,),
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    os.close(writer)
    try:
        ready = select.select([reader], [], [], 20)[0]
        assert ready and os.read(reader, 1) == b'R', (point, process.poll())
        process.kill()
        process.communicate(timeout=5)
        assert process.returncode == -signal.SIGKILL
    finally:
        os.close(reader)
        if process.poll() is None:
            process.kill()
            process.wait()


with tempfile.TemporaryDirectory(prefix='mdb-ddl-') as folder:
    path = Path(folder)
    fixture(path)
    run(path, "INSERT INTO characters VALUES ('old table',7);")
    original = (path / '.mdb-base/manifest').read_bytes()
    _, results = run(path, CREATE + "INSERT INTO aaa VALUES ('event',5); SELECT * FROM aaa;")
    assert results[0]['commit'] == 'DURABLE'
    assert results[-1]['rows'] == [['1', 'event', '5']]
    assert (path / '.mdb-base/manifest').read_bytes() == original
    assert not (path / 'aaa.schema').exists(), 'WAL owns new definitions'
    for _ in range(2):
        _, results = run(path, "SELECT name,attack FROM characters; SELECT event_id,amount FROM aaa;")
        assert [r['rows'] for r in results] == [[['old table', '7']], [['event', '5']]]
    _, results = run(path, "CREATE TABLE zzz (id INT AUTO, amount INT); INSERT INTO zzz VALUES (9);"
                          "BEGIN; INSERT INTO aaa VALUES ('second',6); INSERT INTO items VALUES ('old index',2); COMMIT;")
    _, results = run(path, 'SELECT amount FROM zzz; SELECT COUNT(*) FROM aaa; SELECT name FROM items;')
    assert [r['rows'] for r in results] == [[['9']], [['2']], [['old index']]]
    size = (path / '.mdb.wal').stat().st_size
    _, results = run(path, 'CREATE TABLE IF NOT EXISTS aaa (id INT AUTO);')
    assert results[0]['commit'] == 'NO_CHANGES'
    assert (path / '.mdb.wal').stat().st_size == size
    for sql in [CREATE, 'CREATE TABLE bad (id INT);', 'CREATE TABLE bad (name VARCHAR(4));',
                'CREATE TABLE bad (id INT AUTO, id INT);',
                'CREATE TABLE bad (id INT AUTO, x VARCHAR(256));',
                'CREATE TABLE bad (id INT AUTO, x INT AUTO);',
                'BEGIN; CREATE TABLE bad (id INT AUTO);']:
        process, _ = run(path, sql, check=False)
        assert process.returncode != 0, sql

for point, exists in [('create_pre_wal', False), ('create_durable', True)]:
    with tempfile.TemporaryDirectory(prefix='mdb-ddl-crash-') as folder:
        path = Path(folder)
        fixture(path)
        run(path, "INSERT INTO characters VALUES ('survives',1);")
        kill_at(path, point)
        for _ in range(2):
            process, result = run(path, 'SELECT COUNT(*) FROM aaa;', check=False)
            assert (process.returncode == 0) == exists, (point, result)
        if not exists:
            run(path, CREATE)
        run(path, "INSERT INTO aaa VALUES ('after recovery',1);")
        _, result = run(path, 'SELECT COUNT(*) FROM aaa; SELECT name FROM characters;')
        assert [r['rows'] for r in result] == [[['1']], [['survives']]]

with tempfile.TemporaryDirectory(prefix='mdb-ddl-torn-') as folder:
    path = Path(folder)
    fixture(path)
    run(path, "INSERT INTO characters VALUES ('preserved',1);")
    run(path, CREATE)
    wal = path / '.mdb.wal'
    raw = wal.read_bytes()
    offset = 8
    last = None
    while offset < len(raw):
        kind, length = struct.unpack_from('<II', raw, offset + 4)
        if kind == 7:
            last = offset
        offset += 32 + length
    assert last is not None
    wal.write_bytes(raw[:-3])
    process, _ = run(path, 'SELECT * FROM aaa;', check=False)
    assert process.returncode != 0
    assert wal.stat().st_size == last
    run(path, CREATE)
    _, result = run(path, 'SELECT name FROM characters; SELECT COUNT(*) FROM aaa;')
    assert [r['rows'] for r in result] == [[['preserved']], [['0']]]

print('PASS CREATE TABLE: catalog, stable indices, transactions, validation, restart, SIGKILL and torn-tail boundaries')
