"""UNIQUE catalog and data recovery through actual CLI processes and SIGKILL."""
import os
from pathlib import Path
import select
import signal
import struct
import subprocess
import tempfile

from helpers import CLI, fixture, run

CREATE = 'CREATE UNIQUE INDEX u_name ON characters(name);'


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


with tempfile.TemporaryDirectory(prefix='mdb-unique-') as folder:
    path = Path(folder)
    fixture(path)
    # Historical duplicates are allowed before constraint creation; the final
    # current table must be unique when the index is installed.
    run(path, "INSERT INTO characters VALUES ('same',1); INSERT INTO characters VALUES ('same',2);")
    before = (path / '.mdb.wal').stat().st_size
    process, result = run(path, CREATE, check=False)
    assert process.returncode and result[-1]['commit'] == 'NOT_COMMITTED'
    assert (path / '.mdb.wal').stat().st_size == before
    run(path, "DELETE FROM characters WHERE attack=2;" + CREATE)
    for value in ['', '한글', '한글😀', 'A', 'a', "O''Brien"]:
        run(path, f"INSERT INTO characters VALUES ('{value}',3);")
        process, result = run(path, f"INSERT INTO characters VALUES ('{value}',4);", check=False)
        assert process.returncode and result[-1]['error']['code'] == 'ERR_DUPLICATE_ID'
    run(path, 'CREATE UNIQUE INDEX u_id ON characters(id);')
    size = (path / '.mdb.wal').stat().st_size
    _, result = run(path, 'CREATE UNIQUE INDEX IF NOT EXISTS u_name ON characters(name);')
    assert result[-1]['commit'] == 'NO_CHANGES'
    assert (path / '.mdb.wal').stat().st_size == size
    for sql in [CREATE, 'CREATE UNIQUE INDEX IF NOT EXISTS u_name ON characters(attack);',
                'CREATE UNIQUE INDEX bad ON missing(name);',
                'CREATE UNIQUE INDEX bad ON characters(missing);',
                'CREATE UNIQUE INDEX bad ON characters(name,attack);',
                'CREATE INDEX bad ON characters(name);',
                'BEGIN; CREATE UNIQUE INDEX bad ON characters(attack);']:
        process, _ = run(path, sql, check=False)
        assert process.returncode, sql
    # Append another table after an index without changing old WAL indices.
    run(path, 'CREATE TABLE aaa (id INT AUTO, x INT); CREATE UNIQUE INDEX u_x ON aaa(x);')
    run(path, 'INSERT INTO aaa VALUES (-9223372036854775808); INSERT INTO aaa VALUES (9223372036854775807);')
    process, _ = run(path, 'INSERT INTO aaa VALUES (-9223372036854775808);', check=False)
    assert process.returncode

for point, installed in [('unique_pre_wal', False), ('unique_durable', True)]:
    with tempfile.TemporaryDirectory(prefix='mdb-unique-crash-') as folder:
        path = Path(folder)
        fixture(path)
        run(path, "INSERT INTO characters VALUES ('kept',1);")
        kill_at(path, point)
        for _ in range(2):
            _, result = run(path, 'SELECT name FROM characters;')
            assert result[-1]['rows'] == [['kept']]
        if installed:
            process, _ = run(path, "INSERT INTO characters VALUES ('kept',2);", check=False)
            assert process.returncode
        else:
            # A different valid definition using the same name proves the first
            # CREATE did not survive the pre-WAL kill.
            run(path, 'CREATE UNIQUE INDEX u_name ON characters(attack);')
            run(path, "INSERT INTO characters VALUES ('kept',2);")

with tempfile.TemporaryDirectory(prefix='mdb-unique-tail-') as folder:
    path = Path(folder)
    fixture(path)
    run(path, "INSERT INTO characters VALUES ('kept',1);" + CREATE)
    wal = path / '.mdb.wal'
    raw = wal.read_bytes()
    offset, unique_at = 8, None
    while offset < len(raw):
        kind, size = struct.unpack_from('<II', raw, offset + 4)
        if kind == 8:
            unique_at = offset
        offset += 32 + size
    assert unique_at is not None
    wal.write_bytes(raw[:-3])
    run(path, 'SELECT * FROM characters;')
    assert wal.stat().st_size == unique_at
    run(path, "INSERT INTO characters VALUES ('kept',2);")
    process, _ = run(path, CREATE, check=False)
    assert process.returncode

print('PASS UNIQUE: existing duplicates, exact text/int equality, DDL validation, WAL order, SIGKILL and torn-tail recovery')
