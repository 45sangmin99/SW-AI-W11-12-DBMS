#!/usr/bin/env python3
import pathlib,tempfile
from helpers import fixture,run
for fault,commit,expected in [('wal_write','UNKNOWN',None),('wal_fsync','UNKNOWN',None),('binary_prepare','NOT_COMMITTED',1),('binary_write','DURABLE',2),('applied_write','DURABLE',2),('index_insert','DURABLE',2)]:
    with tempfile.TemporaryDirectory(prefix='mdb-io-') as folder:
        p=pathlib.Path(folder);fixture(p)
        run(p,"BEGIN; INSERT INTO characters VALUES ('confirmed',1); INSERT INTO items VALUES ('confirmed',1); COMMIT;")
        proc,results=run(p,"BEGIN; INSERT INTO characters VALUES ('test',2); INSERT INTO items VALUES ('test',2); COMMIT;",{'MDB_IO_FAIL':fault},check=False)
        assert proc.returncode!=0,(fault,proc.stdout,proc.stderr)
        assert results and results[-1]['commit']==commit,(fault,results,proc.stderr)
        if commit in ('UNKNOWN','DURABLE'):assert results[-1]['recovery_required'],(fault,results)
        _,out=run(p,'SELECT COUNT(*) FROM characters; SELECT COUNT(*) FROM items;')
        values=[int(r['rows'][0][0]) for r in out]
        assert values[0]==values[1] and values[0] in (1,2),(fault,out)
        if expected is not None:assert values[0]==expected,(fault,out)
        print('PASS injected I/O:',fault,'response='+commit,'recovered='+str(values))

# A real partial capacity flush must stop the engine and discard the unsent
# suffix on close. Recovery must allow subsequent commits without duplication.
with tempfile.TemporaryDirectory(prefix='mdb-wal-flush-') as folder:
    p=pathlib.Path(folder);fixture(p)
    run(p,"BEGIN; INSERT INTO characters VALUES ('confirmed',1); INSERT INTO items VALUES ('confirmed',1); COMMIT;")
    sql='BEGIN;'+''.join("INSERT INTO %s VALUES ('bulk',%d);"%(table,i)
                        for table in ('characters','items') for i in range(800))+'COMMIT;'
    proc,results=run(p,sql,{'MDB_IO_FAIL':'wal_flush'},check=False)
    assert proc.returncode and results[-1]['commit']=='UNKNOWN' and results[-1]['recovery_required'],results[-1]
    _,out=run(p,'SELECT COUNT(*) FROM characters; SELECT COUNT(*) FROM items;')
    assert [r['rows'] for r in out]==[[['1']],[['1']]],out
    run(p,"INSERT INTO characters VALUES ('after-recovery',2);")
    _,out=run(p,'SELECT COUNT(*) FROM characters; SELECT COUNT(*) FROM items;')
    assert [r['rows'] for r in out]==[[['2']],[['1']]],out
    print('PASS injected partial WAL buffer flush: UNKNOWN, committed prefix preserved, later commit recovered')
