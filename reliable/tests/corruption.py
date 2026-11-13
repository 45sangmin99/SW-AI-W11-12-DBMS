#!/usr/bin/env python3
import pathlib, struct, tempfile
from helpers import fixture,run

def seed(p):
    fixture(p);run(p,"INSERT INTO characters VALUES ('confirmed',1);")
def rejected(p):
    result,_=run(p,'SELECT * FROM characters;',check=False)
    assert result.returncode!=0,(result.stdout,result.stderr)
    return result.stderr
# First-time initialization must not overwrite an unknown binary file. Without
# the baseline/WAL metadata there is no authority for treating it as derived.
with tempfile.TemporaryDirectory(prefix='mdb-unowned-binary-') as folder:
    p=pathlib.Path(folder);fixture(p)
    original=b'pre-existing binary data must not be overwritten\x00\xff'
    (p/'characters.bin').write_bytes(original)
    rejected(p)
    assert (p/'characters.bin').read_bytes()==original
    assert not (p/'.mdb-base').exists()
    assert not (p/'.mdb.init').exists()
for kind in ('wal_checksum','wal_missing','baseline_missing','schema_change','marker_missing'):
    with tempfile.TemporaryDirectory(prefix='mdb-corruption-') as folder:
        p=pathlib.Path(folder);seed(p)
        wal=p/'.mdb.wal'
        assert wal.exists()
        if kind=='wal_checksum':
            b=bytearray(wal.read_bytes());assert len(b)>32;b[20]^=0x80;wal.write_bytes(b)
        elif kind=='wal_missing':wal.unlink()
        elif kind=='baseline_missing':(p/'.mdb-base'/'characters.csv').unlink()
        elif kind=='schema_change':(p/'characters.schema').write_text('id INT AUTO\nname VARCHAR(79)\nattack INT\n')
        elif kind=='marker_missing':(p/'.mdb.init').unlink()
        rejected(p)
        assert (p/'characters.bin').stat().st_size > 32,kind
with tempfile.TemporaryDirectory(prefix='mdb-tail-') as folder:
    p=pathlib.Path(folder);seed(p)
    with (p/'.mdb.wal').open('ab') as f:f.write(b'\x01\x02\x03')
    _,out=run(p,'SELECT COUNT(*) FROM characters;');assert out[-1]['rows']==[['1']],out
    run(p,"INSERT INTO characters VALUES ('after-tail',2);")
    _,out=run(p,'SELECT COUNT(*) FROM characters;');assert out[-1]['rows']==[['2']],out
print('PASS corruption: checksum/missing WAL/baseline/schema/init fail closed; incomplete tail repaired before append')
