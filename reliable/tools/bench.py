#!/usr/bin/env python3
import argparse,json,os,pathlib,platform,subprocess,tempfile,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--count',type=int,default=1000000);p.add_argument('--batch-size',type=int,help='rows per explicit transaction; default: all rows in one transaction');p.add_argument('--output',default=str(ROOT/'binary-bench-results.json'));a=p.parse_args()
if not 1<=a.count<=10000000 or (a.batch_size is not None and not 1<=a.batch_size<=10000000):
    p.error('count and batch size must be in 1..10000000')
subprocess.run(['make','build/bench'],cwd=ROOT,check=True)
with tempfile.TemporaryDirectory(prefix='mdb-bench-') as folder:
    data=pathlib.Path(folder)
    start=time.perf_counter()
    (data/'records.schema').write_text('id INT AUTO\nname VARCHAR(32)\nk INT\n')
    (data/'records.csv').write_bytes(b'')
    generated=time.perf_counter()-start
    command=[str(ROOT/'build/bench'),folder,str(a.count)]
    if a.batch_size is not None:
        command.append(str(a.batch_size))
    proc=subprocess.run(command,capture_output=True,text=True)
    if proc.returncode:raise SystemExit(proc.stderr+'\n'+proc.stdout)
    result=json.loads(proc.stdout)
    with (data/'records.bin').open('rb') as stored:
        header=stored.read(32)
    if len(header)!=32 or header[:8]!=b'MDBBIN1\n':
        raise SystemExit('benchmark did not produce the expected binary storage format')
    slot_size=int.from_bytes(header[16:20],'little')
    if result['update_storage_bytes']!=slot_size or result['delete_storage_bytes']!=(slot_size if a.count>=2 else 0):
        raise SystemExit('single-row UPDATE/DELETE rewrote more or less than one binary slot')
    if result['commit_storage_bytes']!=a.count*slot_size:
        raise SystemExit('INSERT storage byte count is not exactly one slot per inserted row')
    result.update({'record_size_bytes':slot_size,'single_row_slot_write_verified':True,'insert_slot_bytes_verified':True})
    result.update({'platform':platform.platform(),'processor':platform.machine(),'compiler':subprocess.check_output(['cc','--version'],text=True).splitlines()[0],'schema_generation_s':generated,'insert_generation':'snprintf per SQL included in sql_insert_s','data_distribution':'sequential automatic ID; unique k equals ID; constant name','cache':'warm process/OS cache; no OS cache eviction; not a cold-cache claim','output_excluded':True,'storage_format':'fixed-width little-endian binary slots; immutable CSV baseline retained for initial import/recovery','durability':f"normal WAL/binary fsync policy; {result['transaction_count']} explicit insertion transaction(s), up to {result['transaction_size']} row(s) per transaction; durable ID ranges; UPDATE and DELETE each autocommit",'batch_mode':'explicit BEGIN/COMMIT per batch; size 1 uses the autocommit durable commit path but also parses BEGIN and COMMIT SQL','timing_notes':'SQL insertion, BEGIN and COMMIT times are cumulative across batches; insert_phase_wall_s includes all three and loop overhead; commit includes snapshot preparation, WAL, changed binary slots and engine index maintenance; storage byte counters exclude WAL and filesystem write amplification; reopen includes full baseline/WAL replay and index construction; standalone BPT times exclude storage and compare shuffle construction; max is not p99','unix_time':time.time()})
    result['files']={str(x.relative_to(data)):x.stat().st_size for x in data.rglob('*') if x.is_file()}
    pathlib.Path(a.output).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2))
