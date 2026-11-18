#!/usr/bin/env python3
"""Start a fresh DB and verify Node's real HTTP business API and static assets."""
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
from helpers import ROOT, run

with tempfile.TemporaryDirectory(prefix='mdb-drop-') as tmp:
    data = pathlib.Path(tmp)
    demo = ROOT.parent / 'examples/limited-drop'
    for src in (demo / 'data').iterdir():
        shutil.copyfile(src, data / src.name)
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    env = dict(os.environ, MINIDB_TOKEN='drop-integration', MINIDB_URL=f'http://127.0.0.1:{port}')
    server = pathlib.Path(os.environ.get('MDB_SERVER', ROOT / 'build/mini-db-server'))
    with open(data / 'server.log', 'w+') as log:
        proc = subprocess.Popen([str(server), '--data-dir', str(data), '--port', str(port), '--threads', '8'], env=env, stdout=log, stderr=log)
        try:
            for _ in range(200):
                if proc.poll() is not None:
                    log.seek(0)
                    raise AssertionError(log.read())
                try:
                    req = urllib.request.Request(env['MINIDB_URL'] + '/health', headers={'Authorization': 'Bearer drop-integration'})
                    if urllib.request.urlopen(req, timeout=1).status == 200:
                        break
                except OSError:
                    time.sleep(.025)
            else:
                raise AssertionError('DB startup timeout')
            subprocess.run(['node', str(demo / 'verify.mjs')], env=env, check=True, timeout=30)
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=15)
    _, recovered = run(data, 'SELECT stock FROM drops WHERE id=1;SELECT COUNT(*) FROM orders;')
    assert recovered[0]['rows'] == [['0']] and recovered[1]['rows'] == [['3']], recovered
    print('PASS demo recovery: final stock=0, committed orders=3')
