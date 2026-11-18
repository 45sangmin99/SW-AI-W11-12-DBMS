#!/usr/bin/env python3
"""Check fixed slot layout in one live engine, then verify WAL-only recovery.

Separate CLI invocations recover/repack derived files on every open, so they
cannot prove that an UPDATE or DELETE writes only its existing slot.
"""
import http.client
import json
import os
import pathlib
import signal
import socket
import struct
import subprocess
import tempfile
import time

from helpers import ROOT, fixture, run

SERVER = pathlib.Path(os.environ.get("MDB_SERVER", ROOT / "build/mini-db-server"))
TOKEN = "binary-storage-test-token"
HEADER_SIZE = 32


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def storage(path):
    raw = path.read_bytes()
    assert raw[:8] == b"MDBBIN1\n", raw[:32]
    width, columns = struct.unpack_from("<II", raw, 16)
    assert columns == 3 and width == 116, (columns, width)
    assert (len(raw) - HEADER_SIZE) % width == 0, len(raw)
    slots = [raw[pos:pos + width] for pos in range(HEADER_SIZE, len(raw), width)]
    return raw, slots


def live_slot(slot):
    assert slot[0] == 1 and slot[1:8] == bytes(7)
    identifier = struct.unpack_from("<q", slot, 16)[0]
    length = struct.unpack_from("<I", slot, 24)[0]
    assert length <= 80
    assert slot[28 + length:108] == bytes(80 - length), "Nonzero string padding"
    name = slot[28:28 + length].decode("utf-8")
    attack = struct.unpack_from("<q", slot, 108)[0]
    return identifier, name, attack


def main():
    with tempfile.TemporaryDirectory(prefix="mdb-binary-") as directory:
        path = pathlib.Path(directory)
        fixture(path)
        source = {name: (path / name).read_bytes()
                  for name in ("characters.csv", "items.csv")}
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        with (path / "server.log").open("w+") as log:
            process = subprocess.Popen(
                [str(SERVER), "--data-dir", str(path), "--port", str(port),
                 "--threads", "3", "--queue", "8"],
                env=dict(os.environ, MINIDB_TOKEN=TOKEN), stdout=log, stderr=log)

            def request(method, route, body=None):
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
                headers = {"Authorization": "Bearer " + TOKEN,
                           "Content-Type": "application/json"}
                if isinstance(body, dict):
                    body = json.dumps(body, ensure_ascii=False).encode("utf-8")
                try:
                    connection.request(method, route, body, headers)
                    response = connection.getresponse()
                    return response.status, json.loads(response.read())
                finally:
                    connection.close()

            def query(sql, session=None, status=200):
                route = "/query" if session is None else "/sessions/" + session + "/query"
                received, result = request("POST", route, {"sql": sql})
                assert received == status, (sql, received, result)
                return result

            try:
                for _ in range(100):
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError("Server startup failed: " + log.read())
                    try:
                        if request("GET", "/health")[0] == 200:
                            break
                    except (OSError, http.client.HTTPException):
                        time.sleep(.05)
                else:
                    raise AssertionError("Server startup timed out")

                first = query("INSERT INTO characters VALUES ('short',9)")["inserted_id"]
                special = '한글, "quote"\n줄바꿈'
                second = query("INSERT INTO characters VALUES (" + literal(special) +
                               ",-9223372036854775808)")["inserted_id"]
                binary = path / "characters.bin"
                inode = binary.stat().st_ino
                before, before_slots = storage(binary)
                assert len(before_slots) == 2
                assert [struct.unpack_from("<q", slot, 16)[0] for slot in before_slots] == [
                    int(first), int(second)]
                assert live_slot(before_slots[0]) == (int(first), "short", 9)
                assert live_slot(before_slots[1]) == (int(second), special, -(1 << 63))
                assert query("SELECT name FROM characters WHERE id=" + second)["rows"] == [[special]]

                received, opened = request("POST", "/sessions", b"")
                assert received == 201, (received, opened)
                reader = opened["session"]
                query("BEGIN", reader)
                assert query("SELECT name,attack FROM characters WHERE id=" + first,
                             reader)["rows"] == [["short", "9"]]

                # The longest legal UTF-8 value still fits the same physical slot.
                replacement = "가" * 26 + "ab"
                assert len(replacement.encode("utf-8")) == 80
                query("UPDATE characters SET name=" + literal(replacement) +
                      ",attack=9223372036854775807 WHERE id=" + first)
                updated, updated_slots = storage(binary)
                assert binary.stat().st_ino == inode
                assert len(updated) == len(before) and updated[:HEADER_SIZE] == before[:HEADER_SIZE]
                assert updated_slots[0] != before_slots[0]
                assert updated_slots[1] == before_slots[1], "UPDATE changed an unrelated slot"
                assert live_slot(updated_slots[0]) == (int(first), replacement, (1 << 63) - 1)
                assert query("SELECT name,attack FROM characters WHERE id=" + first)["rows"] == [
                    [replacement, "9223372036854775807"]]
                # Old views must survive in-place writes, including the indexed path.
                assert query("SELECT name,attack FROM characters WHERE id=" + first,
                             reader)["rows"] == [["short", "9"]]
                assert query("SELECT name FROM characters WHERE attack=9", reader)["rows"] == [["short"]]

                query("UPDATE characters SET name=" + literal("가" * 27) +
                      " WHERE id=" + first, status=413)
                assert binary.read_bytes() == updated, "Rejected update changed storage"

                query("DELETE FROM characters WHERE id=" + second)
                deleted, deleted_slots = storage(binary)
                assert binary.stat().st_ino == inode and len(deleted) == len(updated)
                assert deleted_slots[0] == updated_slots[0]
                assert deleted_slots[1][0] == 0, "DELETE must write a tombstone"
                assert query("SELECT * FROM characters WHERE id=" + second)["rows"] == []
                assert query("SELECT name FROM characters WHERE id=" + second,
                             reader)["rows"] == [[special]]

                third_name = "tail spaces  "
                third = query("INSERT INTO characters VALUES (" + literal(third_name) +
                              ",7)")["inserted_id"]
                appended, appended_slots = storage(binary)
                assert binary.stat().st_ino == inode
                assert len(appended) == len(deleted) + 116
                assert appended_slots[:2] == deleted_slots, "INSERT rewrote prior slots"
                assert appended_slots[2][0] == 1
                assert struct.unpack_from("<q", appended_slots[2], 16)[0] == int(third)
                assert live_slot(appended_slots[2]) == (int(third), third_name, 7)
                assert query("SELECT * FROM characters WHERE id=" + third, reader)["rows"] == []
                assert query("SELECT COUNT(*) FROM characters", reader)["rows"] == [["2"]]
                for identifier in (first, second, third):
                    indexed = query("SELECT * FROM characters WHERE id=" + identifier)
                    scanned = query("SELECT * FROM characters WHERE id=" + identifier +
                                    " OR id=" + identifier)
                    assert indexed["used_index"] and not scanned["used_index"]
                    assert indexed["rows"] == scanned["rows"], (indexed, scanned)
                query("COMMIT", reader)
                assert request("DELETE", "/sessions/" + reader)[0] == 200
                for name, content in source.items():
                    assert (path / name).read_bytes() == content, "Runtime rewrote initial CSV"
                    assert (path / ".mdb-base" / name).read_bytes() == content
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            assert process.returncode == 0, process.returncode

        # Derived binary data is disposable: replay is authoritative and repeatable.
        binary.write_bytes(b"damaged current binary")
        _, results = run(path, "SELECT name,attack FROM characters ORDER BY id")
        expected = [[replacement, "9223372036854775807"], [third_name, "7"]]
        assert results[-1]["rows"] == expected, results
        recovered = binary.read_bytes()
        _, results = run(path, "SELECT name,attack FROM characters ORDER BY id")
        assert results[-1]["rows"] == expected
        assert binary.read_bytes() == recovered, "Repeated recovery changed binary state"
        for name, content in source.items():
            assert (path / name).read_bytes() == content

    print("PASS binary storage: fixed offsets, partial UPDATE, DELETE tombstone, append-only "
          "INSERT, retained snapshots, UTF-8 limits, index/scan agreement and repeated recovery")


if __name__ == "__main__":
    main()
