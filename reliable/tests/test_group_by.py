#!/usr/bin/env python3
"""GROUP BY/COUNT(*) through the real CLI and HTTP transaction sessions."""
import collections
import http.client
import json
import os
import pathlib
import random
import signal
import socket
import subprocess
import tempfile
import time

from helpers import ROOT, fixture, run

SERVER = pathlib.Path(os.environ.get("MDB_SERVER", ROOT / "build/mini-db-server"))
TOKEN = "group-by-test-token"
DATA = [
    ("alpha", 10), ("beta", 10), ("총웨", 20),
    ("alpha", 10), ("beta", 10), ("총웨", 20), ("총웨", 20),
    ("", 0), ("ab", 12), ("ab1", 2), ('pipe|,\n"quote"', -1),
    ("min", -(1 << 63)), ("max", (1 << 63) - 1), ("alpha", -10),
]


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def seed(path):
    fixture(path)
    statements = ["BEGIN"]
    statements.extend("INSERT INTO characters VALUES (" + literal(name) + "," +
                      str(attack) + ")" for name, attack in DATA)
    statements.append("COMMIT")
    run(path, ";".join(statements) + ";")


def name_counts(data):
    counts = collections.Counter(name for name, _ in data)
    return [[name, str(count)] for name, count in
            sorted(counts.items(), key=lambda item: (-item[1], item[0].encode("utf-8"))) ]


def cli_cases(path):
    def query(sql, expected, names=None, env=None):
        _, results = run(path, sql, env=env)
        assert results and results[-1]["rows"] == expected, (sql, results, expected)
        if names is not None:
            assert [column["name"] for column in results[-1]["columns"]] == names, results
        return results[-1]

    ordered = name_counts(DATA)
    query("SELECT name,COUNT(*) FROM characters GROUP BY name "
          "ORDER BY COUNT(*) DESC,name ASC", ordered, ["name", "count"])
    query("SELECT COUNT(*),name FROM characters GROUP BY name "
          "ORDER BY COUNT(*) DESC,name ASC", [[count, name] for name, count in ordered],
          ["count", "name"])
    # COUNT may be an ORDER BY key without appearing in the projection.
    query("SELECT name FROM characters GROUP BY name ORDER BY COUNT(*) DESC,name LIMIT 2",
          [[name] for name, _ in ordered[:2]], ["name"])
    query("SELECT name,COUNT(*) FROM characters GROUP BY name "
          "ORDER BY COUNT(*) DESC,name LIMIT 1", ordered[:1])

    counts = collections.Counter(DATA)
    composite = [[str(count), name, str(attack)] for (name, attack), count in
                 sorted(counts.items(), key=lambda item: (item[0][0].encode("utf-8"),
                                                         -item[0][1]))]
    query("SELECT COUNT(*),name,attack FROM characters GROUP BY name,attack "
          "ORDER BY name,attack DESC", composite, ["count", "name", "attack"])
    query("SELECT name,COUNT(*),attack FROM characters GROUP BY name,attack "
          "ORDER BY name,attack DESC", [[name, count, attack] for count, name, attack in composite],
          ["name", "count", "attack"])
    # These keys collide if strings and integers are naively concatenated.
    query("SELECT name,attack,COUNT(*) FROM characters WHERE name='ab' OR name='ab1' "
          "GROUP BY name,attack ORDER BY name", [["ab", "12", "1"], ["ab1", "2", "1"]])
    query("SELECT name,attack FROM characters GROUP BY attack,name ORDER BY name,attack DESC",
          [[name, str(attack)] for name, attack in
           sorted(set(DATA), key=lambda item: (item[0].encode("utf-8"), -item[1]))])

    attack_counts = collections.Counter(attack for _, attack in DATA)
    query("SELECT COUNT(*) FROM characters GROUP BY attack ORDER BY attack",
          [[str(attack_counts[attack])] for attack in sorted(attack_counts)], ["count"])
    query("SELECT attack FROM characters GROUP BY attack ORDER BY attack DESC LIMIT 3",
          [[str(attack)] for attack in sorted(attack_counts, reverse=True)[:3]])
    query("SELECT name,COUNT(*) FROM characters WHERE attack>=10 GROUP BY name "
          "ORDER BY COUNT(*) DESC,name", name_counts([row for row in DATA if row[1] >= 10]))
    query("SELECT name,COUNT(*) FROM characters WHERE id=1 GROUP BY name", [["alpha", "1"]])

    # A tied ORDER BY must preserve the order produced before sorting.
    _, original = run(path, "SELECT name,COUNT(*) FROM characters GROUP BY name")
    unsorted = original[-1]["rows"]
    query("SELECT name,COUNT(*) FROM characters GROUP BY name ORDER BY COUNT(*) DESC",
          sorted(unsorted, key=lambda value: -int(value[1])))

    query("SELECT * FROM characters GROUP BY id,name,attack ORDER BY id LIMIT 3",
          [[str(i + 1), name, str(attack)] for i, (name, attack) in enumerate(DATA[:3])],
          ["id", "name", "attack"])
    query("SELECT name,COUNT(*) FROM characters WHERE id=-1 GROUP BY name", [])
    query("SELECT COUNT(*) FROM characters WHERE id=-1 GROUP BY attack", [])
    query("SELECT name FROM items GROUP BY name", [])
    query("SELECT COUNT(*) FROM items", [["0"]], ["count"])
    query("SELECT COUNT(*) FROM characters", [[str(len(DATA))]], ["count"])
    query("SELECT COUNT(*) FROM characters ORDER BY COUNT(*) DESC", [[str(len(DATA))]])
    query("SELECT COUNT(*) FROM characters WHERE id=-1", [["0"]])
    query("SELECT COUNT(*) FROM characters LIMIT 0", [])
    query("SELECT name,COUNT(*) FROM characters GROUP BY name ORDER BY name LIMIT 0", [])

    rejected = [
        "SELECT attack,COUNT(*) FROM characters GROUP BY name",
        "SELECT * FROM characters GROUP BY name",
        "SELECT name,COUNT(*) FROM characters",
        "SELECT name FROM characters GROUP BY name ORDER BY attack",
        "SELECT name,COUNT(*) FROM characters GROUP BY missing",
        "SELECT missing,COUNT(*) FROM characters GROUP BY name",
        "SELECT name FROM characters GROUP BY name,name",
        "SELECT name FROM characters GROUP BY COUNT(*)",
        "SELECT name,COUNT(*) FROM characters GROUP BY name HAVING COUNT(*)>1",
        "SELECT name,SUM(attack) FROM characters GROUP BY name",
        "SELECT name AS label,COUNT(*) FROM characters GROUP BY name",
    ]
    for sql in rejected:
        process, results = run(path, sql, check=False)
        assert process.returncode and results and not results[-1]["ok"], (sql, results)
        assert results[-1]["commit"] == "NOT_COMMITTED", (sql, results)

    # LIMIT must not avoid accumulation or turn a workspace overflow into a
    # silently partial result. Unrelated global COUNT needs no group workspace.
    env = {"MDB_MAX_GROUPS": "2"}
    process, results = run(path, "SELECT name,COUNT(*) FROM characters GROUP BY name "
                           "ORDER BY COUNT(*) DESC LIMIT 1", env=env, check=False)
    assert process.returncode and results[-1]["error"]["code"] == "ERR_LIMIT_EXCEEDED", results
    assert results[-1]["rows"] == [], results
    query("SELECT name,COUNT(*) FROM characters WHERE name='alpha' OR name='beta' "
          "GROUP BY name ORDER BY name", [["alpha", "3"], ["beta", "2"]], env=env)
    query("SELECT COUNT(*) FROM characters", [[str(len(DATA))]], env=env)


def http_cases(path):
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

        def session():
            received, result = request("POST", "/sessions", b"")
            assert received == 201, (received, result)
            return result["session"]

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

            grouped = ("SELECT name,COUNT(*) FROM characters GROUP BY name "
                       "ORDER BY COUNT(*) DESC,name")
            reader, writer = session(), session()
            query("BEGIN", reader)
            before = name_counts(DATA)
            assert query(grouped, reader)["rows"] == before

            def staged_changes():
                query("BEGIN", writer)
                query("UPDATE characters SET name='beta' WHERE id=1", writer)
                query("DELETE FROM characters WHERE id=2", writer)
                query("INSERT INTO characters VALUES ('beta',10)", writer)
                query("INSERT INTO characters VALUES ('private',777)", writer)

            staged = list(DATA)
            staged[0] = ("beta", 10)
            del staged[1]
            staged.extend([("beta", 10), ("private", 777)])
            after = name_counts(staged)
            staged_changes()
            assert query(grouped, writer)["rows"] == after
            assert query("SELECT name,COUNT(*) FROM characters WHERE id=1 GROUP BY name",
                         writer)["rows"] == [["beta", "1"]]
            assert query(grouped, reader)["rows"] == before
            assert query(grouped)["rows"] == before
            query("ROLLBACK", writer)
            assert query(grouped)["rows"] == before

            staged_changes()
            query("COMMIT", writer)
            assert query(grouped)["rows"] == after
            assert query(grouped, reader)["rows"] == before
            assert query("SELECT name,COUNT(*) FROM characters WHERE id=1 GROUP BY name",
                         reader)["rows"] == [["alpha", "1"]]
            query("COMMIT", reader)
            assert query(grouped, reader)["rows"] == after

            query("BEGIN", writer)
            query("SELECT attack,COUNT(*) FROM characters GROUP BY name", writer, status=400)
            query(grouped, writer, status=409)
            query("ROLLBACK", writer)
            assert query(grouped, writer)["rows"] == after
            for identifier in (reader, writer):
                assert request("DELETE", "/sessions/" + identifier)[0] == 200
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        assert process.returncode == 0, process.returncode

    _, results = run(path, "SELECT name,COUNT(*) FROM characters GROUP BY name "
                      "ORDER BY COUNT(*) DESC,name")
    assert results[-1]["rows"] == after, results


def randomized_cases():
    """A separately computed Counter model checks 96 reproducible SQL queries."""
    rng = random.Random(20261113)
    names = ["", "ab", "ab1", "a|b", "a,b", "a\nb", 'a"b', "a'b", "총웨", "😀",
             " leading", "trailing ", " \t ", "semicolon;and'quote"]
    names.extend("name" + str(i) for i in range(12))
    attacks = [-(1 << 63), -999, -10, -1, 0, 1, 2, 10, 12, 999, (1 << 63) - 1]
    data = [(rng.choice(names), rng.choice(attacks)) for _ in range(480)]
    statements, expected = [], []

    for iteration in range(96):
        minimum, excluded, exception = rng.choice(attacks), rng.choice(names), rng.choice(attacks)
        if iteration % 12 == 0:
            predicate = "attack > 9223372036854775807"
            filtered = []
        elif iteration % 12 == 1:
            predicate = "name = " + literal(excluded)
            filtered = [row for row in data if row[0] == excluded]
        else:
            predicate = ("attack >= " + str(minimum) + " AND (name != " + literal(excluded) +
                         " OR attack = " + str(exception) + ")")
            filtered = [row for row in data if row[1] >= minimum and
                        (row[0] != excluded or row[1] == exception)]
        fields = rng.choice([("name",), ("attack",), ("name", "attack"), ("attack", "name")])
        positions = [0 if field == "name" else 1 for field in fields]
        counts = collections.Counter(tuple(row[position] for position in positions)
                                     for row in filtered)
        groups = list(counts.items())
        projection = list(fields)
        if iteration % 4 != 0:
            projection.insert(rng.randrange(len(projection) + 1), "COUNT(*)")
        if iteration % 8 == 0:
            projection = ["COUNT(*)"]
        ordering = [("COUNT(*)", bool(rng.randrange(2)))]
        ordering.extend((field, bool(rng.randrange(2))) for field in fields)
        for field, descending in reversed(ordering):
            if field == "COUNT(*)":
                groups.sort(key=lambda group: group[1], reverse=descending)
            else:
                position = fields.index(field)

                def key(group):
                    value = group[0][position]
                    return value.encode("utf-8") if isinstance(value, str) else value

                groups.sort(key=key, reverse=descending)
        limit = rng.choice([None, 0, 1, 3, 7, 1000])
        if limit is not None:
            groups = groups[:limit]
        result = []
        for key_values, count in groups:
            result.append([str(count) if field == "COUNT(*)" else
                           str(key_values[fields.index(field)]) for field in projection])
        sql = ("SELECT " + ",".join(projection) + " FROM characters WHERE " + predicate +
               " GROUP BY " + ",".join(fields) + " ORDER BY " +
               ",".join(field + (" DESC" if descending else " ASC")
                        for field, descending in ordering))
        if limit is not None:
            sql += " LIMIT " + str(limit)
        statements.append(sql)
        expected.append(result)

    with tempfile.TemporaryDirectory(prefix="mdb-group-model-") as directory:
        path = pathlib.Path(directory)
        fixture(path)
        inserts = ["BEGIN"]
        inserts.extend("INSERT INTO characters VALUES (" + literal(name) + "," + str(attack) + ")"
                       for name, attack in data)
        inserts.append("COMMIT")
        run(path, ";".join(inserts) + ";")
        _, actual = run(path, ";".join(statements) + ";")
        assert len(actual) == len(expected), len(actual)
        for index, (result, wanted) in enumerate(zip(actual, expected)):
            assert result["rows"] == wanted, (index, statements[index], result, wanted)


def main():
    with tempfile.TemporaryDirectory(prefix="mdb-group-by-") as directory:
        path = pathlib.Path(directory)
        seed(path)
        cli_cases(path)
        http_cases(path)
    randomized_cases()
    print("PASS GROUP BY: typed composite keys, COUNT projections/order, WHERE-before-group, "
          "LIMIT-after-group, empty/global COUNT, validation, workspace limit, CLI recovery "
          "and HTTP snapshot/overlay isolation; 96 seeded Counter-model queries")


if __name__ == "__main__":
    main()
