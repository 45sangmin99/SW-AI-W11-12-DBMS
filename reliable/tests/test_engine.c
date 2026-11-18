#define _DARWIN_C_SOURCE 1
#include "mdb.h"
#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static void file(const char *dir, const char *name, const char *content) {
    char p[512];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "w");
    assert(f);
    assert(fputs(content, f) >= 0);
    assert(!fclose(f));
}
static mdb_result execute(mdb_session *s, const char *sql) {
    mdb_result r;
    int rc = mdb_execute(s, sql, &r);
    if (rc) {
        fprintf(stderr, "SQL failed %s: %s\n", sql, r.error.message);
        abort();
    }
    return r;
}
static void ok(mdb_session *s, const char *sql) {
    mdb_result r = execute(s, sql);
    mdb_result_free(&r);
}
static void bad(mdb_session *s, const char *sql, mdb_code expected) {
    mdb_result r;
    assert(mdb_execute(s, sql, &r) < 0);
    assert(r.error.code == expected);
    mdb_result_free(&r);
}
static int64_t scalar(mdb_session *s, const char *sql) {
    mdb_result r = execute(s, sql);
    assert(r.nrows == 1 && r.ncolumns == 1);
    int64_t v = r.rows[0].values[0].i;
    mdb_result_free(&r);
    return v;
}
static int64_t insert(mdb_session *s, const char *name, int n) {
    char q[512];
    snprintf(q, sizeof q, "INSERT INTO characters (name,attack) VALUES ('%s',%d)", name, n);
    mdb_result r = execute(s, q);
    assert(r.has_inserted_id);
    int64_t id = r.inserted_id;
    mdb_result_free(&r);
    return id;
}
typedef struct {
    mdb_db *db;
    int64_t ids[20];
} worker;
static void *concurrent(void *p) {
    worker *w = p;
    mdb_error e;
    mdb_session *s = mdb_session_open(w->db, &e);
    assert(s);
    ok(s, "BEGIN");
    for (int i = 0; i < 20; i++)
        w->ids[i] = insert(s, "thread", i);
    ok(s, "ROLLBACK");
    mdb_session_close(s);
    return NULL;
}
#ifdef MDB_TESTING
extern void mdb_test_read_hook(mdb_db *, void (*)(void *), void *);
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    int entered, release;
} gate;
static void read_hook(void *context) {
    gate *g = context;
    pthread_mutex_lock(&g->mutex);
    g->entered++;
    pthread_cond_broadcast(&g->ready);
    while (!g->release)
        pthread_cond_wait(&g->ready, &g->mutex);
    pthread_mutex_unlock(&g->mutex);
}
static void *read_once(void *context) {
    mdb_db *db = context;
    mdb_error e;
    mdb_session *s = mdb_session_open(db, &e);
    assert(s);
    assert(scalar(s, "SELECT attack FROM characters WHERE id=1") == INT64_MIN);
    mdb_session_close(s);
    return NULL;
}
static void parallel_reads(mdb_db *db) {
    gate g;
    memset(&g, 0, sizeof g);
    assert(!pthread_mutex_init(&g.mutex, NULL));
    assert(!pthread_cond_init(&g.ready, NULL));
    mdb_test_read_hook(db, read_hook, &g);
    pthread_t a, b;
    assert(!pthread_create(&a, NULL, read_once, db));
    assert(!pthread_create(&b, NULL, read_once, db));
    pthread_mutex_lock(&g.mutex);
    struct timespec deadline;
    assert(!clock_gettime(CLOCK_REALTIME, &deadline));
    deadline.tv_sec += 5;
    while (g.entered < 2)
        assert(!pthread_cond_timedwait(&g.ready, &g.mutex, &deadline));
    g.release = 1;
    pthread_cond_broadcast(&g.ready);
    pthread_mutex_unlock(&g.mutex);
    assert(!pthread_join(a, NULL) && !pthread_join(b, NULL));
    mdb_test_read_hook(db, NULL, NULL);
    pthread_cond_destroy(&g.ready);
    pthread_mutex_destroy(&g.mutex);
}

typedef struct {
    mdb_db *db;
    mdb_result result;
} scan_job;

static void *scan_once(void *context) {
    scan_job *job = context;
    mdb_error error;
    mdb_session *session = mdb_session_open(job->db, &error);
    assert(session);
    job->result = execute(session, "SELECT id,name,attack FROM characters ORDER BY id");
    mdb_session_close(session);
    return NULL;
}

static void scan_snapshot_across_cache_boundary(void) {
    enum { ROW_COUNT = 1000 };
    char dir[] = "/tmp/mdb-scan-snapshot-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(80)\nattack INT\n");
    file(dir, "characters.csv", "");
    mdb_error error;
    mdb_db *db = mdb_open(dir, &error);
    assert(db);
    mdb_session *writer = mdb_session_open(db, &error);
    assert(writer);
    ok(writer, "BEGIN");
    for (int i = 1; i <= ROW_COUNT; i++)
        assert(insert(writer, "before", i) == i);
    ok(writer, "COMMIT");

    gate g;
    memset(&g, 0, sizeof g);
    assert(!pthread_mutex_init(&g.mutex, NULL));
    assert(!pthread_cond_init(&g.ready, NULL));
    mdb_test_scan_hook(db, read_hook, &g);
    scan_job job = {.db = db};
    pthread_t reader;
    assert(!pthread_create(&reader, NULL, scan_once, &job));
    pthread_mutex_lock(&g.mutex);
    struct timespec deadline;
    assert(!clock_gettime(CLOCK_REALTIME, &deadline));
    deadline.tv_sec += 5;
    while (!g.entered)
        assert(!pthread_cond_timedwait(&g.ready, &g.mutex, &deadline));
    pthread_mutex_unlock(&g.mutex);

    /* The reader has copied the first 564 fixed-width (116-byte) slots.
     * Change one cached row and two rows beyond that cache, then append.
     * Its next chunk must come from retained rows, not the newer file. */
    ok(writer, "BEGIN");
    ok(writer, "UPDATE characters SET name='inside',attack=-2 WHERE id=2");
    ok(writer, "UPDATE characters SET name='outside',attack=-800 WHERE id=800");
    ok(writer, "DELETE FROM characters WHERE id=900");
    assert(insert(writer, "after", 7777) == ROW_COUNT + 1);
    ok(writer, "COMMIT");

    pthread_mutex_lock(&g.mutex);
    g.release = 1;
    pthread_cond_broadcast(&g.ready);
    pthread_mutex_unlock(&g.mutex);
    assert(!pthread_join(reader, NULL));
    mdb_test_scan_hook(db, NULL, NULL);
    assert(g.entered == 1);
    assert(!job.result.used_index);
    assert(job.result.nrows == ROW_COUNT && job.result.ncolumns == 3);
    for (size_t i = 0; i < job.result.nrows; i++) {
        mdb_value *values = job.result.rows[i].values;
        assert(values[0].i == (int64_t)i + 1);
        assert(!strcmp(values[1].s, "before"));
        assert(values[2].i == (int64_t)i + 1);
    }
    mdb_result_free(&job.result);

    mdb_result current = execute(writer, "SELECT id,name,attack FROM characters ORDER BY id");
    assert(!current.used_index);
    assert(current.nrows == ROW_COUNT && current.ncolumns == 3);
    for (size_t i = 0; i < current.nrows; i++) {
        int64_t id = (int64_t)i + 1;
        if (id >= 900)
            id++;
        const char *name = "before";
        int64_t attack = id;
        if (id == 2) {
            name = "inside";
            attack = -2;
        } else if (id == 800) {
            name = "outside";
            attack = -800;
        } else if (id == ROW_COUNT + 1) {
            name = "after";
            attack = 7777;
        }
        mdb_value *values = current.rows[i].values;
        assert(values[0].i == id);
        assert(!strcmp(values[1].s, name));
        assert(values[2].i == attack);
    }
    mdb_result_free(&current);
    assert(!pthread_cond_destroy(&g.ready));
    assert(!pthread_mutex_destroy(&g.mutex));
    mdb_session_close(writer);
    mdb_close(db);
    char command[600];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
}
#endif
static void io_failures(void) {
    const char *points[] = {"wal_write", "wal_fsync", "applied_write", "index_insert"};
    for (size_t i = 0; i < sizeof points / sizeof *points; i++) {
        char dir[] = "/tmp/mdb-io-XXXXXX";
        assert(mkdtemp(dir));
        file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(80)\nattack INT\n");
        file(dir, "characters.csv", "");
        mdb_error e;
        mdb_db *db = mdb_open(dir, &e);
        assert(db);
        mdb_session *session = mdb_session_open(db, &e);
        assert(session);
        insert(session, "kept", 1);
        assert(!setenv("MDB_IO_FAIL", points[i], 1));
        mdb_result r;
        assert(mdb_execute(session, "INSERT INTO characters VALUES ('uncertain',2)", &r) < 0);
        assert(r.error.code == MDB_IO && r.recovery_required);
        assert(r.commit == (i >= 2 ? MDB_DURABLE : MDB_UNKNOWN));
        uint64_t txid = r.txid;
        mdb_result_free(&r);
        assert(!mdb_healthy(db));
        bad(session, "SELECT * FROM characters", MDB_IO);
        unsetenv("MDB_IO_FAIL");
        mdb_session_close(session);
        mdb_close(db);
        db = mdb_open(dir, &e);
        if (!db) {
            fprintf(stderr, "IO reopen %s: %s\n", points[i], e.message);
            abort();
        }
        session = mdb_session_open(db, &e);
        assert(session);
        int64_t expected = i == 0 ? 1 : 2;
        assert(scalar(session, "SELECT COUNT(*) FROM characters") == expected);
        assert(mdb_transaction_status(db, txid) == (i != 0));
        mdb_session_close(session);
        mdb_close(db);
        db = mdb_open(dir, &e);
        assert(db);
        session = mdb_session_open(db, &e);
        assert(scalar(session, "SELECT COUNT(*) FROM characters") == expected);
        mdb_session_close(session);
        mdb_close(db);
        char command[600];
        snprintf(command, sizeof command, "rm -rf %s", dir);
        assert(system(command) == 0);
    }
}
static void limits(void) {
    char dir[] = "/tmp/mdb-limit-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(80)\nattack INT\n");
    file(dir, "characters.csv", "");
    assert(!setenv("MDB_MAX_SESSIONS", "2", 1));
    assert(!setenv("MDB_TX_MAX_CHANGES", "2", 1));
    mdb_error e;
    mdb_db *db = mdb_open(dir, &e);
    assert(db);
    unsetenv("MDB_MAX_SESSIONS");
    unsetenv("MDB_TX_MAX_CHANGES");
    mdb_session *a = mdb_session_open(db, &e), *b = mdb_session_open(db, &e);
    assert(a && b);
    assert(!mdb_session_open(db, &e) && e.code == MDB_LIMIT);
    ok(a, "BEGIN");
    insert(a, "one", 1);
    insert(a, "two", 2);
    bad(a, "INSERT INTO characters VALUES ('three',3)", MDB_LIMIT);
    bad(a, "SELECT * FROM characters", MDB_FAILED);
    ok(a, "ROLLBACK");
    assert(scalar(a, "SELECT COUNT(*) FROM characters") == 0);
    bad(a, "SELECT * FROM characters LIMIT 10001", MDB_LIMIT);
    mdb_session_close(a);
    mdb_session_close(b);
    mdb_close(db);
    char command[600];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
}
static void partial_writes(void) {
    char dir[] = "/tmp/mdb-partial-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(80)\nattack INT\n");
    file(dir, "characters.csv", "");
    mdb_error error;
    mdb_db *db = mdb_open(dir, &error);
    assert(db);
    mdb_session *session = mdb_session_open(db, &error);
    assert(session);
    ok(session, "BEGIN");
    for (int i = 0; i < 200; i++)
        insert(session, "fixed", i);
    ok(session, "COMMIT");
    mdb_metrics before, after;
    mdb_get_metrics(db, &before);
    mdb_result result = execute(session, "UPDATE characters SET name='longer, \"value\"',attack=120 WHERE id=1");
    assert(result.used_index && result.affected == 1);
    mdb_result_free(&result);
    mdb_get_metrics(db, &after);
    assert(after.storage_bytes - before.storage_bytes == 116);
    assert(after.index_build_s == before.index_build_s);
    before = after;
    result = execute(session, "DELETE FROM characters WHERE id=2");
    assert(result.used_index && result.affected == 1);
    mdb_result_free(&result);
    mdb_get_metrics(db, &after);
    assert(after.storage_bytes - before.storage_bytes == 116);
    assert(after.index_build_s == before.index_build_s);
    assert(scalar(session, "SELECT COUNT(*) FROM characters") == 199);
    before = after;
    insert(session, "appended", 999);
    mdb_get_metrics(db, &after);
    assert(after.storage_bytes - before.storage_bytes == 116);
    mdb_session_close(session);
    mdb_close(db);
    char command[600];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
}
static void catalog_ddl(void) {
    char dir[] = "/tmp/mdb-catalog-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(64)\nattack INT\n");
    file(dir, "characters.csv", "");
    mdb_error e;
    mdb_db *db = mdb_open(dir, &e);
    assert(db);
    mdb_session *a = mdb_session_open(db, &e), *b = mdb_session_open(db, &e);
    assert(a && b);
    ok(b, "BEGIN;");
    /* Publishing another snapshot must not hide an older live transaction. */
    ok(a, "INSERT INTO characters VALUES ('new version',1);");
    bad(a, "CREATE TABLE aaa (id INT AUTO, n INT);", MDB_BUSY);
    assert(scalar(b, "SELECT COUNT(*) FROM characters;") == 0);
    ok(b, "ROLLBACK;");
    ok(a, "CREATE TABLE aaa (id INT AUTO, n INT);");
    ok(b, "INSERT INTO aaa VALUES (7);");
    ok(a, "BEGIN;");
    bad(a, "CREATE TABLE bbb (id INT AUTO);", MDB_STATE);
    ok(a, "ROLLBACK;");
    mdb_session_close(a);
    mdb_session_close(b);
    mdb_close(db);
    db = mdb_open(dir, &e);
    assert(db);
    a = mdb_session_open(db, &e);
    assert(scalar(a, "SELECT n FROM aaa;") == 7);
    mdb_session_close(a);
    mdb_close(db);
    char command[256];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
}

static void unique_constraints(void) {
    char dir[] = "/tmp/mdb-unique-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "id INT AUTO\nname VARCHAR(64)\nattack INT\n");
    file(dir, "characters.csv", "");
    file(dir, "items.schema", "id INT AUTO\nname VARCHAR(64)\nprice INT\n");
    file(dir, "items.csv", "");
    mdb_error e;
    mdb_db *db = mdb_open(dir, &e);
    assert(db);
    mdb_session *a = mdb_session_open(db, &e), *b = mdb_session_open(db, &e);
    ok(a, "INSERT INTO characters VALUES ('a',1);");
    ok(a, "INSERT INTO characters VALUES ('b',2);");
    ok(a, "CREATE UNIQUE INDEX unique_name ON characters(name);");
    ok(a, "CREATE UNIQUE INDEX unique_attack ON characters(attack);");
    bad(a, "INSERT INTO characters VALUES ('a',3);", MDB_DUPLICATE);
    bad(a, "INSERT INTO characters VALUES ('c',2);", MDB_DUPLICATE);
    bad(a, "UPDATE characters SET name='a';", MDB_DUPLICATE);
    assert(scalar(a, "SELECT COUNT(*) FROM characters;") == 2);
    ok(a, "BEGIN;");
    ok(a, "INSERT INTO characters VALUES ('c',3);");
    ok(a, "INSERT INTO characters VALUES ('c',4);");
    ok(a, "INSERT INTO items VALUES ('must rollback',1);");
    bad(a, "COMMIT;", MDB_DUPLICATE);
    bad(a, "SELECT * FROM characters;", MDB_FAILED);
    ok(a, "ROLLBACK;");
    assert(scalar(a, "SELECT COUNT(*) FROM items;") == 0);
    /* Constraints check final transaction state, allowing an atomic swap. */
    ok(a, "BEGIN;");
    ok(a, "UPDATE characters SET name='temp' WHERE name='a';");
    ok(a, "UPDATE characters SET name='a' WHERE name='b';");
    ok(a, "UPDATE characters SET name='b' WHERE name='temp';");
    ok(a, "COMMIT;");
    ok(a, "BEGIN;");
    ok(a, "DELETE FROM characters WHERE name='a';");
    ok(a, "INSERT INTO characters VALUES ('a',2);");
    ok(a, "COMMIT;");
    ok(a, "BEGIN;");
    ok(b, "BEGIN;");
    ok(a, "INSERT INTO characters VALUES ('c',3);");
    ok(b, "INSERT INTO characters VALUES ('c',3);");
    ok(a, "COMMIT;");
    bad(b, "COMMIT;", MDB_DUPLICATE);
    ok(b, "ROLLBACK;");
    assert(scalar(a, "SELECT COUNT(*) FROM characters WHERE name='c';") == 1);
    ok(b, "BEGIN;");
    ok(a, "INSERT INTO characters VALUES ('d',4);");
    bad(a, "CREATE UNIQUE INDEX new_index ON characters(name);", MDB_BUSY);
    ok(b, "ROLLBACK;");
    ok(a, "BEGIN;");
    bad(a, "CREATE UNIQUE INDEX new_index ON characters(name);", MDB_STATE);
    ok(a, "ROLLBACK;");
    mdb_session_close(a);
    mdb_session_close(b);
    mdb_close(db);
    db = mdb_open(dir, &e);
    assert(db);
    a = mdb_session_open(db, &e);
    bad(a, "INSERT INTO characters VALUES ('a',5);", MDB_DUPLICATE);
    bad(a, "UPDATE characters SET attack=1 WHERE name='d';", MDB_DUPLICATE);
    assert(scalar(a, "SELECT COUNT(*) FROM characters;") == 4);
    ok(a, "INSERT INTO characters VALUES ('e',5);");
    mdb_session_close(a);
    mdb_close(db);
    char command[256];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
}

int main(void) {
    unique_constraints();
    catalog_ddl();
    char dir[] = "/tmp/mdb-engine-XXXXXX";
    assert(mkdtemp(dir));
    file(dir, "characters.schema", "# sample\nid INT AUTO\nname VARCHAR(80)\nattack INT\n");
    file(dir, "items.schema", "id INT AUTO\nname VARCHAR(80)\n");
    file(dir, "characters.csv", "1,\"초기,\"\"값\"\"\n다음\",-9223372036854775808\r\n");
    file(dir, "items.csv", "");
    mdb_error e;
    mdb_db *db = mdb_open(dir, &e);
    if (!db) {
        fprintf(stderr, "open: %s\n", e.message);
        return 1;
    }
    mdb_session *a = mdb_session_open(db, &e), *b = mdb_session_open(db, &e);
    assert(a && b);
    assert(scalar(a, "SELECT attack FROM characters WHERE id=1") == INT64_MIN);
    mdb_result r = execute(a, "SELECT name FROM characters WHERE id=1");
    assert(r.used_index && strcmp(r.rows[0].values[0].s, "초기,\"값\"\n다음") == 0);
    mdb_result_free(&r);
    mdb_db *again = mdb_open(dir, &e);
    assert(!again && e.code == MDB_BUSY);
#ifdef MDB_TESTING
    parallel_reads(db);
#endif

    ok(a, "BEGIN");
    int64_t rolled = insert(a, "rolled", 2);
    assert(scalar(a, "SELECT COUNT(*) FROM characters") == 2);
    assert(scalar(b, "SELECT COUNT(*) FROM characters") == 1);
    ok(a, "ROLLBACK");
    int64_t saved = insert(a, "saved", 10);
    assert(saved > rolled);
    assert(scalar(b, "SELECT COUNT(*) FROM characters") == 2);
    ok(a, "BEGIN");
    ok(b, "BEGIN");
    ok(a, "UPDATE characters SET attack=20 WHERE name='saved'");
    ok(a, "INSERT INTO items VALUES ('one')");
    ok(a, "COMMIT");
    assert(scalar(b, "SELECT attack FROM characters WHERE name='saved'") == 10);
    assert(scalar(b, "SELECT COUNT(*) FROM items") == 0);
    ok(b, "COMMIT");
    assert(scalar(b, "SELECT COUNT(*) FROM items") == 1);
    ok(a, "BEGIN");
    ok(b, "BEGIN");
    ok(a, "UPDATE characters SET attack=30 WHERE name='saved'");
    bad(b, "UPDATE characters SET attack=40 WHERE name='saved'", MDB_BUSY);
    ok(a, "COMMIT");
    bad(b, "COMMIT", MDB_FAILED);
    bad(b, "SELECT * FROM items", MDB_FAILED);
    ok(b, "ROLLBACK");
    assert(scalar(b, "SELECT attack FROM characters WHERE name='saved'") == 30);
    ok(a, "BEGIN");
    int64_t transient = insert(a, "transient", 5);
    char query[256];
    snprintf(query, sizeof query, "UPDATE characters SET attack=9 WHERE id=%lld",
             (long long)transient);
    ok(a, query);
    snprintf(query, sizeof query, "SELECT attack FROM characters WHERE id=%lld",
             (long long)transient);
    assert(scalar(a, query) == 9);
    assert(!setenv("MDB_FORCE_SCAN", "1", 1));
    assert(scalar(a, query) == 9);
    unsetenv("MDB_FORCE_SCAN");
    ok(a, "DELETE FROM characters WHERE name='transient'");
    ok(a, "COMMIT");
    assert(scalar(a, "SELECT COUNT(*) FROM characters WHERE name='transient'") == 0);
    bad(a, "INSERT INTO characters (id,name,attack) VALUES (99,'x',1)", MDB_COLUMN_COUNT);
    bad(a, "SELECT * FROM characters WHERE attack='x'", MDB_TYPE);
    ok(a, "BEGIN");
    bad(a, "SELECT missing FROM characters", MDB_UNKNOWN_COLUMN);
    bad(a, "COMMIT", MDB_FAILED);
    ok(a, "ROLLBACK");
    ok(a, "BEGIN");
    int64_t dupsort = insert(a, "z", 30);
    (void)dupsort;
    insert(a, "y", 30);
    r = execute(a, "SELECT name,attack FROM characters WHERE attack=30 ORDER BY attack");
    assert(r.nrows == 3 && strcmp(r.rows[0].values[0].s, "saved") == 0 &&
           strcmp(r.rows[1].values[0].s, "z") == 0 && strcmp(r.rows[2].values[0].s, "y") == 0);
    mdb_result_free(&r);
    ok(a, "ROLLBACK");
    worker workers[4] = {0};
    pthread_t threads[4];
    for (int i = 0; i < 4; i++) {
        workers[i].db = db;
        assert(!pthread_create(&threads[i], NULL, concurrent, &workers[i]));
    }
    for (int i = 0; i < 4; i++)
        assert(!pthread_join(threads[i], NULL));
    for (int i = 0; i < 4; i++)
        for (int j = 0; j < 20; j++)
            for (int k = 0; k <= i; k++)
                for (int l = 0; l < 20; l++)
                    if (k != i || l != j)
                        assert(workers[i].ids[j] != workers[k].ids[l]);
    assert(!setenv("MDB_IO_FAIL", "binary_prepare", 1));
    mdb_result injected;
    assert(mdb_execute(a, "INSERT INTO characters VALUES ('fail',1)", &injected) < 0);
    assert(injected.commit == MDB_NOT_COMMITTED && injected.error.code == MDB_IO &&
           mdb_healthy(db));
    mdb_result_free(&injected);
    unsetenv("MDB_IO_FAIL");
    assert(scalar(a, "SELECT COUNT(*) FROM characters WHERE name='fail'") == 0);
    int64_t before = insert(a, "before-restart", 77);
    mdb_session_close(a);
    mdb_session_close(b);
    mdb_close(db);
    db = mdb_open(dir, &e);
    if (!db) {
        fprintf(stderr, "reopen: %s\n", e.message);
        return 1;
    }
    a = mdb_session_open(db, &e);
    assert(scalar(a, "SELECT COUNT(*) FROM characters") == 3);
    assert(insert(a, "after-restart", 88) > before);
    ok(a, "BEGIN");
    ok(a, "UPDATE characters SET name='a,b' WHERE name='saved'");
    ok(a, "ROLLBACK");
    assert(scalar(a, "SELECT COUNT(*) FROM characters WHERE name='saved'") == 1);
    mdb_session_close(a);
    mdb_close(db);
    char command[600];
    snprintf(command, sizeof command, "rm -rf %s", dir);
    assert(system(command) == 0);
    io_failures();
    limits();
    partial_writes();
#ifdef MDB_TESTING
    scan_snapshot_across_cache_boundary();
#endif
    puts("PASS engine: binary slots, CSV import/int64, snapshots, own writes, rollback, conflict, atomic "
         "tables, ID reservations, concurrent IDs, restart, process lock");
    return 0;
}
