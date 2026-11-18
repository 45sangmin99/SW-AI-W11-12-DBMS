#define _DARWIN_C_SOURCE 1
#include "mdb.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    int waiting;
} barrier;
typedef struct {
    mdb_session *session;
    const char *sql;
    mdb_result result;
    int rc;
} job;
static void observed_wait(void *context) {
    barrier *b = context;
    pthread_mutex_lock(&b->mutex);
    b->waiting++;
    pthread_cond_signal(&b->changed);
    pthread_mutex_unlock(&b->mutex);
}
static void *execute(void *context) {
    job *j = context;
    j->rc = mdb_execute(j->session, j->sql, &j->result);
    return NULL;
}
static void ok(mdb_session *s, const char *sql) {
    mdb_result r;
    int rc = mdb_execute(s, sql, &r);
    if (rc) fprintf(stderr, "%s: %s\n", sql, r.error.message);
    assert(!rc);
    mdb_result_free(&r);
}
static void await_wait(barrier *b) {
    struct timespec until;
    clock_gettime(CLOCK_REALTIME, &until); until.tv_sec += 5;
    pthread_mutex_lock(&b->mutex);
    while (!b->waiting) assert(!pthread_cond_timedwait(&b->changed, &b->mutex, &until));
    b->waiting = 0;
    pthread_mutex_unlock(&b->mutex);
}
int main(void) {
    char path[] = "/tmp/mdb-locking-XXXXXX", file[256];
    assert(mkdtemp(path));
    snprintf(file, sizeof file, "%s/t.schema", path);
    FILE *f = fopen(file, "w"); assert(f); fputs("id INT AUTO\nvalue INT\n", f); fclose(f);
    snprintf(file, sizeof file, "%s/t.csv", path);
    f = fopen(file, "w"); assert(f); fputs("1,10\n2,20\n", f); fclose(f);
    mdb_error e;
    mdb_db *db = mdb_open(path, &e); assert(db);
    mdb_session *a = mdb_session_open(db, &e), *b = mdb_session_open(db, &e);
    assert(a && b);
    barrier sync = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0};
    mdb_test_lock_hook(db, observed_wait, &sync);
    ok(a, "BEGIN"); ok(b, "BEGIN");
    ok(a, "SELECT * FROM t WHERE id=1 FOR UPDATE");
    job j = {b, "SELECT value FROM t WHERE id=1 FOR UPDATE", {0}, -1};
    pthread_t thread;
    assert(!pthread_create(&thread, NULL, execute, &j));
    await_wait(&sync); /* Engine reached conflicting-lock path, before release. */
    ok(a, "UPDATE t SET value=11 WHERE id=1");
    ok(a, "COMMIT");
    pthread_join(thread, NULL);
    assert(!j.rc && j.result.nrows == 1 && j.result.rows[0].values[0].i == 11);
    mdb_result_free(&j.result); ok(b, "ROLLBACK");
    /* Missing-row lock must also block the automatic ID allocated by INSERT. */
    ok(a, "BEGIN"); ok(b, "BEGIN");
    ok(a, "SELECT * FROM t WHERE id=3 FOR UPDATE");
    j = (job){b, "INSERT INTO t VALUES (30)", {0}, -1};
    assert(!pthread_create(&thread, NULL, execute, &j));
    await_wait(&sync);
    mdb_session_close(a); /* Session close releases absent-key locks too. */
    pthread_join(thread, NULL);
    assert(!j.rc && j.result.inserted_id == 3);
    mdb_result_free(&j.result); ok(b, "COMMIT");
    a = mdb_session_open(db, &e); assert(a);
    /* Cross-row deadlock ends through bounded timeout, then ROLLBACK wakes peer. */
    ok(a, "BEGIN"); ok(b, "BEGIN");
    ok(a, "UPDATE t SET value=12 WHERE id=1");
    ok(b, "UPDATE t SET value=22 WHERE id=2");
    j = (job){b, "UPDATE t SET value=13 WHERE id=1", {0}, -1};
    assert(!pthread_create(&thread, NULL, execute, &j));
    await_wait(&sync);
    mdb_result r;
    assert(mdb_execute(a, "UPDATE t SET value=23 WHERE id=2", &r));
    assert(r.error.code == MDB_BUSY); mdb_result_free(&r);
    pthread_join(thread, NULL);
    assert(j.rc && j.result.error.code == MDB_BUSY); mdb_result_free(&j.result);
    ok(a, "ROLLBACK"); ok(b, "ROLLBACK");
    mdb_test_lock_hook(db, NULL, NULL);
    mdb_session_close(a); mdb_session_close(b); mdb_close(db);
    pthread_cond_destroy(&sync.changed); pthread_mutex_destroy(&sync.mutex);
    /* mkdtemp supplies a fixed-prefix path without shell metacharacters. */
    char command[320]; snprintf(command, sizeof command, "rm -rf %s", path);
    assert(system(command) == 0);
    puts("PASS locking: observed waits, latest read after commit, absent-key insert, session close, bounded deadlock and rollback");
}
