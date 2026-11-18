#define _DARWIN_C_SOURCE 1
#include "mdb.h"
#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_THREADS 8
#define ORDERS 2000

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    int ready, started;
} start_gate;

typedef struct {
    mdb_db *db;
    start_gate *gate;
    int row_id, orders, pessimistic, max_attempts;
    int worker_id, detail_count;
    unsigned long long detail_inserts;
    double cpu_ms;
    int committed, conflicts, timeouts;
    double success_us[ORDERS], attempt_us[ORDERS];
} worker;

static double clock_seconds(clockid_t clock_id) {
    struct timespec ts;
    assert(!clock_gettime(clock_id, &ts));
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static double now(void) { return clock_seconds(CLOCK_MONOTONIC); }

static mdb_code query(mdb_session *session, const char *sql, int64_t *value) {
    mdb_result result;
    int rc = mdb_execute(session, sql, &result);
    mdb_code code = rc ? result.error.code : MDB_OK;
    if (rc && code != MDB_CONFLICT && code != MDB_BUSY) {
        fprintf(stderr, "%s: %s (%s)\n", sql, result.error.message, mdb_code_name(code));
        abort();
    }
    if (!rc && value) {
        assert(result.nrows == 1 && result.ncolumns == 1);
        *value = result.rows[0].values[0].i;
    }
    if (!rc && !strcmp(sql, "COMMIT")) assert(result.commit == MDB_DURABLE);
    mdb_result_free(&result);
    return code;
}

static mdb_code submit_order(mdb_session *session, const char *select,
                             const char *insert, worker *w, int order_id) {
    char update[128];
    int64_t stock;
    assert(query(session, "BEGIN", NULL) == MDB_OK);
    mdb_code code = query(session, select, &stock);
    if (code == MDB_OK) {
        assert(stock > 0);
        snprintf(update, sizeof update, "UPDATE inventory SET stock=%" PRId64 " WHERE id=%d",
                 stock - 1, w->row_id);
        code = query(session, update, NULL);
    }
    if (code == MDB_OK) code = query(session, insert, NULL);
    for (int i = 1; code == MDB_OK && i <= w->detail_count; i++) {
        char line[144];
        snprintf(line, sizeof line, "INSERT INTO order_lines VALUES (%d,%d)", order_id, i);
        code = query(session, line, NULL);
        if (code == MDB_OK) w->detail_inserts++;
    }
    if (code == MDB_OK) code = query(session, "COMMIT", NULL);
    return code;
}

static void *run_worker(void *arg) {
    worker *w = arg;
    mdb_error error;
    mdb_session *session = mdb_session_open(w->db, &error);
    assert(session);
    char select[128], insert[128];
    snprintf(select, sizeof select, "SELECT stock FROM inventory WHERE id=%d%s",
             w->row_id, w->pessimistic ? " FOR UPDATE" : "");
    snprintf(insert, sizeof insert, "INSERT INTO orders VALUES (%d)", w->row_id);
    pthread_mutex_lock(&w->gate->mutex);
    w->gate->ready++;
    pthread_cond_broadcast(&w->gate->changed);
    while (!w->gate->started) pthread_cond_wait(&w->gate->changed, &w->gate->mutex);
    pthread_mutex_unlock(&w->gate->mutex);

    double cpu_started = clock_seconds(CLOCK_THREAD_CPUTIME_ID);
    for (int i = 0; i < w->orders; i++) {
        double started = now();
        for (int attempt = 0; attempt < w->max_attempts; attempt++) {
            mdb_code code = submit_order(session, select, insert, w, w->worker_id * w->orders + i + 1);
            if (code == MDB_OK) {
                w->success_us[w->committed++] = (now() - started) * 1e6;
                break;
            }
            if (code == MDB_CONFLICT) w->conflicts++;
            else w->timeouts++;
            assert(query(session, "ROLLBACK", NULL) == MDB_OK);
        }
        w->attempt_us[i] = (now() - started) * 1e6;
    }
    w->cpu_ms = (clock_seconds(CLOCK_THREAD_CPUTIME_ID) - cpu_started) * 1000.0;
    mdb_session_close(session);
    return NULL;
}

static int compare(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static double percentile(const double *values, int count, int percent) {
    assert(count > 0);
    return values[(count * percent + 99) / 100 - 1];
}

int main(int argc, char **argv) {
    assert(argc == 5 || argc == 6);
    int detail_count = argc == 6 ? atoi(argv[5]) : 0;
    assert(detail_count >= 0 && detail_count <= 4096);
    int order_count = detail_count ? 256 : ORDERS;
    int max_attempts = atoi(argv[4]);
    assert(max_attempts > 0 && max_attempts <= 2000);
    int pessimistic = !strcmp(argv[2], "pessimistic");
    int threads = !strcmp(argv[3], "single") ? 1 : MAX_THREADS;
    int disjoint = !strcmp(argv[3], "disjoint");
    mdb_error error;
    mdb_db *db = mdb_open(argv[1], &error);
    if (!db) {
        fprintf(stderr, "open: %s\n", error.message);
        return EXIT_FAILURE;
    }
    static worker workers[MAX_THREADS];
    pthread_t tids[MAX_THREADS];
    start_gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
    for (int i = 0; i < threads; i++) {
        worker *w = &workers[i];
        w->db = db;
        w->gate = &gate;
        w->row_id = disjoint ? i + 1 : 1;
        w->orders = order_count / threads;
        w->worker_id = i;
        w->detail_count = detail_count;
        w->max_attempts = max_attempts;
        w->pessimistic = pessimistic;
        assert(!pthread_create(&tids[i], NULL, run_worker, w));
    }
    pthread_mutex_lock(&gate.mutex);
    while (gate.ready < threads) pthread_cond_wait(&gate.changed, &gate.mutex);
    double started = now();
    gate.started = 1;
    pthread_cond_broadcast(&gate.changed);
    pthread_mutex_unlock(&gate.mutex);
    for (int i = 0; i < threads; i++) assert(!pthread_join(tids[i], NULL));
    double seconds = now() - started;
    int counts[MAX_THREADS] = {0}, committed = 0, conflicts = 0, timeouts = 0;
    unsigned long long detail_inserts = 0;
    double cpu_ms = 0;
    double success_us[ORDERS], attempt_us[ORDERS];
    for (int i = 0; i < threads; i++) {
        const worker *w = &workers[i];
        counts[w->row_id - 1] += w->committed;
        memcpy(success_us + committed, w->success_us, w->committed * sizeof(double));
        memcpy(attempt_us + i * w->orders, w->attempt_us, w->orders * sizeof(double));
        committed += w->committed;
        conflicts += w->conflicts;
        timeouts += w->timeouts;
        detail_inserts += w->detail_inserts;
        cpu_ms += w->cpu_ms;
    }
    assert(committed > 0 && committed <= order_count);
    if (max_attempts == 1) assert(committed + conflicts + timeouts == order_count);
    if (detail_count) assert(committed == order_count);
    mdb_close(db);
    db = mdb_open(argv[1], &error);
    assert(db);
    mdb_session *check = mdb_session_open(db, &error);
    assert(check);
    int64_t actual;
    assert(query(check, "SELECT COUNT(*) FROM orders", &actual) == MDB_OK && actual == committed);
    for (int row = 1; row <= MAX_THREADS; row++) {
        char sql[128];
        snprintf(sql, sizeof sql, "SELECT stock FROM inventory WHERE id=%d", row);
        assert(query(check, sql, &actual) == MDB_OK && actual == 10000 - counts[row - 1]);
        snprintf(sql, sizeof sql, "SELECT COUNT(*) FROM orders WHERE inventory_id=%d", row);
        assert(query(check, sql, &actual) == MDB_OK && actual == counts[row - 1]);
    }
    if (detail_count)
        assert(query(check, "SELECT COUNT(*) FROM order_lines", &actual) == MDB_OK &&
               actual == (int64_t)committed * detail_count);
    mdb_session_close(check);
    mdb_close(db);
    pthread_cond_destroy(&gate.changed);
    pthread_mutex_destroy(&gate.mutex);
    qsort(success_us, committed, sizeof(double), compare);
    qsort(attempt_us, order_count, sizeof(double), compare);
    printf("{\"orders\":%d,\"attempts\":%d,\"committed\":%d,\"conflicts\":%d,\"timeouts\":%d,"
           "\"elapsed_ms\":%.3f,\"commits_per_sec\":%.3f,\"commit_p50_ms\":%.6f,"
           "\"commit_p95_ms\":%.6f,\"order_p95_ms\":%.6f,\"cpu_ms\":%.6f,\"replayed_detail_inserts\":%llu}\n",
           order_count, committed + conflicts + timeouts, committed, conflicts, timeouts, seconds * 1000, committed / seconds,
           percentile(success_us, committed, 50) / 1000, percentile(success_us, committed, 95) / 1000,
           percentile(attempt_us, order_count, 95) / 1000, cpu_ms,
           detail_inserts - (unsigned long long)committed * detail_count);
}
