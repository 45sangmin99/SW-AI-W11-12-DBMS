#define _DARWIN_C_SOURCE 1
#include "mdb.h"
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    mdb_session *session;
    int row, pessimistic;
    int waiting, staged, may_commit;
    int64_t stock;
    mdb_code commit;
    int64_t started_ns, finished_ns;
} schedule;

static mdb_code execute(mdb_session *session, const char *sql, int64_t *value) {
    mdb_result result;
    int rc = mdb_execute(session, sql, &result);
    mdb_code code = rc ? result.error.code : MDB_OK;
    if (rc && code != MDB_CONFLICT) fprintf(stderr, "%s: %s\n", sql, result.error.message);
    if (!rc && value) {
        assert(result.nrows == 1 && result.ncolumns == 1);
        *value = result.rows[0].values[0].i;
    }
    if (!rc && !strcmp(sql, "COMMIT")) assert(result.commit == MDB_DURABLE);
    mdb_result_free(&result);
    return code;
}

static int64_t stage_order(mdb_session *session, int row, int pessimistic) {
    char sql[128];
    int64_t stock;
    snprintf(sql, sizeof sql, "SELECT stock FROM inventory WHERE id=%d%s",
             row, pessimistic ? " FOR UPDATE" : "");
    assert(execute(session, sql, &stock) == MDB_OK && stock > 0);
    snprintf(sql, sizeof sql, "UPDATE inventory SET stock=%" PRId64 " WHERE id=%d", stock - 1, row);
    assert(execute(session, sql, NULL) == MDB_OK);
    snprintf(sql, sizeof sql, "INSERT INTO orders VALUES (%d)", row);
    assert(execute(session, sql, NULL) == MDB_OK);
    return stock;
}

#ifdef BENCH_PESSIMISTIC
static void entered_lock_wait(void *context) {
    schedule *s = context;
    pthread_mutex_lock(&s->mutex);
    s->waiting = 1;
    pthread_cond_signal(&s->changed);
    pthread_mutex_unlock(&s->mutex);
}
#endif

static int64_t monotonic_ns(void) {
    struct timespec ts;
    assert(!clock_gettime(CLOCK_MONOTONIC, &ts));
    return (int64_t)ts.tv_sec * INT64_C(1000000000) + ts.tv_nsec;
}

static void *second_order(void *context) {
    schedule *s = context;
    pthread_mutex_lock(&s->mutex);
    s->started_ns = monotonic_ns();
    pthread_cond_signal(&s->changed);
    pthread_mutex_unlock(&s->mutex);
    s->stock = stage_order(s->session, s->row, s->pessimistic);
    pthread_mutex_lock(&s->mutex);
    s->staged = 1;
    pthread_cond_signal(&s->changed);
    while (!s->may_commit) pthread_cond_wait(&s->changed, &s->mutex);
    pthread_mutex_unlock(&s->mutex);
    s->commit = execute(s->session, "COMMIT", NULL);
    s->finished_ns = monotonic_ns();
    if (s->commit == MDB_CONFLICT) assert(execute(s->session, "ROLLBACK", NULL) == MDB_OK);
    return NULL;
}

static int64_t scalar(mdb_session *session, const char *sql) {
    int64_t value;
    assert(execute(session, sql, &value) == MDB_OK);
    return value;
}

/* The first order is canceled at a fixed deadline from the second order's start.
 * The second order is free to commit as soon as its SQL finishes. */
static void measure_cancellation(mdb_session *a, schedule *order, int hold_ms) {
    order->may_commit = 1;
    pthread_t worker;
    assert(!pthread_create(&worker, NULL, second_order, order));
    pthread_mutex_lock(&order->mutex);
    while (!order->started_ns) pthread_cond_wait(&order->changed, &order->mutex);
    int64_t target = order->started_ns + (int64_t)hold_ms * 1000000;
    pthread_mutex_unlock(&order->mutex);
    for (int64_t remaining; (remaining = target - monotonic_ns()) > 0;) {
        struct timespec sleep = {remaining / 1000000000, remaining % 1000000000};
        if (nanosleep(&sleep, NULL)) assert(errno == EINTR);
    }
    int64_t rollback_started = monotonic_ns();
    assert(execute(a, "ROLLBACK", NULL) == MDB_OK);
    assert(!pthread_join(worker, NULL));
    assert(order->commit == MDB_OK && order->stock == 2);
    assert(scalar(a, "SELECT COUNT(*) FROM orders") == 1);
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=1") == (order->row == 1 ? 1 : 2));
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=2") == (order->row == 2 ? 1 : 2));
    if (order->pessimistic && order->row == 1) {
        assert(order->waiting && order->finished_ns >= rollback_started);
    } else assert(!order->waiting);
    printf("{\"order_ms\":%.6f,\"cancel_after_ms\":%.6f,\"completed_before_cancel\":%s,\"committed_orders\":1}\n",
           (order->finished_ns - order->started_ns) / 1e6,
           (rollback_started - order->started_ns) / 1e6,
           order->finished_ns < rollback_started ? "true" : "false");
}

static void measure_pairs(mdb_db *db, mdb_session *a, mdb_session *b,
                          int pessimistic, int global) {
    enum { pairs = 2000, starting_stock = 10000 };
    int committed = 0, conflicts = 0;
    int64_t start = monotonic_ns();
    for (int i = 0; i < pairs; i++) {
        assert(execute(a, "BEGIN", NULL) == MDB_OK);
        assert(execute(b, "BEGIN", NULL) == MDB_OK);
        assert(stage_order(a, 1, pessimistic) == starting_stock - i);
        assert(stage_order(b, 2, pessimistic) == starting_stock - (global ? 0 : i));
        assert(execute(a, "COMMIT", NULL) == MDB_OK);
        committed++;
        mdb_code result = execute(b, "COMMIT", NULL);
        assert(result == (global ? MDB_CONFLICT : MDB_OK));
        if (result == MDB_OK) committed++;
        else {
            conflicts++;
            assert(execute(b, "ROLLBACK", NULL) == MDB_OK);
        }
    }
    double elapsed_ms = (monotonic_ns() - start) / 1e6;
    assert(mdb_healthy(db));
    assert(scalar(a, "SELECT COUNT(*) FROM orders WHERE inventory_id=1") == pairs);
    assert(scalar(a, "SELECT COUNT(*) FROM orders WHERE inventory_id=2") == (global ? 0 : pairs));
    assert(scalar(a, "SELECT COUNT(*) FROM orders") == committed);
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=1") == starting_stock - pairs);
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=2") == starting_stock - (global ? 0 : pairs));
    printf("{\"pairs\":%d,\"attempts\":%d,\"committed\":%d,\"conflicts\":%d,"
           "\"elapsed_ms\":%.6f,\"commits_per_sec\":%.3f}\n",
           pairs, pairs * 2, committed, conflicts, elapsed_ms, committed * 1000.0 / elapsed_ms);
}

static void measure_batch(mdb_session *a, mdb_session *b, int pessimistic, int width) {
    enum { rows_per_session = 12288 };
    assert(width > 0 && rows_per_session % width == 0);
    int pairs = rows_per_session / width;
    int64_t preparation_ns = 0, commit_ns = 0;
    int64_t started = monotonic_ns();
    for (int i = 0; i < pairs; i++) {
        int64_t preparation_started = monotonic_ns();
        assert(execute(a, "BEGIN", NULL) == MDB_OK);
        assert(execute(b, "BEGIN", NULL) == MDB_OK);
        for (int j = 0; j < width; j++) {
            assert(stage_order(a, j + 1, pessimistic) == 10000 - i);
            assert(stage_order(b, width + j + 1, pessimistic) == 10000 - i);
        }
        int64_t commit_started = monotonic_ns();
        preparation_ns += commit_started - preparation_started;
        assert(execute(a, "COMMIT", NULL) == MDB_OK);
        assert(execute(b, "COMMIT", NULL) == MDB_OK);
        commit_ns += monotonic_ns() - commit_started;
    }
    double elapsed_ms = (monotonic_ns() - started) / 1e6;
    assert(scalar(a, "SELECT COUNT(*) FROM orders") == rows_per_session * 2);
    char sql[128];
    for (int i = 1; i <= width * 2; i++) {
        snprintf(sql, sizeof sql, "SELECT stock FROM inventory WHERE id=%d", i);
        assert(scalar(a, sql) == 10000 - pairs);
    }
    printf("{\"orders\":%d,\"commits\":%d,\"prepare_ms\":%.6f,\"commit_ms\":%.6f,\"elapsed_ms\":%.6f}\n",
           rows_per_session * 2, pairs * 2, preparation_ns / 1e6, commit_ns / 1e6, elapsed_ms);
}

typedef struct {
    schedule ready;
    int detail_count, conflicts;
    int64_t first_stock, second_cpu_ns;
} contention;

static int64_t thread_cpu_ns(void) {
    struct timespec ts;
    assert(!clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts));
    return (int64_t)ts.tv_sec * INT64_C(1000000000) + ts.tv_nsec;
}

static int64_t read_stock(mdb_session *session, int pessimistic) {
    return scalar(session, pessimistic ? "SELECT stock FROM inventory WHERE id=1 FOR UPDATE"
                                      : "SELECT stock FROM inventory WHERE id=1");
}

static void prepare_purchase(mdb_session *session, int64_t stock, int request, int detail_count) {
    char sql[128];
    assert(stock > 0);
    snprintf(sql, sizeof sql, "UPDATE inventory SET stock=%" PRId64 " WHERE id=1", stock - 1);
    assert(execute(session, sql, NULL) == MDB_OK);
    assert(execute(session, "INSERT INTO orders VALUES (1)", NULL) == MDB_OK);
    for (int i = 1; i <= detail_count; i++) {
        snprintf(sql, sizeof sql, "INSERT INTO order_lines VALUES (%d,%d)", request, i);
        assert(execute(session, sql, NULL) == MDB_OK);
    }
}

static void *competing_purchase(void *context) {
    contention *trial = context;
    schedule *s = &trial->ready;
    int64_t cpu_start = thread_cpu_ns();
    s->started_ns = monotonic_ns();
    assert(execute(s->session, "BEGIN", NULL) == MDB_OK);
    trial->first_stock = read_stock(s->session, s->pessimistic);
    pthread_mutex_lock(&s->mutex);
    s->staged = 1; /* The read finished. Work on both orders may now overlap. */
    pthread_cond_signal(&s->changed);
    pthread_mutex_unlock(&s->mutex);
    prepare_purchase(s->session, trial->first_stock, 2, trial->detail_count);
    pthread_mutex_lock(&s->mutex);
    while (!s->may_commit) pthread_cond_wait(&s->changed, &s->mutex);
    pthread_mutex_unlock(&s->mutex);
    mdb_code result = execute(s->session, "COMMIT", NULL);
    assert(result == (s->pessimistic ? MDB_OK : MDB_CONFLICT));
    if (result == MDB_CONFLICT) {
        trial->conflicts++;
        assert(execute(s->session, "ROLLBACK", NULL) == MDB_OK);
        assert(execute(s->session, "BEGIN", NULL) == MDB_OK);
        int64_t stock = read_stock(s->session, 0);
        assert(stock == 1);
        prepare_purchase(s->session, stock, 2, trial->detail_count);
        assert(execute(s->session, "COMMIT", NULL) == MDB_OK);
    }
    s->finished_ns = monotonic_ns();
    trial->second_cpu_ns = thread_cpu_ns() - cpu_start;
    return NULL;
}

static void measure_contention(mdb_db *db, mdb_session *a, mdb_session *b,
                               int pessimistic, int detail_count) {
    assert(detail_count > 0 && detail_count <= 4096);
    contention trial = {.ready = {.mutex = PTHREAD_MUTEX_INITIALIZER,
                                  .changed = PTHREAD_COND_INITIALIZER,
                                  .session = b, .pessimistic = pessimistic}, .detail_count = detail_count};
    schedule *s = &trial.ready;
#ifdef BENCH_PESSIMISTIC
    mdb_test_lock_hook(db, entered_lock_wait, s);
#else
    (void)db;
#endif
    int64_t cpu_start = thread_cpu_ns();
    int64_t start = monotonic_ns();
    assert(execute(a, "BEGIN", NULL) == MDB_OK);
    int64_t stock = read_stock(a, pessimistic);
    assert(stock == 2);
    pthread_t worker;
    assert(!pthread_create(&worker, NULL, competing_purchase, &trial));
    struct timespec deadline;
    assert(!clock_gettime(CLOCK_REALTIME, &deadline));
    deadline.tv_sec += 3;
    pthread_mutex_lock(&s->mutex);
    while (!s->staged && !s->waiting)
        assert(!pthread_cond_timedwait(&s->changed, &s->mutex, &deadline));
    assert(s->waiting == pessimistic && s->staged == !pessimistic);
    pthread_mutex_unlock(&s->mutex);
    prepare_purchase(a, stock, 1, detail_count);
    assert(execute(a, "COMMIT", NULL) == MDB_OK);
    pthread_mutex_lock(&s->mutex);
    s->may_commit = 1;
    pthread_cond_signal(&s->changed);
    pthread_mutex_unlock(&s->mutex);
    assert(!pthread_join(worker, NULL));
    double cpu_ms = (thread_cpu_ns() - cpu_start + trial.second_cpu_ns) / 1e6;
#ifdef BENCH_PESSIMISTIC
    mdb_test_lock_hook(db, NULL, NULL);
#endif
    assert(trial.first_stock == (pessimistic ? 1 : 2));
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=1") == 0);
    assert(scalar(a, "SELECT COUNT(*) FROM orders") == 2);
    assert(scalar(a, "SELECT COUNT(*) FROM order_lines WHERE request_id=1") == detail_count);
    assert(scalar(a, "SELECT COUNT(*) FROM order_lines WHERE request_id=2") == detail_count);
    assert(scalar(a, "SELECT COUNT(*) FROM order_lines") == 2 * detail_count);
    printf("{\"pair_ms\":%.6f,\"second_ms\":%.6f,\"cpu_ms\":%.6f,\"committed\":2,\"conflicts\":%d,\"replayed_detail_inserts\":%d}\n",
           (s->finished_ns - start) / 1e6, (s->finished_ns - s->started_ns) / 1e6, cpu_ms,
           trial.conflicts, trial.conflicts * detail_count);
    pthread_cond_destroy(&s->changed);
    pthread_mutex_destroy(&s->mutex);
}

int main(int argc, char **argv) {
    assert(argc == 4 || argc == 5);
    int pessimistic = !strcmp(argv[2], "pessimistic");
    int global = !strcmp(argv[2], "occ_global");
    int same_row = !strcmp(argv[3], "hot");
    mdb_error error;
    mdb_db *db = mdb_open(argv[1], &error);
    assert(db);
    mdb_session *a = mdb_session_open(db, &error), *b = mdb_session_open(db, &error);
    assert(a && b);
    if (!strcmp(argv[3], "contention")) {
        assert(argc == 5);
        measure_contention(db, a, b, pessimistic, atoi(argv[4]));
        mdb_session_close(a);
        mdb_session_close(b);
        mdb_close(db);
        return 0;
    }
    if (!strcmp(argv[3], "batch")) {
        assert(argc == 5);
        measure_batch(a, b, pessimistic, atoi(argv[4]));
        mdb_session_close(a);
        mdb_session_close(b);
        mdb_close(db);
        return 0;
    }
    if (!strcmp(argv[3], "paired")) {
        measure_pairs(db, a, b, pessimistic, global);
        mdb_session_close(a);
        mdb_session_close(b);
        mdb_close(db);
        return 0;
    }
    assert(execute(a, "BEGIN", NULL) == MDB_OK);
    assert(execute(b, "BEGIN", NULL) == MDB_OK);
    assert(stage_order(a, 1, pessimistic) == 2);

    schedule order = {.mutex = PTHREAD_MUTEX_INITIALIZER, .changed = PTHREAD_COND_INITIALIZER,
                      .session = b, .row = same_row ? 1 : 2, .pessimistic = pessimistic};
#ifdef BENCH_PESSIMISTIC
    mdb_test_lock_hook(db, entered_lock_wait, &order);
#endif
    if (argc == 5) {
        int hold_ms = atoi(argv[4]);
        assert(hold_ms >= 10 && hold_ms <= 100);
        measure_cancellation(a, &order, hold_ms);
        goto done;
    }
    pthread_t worker;
    assert(!pthread_create(&worker, NULL, second_order, &order));
    struct timespec deadline;
    assert(!clock_gettime(CLOCK_REALTIME, &deadline));
    deadline.tv_sec += 3;
    pthread_mutex_lock(&order.mutex);
    while (!order.waiting && !order.staged)
        assert(!pthread_cond_timedwait(&order.changed, &order.mutex, &deadline));
    int waited_before_commit = order.waiting;
    assert(waited_before_commit == (pessimistic && same_row));
    assert(order.staged == !waited_before_commit);
    pthread_mutex_unlock(&order.mutex);

    assert(execute(a, "COMMIT", NULL) == MDB_OK);
    pthread_mutex_lock(&order.mutex);
    order.may_commit = 1;
    pthread_cond_signal(&order.changed);
    pthread_mutex_unlock(&order.mutex);
    assert(!pthread_join(worker, NULL));
    assert(order.stock == (waited_before_commit ? 1 : 2));
    int conflict = global || (same_row && !pessimistic);
    assert(order.commit == (conflict ? MDB_CONFLICT : MDB_OK));
    int64_t initial_orders = scalar(a, "SELECT COUNT(*) FROM orders");
    int64_t stock_before_retry = scalar(a, "SELECT stock FROM inventory WHERE id=1");
    assert(initial_orders == (conflict ? 1 : 2));
    assert(stock_before_retry == (same_row && !conflict ? 0 : 1));
    int64_t retry_stock = -1;
    if (conflict) {
        assert(execute(b, "BEGIN", NULL) == MDB_OK);
        retry_stock = stage_order(b, order.row, pessimistic);
        assert(retry_stock == (same_row ? 1 : 2));
        assert(execute(b, "COMMIT", NULL) == MDB_OK);
    }
    assert(scalar(a, "SELECT COUNT(*) FROM orders") == 2);
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=1") == (same_row ? 0 : 1));
    assert(scalar(a, "SELECT stock FROM inventory WHERE id=2") == (same_row ? 2 : 1));
    printf("{\"before_a_commit\":\"%s\",\"b_read\":%" PRId64 ",\"b_commit\":\"%s\","
           "\"orders_before_retry\":%" PRId64 ",\"row1_stock_before_retry\":%" PRId64 ",\"retry_read\":",
           waited_before_commit ? "lock_wait" : "order_staged", order.stock,
           conflict ? "conflict" : "durable", initial_orders, stock_before_retry);
    if (conflict) printf("%" PRId64, retry_stock);
    else printf("null");
    puts(",\"final_orders\":2}");
done:
#ifdef BENCH_PESSIMISTIC
    mdb_test_lock_hook(db, NULL, NULL);
#endif
    mdb_session_close(a);
    mdb_session_close(b);
    mdb_close(db);
    pthread_cond_destroy(&order.changed);
    pthread_mutex_destroy(&order.mutex);
}
