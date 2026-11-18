#define _DARWIN_C_SOURCE 1
#include "bpt.h"
#include "binary.h"
#include "group.h"
#include "slots.h"
#include "mdb.h"
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* Snapshot references and row-store COW mutations are protected by db->mutex.
 * Rows in published snapshots are immutable. Binary slots and the shared B+ tree
 * are protected by tablefile.lock; readers never take db->mutex while holding it.
 * Historical views read retained row versions when the backing file has advanced. */
#define RECORD_MAX (1024U * 1024U)
#define BINARY_RECORD_MAX (MDB_BIN_SLOT_HEADER + MDB_COL_MAX * (4U + MDB_TEXT_MAX))
#define BINARY_READ_BUFFER 65536U
#define CSV_RECORD_MAX (MDB_COL_MAX * (MDB_TEXT_MAX * 2U + 40U))
#define RESERVE_BLOCK 4096U
#ifndef MDB_WAL_BUFFER_SIZE
#define MDB_WAL_BUFFER_SIZE 65536U
#endif
_Static_assert(MDB_WAL_BUFFER_SIZE > 0, "WAL buffer must not be empty");
#define WAL_HEADER "MDBWAL1\n"
#define WAL_MAGIC 0x314c574dU
#define LOG_RESERVE 1U
#define LOG_TXIDS 2U
#define LOG_BEGIN 3U
#define LOG_ROW 4U
#define LOG_COMMIT 5U
#define LOG_APPLIED 6U
#define LOG_CREATE 7U
#define LOG_UNIQUE 8U
#define UNIQUE_MAX 128U
#define FNV_INIT UINT64_C(1469598103934665603)
#define FNV_PRIME UINT64_C(1099511628211)

typedef struct {
    int used;
    int64_t key;
    size_t value;
} bucket;
typedef struct {
    bucket *b;
    size_t cap, n;
} idmap;
typedef struct row {
    size_t refs;
    mdb_value v[];
} row;
typedef struct {
    char name[MDB_NAME_MAX + 1];
    mdb_type type;
    size_t max;
} column;
typedef struct {
    char name[MDB_NAME_MAX + 1];
    column cols[MDB_COL_MAX];
    size_t ncols;
    uint64_t schemahash, basehash, next_id, reserved;
    /* Set only on a statement-local schema for SELECT aliases and JOIN. */
    const char *source_names[2];
    size_t right_start;
} tabledef;
typedef struct {
    size_t refs, record_size;
    int fd, lock_ready;
    uint64_t version;
    mdb_bpt *index;
    pthread_rwlock_t lock;
} tablefile;
typedef struct view {
    size_t refs, n;
    uint64_t version;
    tablefile *file;
    mdb_slots *rows;
} view;
typedef struct snapshot {
    size_t refs;
    uint64_t version;
    view **tables;
} snapshot;
typedef struct {
    int64_t id;
    row *value;
    int deleted, inserted;
    size_t slot; /* Assigned during commit preparation, never derived from ID. */
} change;
typedef struct {
    change *v;
    size_t n, cap;
    idmap map;
} overlay;
typedef struct transaction {
    snapshot *snap;
    overlay *tables;
    uint64_t id;
    size_t nchanges, nlocks;
    int failed, writes;
} transaction;
typedef struct {
    const mdb_value *value; /* Kept alive by the published snapshot. */
    int64_t row_id;
} unique_item;
typedef struct {
    char name[MDB_NAME_MAX + 1];
    size_t table, column;
    unique_item *items;
    size_t nitems;
} unique_index;
typedef struct row_lock {
    struct row_lock *next, *hash_next;
    transaction *owner;
    size_t table;
    int64_t id;
    int whole; /* Whole-table lock conflicts with every writer/locking reader. */
} row_lock;
struct mdb_db {
    pthread_mutex_t mutex;
    pthread_cond_t lock_changed;
    int cond_ready;
    row_lock *locks, **lock_buckets;
    size_t nlocks, lock_capacity;
    int mutex_ready, dirfd, basefd, lockfd, walfd, healthy;
    tabledef *defs;
    size_t ntables, base_tables, active_transactions;
    unique_index unique[UNIQUE_MAX];
    size_t nunique;
    snapshot *current;
    uint64_t txnext, txreserved;
    uint64_t *committed;
    size_t ncommitted, capcommitted, nsessions, max_sessions, max_changes, max_sort_rows, max_groups, max_locks;
    int recovering;
    mdb_metrics metrics;
    /* Owned by the directory owner and serialized by db->mutex. Never flushed
     * by close: every successful durable boundary must flush explicitly. */
    size_t wal_used;
    unsigned char wal_buffer[MDB_WAL_BUFFER_SIZE];
#ifdef MDB_TESTING
    void (*lock_hook)(void *);
    void *lock_hook_context;
    void (*read_hook)(void *);
    void *read_hook_context;
    void (*scan_hook)(void *);
    void *scan_hook_context;
#endif
};
struct mdb_session {
    mdb_db *db;
    transaction *tx;
};
typedef struct {
    int fd;
    uint64_t offset;
    unsigned char buf[8192];
    size_t pos, len;
} csv_reader;
typedef struct {
    row **v;
    size_t n, cap;
    idmap map;
} rowset;

static double seconds(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts))
        return 0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
static int fail(mdb_error *e, mdb_code c, const char *m) {
    if (e) {
        e->code = c;
        e->offset = 0;
        (void)snprintf(e->message, sizeof e->message, "%s", m);
    }
    return -1;
}
static uint64_t hashbytes(uint64_t h, const void *p, size_t n) {
    const unsigned char *b = p;
    for (size_t i = 0; i < n; i++) {
        h ^= b[i];
        h *= FNV_PRIME;
    }
    return h;
}
static uint64_t hashkey(int64_t k) {
    uint64_t x = (uint64_t)k;
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}
static int map_get(const idmap *m, int64_t key, size_t *value) {
    if (!m->cap)
        return 0;
    size_t i = (size_t)hashkey(key) & (m->cap - 1);
    while (m->b[i].used) {
        if (m->b[i].key == key) {
            *value = m->b[i].value;
            return 1;
        }
        i = (i + 1) & (m->cap - 1);
    }
    return 0;
}
static int map_reserve(idmap *m, size_t count) {
    size_t cap = m->cap ? m->cap : 16;
    if (count > SIZE_MAX / 2)
        return -1;
    while (count > cap / 2) {
        if (cap > SIZE_MAX / 2)
            return -1;
        cap *= 2;
    }
    if (cap == m->cap)
        return 0;
    bucket *b = calloc(cap, sizeof *b);
    if (!b)
        return -1;
    for (size_t j = 0; j < m->cap; j++)
        if (m->b[j].used) {
            size_t i = (size_t)hashkey(m->b[j].key) & (cap - 1);
            while (b[i].used)
                i = (i + 1) & (cap - 1);
            b[i] = m->b[j];
        }
    free(m->b);
    m->b = b;
    m->cap = cap;
    return 0;
}
static void map_put(idmap *m, int64_t key, size_t value) {
    size_t i = (size_t)hashkey(key) & (m->cap - 1);
    while (m->b[i].used && m->b[i].key != key)
        i = (i + 1) & (m->cap - 1);
    if (!m->b[i].used) {
        m->b[i].used = 1;
        m->b[i].key = key;
        m->n++;
    }
    m->b[i].value = value;
}
static int grow(void **p, size_t *cap, size_t need, size_t size) {
    if (need <= *cap)
        return 0;
    size_t c = *cap ? *cap : 16;
    while (c < need) {
        if (c > SIZE_MAX / 2)
            return -1;
        c *= 2;
    }
    if (size && c > SIZE_MAX / size)
        return -1;
    void *q = realloc(*p, c * size);
    if (!q)
        return -1;
    *p = q;
    *cap = c;
    return 0;
}
static row *row_new(size_t n) {
    if (n > (SIZE_MAX - sizeof(row)) / sizeof(mdb_value))
        return NULL;
    row *r = calloc(1, sizeof *r + n * sizeof(mdb_value));
    if (r)
        r->refs = 1;
    return r;
}
static void row_drop(row *r, size_t n) {
    if (!r || --r->refs)
        return;
    for (size_t i = 0; i < n; i++)
        mdb_value_clear(&r->v[i]);
    free(r);
}
static row *row_clone(const row *r, size_t n) {
    row *v = row_new(n);
    if (!v)
        return NULL;
    for (size_t i = 0; i < n; i++)
        if (mdb_value_copy(&v->v[i], &r->v[i])) {
            row_drop(v, n);
            return NULL;
        }
    return v;
}
static void overlay_clear(overlay *o, size_t ncols) {
    for (size_t i = 0; i < o->n; i++)
        row_drop(o->v[i].value, ncols);
    free(o->v);
    free(o->map.b);
    memset(o, 0, sizeof *o);
}
static int overlay_set(overlay *o, int64_t id, row *value, int deleted, int inserted,
                       size_t ncols) {
    size_t i;
    if (map_get(&o->map, id, &i)) {
        row_drop(o->v[i].value, ncols);
        o->v[i].value = value;
        o->v[i].deleted = deleted;
        return 0;
    }
    if (grow((void **)&o->v, &o->cap, o->n + 1, sizeof *o->v) || map_reserve(&o->map, o->n + 1))
        return -1;
    i = o->n++;
    o->v[i] = (change){.id = id, .value = value, .deleted = deleted, .inserted = inserted};
    map_put(&o->map, id, i);
    return 0;
}
static void slot_retain(void *value, void *context) {
    (void)context;
    ((row *)value)->refs++;
}
static void slot_release(void *value, void *context) {
    row_drop(value, ((tabledef *)context)->ncols);
}
static void file_drop(tablefile *f) {
    if (!f || --f->refs)
        return;
    if (f->fd >= 0)
        close(f->fd);
    if (f->lock_ready)
        pthread_rwlock_destroy(&f->lock);
    mdb_bpt_free(f->index);
    free(f);
}
static void view_drop(mdb_db *db, view *v, size_t table) {
    (void)db;
    (void)table;
    if (!v || --v->refs)
        return;
    mdb_slots_free(v->rows);
    file_drop(v->file);
    free(v);
}
static row *view_row(const view *v, size_t slot) {
    return slot < v->n ? mdb_slots_get(v->rows, slot) : NULL;
}
static int view_search(const view *v, int64_t id, uint64_t *offset) {
    pthread_rwlock_rdlock(&v->file->lock);
    int found = mdb_bpt_search(v->file->index, id, offset);
    pthread_rwlock_unlock(&v->file->lock);
    if (!found || *offset < MDB_BIN_HEADER_SIZE ||
        (*offset - MDB_BIN_HEADER_SIZE) % v->file->record_size)
        return 0;
    uint64_t slot = (*offset - MDB_BIN_HEADER_SIZE) / v->file->record_size;
    if (slot >= v->n)
        return 0;
    row *r = view_row(v, (size_t)slot);
    return r && r->v[0].i == id;
}
static void snapshot_drop(mdb_db *db, snapshot *s) {
    if (!s || --s->refs)
        return;
    for (size_t i = 0; i < db->ntables; i++)
        view_drop(db, s->tables[i], i);
    free(s->tables);
    free(s);
}
static size_t lock_bucket(size_t table, int64_t id, size_t capacity) {
    uint64_t hash = (uint64_t)id ^ ((uint64_t)table * UINT64_C(0x9e3779b97f4a7c15));
    hash ^= hash >> 30; hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27; hash *= UINT64_C(0x94d049bb133111eb);
    hash ^= hash >> 31;
    return (size_t)hash & (capacity - 1);
}
static int lock_reserve(mdb_db *db) {
    if (db->lock_capacity && db->nlocks < db->lock_capacity * 2) return 0;
    size_t cap = db->lock_capacity ? db->lock_capacity * 2 : 64;
    if (cap < db->lock_capacity || cap > SIZE_MAX / sizeof(row_lock *)) return -1;
    row_lock **buckets = calloc(cap, sizeof *buckets);
    if (!buckets) return -1;
    for (row_lock *l = db->locks; l; l = l->next) {
        size_t b = lock_bucket(l->table, l->id, cap);
        l->hash_next = buckets[b]; buckets[b] = l;
    }
    free(db->lock_buckets); db->lock_buckets = buckets; db->lock_capacity = cap;
    return 0;
}
static void tx_drop(mdb_db *db, transaction *tx) {
    if (!tx)
        return;
    for (size_t i = 0; i < db->ntables; i++)
        overlay_clear(&tx->tables[i], db->defs[i].ncols);
    free(tx->tables);
    row_lock **link = &db->locks;
    while (*link) {
        row_lock *lock = *link;
        if (lock->owner == tx) {
            *link = lock->next;
            size_t b = lock_bucket(lock->table, lock->id, db->lock_capacity);
            row_lock **hashed = &db->lock_buckets[b];
            while (*hashed != lock) hashed = &(*hashed)->hash_next;
            *hashed = lock->hash_next;
            db->nlocks--;
            free(lock);
        }
        else link = &lock->next;
    }
    pthread_cond_broadcast(&db->lock_changed);
    snapshot_drop(db, tx->snap);
    db->active_transactions--;
    free(tx);
}
static void fault(const char *name) {
    const char *f = getenv("MDB_FAILPOINT");
    if (f && strcmp(f, name) == 0) {
        const char *action = getenv("MDB_FAILPOINT_ACTION");
        if (action && strcmp(action, "pause") == 0) {
            /* Test parent owns termination. Handshake avoids timing-based kills. */
            const char *ready = getenv("MDB_FAILPOINT_READY_FD");
            char *end = NULL;
            if (!ready || !*ready)
                _exit(138);
            errno = 0;
            long descriptor = strtol(ready, &end, 10);
            if (errno || !end || *end || descriptor < 3 || descriptor > INT_MAX)
                _exit(138);
            ssize_t count;
            do {
                count = write((int)descriptor, "R", 1);
            } while (count < 0 && errno == EINTR);
            if (count != 1)
                _exit(138);
            for (;;)
                pause();
        }
        (void)kill(getpid(), SIGKILL);
        _exit(137);
    }
}
static int full_write(int fd, const void *data, size_t n) {
    const unsigned char *p = data;
    while (n) {
        ssize_t k = write(fd, p, n);
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        p += (size_t)k;
        n -= (size_t)k;
    }
    return 0;
}
static int full_pread(int fd, void *data, size_t n, uint64_t offset) {
    unsigned char *p = data;
    if (offset > (uint64_t)INT64_MAX || n > (uint64_t)INT64_MAX - offset)
        return -1;
    while (n) {
        ssize_t k = pread(fd, p, n, (off_t)offset);
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        p += (size_t)k;
        n -= (size_t)k;
        offset += (size_t)k;
    }
    return 0;
}
static int full_pwrite(mdb_db *db, int fd, const void *data, size_t n, uint64_t offset) {
    (void)db;
    const unsigned char *p = data;
    if (offset > (uint64_t)INT64_MAX || n > (uint64_t)INT64_MAX - offset)
        return -1;
    while (n) {
#ifdef MDB_PROFILE
        double syscall_started = seconds();
#endif
        ssize_t k = pwrite(fd, p, n, (off_t)offset);
#ifdef MDB_PROFILE
        int syscall_errno = errno;
        db->metrics.data_pwrite_s += seconds() - syscall_started;
        errno = syscall_errno;
        db->metrics.data_pwrite_calls++;
        if (k > 0)
            db->metrics.data_pwrite_bytes += (uint64_t)k;
#endif
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        p += (size_t)k;
        n -= (size_t)k;
        offset += (size_t)k;
    }
    return 0;
}
static int syncfd(int fd) {
    int k;
    do {
        k = fsync(fd);
    } while (k < 0 && errno == EINTR);
    return k;
}
static int valid_name(const char *s) {
    size_t n = strlen(s);
    if (!n || n > MDB_NAME_MAX)
        return 0;
    if (!((s[0] >= 'a' && s[0] <= 'z') || s[0] == '_'))
        return 0;
    for (size_t i = 0; i < n; i++)
        if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '_'))
            return 0;
    return 1;
}
static int filehash(int fd, uint64_t *hash) {
    unsigned char b[8192];
    uint64_t off = 0, h = FNV_INIT;
    for (;;) {
        ssize_t n = pread(fd, b, sizeof b, (off_t)off);
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0)
            return -1;
        if (!n)
            break;
        h = hashbytes(h, b, (size_t)n);
        off += (size_t)n;
    }
    *hash = h;
    return 0;
}
static int find_table(mdb_db *db, const char *name) {
    for (size_t i = 0; i < db->ntables; i++)
        if (strcmp(db->defs[i].name, name) == 0)
            return (int)i;
    return -1;
}
static int qualifier_matches(const char *name, const char *dot, const char *qualifier) {
    size_t length = (size_t)(dot - name);
    return strlen(qualifier) == length && !memcmp(name, qualifier, length);
}
static int find_column(const tabledef *t, const char *name) {
    size_t start = 0, end = t->ncols;
    const char *dot = strchr(name, '.');
    if (dot) {
        const char *left = t->source_names[0] ? t->source_names[0] : t->name;
        if (qualifier_matches(name, dot, left))
            end = t->source_names[1] ? t->right_start : end;
        else if (t->source_names[1] && qualifier_matches(name, dot, t->source_names[1]))
            start = t->right_start;
        else return -1;
        name = dot + 1;
    }
    int position = -1;
    for (size_t i = start; i < end; i++) {
        if (strcmp(t->cols[i].name, name))
            continue;
        if (position >= 0)
            return -2;
        position = (int)i;
        if (!t->source_names[1]) break;
    }
    return position;
}
static int column_error(mdb_error *e, int position, const char *message) {
    return fail(e, MDB_UNKNOWN_COLUMN,
                position == -2 ? "Ambiguous column; qualify it with a table name or alias" : message);
}
static int create_definition(const mdb_stmt *st, tabledef *t, mdb_error *e) {
    if (st->kind != MDB_CREATE || !valid_name(st->table) || !st->ncolumns)
        return fail(e, MDB_CORRUPT, "Invalid CREATE TABLE definition");
    memset(t, 0, sizeof *t);
    (void)snprintf(t->name, sizeof t->name, "%s", st->table);
    t->ncols = st->ncolumns;
    t->next_id = 1;
    uint64_t h = FNV_INIT;
    for (size_t i = 0; i < t->ncols; i++) {
        if (!valid_name(st->columns[i]))
            return fail(e, MDB_TYPE, "Invalid column name");
        column *c = &t->cols[i];
        (void)snprintf(c->name, sizeof c->name, "%s", st->columns[i]);
        c->type = st->create_types[i];
        c->max = st->create_sizes[i];
        char line[128];
        int n = c->type == MDB_INT
                    ? snprintf(line, sizeof line, "%s INT%s\n", c->name, i ? "" : " AUTO")
                    : snprintf(line, sizeof line, "%s VARCHAR(%zu)\n", c->name, c->max);
        h = hashbytes(h, line, (size_t)n);
    }
    t->schemahash = h;
    return 0;
}
static int find_unique(mdb_db *db, const char *name) {
    for (size_t i = 0; i < db->nunique; i++)
        if (!strcmp(db->unique[i].name, name))
            return (int)i;
    return -1;
}
static int unique_definition(mdb_db *db, const mdb_stmt *st, unique_index *index, mdb_error *e) {
    if (st->kind != MDB_CREATE_INDEX || !valid_name(st->index_name) || st->ncolumns != 1)
        return fail(e, MDB_SYNTAX, "Expected a named single-column UNIQUE index");
    int ti = find_table(db, st->table);
    if (ti < 0)
        return fail(e, MDB_UNKNOWN_TABLE, "Unknown indexed table");
    int ci = find_column(&db->defs[ti], st->columns[0]);
    if (ci < 0)
        return fail(e, MDB_UNKNOWN_COLUMN, "Unknown indexed column");
    memset(index, 0, sizeof *index);
    (void)snprintf(index->name, sizeof index->name, "%s", st->index_name);
    index->table = (size_t)ti;
    index->column = (size_t)ci;
    return 0;
}
static int unique_compare(const void *a, const void *b) {
    const mdb_value *x = ((const unique_item *)a)->value, *y = ((const unique_item *)b)->value;
    if (x->type == MDB_INT)
        return (x->i > y->i) - (x->i < y->i);
    return strcmp(x->s, y->s);
}
/* Build an ordered index over the final candidate rows, including own writes
 * and deletes. VARCHAR equality is exact UTF-8 byte equality, never hash-only. */
static int unique_build(const unique_index *index, const view *v, const rowset *set,
                        unique_item **out, size_t *count, mdb_error *e) {
    size_t cap = v ? v->n : set->n, n = 0;
    if (cap > SIZE_MAX / sizeof(unique_item))
        return fail(e, MDB_LIMIT, "UNIQUE index size limit");
    unique_item *items = malloc((cap ? cap : 1) * sizeof *items);
    if (!items)
        return fail(e, MDB_NOMEM, "Cannot allocate UNIQUE index");
    for (size_t i = 0; i < cap; i++) {
        row *r = v ? view_row(v, i) : set->v[i];
        if (r)
            items[n++] = (unique_item){.value = &r->v[index->column], .row_id = r->v[0].i};
    }
    qsort(items, n, sizeof *items, unique_compare);
    for (size_t i = 1; i < n; i++)
        if (!unique_compare(&items[i - 1], &items[i])) {
            char message[256];
            (void)snprintf(message, sizeof message, "UNIQUE index %s rejects duplicate values", index->name);
            free(items);
            return fail(e, MDB_DUPLICATE, message);
        }
    *out = items;
    *count = n;
    return 0;
}
static int unique_check_replay(mdb_db *db, rowset *sets, overlay *pending, mdb_error *e) {
    for (size_t i = 0; i < db->nunique; i++) {
        const unique_index *index = &db->unique[i];
        if (!pending[index->table].n)
            continue;
        unique_item *items = NULL;
        size_t count = 0;
        if (unique_build(index, NULL, &sets[index->table], &items, &count, e)) {
            if (e && e->code == MDB_DUPLICATE)
                e->code = MDB_CORRUPT;
            return -1;
        }
        free(items);
    }
    return 0;
}
static void put32(unsigned char *p, uint32_t x) {
    for (size_t i = 0; i < 4; i++)
        p[i] = (unsigned char)(x >> (i * 8));
}
static void put64(unsigned char *p, uint64_t x) {
    for (size_t i = 0; i < 8; i++)
        p[i] = (unsigned char)(x >> (i * 8));
}
static uint32_t get32(const unsigned char *p) {
    uint32_t v = 0;
    for (size_t i = 0; i < 4; i++)
        v |= (uint32_t)p[i] << (i * 8);
    return v;
}
static uint64_t get64(const unsigned char *p) {
    uint64_t v = 0;
    for (size_t i = 0; i < 8; i++)
        v |= (uint64_t)p[i] << (i * 8);
    return v;
}
static int64_t signed64(uint64_t x) {
    if (x <= INT64_MAX)
        return (int64_t)x;
    return -1 - (int64_t)(UINT64_MAX - x);
}

/* Reader finds physical byte offsets while keeping quoted CR/LF inside a row. */
static int reader_byte(csv_reader *r, unsigned char *c) {
    if (r->pos == r->len) {
        ssize_t n;
        do {
            n = pread(r->fd, r->buf, sizeof r->buf, (off_t)r->offset);
        } while (n < 0 && errno == EINTR);
        if (n < 0)
            return -1;
        if (!n)
            return 0;
        r->len = (size_t)n;
        r->pos = 0;
    }
    *c = r->buf[r->pos++];
    r->offset++;
    return 1;
}
static int csv_record(csv_reader *r, char **out, size_t *length, uint64_t *start, mdb_error *e) {
    size_t n = 0, cap = 0;
    char *p = NULL;
    int quoted = 0;
    *start = r->offset;
    for (;;) {
        unsigned char c;
        int k = reader_byte(r, &c);
        if (k < 0) {
            free(p);
            return fail(e, MDB_IO, "CSV read failed");
        }
        if (!k) {
            if (!n) {
                free(p);
                return 0;
            }
            if (quoted) {
                free(p);
                return fail(e, MDB_CORRUPT, "Unterminated CSV quoted field");
            }
            break;
        }
        if (c == 0) {
            free(p);
            return fail(e, MDB_CORRUPT, "NUL in CSV");
        }
        if (c == '"')
            quoted = !quoted;
        if (c == '\n' && !quoted) {
            if (n && p[n - 1] == '\r')
                n--;
            break;
        }
        if (n >= CSV_RECORD_MAX) {
            free(p);
            return fail(e, MDB_CORRUPT, "CSV record exceeds limit");
        }
        if (grow((void **)&p, &cap, n + 2, 1)) {
            free(p);
            return fail(e, MDB_NOMEM, "Allocation failed");
        }
        p[n++] = (char)c;
    }
    if (!p) {
        p = malloc(1);
        if (!p)
            return fail(e, MDB_NOMEM, "Allocation failed");
    }
    p[n] = 0;
    *out = p;
    *length = n;
    return 1;
}
static int parse_integer(const char *s, int64_t *v) {
    if (!*s)
        return -1;
    const char *p = s;
    if (*p == '-')
        p++;
    if (!*p)
        return -1;
    for (; *p; p++)
        if (*p < '0' || *p > '9')
            return -1;
    errno = 0;
    char *end;
    long long k = strtoll(s, &end, 10);
    if (errno == ERANGE || *end)
        return -1;
    *v = (int64_t)k;
    return 0;
}
static row *csv_parse(const tabledef *t, const char *p, size_t n, mdb_error *e) {
    row *r = row_new(t->ncols);
    if (!r) {
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    size_t pos = 0;
    for (size_t col = 0; col < t->ncols; col++) {
        char value[MDB_TEXT_MAX + 32];
        size_t len = 0;
        int quoted = pos < n && p[pos] == '"';
        if (quoted)
            pos++;
        int closed = !quoted;
        while (pos < n) {
            char c = p[pos++];
            if (quoted) {
                if (c == '"') {
                    if (pos < n && p[pos] == '"') {
                        pos++;
                        c = '"';
                    } else {
                        closed = 1;
                        break;
                    }
                }
            } else if (c == ',') {
                pos--;
                break;
            } else if (c == '"' || c == '\r' || c == '\n') {
                fail(e, MDB_CORRUPT, "Invalid unquoted CSV field");
                goto bad;
            }
            if (len + 1 >= sizeof value) {
                fail(e, MDB_CORRUPT, "CSV field exceeds limit");
                goto bad;
            }
            value[len++] = c;
        }
        if (!closed) {
            fail(e, MDB_CORRUPT, "Unterminated CSV field");
            goto bad;
        }
        value[len] = 0;
        if (col + 1 < t->ncols) {
            if (pos >= n || p[pos] != ',') {
                fail(e, MDB_CORRUPT, "CSV column count mismatch");
                goto bad;
            }
            pos++;
        } else if (pos != n) {
            fail(e, MDB_CORRUPT, "CSV column count mismatch");
            goto bad;
        }
        r->v[col].type = t->cols[col].type;
        if (t->cols[col].type == MDB_INT) {
            if (parse_integer(value, &r->v[col].i)) {
                fail(e, MDB_CORRUPT, "Invalid CSV integer");
                goto bad;
            }
        } else {
            if (len > t->cols[col].max || !mdb_utf8(value, len)) {
                fail(e, MDB_CORRUPT, "Invalid CSV string");
                goto bad;
            }
            r->v[col].s = strdup(value);
            if (!r->v[col].s) {
                fail(e, MDB_NOMEM, "Allocation failed");
                goto bad;
            }
        }
    }
    return r;
bad:
    row_drop(r, t->ncols);
    return NULL;
}
static size_t binary_columns(const tabledef *t, mdb_binary_column *columns) {
    for (size_t i = 0; i < t->ncols; i++)
        columns[i] = (mdb_binary_column){t->cols[i].type, t->cols[i].max};
    return mdb_binary_record_size(columns, t->ncols);
}
typedef struct {
    uint64_t offset;
    size_t length;
    unsigned char bytes[BINARY_READ_BUFFER];
#ifdef MDB_TESTING
    void (*hook)(void *);
    void *hook_context;
#endif
} binary_reader;
/* The cache contains only bytes from this view's generation. If a writer has
 * advanced the shared file, retained immutable rows supply the old snapshot. */
static row *view_read_cached(const tabledef *t, const view *v, uint64_t offset,
                             binary_reader *cache, mdb_error *e) {
    size_t size = v->file->record_size;
    if (offset < MDB_BIN_HEADER_SIZE || (offset - MDB_BIN_HEADER_SIZE) % size)
        return fail(e, MDB_CORRUPT, "Unaligned binary record offset"), NULL;
    uint64_t slot = (offset - MDB_BIN_HEADER_SIZE) / size;
    if (slot >= v->n)
        return fail(e, MDB_CORRUPT, "Index offset outside binary snapshot"), NULL;
    row *expected = view_row(v, (size_t)slot);
    if (!expected)
        return fail(e, MDB_CORRUPT, "Index points at a deleted snapshot slot"), NULL;
    unsigned char local[BINARY_RECORD_MAX];
    const unsigned char *bytes = NULL;
    if (cache && offset >= cache->offset && offset - cache->offset < cache->length)
        bytes = cache->bytes + (size_t)(offset - cache->offset);
    else {
        pthread_rwlock_rdlock(&v->file->lock);
        if (v->version != v->file->version) {
            pthread_rwlock_unlock(&v->file->lock);
            row *copy = row_clone(expected, t->ncols);
            if (!copy)
                fail(e, MDB_NOMEM, "Allocation failed");
            return copy;
        }
        int rc;
        if (cache) {
            size_t records = sizeof cache->bytes / size;
            if (records > v->n - (size_t)slot)
                records = v->n - (size_t)slot;
            cache->offset = offset;
            cache->length = records * size;
            rc = full_pread(v->file->fd, cache->bytes, cache->length, offset);
            bytes = cache->bytes;
        } else {
            rc = full_pread(v->file->fd, local, size, offset);
            bytes = local;
        }
        pthread_rwlock_unlock(&v->file->lock);
        if (rc)
            return fail(e, MDB_IO, "Binary record read failed"), NULL;
#ifdef MDB_TESTING
        if (cache && cache->hook)
            cache->hook(cache->hook_context);
#endif
    }
    mdb_binary_column columns[MDB_COL_MAX];
    (void)binary_columns(t, columns);
    mdb_value *values = NULL;
    int deleted;
    if (mdb_binary_decode(columns, t->ncols, bytes, size, &values, &deleted, e))
        return NULL;
    if (deleted)
        return fail(e, MDB_CORRUPT, "Unexpected binary tombstone"), NULL;
    row *r = row_new(t->ncols);
    if (r) {
        memcpy(r->v, values, t->ncols * sizeof *values);
        free(values);
        if (r->v[0].i != expected->v[0].i) {
            row_drop(r, t->ncols);
            return fail(e, MDB_CORRUPT, "Index/binary key mismatch"), NULL;
        }
        return r;
    }
    for (size_t i = 0; i < t->ncols; i++)
        mdb_value_clear(&values[i]);
    free(values);
    return fail(e, MDB_NOMEM, "Allocation failed"), NULL;
}
static row *view_read(const tabledef *t, const view *v, uint64_t offset, mdb_error *e) {
    return view_read_cached(t, v, offset, NULL, e);
}
static int rowset_add(rowset *s, row *r, mdb_error *e) {
    size_t old;
    if (map_get(&s->map, r->v[0].i, &old))
        return fail(e, MDB_DUPLICATE, "Duplicate ID in CSV");
    if (grow((void **)&s->v, &s->cap, s->n + 1, sizeof *s->v) || map_reserve(&s->map, s->n + 1))
        return fail(e, MDB_NOMEM, "Allocation failed");
    map_put(&s->map, r->v[0].i, s->n);
    s->v[s->n++] = r;
    return 0;
}
static void rowset_clear(rowset *s, size_t ncols) {
    for (size_t i = 0; i < s->n; i++)
        row_drop(s->v[i], ncols);
    free(s->v);
    free(s->map.b);
    memset(s, 0, sizeof *s);
}
static int read_rows(int fd, const tabledef *t, rowset *s, mdb_error *e) {
    csv_reader reader = {.fd = fd};
    for (;;) {
        char *text = NULL;
        size_t n;
        uint64_t off;
        int k = csv_record(&reader, &text, &n, &off, e);
        if (k <= 0)
            return k;
        row *r = csv_parse(t, text, n, e);
        free(text);
        if (!r)
            return -1;
        if (rowset_add(s, r, e)) {
            row_drop(r, t->ncols);
            return -1;
        }
    }
}
static int defcmp(const void *a, const void *b) {
    return strcmp(((const tabledef *)a)->name, ((const tabledef *)b)->name);
}
static int load_schema(mdb_db *db, tabledef *t, mdb_error *e) {
    char name[MDB_NAME_MAX + 16];
    (void)snprintf(name, sizeof name, "%s.schema", t->name);
    int fd = openat(db->dirfd, name, O_RDONLY);
    if (fd < 0)
        return fail(e, MDB_IO, "Cannot open schema");
    struct stat schema_stat;
    if (fstat(fd, &schema_stat) || schema_stat.st_size > 65536) {
        close(fd);
        return fail(e, MDB_CORRUPT, "Schema file exceeds limit");
    }
    if (filehash(fd, &t->schemahash)) {
        close(fd);
        return fail(e, MDB_IO, "Cannot read schema");
    }
    FILE *f = fdopen(fd, "r");
    if (!f) {
        close(fd);
        return fail(e, MDB_IO, "Cannot read schema");
    }
    char *line = NULL;
    size_t cap = 0;
    int rc = -1;
    ssize_t len;
    while ((len = getline(&line, &cap, f)) >= 0) {
        if ((size_t)len > 4096) {
            fail(e, MDB_CORRUPT, "Schema line too long");
            goto done;
        }
        char *comment = strchr(line, '#');
        if (comment)
            *comment = 0;
        char *save = NULL, *col = strtok_r(line, " \t\r\n", &save);
        if (!col)
            continue;
        char *type = strtok_r(NULL, " \t\r\n", &save), *option = strtok_r(NULL, " \t\r\n", &save),
             *extra = strtok_r(NULL, " \t\r\n", &save);
        if (!type || extra || t->ncols >= MDB_COL_MAX) {
            fail(e, MDB_CORRUPT, "Invalid schema definition");
            goto done;
        }
        for (char *p = col; *p; p++)
            *p = (char)tolower((unsigned char)*p);
        for (char *p = type; *p; p++)
            *p = (char)tolower((unsigned char)*p);
        if (option)
            for (char *p = option; *p; p++)
                *p = (char)tolower((unsigned char)*p);
        if (!valid_name(col) || find_column(t, col) >= 0) {
            fail(e, MDB_CORRUPT, "Invalid or duplicate column name");
            goto done;
        }
        column *c = &t->cols[t->ncols];
        (void)snprintf(c->name, sizeof c->name, "%s", col);
        if (strcmp(type, "int") == 0)
            c->type = MDB_INT;
        else if (strncmp(type, "varchar(", 8) == 0) {
            size_t n = strlen(type);
            if (n < 10 || type[n - 1] != ')') {
                fail(e, MDB_CORRUPT, "Invalid VARCHAR size");
                goto done;
            }
            type[n - 1] = 0;
            int64_t k;
            if (parse_integer(type + 8, &k) || k < 1 || k > MDB_TEXT_MAX) {
                fail(e, MDB_CORRUPT, "Invalid VARCHAR size");
                goto done;
            }
            c->type = MDB_STRING;
            c->max = (size_t)k;
        } else {
            fail(e, MDB_CORRUPT, "Unsupported schema type");
            goto done;
        }
        if (t->ncols == 0) {
            if (strcmp(col, "id") || c->type != MDB_INT || !option || strcmp(option, "auto")) {
                fail(e, MDB_CORRUPT, "First column must be id INT AUTO");
                goto done;
            }
        } else if (option || strcmp(col, "id") == 0) {
            fail(e, MDB_CORRUPT, "Unsupported schema option");
            goto done;
        }
        t->ncols++;
    }
    if (ferror(f)) {
        fail(e, MDB_IO, "Schema read failed");
        goto done;
    }
    if (!t->ncols) {
        fail(e, MDB_CORRUPT, "Empty schema");
        goto done;
    }
    rc = 0;
done:
    free(line);
    fclose(f);
    return rc;
}
static int discover(mdb_db *db, mdb_error *e) {
    int scanfd = dup(db->dirfd);
    if (scanfd < 0)
        return fail(e, MDB_IO, "Cannot scan data directory");
    DIR *d = fdopendir(scanfd);
    if (!d) {
        close(scanfd);
        return fail(e, MDB_IO, "Cannot scan data directory");
    }
    /* Row-slot retain/release callbacks keep tabledef addresses. Reserve the
     * catalog limit up front so online CREATE never relocates those contexts. */
    size_t cap = 128;
    db->defs = calloc(cap, sizeof *db->defs);
    if (!db->defs) {
        closedir(d);
        return fail(e, MDB_NOMEM, "Allocation failed");
    }
    struct dirent *ent;
    int rc = -1;
    errno = 0;
    while ((ent = readdir(d))) {
        size_t n = strlen(ent->d_name);
        if (n <= 7 || strcmp(ent->d_name + n - 7, ".schema"))
            continue;
        if (n - 7 > MDB_NAME_MAX) {
            fail(e, MDB_CORRUPT, "Invalid table name");
            goto done;
        }
        char name[MDB_NAME_MAX + 1];
        memcpy(name, ent->d_name, n - 7);
        name[n - 7] = 0;
        if (!valid_name(name)) {
            fail(e, MDB_CORRUPT, "Invalid table filename");
            goto done;
        }
        if (db->ntables >= 128) {
            fail(e, MDB_LIMIT, "Maximum table count is 128");
            goto done;
        }
        if (grow((void **)&db->defs, &cap, db->ntables + 1, sizeof *db->defs)) {
            fail(e, MDB_NOMEM, "Allocation failed");
            goto done;
        }
        tabledef *t = &db->defs[db->ntables++];
        memset(t, 0, sizeof *t);
        (void)snprintf(t->name, sizeof t->name, "%s", name);
    }
    if (errno) {
        fail(e, MDB_IO, "Directory scan failed");
        goto done;
    }
    if (!db->ntables) {
        fail(e, MDB_CORRUPT, "No prepared tables");
        goto done;
    }
    qsort(db->defs, db->ntables, sizeof *db->defs, defcmp);
    for (size_t i = 0; i < db->ntables; i++)
        if (load_schema(db, &db->defs[i], e))
            goto done;
    rc = 0;
done:
    closedir(d);
    return rc;
}
static int copy_file(int source, int target) {
    unsigned char b[8192];
    uint64_t off = 0;
    for (;;) {
        ssize_t n;
        do {
            n = pread(source, b, sizeof b, (off_t)off);
        } while (n < 0 && errno == EINTR);
        if (n < 0)
            return -1;
        if (!n)
            break;
        if (full_write(target, b, (size_t)n))
            return -1;
        off += (size_t)n;
    }
    return syncfd(target);
}
static int exists_at(int fd, const char *name) {
    struct stat st;
    return fstatat(fd, name, &st, 0) == 0;
}
static int initialize(mdb_db *db, mdb_error *e) {
    if (exists_at(db->dirfd, ".mdb-base") || exists_at(db->dirfd, ".mdb-base.tmp") ||
        exists_at(db->dirfd, ".mdb.wal"))
        return fail(e, MDB_CORRUPT,
                    "Initialization incomplete: required initialization marker missing");
    for (size_t i = 0; i < db->ntables; i++) {
        char name[MDB_NAME_MAX + 16];
        (void)snprintf(name, sizeof name, "%s.bin", db->defs[i].name);
        if (exists_at(db->dirfd, name))
            return fail(e, MDB_CORRUPT,
                        "Binary table without recovery metadata; refusing initialization");
    }
    if (mkdirat(db->dirfd, ".mdb-base.tmp", 0700))
        return fail(e, MDB_IO, "Cannot prepare baseline");
    int base = openat(db->dirfd, ".mdb-base.tmp", O_RDONLY | O_DIRECTORY);
    if (base < 0)
        return fail(e, MDB_IO, "Cannot open baseline");
    int manifest = openat(base, "manifest", O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (manifest < 0) {
        close(base);
        return fail(e, MDB_IO, "Cannot create baseline manifest");
    }
    char line[256];
    int n = snprintf(line, sizeof line, "MDBBASE1\n%zu\n", db->ntables);
    int rc = -1;
    if (n < 0 || full_write(manifest, line, (size_t)n))
        goto io;
    for (size_t i = 0; i < db->ntables; i++) {
        tabledef *t = &db->defs[i];
        char filename[MDB_NAME_MAX + 16];
        (void)snprintf(filename, sizeof filename, "%s.csv", t->name);
        int source = openat(db->dirfd, filename, O_RDONLY);
        if (source < 0)
            goto io;
        rowset checked = {0};
        if (read_rows(source, t, &checked, e)) {
            rowset_clear(&checked, t->ncols);
            close(source);
            goto done;
        }
        rowset_clear(&checked, t->ncols);
        if (filehash(source, &t->basehash)) {
            close(source);
            goto io;
        }
        int target = openat(base, filename, O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (target < 0) {
            close(source);
            goto io;
        }
        int k = copy_file(source, target);
        close(source);
        close(target);
        if (k)
            goto io;
        if (i == 0)
            fault("init_baseline");
        n = snprintf(line, sizeof line, "%s %016llx %016llx\n", t->name,
                     (unsigned long long)t->schemahash, (unsigned long long)t->basehash);
        if (n < 0 || (size_t)n >= sizeof line || full_write(manifest, line, (size_t)n))
            goto io;
    }
    if (syncfd(manifest) || syncfd(base))
        goto io;
    if (renameat(db->dirfd, ".mdb-base.tmp", db->dirfd, ".mdb-base") || syncfd(db->dirfd))
        goto io;
    int wal = openat(db->dirfd, ".mdb.wal", O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (wal < 0)
        goto io;
    int k = full_write(wal, WAL_HEADER, 8) || syncfd(wal);
    close(wal);
    if (k)
        goto io;
    int marker = openat(db->dirfd, ".mdb.init.tmp", O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (marker < 0)
        goto io;
    k = full_write(marker, "MDBINIT1\n", 9) || syncfd(marker);
    close(marker);
    if (k || renameat(db->dirfd, ".mdb.init.tmp", db->dirfd, ".mdb.init") || syncfd(db->dirfd))
        goto io;
    rc = 0;
    goto done;
io:
    fail(e, MDB_IO, "Cannot persist initial database baseline");
done:
    close(manifest);
    close(base);
    return rc;
}
static int verify_baseline(mdb_db *db, mdb_error *e) {
    int marker = openat(db->dirfd, ".mdb.init", O_RDONLY);
    char check[10] = {0};
    if (marker < 0 || full_pread(marker, check, 9, 0) || strcmp(check, "MDBINIT1\n")) {
        if (marker >= 0)
            close(marker);
        return fail(e, MDB_CORRUPT, "Invalid initialization marker");
    }
    struct stat st;
    if (fstat(marker, &st) || st.st_size != 9) {
        close(marker);
        return fail(e, MDB_CORRUPT, "Invalid initialization marker");
    }
    close(marker);
    db->basefd = openat(db->dirfd, ".mdb-base", O_RDONLY | O_DIRECTORY);
    if (db->basefd < 0)
        return fail(e, MDB_CORRUPT, "Required baseline missing");
    int fd = openat(db->basefd, "manifest", O_RDONLY);
    if (fd < 0)
        return fail(e, MDB_CORRUPT, "Required baseline manifest missing");
    FILE *f = fdopen(fd, "r");
    if (!f) {
        close(fd);
        return fail(e, MDB_IO, "Cannot read baseline manifest");
    }
    char magic[32];
    size_t count;
    int rc = -1;
    if (!fgets(magic, sizeof magic, f) || strcmp(magic, "MDBBASE1\n") ||
        fscanf(f, "%zu", &count) != 1 || count != db->ntables) {
        fail(e, MDB_CORRUPT, "Baseline schema set mismatch");
        goto done;
    }
    for (size_t i = 0; i < count; i++) {
        char name[MDB_NAME_MAX + 1];
        unsigned long long sh, bh;
        if (fscanf(f, "%63s %llx %llx", name, &sh, &bh) != 3 || strcmp(name, db->defs[i].name) ||
            (uint64_t)sh != db->defs[i].schemahash) {
            fail(e, MDB_CORRUPT, "Baseline schema fingerprint mismatch");
            goto done;
        }
        db->defs[i].basehash = (uint64_t)bh;
        char path[MDB_NAME_MAX + 16];
        (void)snprintf(path, sizeof path, "%s.csv", name);
        int csv = openat(db->basefd, path, O_RDONLY);
        uint64_t hash;
        if (csv < 0 || filehash(csv, &hash) || hash != (uint64_t)bh) {
            if (csv >= 0)
                close(csv);
            fail(e, MDB_CORRUPT, "Baseline missing or checksum mismatch");
            goto done;
        }
        close(csv);
    }
    int ch;
    do {
        ch = fgetc(f);
    } while (ch != EOF && isspace((unsigned char)ch));
    if (ch != EOF || ferror(f)) {
        fail(e, MDB_CORRUPT, "Malformed baseline manifest");
        goto done;
    }
    rc = 0;
done:
    fclose(f);
    return rc;
}

/* WAL v1: 32-byte LE header: magic/type/length/payload-checksum/txid,
 * independent header-checksum and reserved zero. Checksums are low32(FNV64).
 * Header integrity is verified BEFORE trusting length or truncating a tail. */
/* Instrument the actual write syscall, including retries. Production builds
 * retain the original full_write path to quantify profiling overhead. */
static int wal_write(mdb_db *db, const void *data, size_t n) {
#ifndef MDB_PROFILE
    return full_write(db->walfd, data, n);
#else
    const unsigned char *p = data;
    while (n) {
        double started = seconds();
        ssize_t k = write(db->walfd, p, n);
        int syscall_errno = errno;
        db->metrics.wal_write_s += seconds() - started;
        errno = syscall_errno;
        db->metrics.wal_write_calls++;
        if (k > 0)
            db->metrics.wal_write_bytes += (uint64_t)k;
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        p += (size_t)k;
        n -= (size_t)k;
    }
    return 0;
#endif
}
static int wal_flush(mdb_db *db) {
    if (!db->wal_used)
        return 0;
    const char *io = getenv("MDB_IO_FAIL");
    if (io && !strcmp(io, "wal_flush") && db->wal_used == sizeof db->wal_buffer) {
        (void)wal_write(db, db->wal_buffer, db->wal_used / 2);
        errno = ENOSPC;
        return -1;
    }
    if (wal_write(db, db->wal_buffer, db->wal_used))
        return -1; /* Caller stops the engine; never retry a partially written buffer. */
    db->wal_used = 0;
    return 0;
}
static int wal_buffer_write(mdb_db *db, const void *data, size_t n) {
#ifdef MDB_WAL_UNBUFFERED
    /* Measurement control only: identical WAL bytes and sync policy. */
    return wal_write(db, data, n);
#else
    const unsigned char *p = data;
    while (n) {
        size_t available = sizeof db->wal_buffer - db->wal_used;
        if (!available) {
            if (wal_flush(db))
                return -1;
            fault("wal_buffer_full");
            available = sizeof db->wal_buffer;
        }
        size_t take = n < available ? n : available;
        memcpy(db->wal_buffer + db->wal_used, p, take);
        db->wal_used += take;
        p += take;
        n -= take;
    }
    return 0;
#endif
}
static int wal_sync(mdb_db *db) {
    if (wal_flush(db))
        return -1;
    return syncfd(db->walfd);
}
static int wal_append(mdb_db *db, uint32_t kind, uint64_t txid, const unsigned char *payload,
                      size_t n) {
    if (n > RECORD_MAX)
        return -1;
    unsigned char h[32] = {0};
    put32(h, WAL_MAGIC);
    put32(h + 4, kind);
    put32(h + 8, (uint32_t)n);
    put32(h + 12, (uint32_t)hashbytes(FNV_INIT, payload, n));
    put64(h + 16, txid);
    put32(h + 24, (uint32_t)hashbytes(FNV_INIT, h, 24));
    if (wal_buffer_write(db, h, sizeof h))
        return -1;
    const char *fp = getenv("MDB_FAILPOINT"), *io = getenv("MDB_IO_FAIL");
    if (kind == LOG_ROW &&
        ((fp && strcmp(fp, "wal_partial") == 0) || (io && strcmp(io, "wal_write") == 0))) {
        if (n)
            (void)wal_buffer_write(db, payload, n / 2);
        (void)wal_flush(db);
        fault("wal_partial");
        errno = EIO;
        return -1;
    }
    if (kind == LOG_APPLIED && io && strcmp(io, "applied_write") == 0) {
        (void)wal_flush(db);
        errno = EIO;
        return -1;
    }
    int rc = wal_buffer_write(db, payload, n);
    if (!rc && kind == LOG_ROW && fp && !strcmp(fp, "wal_buffered"))
        fault("wal_buffered");
    return rc;
}
static int reserve_txid(mdb_db *db, uint64_t *id, mdb_error *e) {
    if (!db->txnext || db->txnext > INT64_MAX)
        return fail(e, MDB_LIMIT, "Transaction identifier space exhausted");
    if (db->txnext > db->txreserved) {
        uint64_t top =
            db->txnext > INT64_MAX - RESERVE_BLOCK ? INT64_MAX : db->txnext + RESERVE_BLOCK - 1;
        unsigned char p[8];
        put64(p, top);
        if (wal_append(db, LOG_TXIDS, 0, p, 8) || wal_sync(db)) {
            db->healthy = 0;
            return fail(e, MDB_IO, "Transaction ID reservation outcome unknown; recovery required");
        }
        db->txreserved = top;
    }
    *id = db->txnext++;
    return 0;
}
static int reserve_id(mdb_db *db, size_t table, int64_t *id, mdb_error *e) {
    pthread_mutex_lock(&db->mutex);
    tabledef *t = &db->defs[table];
    int rc = -1;
    if (!db->healthy) {
        fail(e, MDB_IO, "Engine requires recovery");
        goto done;
    }
    if (t->next_id > (uint64_t)INT64_MAX) {
        fail(e, MDB_LIMIT, "Automatic ID space exhausted");
        goto done;
    }
    if (t->next_id > t->reserved) {
        uint64_t top = t->next_id > (uint64_t)INT64_MAX - RESERVE_BLOCK
                           ? INT64_MAX
                           : t->next_id + RESERVE_BLOCK - 1;
        unsigned char p[12];
        put32(p, (uint32_t)table);
        put64(p + 4, top);
        if (wal_append(db, LOG_RESERVE, 0, p, 12) || wal_sync(db)) {
            db->healthy = 0;
            fail(e, MDB_IO, "ID reservation outcome unknown; recovery required");
            goto done;
        }
        t->reserved = top;
        fault("id_reserved");
    }
    *id = (int64_t)t->next_id++;
    rc = 0;
done:
    pthread_mutex_unlock(&db->mutex);
    return rc;
}
static int ledger_prepare(mdb_db *db) {
    return grow((void **)&db->committed, &db->capcommitted, db->ncommitted + 1,
                sizeof *db->committed);
}
static int encode_change(mdb_db *db, size_t ti, const change *c, unsigned char *p, size_t *length) {
    const tabledef *t = &db->defs[ti];
    put32(p, (uint32_t)ti);
    put32(p + 4, c->deleted ? 3U : (c->inserted ? 1U : 2U));
    put64(p + 8, (uint64_t)c->id);
    size_t n = 16;
    if (!c->deleted)
        for (size_t j = 0; j < t->ncols; j++) {
            if (t->cols[j].type == MDB_INT) {
                put64(p + n, (uint64_t)c->value->v[j].i);
                n += 8;
            } else {
                size_t k = strlen(c->value->v[j].s);
                put32(p + n, (uint32_t)k);
                n += 4;
                memcpy(p + n, c->value->v[j].s, k);
                n += k;
            }
        }
    *length = n;
    return 0;
}
static int decode_change(mdb_db *db, const unsigned char *p, size_t n, size_t *ti, change *c,
                         mdb_error *e) {
    if (n < 16)
        return fail(e, MDB_CORRUPT, "Short WAL change");
    *ti = get32(p);
    uint32_t action = get32(p + 4);
    if (*ti >= db->ntables || action < 1 || action > 3)
        return fail(e, MDB_CORRUPT, "Invalid WAL change");
    const tabledef *t = &db->defs[*ti];
    memset(c, 0, sizeof *c);
    c->id = signed64(get64(p + 8));
    c->deleted = action == 3;
    c->inserted = action == 1;
    size_t off = 16;
    if (c->deleted) {
        if (n != 16)
            return fail(e, MDB_CORRUPT, "Invalid WAL delete");
        return 0;
    }
    c->value = row_new(t->ncols);
    if (!c->value)
        return fail(e, MDB_NOMEM, "Allocation failed");
    for (size_t i = 0; i < t->ncols; i++) {
        mdb_value *v = &c->value->v[i];
        v->type = t->cols[i].type;
        if (v->type == MDB_INT) {
            if (n - off < 8)
                goto corrupt;
            v->i = signed64(get64(p + off));
            off += 8;
        } else {
            if (n - off < 4)
                goto corrupt;
            size_t k = get32(p + off);
            off += 4;
            if (k > t->cols[i].max || k > n - off || memchr(p + off, 0, k) ||
                !mdb_utf8((const char *)p + off, k))
                goto corrupt;
            v->s = malloc(k + 1);
            if (!v->s) {
                row_drop(c->value, t->ncols);
                c->value = NULL;
                return fail(e, MDB_NOMEM, "Allocation failed");
            }
            memcpy(v->s, p + off, k);
            v->s[k] = 0;
            off += k;
        }
    }
    if (off != n || c->value->v[0].i != c->id)
        goto corrupt;
    return 0;
corrupt:
    row_drop(c->value, t->ncols);
    c->value = NULL;
    return fail(e, MDB_CORRUPT, "Malformed WAL row");
}
static int replay_commit(mdb_db *db, rowset *sets, overlay *pending, mdb_error *e) {
    for (size_t ti = 0; ti < db->ntables; ti++) {
        rowset *s = &sets[ti];
        overlay *o = &pending[ti];
        for (size_t j = 0; j < o->n; j++) {
            change *c = &o->v[j];
            size_t index;
            int found = map_get(&s->map, c->id, &index) && s->v[index];
            if (c->inserted) {
                if (found || c->id < 1 || (uint64_t)c->id > db->defs[ti].reserved)
                    return fail(e, MDB_CORRUPT, "WAL insert violates reserved ID or uniqueness");
                if (grow((void **)&s->v, &s->cap, s->n + 1, sizeof *s->v) ||
                    map_reserve(&s->map, s->n + 1))
                    return fail(e, MDB_NOMEM, "Allocation failed");
                c->value->refs++;
                map_put(&s->map, c->id, s->n);
                s->v[s->n++] = c->value;
            } else {
                if (!found)
                    return fail(e, MDB_CORRUPT, "WAL update/delete references missing ID");
                row_drop(s->v[index], db->defs[ti].ncols);
                s->v[index] = c->deleted ? NULL : c->value;
                if (!c->deleted)
                    c->value->refs++;
            }
        }
    }
    return 0;
}
/* CREATE definitions live in the WAL, never in discoverable .schema files.
 * Load complete, checksummed catalog records before allocating recovery arrays.
 * Baseline table indices stay fixed; new tables append in WAL order. */
static int wal_catalog(mdb_db *db, mdb_error *e) {
    struct stat st;
    if (fstat(db->walfd, &st) || st.st_size < 8)
        return fail(e, MDB_CORRUPT, "Required WAL header missing");
    uint64_t offset = 8;
    while (offset + 32 <= (uint64_t)st.st_size) {
        unsigned char h[32];
        if (full_pread(db->walfd, h, sizeof h, offset))
            return fail(e, MDB_IO, "Cannot read WAL catalog");
        uint32_t kind = get32(h + 4), n = get32(h + 8);
        if (get32(h) != WAL_MAGIC || get32(h + 28) ||
            get32(h + 24) != (uint32_t)hashbytes(FNV_INIT, h, 24) || n > RECORD_MAX ||
            kind < LOG_RESERVE || kind > LOG_UNIQUE)
            return fail(e, MDB_CORRUPT, "Invalid WAL catalog header");
        if (n > (uint64_t)st.st_size - offset - 32)
            break; /* The normal replay repairs only incomplete trailing records. */
        if (kind == LOG_CREATE) {
            if (n < 9 || n > MDB_SQL_MAX + 8 || db->ntables == 128)
                return fail(e, MDB_CORRUPT, "Invalid WAL CREATE size");
            unsigned char p[MDB_SQL_MAX + 9];
            if (full_pread(db->walfd, p, n, offset + 32))
                return fail(e, MDB_IO, "Cannot read WAL CREATE");
            if (get32(h + 12) != (uint32_t)hashbytes(FNV_INIT, p, n) || memchr(p + 8, 0, n - 8))
                return fail(e, MDB_CORRUPT, "Invalid WAL CREATE checksum or text");
            p[n] = 0;
            mdb_stmt *definition = NULL;
            if (mdb_parse((char *)p + 8, &definition, e))
                return fail(e, MDB_CORRUPT, "Invalid WAL CREATE SQL");
            tabledef t;
            int bad = create_definition(definition, &t, e);
            mdb_stmt_free(definition);
            if (bad || find_table(db, t.name) >= 0)
                return fail(e, MDB_CORRUPT, "Duplicate or invalid WAL table");
            db->defs[db->ntables++] = t;
        }
        offset += 32 + (uint64_t)n;
    }
    return 0;
}

static int wal_replay(mdb_db *db, rowset *sets, uint64_t *version, mdb_error *e) {
    struct stat st;
    if (fstat(db->walfd, &st) || st.st_size < 8)
        return fail(e, MDB_CORRUPT, "Required WAL header missing");
    char magic[8];
    if (full_pread(db->walfd, magic, 8, 0) || memcmp(magic, WAL_HEADER, 8))
        return fail(e, MDB_CORRUPT, "Invalid WAL header");
    overlay *pending = calloc(db->ntables, sizeof *pending);
    if (!pending)
        return fail(e, MDB_NOMEM, "Allocation failed");
    uint64_t offset = 8, active = 0, count = 0, digest = FNV_INIT, beginver = 0;
    size_t catalog_count = db->base_tables;
    int rc = -1;
    while (offset < (uint64_t)st.st_size) {
        uint64_t remaining = (uint64_t)st.st_size - offset;
        if (remaining < 32) {
            if (ftruncate(db->walfd, (off_t)offset) || syncfd(db->walfd))
                goto io;
            break;
        }
        unsigned char h[32];
        if (full_pread(db->walfd, h, 32, offset))
            goto io;
        uint32_t kind = get32(h + 4), n = get32(h + 8), checksum = get32(h + 12);
        uint64_t txid = get64(h + 16);
        if (get32(h) != WAL_MAGIC || get32(h + 28) != 0 ||
            get32(h + 24) != (uint32_t)hashbytes(FNV_INIT, h, 24) || n > RECORD_MAX ||
            kind < LOG_RESERVE || kind > LOG_UNIQUE) {
            fail(e, MDB_CORRUPT, "Invalid WAL record header");
            goto done;
        }
        if ((uint64_t)n > remaining - 32) {
            if (ftruncate(db->walfd, (off_t)offset) || syncfd(db->walfd))
                goto io;
            break;
        }
        unsigned char *p = malloc(n ? n : 1);
        if (!p) {
            fail(e, MDB_NOMEM, "Allocation failed");
            goto done;
        }
        if (n && full_pread(db->walfd, p, n, offset + 32)) {
            free(p);
            goto io;
        }
        uint64_t sum = hashbytes(FNV_INIT, p, n);
        if ((uint32_t)sum != checksum) {
            free(p);
            fail(e, MDB_CORRUPT, "WAL checksum mismatch");
            goto done;
        }
        int bad = 0;
        if (kind == LOG_RESERVE) {
            if (n != 12 || txid || get32(p) >= catalog_count)
                bad = 1;
            else {
                tabledef *t = &db->defs[get32(p)];
                uint64_t top = get64(p + 4);
                if (top < t->reserved || top > INT64_MAX)
                    bad = 1;
                else
                    t->reserved = top;
            }
        } else if (kind == LOG_TXIDS) {
            if (n != 8 || txid || get64(p) < db->txreserved || get64(p) > INT64_MAX)
                bad = 1;
            else
                db->txreserved = get64(p);
        } else if (kind == LOG_BEGIN) {
            if (n != 8 || !txid || txid > db->txreserved)
                bad = 1;
            else {
                for (size_t i = 0; i < db->ntables; i++)
                    overlay_clear(&pending[i], db->defs[i].ncols);
                active = txid;
                beginver = get64(p);
                count = 0;
                digest = FNV_INIT;
            }
        } else if (kind == LOG_ROW) {
            if (!active || txid != active)
                bad = 1;
            else {
                size_t ti;
                change c;
                if (decode_change(db, p, n, &ti, &c, e)) {
                    free(p);
                    goto done;
                }
                if (ti >= catalog_count) {
                    row_drop(c.value, db->defs[ti].ncols);
                    free(p);
                    fail(e, MDB_CORRUPT, "WAL row precedes table creation");
                    goto done;
                }
                size_t ignored;
                if (map_get(&pending[ti].map, c.id, &ignored)) {
                    row_drop(c.value, db->defs[ti].ncols);
                    bad = 1;
                } else if (overlay_set(&pending[ti], c.id, c.value, c.deleted, c.inserted,
                                       db->defs[ti].ncols)) {
                    row_drop(c.value, db->defs[ti].ncols);
                    free(p);
                    fail(e, MDB_NOMEM, "Allocation failed");
                    goto done;
                } else {
                    count++;
                    digest = hashbytes(digest, p, n);
                }
            }
        } else if (kind == LOG_COMMIT) {
            if (n != 24 || !active || txid != active || get64(p) != count ||
                get64(p + 8) != digest || beginver != *version || *version == UINT64_MAX ||
                get64(p + 16) != *version + 1)
                bad = 1;
            else {
                if (ledger_prepare(db) || replay_commit(db, sets, pending, e) ||
                    unique_check_replay(db, sets, pending, e)) {
                    free(p);
                    if (!e || !e->code)
                        fail(e, MDB_NOMEM, "Allocation failed");
                    goto done;
                }
                db->committed[db->ncommitted++] = txid;
                (*version)++;
                active = 0;
                for (size_t i = 0; i < db->ntables; i++)
                    overlay_clear(&pending[i], db->defs[i].ncols);
            }
        } else if (kind == LOG_CREATE) {
            for (size_t i = 0; i < db->ntables; i++)
                overlay_clear(&pending[i], db->defs[i].ncols);
            active = 0; /* A previous process may have left an uncommitted tail. */
            if (n < 9 || !txid || txid > db->txreserved ||
                catalog_count >= db->ntables || get64(p) != *version || *version == UINT64_MAX)
                bad = 1;
            else if (ledger_prepare(db)) {
                free(p);
                fail(e, MDB_NOMEM, "Allocation failed");
                goto done;
            } else {
                db->committed[db->ncommitted++] = txid;
                catalog_count++;
                (*version)++;
            }
        } else if (kind == LOG_UNIQUE) {
            for (size_t i = 0; i < db->ntables; i++)
                overlay_clear(&pending[i], db->defs[i].ncols);
            active = 0;
            if (n < 9 || n > MDB_SQL_MAX + 8 || !txid || txid > db->txreserved ||
                get64(p) != *version || *version == UINT64_MAX || db->nunique == UNIQUE_MAX ||
                memchr(p + 8, 0, n - 8))
                bad = 1;
            else {
                char sql[MDB_SQL_MAX + 1];
                memcpy(sql, p + 8, n - 8);
                sql[n - 8] = 0;
                mdb_stmt *st = NULL;
                unique_index index;
                int invalid = mdb_parse(sql, &st, e);
                if (!invalid)
                    invalid = unique_definition(db, st, &index, e);
                mdb_stmt_free(st);
                if (invalid || index.table >= catalog_count || find_unique(db, index.name) >= 0) {
                    free(p);
                    fail(e, MDB_CORRUPT, "Invalid WAL UNIQUE definition");
                    goto done;
                }
                unique_item *items = NULL;
                size_t nitems = 0;
                if (unique_build(&index, NULL, &sets[index.table], &items, &nitems, e) ||
                    ledger_prepare(db)) {
                    free(items);
                    free(p);
                    if (e && e->code == MDB_DUPLICATE)
                        e->code = MDB_CORRUPT;
                    else if (!e || !e->code)
                        fail(e, MDB_NOMEM, "Cannot restore UNIQUE catalog");
                    goto done;
                }
                free(items); /* Final retained indexes use the recovered views below. */
                db->unique[db->nunique++] = index;
                db->committed[db->ncommitted++] = txid;
                (*version)++;
            }
        } else if (kind == LOG_APPLIED) {
            if (n || !txid)
                bad = 1;
            else {
                int found = 0;
                for (size_t i = 0; i < db->ncommitted; i++)
                    if (db->committed[i] == txid) {
                        found = 1;
                        break;
                    }
                if (!found)
                    bad = 1;
            }
        }
        free(p);
        if (bad) {
            fail(e, MDB_CORRUPT, "Inconsistent WAL transaction or reservation");
            goto done;
        }
        offset += 32 + (uint64_t)n;
    }
    db->txnext = db->txreserved + 1;
    if (catalog_count != db->ntables) {
        fail(e, MDB_CORRUPT, "WAL catalog count mismatch");
        goto done;
    }
    for (size_t i = 0; i < db->ntables; i++)
        db->defs[i].next_id = db->defs[i].reserved + 1;
    rc = 0;
    goto done;
io:
    fail(e, MDB_IO, "WAL read/truncate failed");
done:
    for (size_t i = 0; i < db->ntables; i++)
        overlay_clear(&pending[i], db->defs[i].ncols);
    free(pending);
    return rc;
}
static void tempfile_name(char *out, size_t cap, const tabledef *t) {
    (void)snprintf(out, cap, ".mdb-new-%s.bin", t->name);
}
/* Full materialization is reserved for startup recovery, not normal commits. */
static view *build_view(mdb_db *db, size_t ti, row **rows, size_t count,
                        uint64_t version, mdb_error *e) {
    tabledef *t = &db->defs[ti];
    view *v = calloc(1, sizeof *v);
    tablefile *f = calloc(1, sizeof *f);
    if (!v || !f) {
        free(v);
        free(f);
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    v->refs = f->refs = 1;
    v->file = f;
    v->version = f->version = version;
    f->fd = -1;
    char filename[MDB_NAME_MAX + 32];
    tempfile_name(filename, sizeof filename, t);
    if (pthread_rwlock_init(&f->lock, NULL)) {
        fail(e, MDB_IO, "Table lock initialization failed");
        goto bad;
    }
    f->lock_ready = 1;
    mdb_binary_column columns[MDB_COL_MAX];
    f->record_size = binary_columns(t, columns);
    if (!f->record_size || count > ((uint64_t)INT64_MAX - MDB_BIN_HEADER_SIZE) / f->record_size ||
        count > SIZE_MAX / sizeof(uint64_t)) {
        fail(e, MDB_LIMIT, "Binary table size overflow");
        goto bad;
    }
    v->rows = mdb_slots_create(slot_retain, slot_release, t);
    if (!v->rows) {
        fail(e, MDB_NOMEM, "Allocation failed");
        goto bad;
    }
    f->fd = openat(db->dirfd, filename, O_CREAT | O_TRUNC | O_RDWR, 0600);
    if (f->fd < 0) {
        fail(e, MDB_IO, "Cannot create recovery binary table");
        goto bad;
    }
    int64_t *keys = malloc((count ? count : 1) * sizeof *keys);
    uint64_t *offsets = malloc((count ? count : 1) * sizeof *offsets);
    if (!keys || !offsets) {
        free(keys);
        free(offsets);
        fail(e, MDB_NOMEM, "Allocation failed");
        goto bad;
    }
    unsigned char header[MDB_BIN_HEADER_SIZE];
    if (mdb_binary_header_encode(header, t->schemahash, t->ncols, f->record_size) ||
        full_write(f->fd, header, sizeof header)) {
        free(keys);
        free(offsets);
        fail(e, MDB_IO, "Binary header write failed");
        goto bad;
    }
    double started = seconds();
    unsigned char buffer[BINARY_READ_BUFFER];
    size_t buffered = 0;
    int failed = 0;
    for (size_t j = 0; j < count; j++) {
        if (sizeof buffer - buffered < f->record_size) {
            if (full_write(f->fd, buffer, buffered)) {
                failed = 1;
                break;
            }
            buffered = 0;
        }
        if (mdb_binary_encode(columns, t->ncols, rows[j]->v, 0, buffer + buffered,
                              f->record_size, e) || mdb_slots_set(v->rows, j, rows[j])) {
            failed = 1;
            break;
        }
        buffered += f->record_size;
        keys[j] = rows[j]->v[0].i;
        offsets[j] = MDB_BIN_HEADER_SIZE + (uint64_t)j * f->record_size;
        v->n++;
    }
    if (!failed && (full_write(f->fd, buffer, buffered) || syncfd(f->fd)))
        failed = 1;
    db->metrics.storage_write_s += seconds() - started;
    if (failed) {
        free(keys);
        free(offsets);
        fail(e, MDB_IO, "Binary recovery materialization failed");
        goto bad;
    }
    db->metrics.storage_bytes += MDB_BIN_HEADER_SIZE + (uint64_t)count * f->record_size;
    started = seconds();
    f->index = mdb_bpt_bulk(keys, offsets, count);
    free(keys);
    free(offsets);
    db->metrics.index_build_s += seconds() - started;
    if (!f->index || mdb_bpt_validate(f->index)) {
        fail(e, MDB_NOMEM, "Cannot build recovery index");
        goto bad;
    }
    return v;
bad:
    view_drop(db, v, ti);
    (void)unlinkat(db->dirfd, filename, 0);
    return NULL;
}
static int install_view(mdb_db *db, size_t ti) {
    char temp[MDB_NAME_MAX + 32], target[MDB_NAME_MAX + 16];
    tempfile_name(temp, sizeof temp, &db->defs[ti]);
    (void)snprintf(target, sizeof target, "%s.bin", db->defs[ti].name);
    return renameat(db->dirfd, temp, db->dirfd, target);
}
static int recover(mdb_db *db, mdb_error *e) {
    double recovery_started = seconds();
    db->recovering = 1;
    rowset *sets = calloc(db->ntables, sizeof *sets);
    snapshot *s = calloc(1, sizeof *s);
    if (!sets || !s) {
        free(sets);
        free(s);
        return fail(e, MDB_NOMEM, "Allocation failed");
    }
    s->refs = 1;
    s->tables = calloc(db->ntables, sizeof *s->tables);
    int rc = -1;
    if (!s->tables) {
        fail(e, MDB_NOMEM, "Allocation failed");
        goto done;
    }
    for (size_t i = 0; i < db->base_tables; i++) {
        char name[MDB_NAME_MAX + 16];
        (void)snprintf(name, sizeof name, "%s.csv", db->defs[i].name);
        int fd = openat(db->basefd, name, O_RDONLY);
        if (fd < 0) {
            fail(e, MDB_CORRUPT, "Required baseline CSV missing");
            goto done;
        }
        int k = read_rows(fd, &db->defs[i], &sets[i], e);
        close(fd);
        if (k)
            goto done;
        uint64_t high = 0;
        for (size_t j = 0; j < sets[i].n; j++)
            if (sets[i].v[j]->v[0].i > 0 && (uint64_t)sets[i].v[j]->v[0].i > high)
                high = (uint64_t)sets[i].v[j]->v[0].i;
        db->defs[i].reserved = high;
    }
    if (wal_replay(db, sets, &s->version, e))
        goto done;
    for (size_t i = 0; i < db->ntables; i++) {
        size_t n = 0;
        for (size_t j = 0; j < sets[i].n; j++)
            if (sets[i].v[j])
                sets[i].v[n++] = sets[i].v[j];
        sets[i].n = n;
        s->tables[i] = build_view(db, i, sets[i].v, n, s->version, e);
        if (!s->tables[i])
            goto done;
        if (i == 0)
            fault("recovery_write");
    }
    for (size_t i = 0; i < db->nunique; i++) {
        unique_index *index = &db->unique[i];
        if (unique_build(index, s->tables[index->table], NULL, &index->items, &index->nitems, e))
            goto done;
    }
    for (size_t i = 0; i < db->ntables; i++)
        if (install_view(db, i)) {
            fail(e, MDB_IO, "Recovery binary replacement failed");
            goto done;
        }
    if (syncfd(db->dirfd)) {
        fail(e, MDB_IO, "Recovery directory synchronization failed");
        goto done;
    }
    db->current = s;
    s = NULL;
    db->recovering = 0;
    db->metrics.recovery_s = seconds() - recovery_started;
    rc = 0;
done:
    for (size_t i = 0; i < db->ntables; i++)
        rowset_clear(&sets[i], db->defs[i].ncols);
    free(sets);
    if (s) {
        if (s->tables)
            snapshot_drop(db, s);
        else
            free(s);
    }
    return rc;
}

static int configured_limit(const char *name, size_t fallback, size_t *out, mdb_error *e) {
    const char *s = getenv(name);
    if (!s) {
        *out = fallback;
        return 0;
    }
    int64_t v;
    if (parse_integer(s, &v) || v < 1 || (uint64_t)v > SIZE_MAX)
        return fail(e, MDB_LIMIT, "Invalid configured resource limit");
    *out = (size_t)v;
    return 0;
}
mdb_db *mdb_open(const char *directory, mdb_error *e) {
    if (e)
        memset(e, 0, sizeof *e);
    mdb_db *db = calloc(1, sizeof *db);
    if (!db) {
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    db->dirfd = db->basefd = db->lockfd = db->walfd = -1;
    db->healthy = 1;
    db->txnext = 1;
    if (configured_limit("MDB_MAX_SESSIONS", 256, &db->max_sessions, e) ||
        configured_limit("MDB_TX_MAX_CHANGES", 2000000, &db->max_changes, e) ||
        configured_limit("MDB_MAX_SORT_ROWS", 1000000, &db->max_sort_rows, e) ||
        configured_limit("MDB_MAX_GROUPS", 1000000, &db->max_groups, e) ||
        configured_limit("MDB_TX_MAX_LOCKS", 2000000, &db->max_locks, e))
        goto bad;
    if (pthread_mutex_init(&db->mutex, NULL)) {
        fail(e, MDB_IO, "Mutex initialization failed");
        goto bad;
    }
    db->mutex_ready = 1;
    if (pthread_cond_init(&db->lock_changed, NULL)) {
        fail(e, MDB_IO, "Lock condition initialization failed");
        goto bad;
    }
    db->cond_ready = 1;
    db->dirfd = open(directory, O_RDONLY | O_DIRECTORY);
    if (db->dirfd < 0) {
        fail(e, MDB_IO, "Cannot open data directory");
        goto bad;
    }
    db->lockfd = openat(db->dirfd, ".mdb.lock", O_CREAT | O_RDWR, 0600);
    if (db->lockfd < 0 || flock(db->lockfd, LOCK_EX | LOCK_NB)) {
        fail(e, MDB_BUSY, "Data directory is owned by another DBMS process");
        goto bad;
    }
    if (discover(db, e))
        goto bad;
    if (!exists_at(db->dirfd, ".mdb.init") && initialize(db, e))
        goto bad;
    if (verify_baseline(db, e))
        goto bad;
    db->base_tables = db->ntables;
    db->walfd = openat(db->dirfd, ".mdb.wal", O_RDWR | O_APPEND);
    if (db->walfd < 0) {
        fail(e, MDB_CORRUPT, "Required WAL missing");
        goto bad;
    }
    if (wal_catalog(db, e) || recover(db, e))
        goto bad;
    return db;
bad:
    mdb_close(db);
    return NULL;
}
void mdb_close(mdb_db *db) {
    if (!db)
        return;
    for (size_t i = 0; i < db->nunique; i++)
        free(db->unique[i].items);
    snapshot_drop(db, db->current);
    /* Unflushed bytes are never an acknowledged durable commit. */
    if (db->walfd >= 0)
        close(db->walfd);
    if (db->basefd >= 0)
        close(db->basefd);
    if (db->lockfd >= 0)
        close(db->lockfd);
    if (db->dirfd >= 0)
        close(db->dirfd);
    if (db->cond_ready)
        pthread_cond_destroy(&db->lock_changed);
    if (db->mutex_ready)
        pthread_mutex_destroy(&db->mutex);
    free(db->lock_buckets);
    free(db->committed);
    free(db->defs);
    free(db);
}
mdb_session *mdb_session_open(mdb_db *db, mdb_error *e) {
    if (!db) {
        fail(e, MDB_STATE, "Invalid engine");
        return NULL;
    }
    mdb_session *s = calloc(1, sizeof *s);
    if (!s) {
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    pthread_mutex_lock(&db->mutex);
    if (db->nsessions >= db->max_sessions) {
        pthread_mutex_unlock(&db->mutex);
        free(s);
        fail(e, MDB_LIMIT, "Session resource limit reached");
        return NULL;
    }
    db->nsessions++;
    pthread_mutex_unlock(&db->mutex);
    s->db = db;
    return s;
}
void mdb_session_close(mdb_session *s) {
    if (!s)
        return;
    pthread_mutex_lock(&s->db->mutex);
    tx_drop(s->db, s->tx);
    s->db->nsessions--;
    pthread_mutex_unlock(&s->db->mutex);
    free(s);
}
#ifdef MDB_TESTING
void mdb_test_read_hook(mdb_db *db, void (*fn)(void *), void *context) {
    pthread_mutex_lock(&db->mutex);
    db->read_hook = fn;
    db->read_hook_context = context;
    pthread_mutex_unlock(&db->mutex);
}
void mdb_test_scan_hook(mdb_db *db, void (*fn)(void *), void *context) {
    pthread_mutex_lock(&db->mutex);
    db->scan_hook = fn;
    db->scan_hook_context = context;
    pthread_mutex_unlock(&db->mutex);
}
#endif
int mdb_healthy(mdb_db *db) {
    pthread_mutex_lock(&db->mutex);
    int v = db->healthy;
    pthread_mutex_unlock(&db->mutex);
    return v;
}
int mdb_transaction_status(mdb_db *db, uint64_t id) {
    pthread_mutex_lock(&db->mutex);
    int found = 0;
    for (size_t i = 0; i < db->ncommitted; i++)
        if (db->committed[i] == id) {
            found = 1;
            break;
        }
    pthread_mutex_unlock(&db->mutex);
    return found;
}
static transaction *tx_begin(mdb_db *db, int need_id, mdb_error *e) {
    transaction *tx = calloc(1, sizeof *tx);
    if (!tx) {
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    pthread_mutex_lock(&db->mutex);
    tx->tables = calloc(db->ntables, sizeof *tx->tables);
    if (!tx->tables) {
        pthread_mutex_unlock(&db->mutex);
        free(tx);
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    if (!db->healthy) {
        fail(e, MDB_IO, "Engine requires recovery");
        goto bad;
    }
    if (need_id && reserve_txid(db, &tx->id, e))
        goto bad;
    tx->snap = db->current;
    tx->snap->refs++;
    db->active_transactions++;
    pthread_mutex_unlock(&db->mutex);
    return tx;
bad:
    pthread_mutex_unlock(&db->mutex);
    free(tx->tables);
    free(tx);
    return NULL;
}
/* Locks protect latest-state write/locking-read statements. Ordinary SELECT keeps
 * the BEGIN snapshot. Timeout aborts the waiting transaction; no unbounded deadlock. */
static snapshot *lock_key(mdb_db *db, transaction *tx, size_t ti,
                          int64_t id, int whole, mdb_error *e) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 1;
    pthread_mutex_lock(&db->mutex);
    for (;;) {
        int blocked = 0, owned = 0;
        if (whole) {
            for (row_lock *l = db->locks; l; l = l->next) {
                if (l->table != ti) continue;
                if (l->owner != tx) blocked = 1;
                else if (l->whole) owned = 1;
            }
        } else if (db->lock_capacity) {
            /* Check the table's whole/intent bucket and then this row's bucket. */
            for (int pass = 0; pass < (id ? 2 : 1); pass++) {
                int64_t key = pass ? id : 0;
                size_t b = lock_bucket(ti, key, db->lock_capacity);
                for (row_lock *l = db->lock_buckets[b]; l; l = l->hash_next) {
                    if (l->table != ti || l->id != key) continue;
                    if (l->owner == tx) {
                        if (l->whole || l->id == id) owned = 1;
                    } else if (l->whole || (id != 0 && l->id == id)) blocked = 1;
                }
            }
        }
        if (!db->healthy) { fail(e, MDB_IO, "Engine requires recovery"); break; }
        if (!blocked) {
            if (!owned) {
                if (tx->nlocks >= db->max_locks) { fail(e, MDB_LIMIT, "Transaction lock limit reached"); break; }
                if (lock_reserve(db)) { fail(e, MDB_NOMEM, "Lock index allocation failed"); break; }
                row_lock *l = malloc(sizeof *l);
                if (!l) { fail(e, MDB_NOMEM, "Lock allocation failed"); break; }
                size_t bucket = lock_bucket(ti, id, db->lock_capacity);
                *l = (row_lock){.next = db->locks, .hash_next = db->lock_buckets[bucket],
                                .owner = tx, .table = ti, .id = id, .whole = whole};
                db->locks = l;
                db->lock_buckets[bucket] = l;
                db->nlocks++;
                tx->nlocks++;
            }
            snapshot *latest = db->current;
            latest->refs++;
            pthread_mutex_unlock(&db->mutex);
            return latest;
        }
#ifdef MDB_TESTING
        if (db->lock_hook) db->lock_hook(db->lock_hook_context);
#endif
        int rc = pthread_cond_timedwait(&db->lock_changed, &db->mutex, &deadline);
        if (rc) { fail(e, MDB_BUSY, "Lock wait timed out; ROLLBACK before retry"); break; }
    }
    pthread_mutex_unlock(&db->mutex);
    return NULL;
}
static snapshot *lock_statement(mdb_db *db, transaction *tx, size_t ti,
                                const mdb_stmt *st, mdb_error *e) {
    int64_t id = 0;
    int whole = st->kind != MDB_INSERT && !mdb_expr_id(st->where, &id);
    return lock_key(db, tx, ti, id, whole, e);
}
#ifdef MDB_TESTING
void mdb_test_lock_hook(mdb_db *db, void (*hook)(void *), void *context) {
    pthread_mutex_lock(&db->mutex);
    db->lock_hook = hook; db->lock_hook_context = context;
    pthread_mutex_unlock(&db->mutex);
}
#endif
static int value_valid(const column *c, const mdb_value *v, mdb_error *e) {
    if (c->type != v->type)
        return fail(e, MDB_TYPE, "Value type mismatch");
    if (v->type == MDB_STRING) {
        size_t n = strlen(v->s);
        if (n > c->max)
            return fail(e, MDB_TOO_LONG, "VARCHAR value exceeds byte limit");
        if (!mdb_utf8(v->s, n))
            return fail(e, MDB_TYPE, "Invalid UTF-8 string");
    }
    return 0;
}
static int operand_type(const tabledef *t, const mdb_operand *o, mdb_type *type, mdb_error *e) {
    if (o->is_column) {
        int i = find_column(t, o->column);
        if (i < 0)
            return column_error(e, i, "Unknown column");
        *type = t->cols[i].type;
    } else {
        *type = o->value.type;
        if (*type == MDB_STRING && !mdb_utf8(o->value.s, strlen(o->value.s)))
            return fail(e, MDB_TYPE, "Invalid UTF-8 string");
    }
    return 0;
}
static int expr_valid(const tabledef *t, const mdb_expr *x, mdb_error *e) {
    if (!x)
        return 0;
    if (x->kind == MDB_AND || x->kind == MDB_OR) {
        if (expr_valid(t, x->left, e))
            return -1;
        return expr_valid(t, x->right, e);
    }
    mdb_type a = MDB_INT, b = MDB_INT;
    if (operand_type(t, &x->a, &a, e) || operand_type(t, &x->b, &b, e))
        return -1;
    if (a != b)
        return fail(e, MDB_TYPE, "Comparison type mismatch");
    return 0;
}
static const mdb_value *operand_value(const tabledef *t, const mdb_operand *o, const row *r) {
    return o->is_column ? &r->v[find_column(t, o->column)] : &o->value;
}
static int value_cmp(const mdb_value *a, const mdb_value *b) {
    if (a->type == MDB_INT)
        return (a->i > b->i) - (a->i < b->i);
    int c = strcmp(a->s, b->s);
    return (c > 0) - (c < 0);
}
static int expr_eval(const tabledef *t, const mdb_expr *x, const row *r) {
    if (!x)
        return 1;
    if (x->kind == MDB_AND)
        return expr_eval(t, x->left, r) && expr_eval(t, x->right, r);
    if (x->kind == MDB_OR)
        return expr_eval(t, x->left, r) || expr_eval(t, x->right, r);
    int c = value_cmp(operand_value(t, &x->a, r), operand_value(t, &x->b, r));
    switch (x->cmp) {
    case MDB_EQ:
        return c == 0;
    case MDB_NE:
        return c != 0;
    case MDB_LT:
        return c < 0;
    case MDB_LE:
        return c <= 0;
    case MDB_GT:
        return c > 0;
    case MDB_GE:
        return c >= 0;
    }
    return 0;
}
static int tx_set(mdb_db *db, transaction *tx, size_t ti, int64_t id, row *value, int deleted,
                  int inserted, mdb_error *e) {
    size_t ignored;
    int existing = map_get(&tx->tables[ti].map, id, &ignored);
    if (!existing && tx->nchanges >= db->max_changes)
        return fail(e, MDB_LIMIT, "Transaction change workspace limit reached");
    if (overlay_set(&tx->tables[ti], id, value, deleted, inserted, db->defs[ti].ncols))
        return fail(e, MDB_NOMEM, "Allocation failed");
    if (!existing)
        tx->nchanges++;
    return 0;
}
static int execute_insert(mdb_db *db, transaction *tx, size_t ti, const mdb_stmt *st,
                          mdb_result *out) {
    tabledef *t = &db->defs[ti];
    int positions[MDB_COL_MAX];
    memset(positions, -1, sizeof positions);
    if (st->nvalues != t->ncols - 1)
        return fail(&out->error, MDB_COLUMN_COUNT, "INSERT requires every nonautomatic column");
    if (st->ncolumns) {
        if (st->ncolumns != st->nvalues)
            return fail(&out->error, MDB_COLUMN_COUNT, "INSERT column/value count mismatch");
        for (size_t i = 0; i < st->ncolumns; i++) {
            int col = find_column(t, st->columns[i]);
            if (col < 0)
                return fail(&out->error, MDB_UNKNOWN_COLUMN, "Unknown column");
            if (col == 0)
                return fail(&out->error, MDB_TYPE, "Automatic id cannot be assigned");
            if (positions[col] >= 0)
                return fail(&out->error, MDB_COLUMN_COUNT, "Duplicate INSERT column");
            positions[col] = (int)i;
        }
    } else
        for (size_t i = 1; i < t->ncols; i++)
            positions[i] = (int)i - 1;
    for (size_t i = 1; i < t->ncols; i++) {
        if (positions[i] < 0)
            return fail(&out->error, MDB_COLUMN_COUNT, "Missing INSERT column");
        if (value_valid(&t->cols[i], &st->values[positions[i]], &out->error))
            return -1;
    }
    row *r = row_new(t->ncols);
    if (!r)
        return fail(&out->error, MDB_NOMEM, "Allocation failed");
    r->v[0].type = MDB_INT;
    for (size_t i = 1; i < t->ncols; i++)
        if (mdb_value_copy(&r->v[i], &st->values[positions[i]])) {
            row_drop(r, t->ncols);
            return fail(&out->error, MDB_NOMEM, "Allocation failed");
        }
    int64_t id;
    if (reserve_id(db, ti, &id, &out->error)) {
        row_drop(r, t->ncols);
        return -1;
    }
    snapshot *locked = lock_key(db, tx, ti, id, 0, &out->error);
    if (!locked) { row_drop(r, t->ncols); return -1; }
    pthread_mutex_lock(&db->mutex);
    snapshot_drop(db, locked);
    pthread_mutex_unlock(&db->mutex);
    r->v[0].i = id;
    if (tx_set(db, tx, ti, id, r, 0, 1, &out->error)) {
        row_drop(r, t->ncols);
        return -1;
    }
    tx->writes = 1;
    out->affected = 1;
    out->inserted_id = id;
    out->has_inserted_id = 1;
    return 0;
}
static int stage_change(mdb_db *db, transaction *tx, size_t ti, const mdb_stmt *st,
                         const int *positions, row *base, int inserted, mdb_result *out) {
    tabledef *t = &db->defs[ti];
    if (!expr_eval(t, st->where, base))
        return 0;
    int64_t id = base->v[0].i;
    row *changed = NULL;
    int deleted = st->kind == MDB_DELETE;
    if (!deleted) {
        changed = row_clone(base, t->ncols);
        if (!changed)
            return fail(&out->error, MDB_NOMEM, "Allocation failed");
        for (size_t j = 0; j < st->nupdates; j++) {
            int col = positions[j];
            mdb_value_clear(&changed->v[col]);
            if (mdb_value_copy(&changed->v[col], &st->updates[j].value)) {
                row_drop(changed, t->ncols);
                return fail(&out->error, MDB_NOMEM, "Allocation failed");
            }
        }
    }
    if (tx_set(db, tx, ti, id, changed, deleted, inserted, &out->error)) {
        row_drop(changed, t->ncols);
        return -1;
    }
    out->affected++;
    tx->writes = 1;
    return 0;
}
static int execute_changes(mdb_db *db, transaction *tx, size_t ti, const mdb_stmt *st,
                           mdb_result *out) {
    tabledef *t = &db->defs[ti];
    if (expr_valid(t, st->where, &out->error))
        return -1;
    int positions[MDB_COL_MAX];
    if (st->nupdates > MDB_COL_MAX)
        return fail(&out->error, MDB_LIMIT, "Too many assignments");
    for (size_t j = 0; j < st->nupdates; j++) {
        int k = find_column(t, st->updates[j].column);
        if (k < 0)
            return fail(&out->error, MDB_UNKNOWN_COLUMN, "Unknown UPDATE column");
        if (k == 0)
            return fail(&out->error, MDB_TYPE, "Automatic id cannot be modified");
        for (size_t i = 0; i < j; i++)
            if (positions[i] == k)
                return fail(&out->error, MDB_COLUMN_COUNT, "Duplicate UPDATE column");
        positions[j] = k;
        if (value_valid(&t->cols[k], &st->updates[j].value, &out->error))
            return -1;
    }
    overlay *o = &tx->tables[ti];
    view *v = tx->snap->tables[ti];
    int64_t candidate;
    const char *force = getenv("MDB_FORCE_SCAN");
    if (!(force && strcmp(force, "0")) && mdb_expr_id(st->where, &candidate)) {
        out->used_index = 1;
        size_t j;
        row *base = NULL;
        int inserted = 0;
        if (map_get(&o->map, candidate, &j)) {
            if (!o->v[j].deleted) {
                base = o->v[j].value;
                inserted = o->v[j].inserted;
            }
        } else {
            uint64_t offset;
            if (view_search(v, candidate, &offset))
                base = view_row(v, (size_t)((offset - MDB_BIN_HEADER_SIZE) / v->file->record_size));
        }
        return base ? stage_change(db, tx, ti, st, positions, base, inserted, out) : 0;
    }
    size_t original_changes =
        o->n; /* Existing rows retain their physical scan position; inserted rows follow. */
    for (size_t phase = 0; phase < 2; phase++) {
        size_t count = phase ? original_changes : v->n;
        for (size_t i = 0; i < count; i++) {
            row *base;
            int inserted = 0;
            int64_t id;
            if (phase) {
                if (!o->v[i].inserted || o->v[i].deleted)
                    continue;
                base = o->v[i].value;
                inserted = 1;
            } else {
                base = view_row(v, i);
                if (!base)
                    continue;
                id = base->v[0].i;
                size_t j;
                if (map_get(&o->map, id, &j)) {
                    if (o->v[j].deleted)
                        continue;
                    base = o->v[j].value;
                }
            }
            if (stage_change(db, tx, ti, st, positions, base, inserted, out))
                return -1;
        }
    }
    return 0;
}
/* WHERE consumes the same snapshot/overlay stream for grouped and plain SELECT.
 * Group keys own only their typed values, never references into a mutable row. */
typedef struct {
    mdb_group *state;
    int projection[MDB_COL_MAX]; /* -1 denotes COUNT(*), otherwise a group key. */
    int order[MDB_COL_MAX], descending[MDB_COL_MAX];
} group_plan;
static int group_key(const int *positions, size_t count, int column) {
    for (size_t i = 0; i < count; i++)
        if (positions[i] == column)
            return (int)i;
    return -1;
}
static int group_prepare(mdb_db *db, const tabledef *t, const mdb_stmt *st,
                          group_plan *plan, mdb_result *out) {
    int positions[MDB_COL_MAX];
    for (size_t i = 0; i < st->ngroup_by; i++) {
        int col = find_column(t, st->group_by[i]);
        if (col < 0)
            return column_error(&out->error, col, "Unknown GROUP BY column");
        if (group_key(positions, i, col) >= 0)
            return fail(&out->error, MDB_COLUMN_COUNT, "Duplicate GROUP BY column");
        positions[i] = col;
    }
    out->ncolumns = st->star ? t->ncols : st->ncolumns;
    out->columns = calloc(out->ncolumns, sizeof *out->columns);
    out->types = calloc(out->ncolumns, sizeof *out->types);
    if (!out->columns || !out->types)
        return fail(&out->error, MDB_NOMEM, "Allocation failed");
    for (size_t i = 0; i < out->ncolumns; i++) {
        if (!st->star && st->column_counts && st->column_counts[i]) {
            plan->projection[i] = -1;
            out->columns[i] = strdup("count");
            out->types[i] = MDB_INT;
        } else {
            int col = st->star ? (int)i : find_column(t, st->columns[i]);
            if (col < 0)
                return column_error(&out->error, col, "Unknown SELECT column");
            int key = group_key(positions, st->ngroup_by, col);
            if (key < 0)
                return fail(&out->error, MDB_SYNTAX,
                            "Every non-aggregate SELECT column must appear in GROUP BY");
            plan->projection[i] = key;
            out->columns[i] = strdup(t->cols[col].name);
            out->types[i] = t->cols[col].type;
        }
        if (!st->star && st->aliases && st->aliases[i]) {
            free(out->columns[i]);
            out->columns[i] = strdup(st->aliases[i]);
        }
        if (!out->columns[i])
            return fail(&out->error, MDB_NOMEM, "Allocation failed");
    }
    for (size_t i = 0; i < st->norder; i++) {
        plan->descending[i] = st->order[i].descending;
        if (st->order[i].count)
            plan->order[i] = -1;
        else {
            int col = find_column(t, st->order[i].column);
            if (col < 0)
                return column_error(&out->error, col, "Unknown ORDER BY column");
            int key = group_key(positions, st->ngroup_by, col);
            if (key < 0)
                return fail(&out->error, MDB_SYNTAX,
                            "ORDER BY column must appear in GROUP BY or use COUNT(*)");
            plan->order[i] = key;
        }
    }
    plan->state = mdb_group_create(positions, st->ngroup_by, db->max_groups, &out->error);
    return plan->state ? 0 : -1;
}
static int group_result(const group_plan *plan, const mdb_stmt *st, mdb_result *out) {
    size_t total = mdb_group_size(plan->state);
    size_t start = (uint64_t)st->offset < total ? (size_t)st->offset : total;
    size_t count = total - start;
    if (st->limit >= 0 && (uint64_t)st->limit < count)
        count = (size_t)st->limit;
    if (count > MDB_RESULT_MAX)
        return fail(&out->error, MDB_LIMIT, "Grouped result exceeds 10000 rows; use LIMIT");
    if (!count)
        return 0;
    size_t *order = NULL;
    if (mdb_group_order(plan->state, plan->order, plan->descending, st->norder,
                        &order, &out->error))
        return -1;
    out->rows = calloc(count, sizeof *out->rows);
    if (!out->rows) {
        free(order);
        return fail(&out->error, MDB_NOMEM, "Allocation failed");
    }
    out->nrows = count;
    int rc = -1;
    for (size_t i = 0; i < count; i++) {
        out->rows[i].values = calloc(out->ncolumns, sizeof(mdb_value));
        if (!out->rows[i].values) {
            fail(&out->error, MDB_NOMEM, "Allocation failed");
            goto done;
        }
        const mdb_value *keys = mdb_group_keys(plan->state, order[start + i]);
        for (size_t j = 0; j < out->ncolumns; j++) {
            int key = plan->projection[j];
            if (key < 0)
                out->rows[i].values[j] = (mdb_value){
                    .type = MDB_INT, .i = mdb_group_count(plan->state, order[start + i])};
            else if (mdb_value_copy(&out->rows[i].values[j], &keys[key])) {
                fail(&out->error, MDB_NOMEM, "Allocation failed");
                goto done;
            }
        }
    }
    rc = 0;
done:
    free(order);
    return rc;
}
#include "analytics.inc"
typedef struct {
    analytics *analytics;
    row **rows;
    size_t n, cap;
    uint64_t count;
    const tabledef *table;
    const mdb_stmt *stmt;
    mdb_error *error;
    size_t max_sort_rows;
    mdb_group *group;
} selection;
/* Takes ownership of r. Return 1 when an unordered LIMIT has been satisfied. */
static int select_visit(selection *s, row *r) {
    if (!expr_eval(s->table, s->stmt->where, r)) {
        row_drop(r, s->table->ncols);
        return 0;
    }
    if (s->analytics) {
        int rc = analytics_visit(s->analytics, r->v, s->error);
        row_drop(r, s->table->ncols);
        return rc;
    }
    if (s->group) {
        int rc = mdb_group_add(s->group, r->v, s->error);
        row_drop(r, s->table->ncols);
        return rc;
    }
    if (s->count == UINT64_MAX) {
        row_drop(r, s->table->ncols);
        return fail(s->error, MDB_LIMIT, "COUNT overflow");
    }
    s->count++;
    if (s->stmt->count) {
        row_drop(r, s->table->ncols);
        return 0;
    }
    if (!s->stmt->norder && s->count <= (uint64_t)s->stmt->offset) {
        row_drop(r, s->table->ncols);
        return 0;
    }
    if (!s->stmt->norder && s->stmt->limit >= 0 && s->n >= (uint64_t)s->stmt->limit) {
        row_drop(r, s->table->ncols);
        return 1;
    }
    if (s->stmt->limit < 0 && s->n >= MDB_RESULT_MAX) {
        row_drop(r, s->table->ncols);
        return fail(s->error, MDB_LIMIT, "Result exceeds 10000 rows; use LIMIT");
    }
    if (s->stmt->norder && s->n >= s->max_sort_rows) {
        row_drop(r, s->table->ncols);
        return fail(s->error, MDB_LIMIT, "ORDER BY workspace limit reached");
    }
    if (grow((void **)&s->rows, &s->cap, s->n + 1, sizeof *s->rows)) {
        row_drop(r, s->table->ncols);
        return fail(s->error, MDB_NOMEM, "Allocation failed");
    }
    s->rows[s->n++] = r;
    return 0;
}
static int sort_compare(const selection *s, const row *a, const row *b) {
    for (size_t i = 0; i < s->stmt->norder; i++) {
        int col = find_column(s->table, s->stmt->order[i].column);
        int c = value_cmp(&a->v[col], &b->v[col]);
        if (c)
            return s->stmt->order[i].descending ? -c : c;
    }
    return 0;
}
static int stable_sort(selection *s) {
    if (s->n < 2)
        return 0;
    row **temp = malloc(s->n * sizeof *temp);
    if (!temp)
        return fail(s->error, MDB_NOMEM, "Allocation failed");
    for (size_t width = 1; width < s->n;) {
        for (size_t start = 0; start < s->n; start += 2 * width) {
            size_t mid = start + width < s->n ? start + width : s->n,
                   end = mid + width < s->n ? mid + width : s->n, a = start, b = mid, k = start;
            while (a < mid && b < end)
                temp[k++] =
                    sort_compare(s, s->rows[a], s->rows[b]) <= 0 ? s->rows[a++] : s->rows[b++];
            while (a < mid)
                temp[k++] = s->rows[a++];
            while (b < end)
                temp[k++] = s->rows[b++];
        }
        memcpy(s->rows, temp, s->n * sizeof *temp);
        if (width > s->n / 2)
            break;
        width *= 2;
    }
    free(temp);
    return 0;
}
static int result_columns(mdb_result *out, const tabledef *t, const mdb_stmt *st, int *positions) {
    out->ncolumns = st->count ? 1 : (st->star ? t->ncols : st->ncolumns);
    out->columns = calloc(out->ncolumns, sizeof *out->columns);
    out->types = calloc(out->ncolumns, sizeof *out->types);
    if (!out->columns || !out->types)
        return fail(&out->error, MDB_NOMEM, "Allocation failed");
    for (size_t i = 0; i < out->ncolumns; i++) {
        if (st->count) {
            out->columns[i] = strdup("count");
            out->types[i] = MDB_INT;
            positions[i] = 0;
        } else {
            int col = st->star ? (int)i : find_column(t, st->columns[i]);
            if (col < 0)
                return column_error(&out->error, col, "Unknown SELECT column");
            positions[i] = col;
            out->columns[i] = strdup(t->cols[col].name);
            out->types[i] = t->cols[col].type;
        }
        if (!st->star && st->aliases && st->aliases[i]) {
            free(out->columns[i]);
            out->columns[i] = strdup(st->aliases[i]);
        }
        if (!out->columns[i])
            return fail(&out->error, MDB_NOMEM, "Allocation failed");
    }
    return 0;
}
typedef struct {
    const view *snapshot_view;
    const overlay *changes;
    size_t slot, change;
} join_cursor;

/* Borrow rows from the pinned snapshot and this session's own overlay. */
static const row *join_next(join_cursor *cursor) {
    while (cursor->slot < cursor->snapshot_view->n) {
        const row *r = view_row(cursor->snapshot_view, cursor->slot++);
        if (!r)
            continue;
        size_t index;
        if (!map_get(&cursor->changes->map, r->v[0].i, &index))
            return r;
        const change *c = &cursor->changes->v[index];
        if (!c->deleted)
            return c->value;
    }
    while (cursor->change < cursor->changes->n) {
        const change *c = &cursor->changes->v[cursor->change++];
        if (c->inserted && !c->deleted)
            return c->value;
    }
    return NULL;
}

typedef struct {
    const row *value;
    const mdb_value *key;
    size_t sequence;
} join_entry;

static int join_compare(const void *a, const void *b) {
    const join_entry *left = a, *right = b;
    int key_order = value_cmp(left->key, right->key);
    if (key_order)
        return key_order;
    return (left->sequence > right->sequence) - (left->sequence < right->sequence);
}

static size_t join_lower_bound(const join_entry *entries, size_t count, const mdb_value *key) {
    size_t first = 0;
    while (first < count) {
        size_t middle = first + (count - first) / 2;
        if (value_cmp(entries[middle].key, key) < 0)
            first = middle + 1;
        else
            count = middle;
    }
    return first;
}

static int join_visit(selection *found, const row *left, const row *right, size_t split) {
    size_t ncols = found->table->ncols;
    row *combined = row_new(ncols);
    if (!combined)
        return fail(found->error, MDB_NOMEM, "JOIN row allocation failed");
    for (size_t i = 0; i < ncols; i++) {
        const mdb_value *source = i < split ? &left->v[i] : &right->v[i - split];
        if (mdb_value_copy(&combined->v[i], source)) {
            row_drop(combined, ncols);
            return fail(found->error, MDB_NOMEM, "JOIN row allocation failed");
        }
    }
    return select_visit(found, combined);
}

/* Sort right-side keys once, then probe for each visible left-side row. */
static int select_join(mdb_db *db, transaction *tx, size_t left_table, size_t right_table,
                       int left_key, int right_key, selection *found) {
    join_cursor left = {.snapshot_view = tx->snap->tables[left_table], .changes = &tx->tables[left_table]};
    join_cursor right = {.snapshot_view = tx->snap->tables[right_table], .changes = &tx->tables[right_table]};
    const row *left_row = join_next(&left);
    if (!left_row)
        return 0;
    join_entry *entries = NULL;
    size_t count = 0, capacity = 0;
    int rc = -1;
    const row *right_row;
    while ((right_row = join_next(&right))) {
        if (count >= db->max_sort_rows) {
            fail(found->error, MDB_LIMIT, "JOIN workspace limit reached");
            goto done;
        }
        if (grow((void **)&entries, &capacity, count + 1, sizeof *entries)) {
            fail(found->error, MDB_NOMEM, "JOIN workspace allocation failed");
            goto done;
        }
        entries[count] = (join_entry){right_row, &right_row->v[right_key], count};
        count++;
    }
    if (count > 1)
        qsort(entries, count, sizeof *entries, join_compare);
    rc = 0;
    if (count == 0)
        goto done;
    do {
        const mdb_value *key = &left_row->v[left_key];
        for (size_t i = join_lower_bound(entries, count, key);
             i < count && value_cmp(entries[i].key, key) == 0; i++) {
            int visit = join_visit(found, left_row, entries[i].value, found->table->right_start);
            if (visit) {
                rc = visit < 0 ? -1 : 0;
                goto done;
            }
        }
    } while ((left_row = join_next(&left)));
done:
    free(entries);
    return rc;
}

static int execute_select(mdb_db *db, transaction *tx, size_t ti, const mdb_stmt *st,
                          mdb_result *out) {
#ifdef MDB_TESTING
    void (*hook)(void *);
    void *hook_context;
    pthread_mutex_lock(&db->mutex);
    hook = db->read_hook;
    hook_context = db->read_hook_context;
    pthread_mutex_unlock(&db->mutex);
    if (hook)
        hook(hook_context);
#endif
    if (st->limit > MDB_RESULT_MAX)
        return fail(&out->error, MDB_LIMIT, "LIMIT exceeds maximum result rows (10000)");
    const tabledef *t = &db->defs[ti];
    tabledef scoped;
    int right_table = -1, left_key = 0, right_key = 0;
    if (st->join_table || st->table_alias) {
        scoped = *t;
        scoped.source_names[0] = st->table_alias ? st->table_alias : st->table;
        t = &scoped;
    }
    if (st->join_table) {
        right_table = find_table(db, st->join_table);
        if (right_table < 0)
            return fail(&out->error, MDB_UNKNOWN_TABLE, "Unknown JOIN table");
        const tabledef *right = &db->defs[right_table];
        if (scoped.ncols + right->ncols > MDB_COL_MAX)
            return fail(&out->error, MDB_LIMIT, "JOIN input exceeds the combined column limit (32)");
        scoped.source_names[1] = st->join_alias ? st->join_alias : st->join_table;
        if (!strcmp(scoped.source_names[0], scoped.source_names[1]))
            return fail(&out->error, MDB_SYNTAX, "JOIN requires distinct table names or aliases");
        scoped.right_start = scoped.ncols;
        memcpy(scoped.cols + scoped.ncols, right->cols, right->ncols * sizeof *right->cols);
        scoped.ncols += right->ncols;
        int a = find_column(t, st->join_a), b = find_column(t, st->join_b);
        if (a < 0 || b < 0)
            return column_error(&out->error, a < 0 ? a : b, "Unknown JOIN column");
        int a_on_right = (size_t)a >= scoped.right_start;
        int b_on_right = (size_t)b >= scoped.right_start;
        if (a_on_right == b_on_right)
            return fail(&out->error, MDB_SYNTAX, "JOIN ON must compare a column from each table");
        if (scoped.cols[a].type != scoped.cols[b].type)
            return fail(&out->error, MDB_TYPE, "JOIN column type mismatch");
        left_key = a_on_right ? b : a;
        right_key = (a_on_right ? a : b) - (int)scoped.right_start;
    }
    if (expr_valid(t, st->where, &out->error))
        return -1;
    int extended = analytics_needed(st);
    /* Resolve output aliases only in ORDER BY, never WHERE or GROUP BY.
     * The AST arena owns strings; these temporary order items borrow them. */
    mdb_stmt resolved = *st;
    mdb_order orders[MDB_COL_MAX];
    if (st->norder && !extended) {
        memcpy(orders, st->order, st->norder * sizeof *orders);
        for (size_t i = 0; i < st->norder; i++) {
            if (orders[i].count)
                continue;
            for (size_t j = 0; j < st->ncolumns; j++)
                if (st->aliases && st->aliases[j] && !strcmp(orders[i].column, st->aliases[j])) {
                    orders[i].column = st->columns[j];
                    orders[i].count = st->column_counts && st->column_counts[j];
                    break;
                }
        }
        resolved.order = orders;
        st = &resolved;
    }
    group_plan grouped = {0};
    int positions[MDB_COL_MAX] = {0};
    analytics analysis = {0};
    if (extended) {
        if (analytics_prepare(db, t, st, &analysis, out)) {
            analytics_clear(&analysis);
            return -1;
        }
    } else if (st->ngroup_by) {
        if (group_prepare(db, t, st, &grouped, out)) {
            mdb_group_free(grouped.state);
            return -1;
        }
    } else {
        for (size_t i = 0; i < st->ncolumns; i++)
            if (st->column_counts && st->column_counts[i] && !st->count)
                return fail(&out->error, MDB_SYNTAX,
                            "Mixed COUNT(*) projections require GROUP BY");
        for (size_t i = 0; i < st->norder; i++) {
            if (st->order[i].count) {
                if (!st->count)
                    return fail(&out->error, MDB_SYNTAX,
                                "ORDER BY COUNT(*) requires grouping or COUNT(*) projection");
            } else {
                int col = find_column(t, st->order[i].column);
                if (col < 0)
                    return column_error(&out->error, col, "Unknown ORDER BY column");
            }
        }
        if (result_columns(out, t, st, positions))
            return -1;
    }
    selection found = {.analytics = extended ? &analysis : NULL, .table = t, .stmt = st, .error = &out->error,
                       .max_sort_rows = db->max_sort_rows, .group = grouped.state};
    overlay *o = &tx->tables[ti];
    view *v = tx->snap->tables[ti];
    int rc = -1;
    int64_t id;
    const char *force = getenv("MDB_FORCE_SCAN");
    int indexed = right_table < 0 && !(force && strcmp(force, "0")) && mdb_expr_id(st->where, &id);
    out->used_index = indexed;
    if (!extended && st->limit == 0 && (!st->count || st->ngroup_by)) {
        rc = 0;
        goto done;
    }
    if (right_table >= 0) {
        if (select_join(db, tx, ti, (size_t)right_table, left_key, right_key, &found))
            goto done;
    } else if (indexed) {
        uint64_t offset; /* Always execute the real tree search, including for own inserted rows. */
        int exists = view_search(v, id, &offset);
        row *r = NULL;
        if (exists) {
            r = view_read(t, v, offset, &out->error);
            if (!r)
                goto done;
            if (r->v[0].i != id) {
                row_drop(r, t->ncols);
                fail(&out->error, MDB_CORRUPT, "Index/binary key mismatch");
                goto done;
            }
        }
        size_t j;
        if (map_get(&o->map, id, &j)) {
            row_drop(r, t->ncols);
            r = NULL;
            if (!o->v[j].deleted) {
                r = row_clone(o->v[j].value, t->ncols);
                if (!r) {
                    fail(&out->error, MDB_NOMEM, "Allocation failed");
                    goto done;
                }
            }
        }
        if (r && select_visit(&found, r) < 0)
            goto done;
    } else {
        binary_reader reader = {0};
#ifdef MDB_TESTING
        pthread_mutex_lock(&db->mutex);
        reader.hook = db->scan_hook;
        reader.hook_context = db->scan_hook_context;
        pthread_mutex_unlock(&db->mutex);
#endif
        int stop = 0;
        for (size_t slot = 0; slot < v->n; slot++) {
            if (!view_row(v, slot))
                continue;
            uint64_t offset = MDB_BIN_HEADER_SIZE + (uint64_t)slot * v->file->record_size;
            row *r = view_read_cached(t, v, offset, &reader, &out->error);
            int k;
            if (!r)
                goto done;
            size_t j;
            if (map_get(&o->map, r->v[0].i, &j)) {
                row_drop(r, t->ncols);
                r = NULL;
                if (!o->v[j].deleted) {
                    r = row_clone(o->v[j].value, t->ncols);
                    if (!r) {
                        fail(&out->error, MDB_NOMEM, "Allocation failed");
                        goto done;
                    }
                }
            }
            if (r) {
                k = select_visit(&found, r);
                if (k < 0)
                    goto done;
                if (k > 0) {
                    stop = 1;
                    break;
                }
            }
        }
        if (!stop)
            for (size_t j = 0; j < o->n; j++)
                if (o->v[j].inserted && !o->v[j].deleted) {
                    row *r = row_clone(o->v[j].value, t->ncols);
                    if (!r) {
                        fail(&out->error, MDB_NOMEM, "Allocation failed");
                        goto done;
                    }
                    int k = select_visit(&found, r);
                    if (k < 0)
                        goto done;
                    if (k > 0)
                        break;
                }
    }
    if (extended) {
        rc = analytics_result(&analysis, out);
        goto done;
    }
    if (grouped.state) {
        rc = group_result(&grouped, st, out);
        goto done;
    }
    if (st->count) {
        if (found.count > INT64_MAX) {
            fail(&out->error, MDB_LIMIT, "COUNT exceeds int64 range");
            goto done;
        }
        if (st->limit != 0 && st->offset == 0) {
            out->rows = calloc(1, sizeof *out->rows);
            if (!out->rows) {
                fail(&out->error, MDB_NOMEM, "Allocation failed");
                goto done;
            }
            out->nrows = 1;
            out->rows[0].values = calloc(1, sizeof(mdb_value));
            if (!out->rows[0].values) {
                fail(&out->error, MDB_NOMEM, "Allocation failed");
                goto done;
            }
            out->rows[0].values[0] = (mdb_value){.type = MDB_INT, .i = (int64_t)found.count};
        }
    } else {
        if (st->norder && stable_sort(&found))
            goto done;
        size_t start = st->norder ? ((uint64_t)st->offset < found.n ? (size_t)st->offset : found.n) : 0;
        size_t count = found.n - start;
        if (st->limit >= 0 && (uint64_t)st->limit < count)
            count = (size_t)st->limit;
        if (count > MDB_RESULT_MAX) {
            fail(&out->error, MDB_LIMIT, "Result exceeds 10000 rows; use LIMIT");
            goto done;
        }
        out->rows = calloc(count ? count : 1, sizeof *out->rows);
        if (!out->rows) {
            fail(&out->error, MDB_NOMEM, "Allocation failed");
            goto done;
        }
        out->nrows = count;
        for (size_t i = 0; i < count; i++) {
            out->rows[i].values = calloc(out->ncolumns, sizeof(mdb_value));
            if (!out->rows[i].values) {
                fail(&out->error, MDB_NOMEM, "Allocation failed");
                goto done;
            }
            for (size_t j = 0; j < out->ncolumns; j++)
                if (mdb_value_copy(&out->rows[i].values[j], &found.rows[start + i]->v[positions[j]])) {
                    fail(&out->error, MDB_NOMEM, "Allocation failed");
                    goto done;
                }
        }
    }
    rc = 0;
done:
    for (size_t i = 0; i < found.n; i++)
        row_drop(found.rows[i], t->ncols);
    free(found.rows);
    mdb_group_free(grouped.state);
    if (extended) analytics_clear(&analysis);
    return rc;
}
/* Prepare immutable row versions without copying the table or writing data files.
 * Only changed radix paths are copied; existing ID-to-slot mappings stay stable. */
static snapshot *prepare_commit(mdb_db *db, transaction *tx, mdb_error *e) {
    snapshot *s = calloc(1, sizeof *s);
    if (!s) {
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    s->refs = 1;
    s->version = db->current->version + 1;
    s->tables = calloc(db->ntables, sizeof *s->tables);
    if (!s->tables) {
        free(s);
        fail(e, MDB_NOMEM, "Allocation failed");
        return NULL;
    }
    for (size_t ti = 0; ti < db->ntables; ti++) {
        overlay *o = &tx->tables[ti];
        view *old = db->current->tables[ti];
        if (!o->n) {
            s->tables[ti] = old;
            old->refs++;
            continue;
        }
        view *v = calloc(1, sizeof *v);
        if (!v) {
            fail(e, MDB_NOMEM, "Allocation failed");
            goto bad;
        }
        s->tables[ti] = v;
        v->refs = 1;
        v->version = s->version;
        v->n = old->n;
        v->file = old->file;
        v->file->refs++;
        v->rows = mdb_slots_clone(old->rows);
        if (!v->rows) {
            fail(e, MDB_NOMEM, "Allocation failed");
            goto bad;
        }
        mdb_binary_column columns[MDB_COL_MAX];
        size_t size = binary_columns(&db->defs[ti], columns);
        unsigned char buffer[BINARY_RECORD_MAX];
        for (size_t j = 0; j < o->n; j++) {
            change *c = &o->v[j];
            if (c->inserted && c->deleted)
                continue;
            if (c->inserted) {
                if (v->n >= ((uint64_t)INT64_MAX - MDB_BIN_HEADER_SIZE) / size) {
                    fail(e, MDB_LIMIT, "Binary table size overflow");
                    goto bad;
                }
                c->slot = v->n++;
            } else {
                uint64_t offset;
                if (!view_search(old, c->id, &offset)) {
                    fail(e, MDB_CORRUPT, "Changed row is missing from snapshot index");
                    goto bad;
                }
                c->slot = (size_t)((offset - MDB_BIN_HEADER_SIZE) / size);
            }
            const char *io = getenv("MDB_IO_FAIL");
            if (io && strcmp(io, "binary_prepare") == 0) {
                fail(e, MDB_IO, "Injected binary preparation failure");
                goto bad;
            }
            mdb_value tombstone = {.type = MDB_INT, .i = c->id};
            const mdb_value *values = c->deleted ? &tombstone : c->value->v;
            if (mdb_binary_encode(columns, db->defs[ti].ncols, values, c->deleted,
                                  buffer, size, e))
                goto bad;
            if (mdb_slots_set(v->rows, c->slot, c->deleted ? NULL : c->value)) {
                fail(e, MDB_NOMEM, "Allocation failed");
                goto bad;
            }
        }
    }
    return s;
bad:
    snapshot_drop(db, s);
    return NULL;
}
/* WAL is already durable. Failure here MUST stop the engine for recovery. */
static int apply_table(mdb_db *db, transaction *tx, snapshot *next, size_t ti) {
    tabledef *t = &db->defs[ti];
    overlay *o = &tx->tables[ti];
    tablefile *f = next->tables[ti]->file;
    mdb_binary_column columns[MDB_COL_MAX];
    size_t size = binary_columns(t, columns);
    unsigned char buffer[BINARY_RECORD_MAX];
    double started = seconds(), index_elapsed = 0;
    int rc = -1;
    pthread_rwlock_wrlock(&f->lock);
    /* Old readers must switch to retained versions even if a write fails halfway. */
    f->version = next->version;
    for (size_t j = 0; j < o->n; j++) {
        change *c = &o->v[j];
        if (c->inserted && c->deleted)
            continue;
        mdb_value tombstone = {.type = MDB_INT, .i = c->id};
        const mdb_value *values = c->deleted ? &tombstone : c->value->v;
        if (mdb_binary_encode(columns, t->ncols, values, c->deleted, buffer, size, NULL))
            goto done;
        uint64_t offset = MDB_BIN_HEADER_SIZE + (uint64_t)c->slot * size;
        const char *fp = getenv("MDB_FAILPOINT"), *io = getenv("MDB_IO_FAIL");
        if ((fp && !strcmp(fp, "binary_partial")) || (io && !strcmp(io, "binary_write"))) {
            (void)full_pwrite(db, f->fd, buffer, size / 2, offset);
            fault("binary_partial");
            goto done;
        }
        if (full_pwrite(db, f->fd, buffer, size, offset))
            goto done;
        db->metrics.storage_bytes += size;
        if (c->inserted) {
            if (io && !strcmp(io, "index_insert"))
                goto done;
            double index_started = seconds();
            int inserted = mdb_bpt_insert(f->index, c->id, offset);
            double elapsed = seconds() - index_started;
            db->metrics.index_build_s += elapsed;
            index_elapsed += elapsed;
            if (inserted)
                goto done;
        }
    }
    double sync_started = seconds();
    int sync_result = syncfd(f->fd);
    db->metrics.data_sync_s += seconds() - sync_started;
    if (sync_result)
        goto done;
    rc = 0;
done:
    pthread_rwlock_unlock(&f->lock);
    db->metrics.storage_write_s += seconds() - started - index_elapsed;
    return rc;
}
static int commit_tx(mdb_db *db, transaction *tx, mdb_result *out) {
    pthread_mutex_lock(&db->mutex);
    snapshot *next = NULL;
    unique_item *unique_next[UNIQUE_MAX] = {0};
    size_t unique_counts[UNIQUE_MAX] = {0};
    int rc = -1;
    out->txid = tx->id;
    if (!db->healthy) {
        fail(&out->error, MDB_IO, "Engine requires recovery");
        goto done;
    }
    if (!tx->writes) {
        out->commit = MDB_NO_CHANGES;
        rc = 0;
        goto done;
    }
    if (db->current->version == UINT64_MAX) {
        fail(&out->error, MDB_LIMIT, "Commit version exhausted");
        goto done;
    }
    if (ledger_prepare(db)) {
        fail(&out->error, MDB_NOMEM, "Allocation failed");
        goto done;
    }
    double stage_started = seconds();
    next = prepare_commit(db, tx, &out->error);
    db->metrics.prepare_s += seconds() - stage_started;
    if (!next)
        goto done;
    /* Validate and reserve every affected UNIQUE index before any commit WAL.
     * A failure leaves the published snapshot/indexes and all other tables intact. */
    for (size_t i = 0; i < db->nunique; i++) {
        unique_index *index = &db->unique[i];
        if (tx->tables[index->table].n &&
            unique_build(index, next->tables[index->table], NULL, &unique_next[i], &unique_counts[i], &out->error))
            goto done;
    }
    stage_started = seconds();
    unsigned char begin[8];
    put64(begin, db->current->version);
    if (wal_append(db, LOG_BEGIN, tx->id, begin, 8))
        goto unknown;
    uint64_t count = 0, digest = FNV_INIT;
    unsigned char record[CSV_RECORD_MAX + 128];
    for (size_t ti = 0; ti < db->ntables; ti++)
        for (size_t j = 0; j < tx->tables[ti].n; j++) {
            change *c = &tx->tables[ti].v[j];
            if (c->inserted && c->deleted)
                continue;
            size_t n;
            encode_change(db, ti, c, record, &n);
            if (wal_append(db, LOG_ROW, tx->id, record, n))
                goto unknown;
            count++;
            digest = hashbytes(digest, record, n);
        }
    unsigned char commit[24];
    put64(commit, count);
    put64(commit + 8, digest);
    put64(commit + 16, next->version);
    if (wal_append(db, LOG_COMMIT, tx->id, commit, sizeof commit))
        goto unknown;
    db->metrics.wal_records_s += seconds() - stage_started;
    fault("before_wal_flush");
    /* COMMIT may be in RAM until here. No successful response or data writes
     * precede flushing every WAL byte through this commit and syncing it. */
    stage_started = seconds();
    if (wal_flush(db))
        goto unknown;
    db->metrics.wal_records_s += seconds() - stage_started;
    fault("after_wal_flush");
    fault("before_commit_sync");
    const char *io = getenv("MDB_IO_FAIL");
    double wal_started = seconds();
    if ((io && strcmp(io, "wal_fsync") == 0) || syncfd(db->walfd))
        goto unknown;
    db->metrics.wal_sync_s += seconds() - wal_started;
    db->metrics.commit_sync_s += seconds() - wal_started;
    out->commit = MDB_DURABLE;
    db->committed[db->ncommitted++] = tx->id;
    fault("after_commit_sync");
    for (size_t ti = 0; ti < db->ntables; ti++)
        if (tx->tables[ti].n) {
            if (apply_table(db, tx, next, ti))
                goto durable_failure;
            fault("after_binary_write");
        }
    stage_started = seconds();
    int directory_synced = syncfd(db->dirfd);
    db->metrics.directory_sync_s += seconds() - stage_started;
    if (directory_synced)
        goto durable_failure;
    stage_started = seconds();
    snapshot *old = db->current;
    db->current = next;
    next = NULL;
    for (size_t i = 0; i < db->nunique; i++)
        if (unique_next[i]) {
            free(db->unique[i].items);
            db->unique[i].items = unique_next[i];
            db->unique[i].nitems = unique_counts[i];
            unique_next[i] = NULL;
        }
    snapshot_drop(db, old);
    db->metrics.publish_s += seconds() - stage_started;
    fault("before_applied");
    stage_started = seconds();
    wal_started = seconds();
    if (wal_append(db, LOG_APPLIED, tx->id, NULL, 0) || wal_sync(db))
        goto durable_failure;
    db->metrics.wal_sync_s += seconds() - wal_started;
    db->metrics.completion_s += seconds() - stage_started;
    rc = 0;
    goto done;
unknown:
    out->commit = MDB_UNKNOWN;
    db->healthy = 0;
    out->recovery_required = 1;
    fail(&out->error, MDB_IO, "Commit outcome unknown; restart to recover before further SQL");
    goto done;
durable_failure:
    out->commit = MDB_DURABLE;
    db->healthy = 0;
    out->recovery_required = 1;
    fail(&out->error, MDB_IO,
         "Commit is durable; binary/completion needs recovery before further SQL");
done:
    for (size_t i = 0; i < db->nunique; i++)
        free(unique_next[i]);
    if (next)
        snapshot_drop(db, next);
    pthread_mutex_unlock(&db->mutex);
    return rc;
}
/* DDL is an autocommit operation and requires no active snapshots. This keeps
 * table indices and snapshot/overlay sizes stable for every running query. */
static int execute_create(mdb_db *db, const mdb_stmt *st, const char *sql, mdb_result *out) {
    int rc = -1, added = 0;
    pthread_mutex_lock(&db->mutex);
    if (!db->healthy) {
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "Engine requires recovery");
        goto done;
    }
    if (find_table(db, st->table) >= 0) {
        if (st->if_not_exists) {
            out->commit = MDB_NO_CHANGES;
            rc = 0;
        } else
            fail(&out->error, MDB_DUPLICATE, "Table already exists");
        goto done;
    }
    if (db->active_transactions) {
        fail(&out->error, MDB_BUSY, "CREATE TABLE requires no active transactions");
        goto done;
    }
    if (db->ntables == 128 || db->current->version == UINT64_MAX) {
        fail(&out->error, MDB_LIMIT, "Catalog or version limit reached");
        goto done;
    }
    tabledef t;
    if (create_definition(st, &t, &out->error))
        goto done;
    view **tables = realloc(db->current->tables, (db->ntables + 1) * sizeof *tables);
    if (!tables) {
        fail(&out->error, MDB_NOMEM, "Allocation failed");
        goto done;
    }
    db->current->tables = tables;
    size_t ti = db->ntables;
    db->defs[ti] = t;
    db->current->tables[ti] = NULL;
    db->ntables++;
    added = 1;
    view *v = build_view(db, ti, NULL, 0, db->current->version + 1, &out->error);
    if (!v)
        goto done;
    db->current->tables[ti] = v;
    if (ledger_prepare(db)) {
        fail(&out->error, MDB_NOMEM, "Allocation failed");
        goto done;
    }
    if (reserve_txid(db, &out->txid, &out->error))
        goto done;
    unsigned char payload[MDB_SQL_MAX + 8];
    put64(payload, db->current->version);
    size_t n = strlen(sql);
    memcpy(payload + 8, sql, n);
    fault("create_pre_wal");
    if (wal_append(db, LOG_CREATE, out->txid, payload, n + 8) || wal_sync(db)) {
        db->healthy = 0;
        out->commit = MDB_UNKNOWN;
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "CREATE outcome unknown; restart to recover");
        goto done;
    }
    out->commit = MDB_DURABLE;
    db->committed[db->ncommitted++] = out->txid;
    db->current->version++;
    fault("create_durable");
    if (install_view(db, ti) || syncfd(db->dirfd)) {
        db->healthy = 0;
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "CREATE is durable; table needs recovery");
        goto done;
    }
    added = 0;
    out->affected = 1;
    (void)snprintf(out->state, sizeof out->state, "COMMITTED");
    rc = 0;
done:
    if (added && out->commit == MDB_NOT_COMMITTED) {
        size_t ti = db->ntables - 1;
        view_drop(db, db->current->tables[ti], ti);
        db->current->tables[ti] = NULL;
        db->ntables--;
    }
    pthread_mutex_unlock(&db->mutex);
    return rc;
}

static int execute_unique(mdb_db *db, const mdb_stmt *st, const char *sql, mdb_result *out) {
    int rc = -1;
    unique_index index = {0};
    pthread_mutex_lock(&db->mutex);
    if (!db->healthy) {
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "Engine requires recovery");
        goto done;
    }
    if (unique_definition(db, st, &index, &out->error))
        goto done;
    int found = find_unique(db, index.name);
    if (found >= 0) {
        const unique_index *old = &db->unique[found];
        if (st->if_not_exists && old->table == index.table && old->column == index.column) {
            out->commit = MDB_NO_CHANGES;
            rc = 0;
        } else
            fail(&out->error, MDB_DUPLICATE, "Index name already exists or targets another column");
        goto done;
    }
    if (db->active_transactions) {
        fail(&out->error, MDB_BUSY, "CREATE UNIQUE INDEX requires no active transactions");
        goto done;
    }
    if (db->nunique == UNIQUE_MAX || db->current->version == UINT64_MAX) {
        fail(&out->error, MDB_LIMIT, "UNIQUE catalog or version limit reached");
        goto done;
    }
    if (unique_build(&index, db->current->tables[index.table], NULL, &index.items, &index.nitems, &out->error))
        goto done;
    if (ledger_prepare(db)) {
        fail(&out->error, MDB_NOMEM, "Allocation failed");
        goto done;
    }
    if (reserve_txid(db, &out->txid, &out->error))
        goto done;
    unsigned char payload[MDB_SQL_MAX + 8];
    put64(payload, db->current->version);
    size_t n = strlen(sql);
    memcpy(payload + 8, sql, n);
    fault("unique_pre_wal");
    if (wal_append(db, LOG_UNIQUE, out->txid, payload, n + 8) || wal_sync(db)) {
        db->healthy = 0;
        out->commit = MDB_UNKNOWN;
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "UNIQUE index outcome unknown; restart to recover");
        goto done;
    }
    out->commit = MDB_DURABLE;
    db->committed[db->ncommitted++] = out->txid;
    db->current->version++;
    fault("unique_durable");
    db->unique[db->nunique++] = index;
    index.items = NULL;
    out->affected = 1;
    (void)snprintf(out->state, sizeof out->state, "COMMITTED");
    rc = 0;
done:
    free(index.items);
    pthread_mutex_unlock(&db->mutex);
    return rc;
}

static void clear_result_data(mdb_result *out) {
    mdb_result saved = *out;
    mdb_result_free(out);
    out->error = saved.error;
    out->commit = saved.commit;
    out->txid = saved.txid;
    out->recovery_required = saved.recovery_required;
    out->staged = saved.staged;
    out->used_index = saved.used_index;
    (void)snprintf(out->state, sizeof out->state, "%s", saved.state);
}
int mdb_execute(mdb_session *session, const char *sql, mdb_result *out) {
    if (!out)
        return -1;
    memset(out, 0, sizeof *out);
    out->commit = MDB_NOT_COMMITTED;
    (void)snprintf(out->state, sizeof out->state, "IDLE");
    if (!session || !sql)
        return fail(&out->error, MDB_STATE, "Invalid execution context");
    mdb_db *db = session->db;
    mdb_stmt *st = NULL;
    int rc = -1, implicit = 0;
    if (mdb_parse(sql, &st, &out->error)) {
        if (session->tx)
            session->tx->failed = 1;
        goto finish;
    }
    if (session->tx && session->tx->failed && st->kind != MDB_ROLLBACK) {
        fail(&out->error, MDB_FAILED, "Transaction is FAILED; ROLLBACK required");
        goto finish;
    }
    if (st->kind == MDB_ROLLBACK) {
        if (!session->tx) {
            fail(&out->error, MDB_STATE, "No active transaction");
            goto finish;
        }
        pthread_mutex_lock(&db->mutex);
        tx_drop(db, session->tx);
        session->tx = NULL;
        pthread_mutex_unlock(&db->mutex);
        (void)snprintf(out->state, sizeof out->state, "ROLLED_BACK");
        rc = 0;
        goto finish;
    }
    if (!mdb_healthy(db)) {
        out->recovery_required = 1;
        fail(&out->error, MDB_IO, "Engine requires recovery before SQL processing");
        goto finish;
    }
    if (st->kind == MDB_BEGIN) {
        if (session->tx) {
            session->tx->failed = 1;
            fail(&out->error, MDB_STATE, "Nested BEGIN is not supported");
            goto finish;
        }
        session->tx = tx_begin(db, 1, &out->error);
        if (!session->tx)
            goto finish;
        out->txid = session->tx->id;
        (void)snprintf(out->state, sizeof out->state, "ACTIVE");
        rc = 0;
        goto finish;
    }
    if (st->kind == MDB_CREATE || st->kind == MDB_CREATE_INDEX) {
        if (session->tx) {
            session->tx->failed = 1;
            fail(&out->error, MDB_STATE, "DDL is autocommit only; end the transaction first");
        } else
            rc = st->kind == MDB_CREATE ? execute_create(db, st, sql, out) : execute_unique(db, st, sql, out);
        goto finish;
    }
    if (st->kind == MDB_COMMIT) {
        if (!session->tx) {
            fail(&out->error, MDB_STATE, "No active transaction");
            goto finish;
        }
        rc = commit_tx(db, session->tx, out);
        if (!rc || out->commit != MDB_NOT_COMMITTED) {
            pthread_mutex_lock(&db->mutex);
            tx_drop(db, session->tx);
            session->tx = NULL;
            pthread_mutex_unlock(&db->mutex);
            (void)snprintf(out->state, sizeof out->state, !rc ? "COMMITTED" : "RECOVERY_REQUIRED");
        } else
            session->tx->failed = 1;
        goto finish;
    }
    if (!session->tx) {
        session->tx = tx_begin(db, st->kind != MDB_SELECT, &out->error);
        if (!session->tx)
            goto finish;
        implicit = 1;
    }
    transaction *tx = session->tx;
    out->txid = tx->id;
    int ti = find_table(db, st->table);
    if (ti < 0) {
        fail(&out->error, MDB_UNKNOWN_TABLE, "Unknown table");
        goto execution_done;
    }
    snapshot *original = tx->snap, *latest = NULL;
    if (st->kind != MDB_SELECT || st->for_update) {
        latest = lock_statement(db, tx, (size_t)ti, st, &out->error);
        if (!latest) goto execution_done;
        tx->snap = latest;
    }
    if (st->kind == MDB_SELECT)
        rc = execute_select(db, tx, (size_t)ti, st, out);
    else if (st->kind == MDB_INSERT)
        rc = execute_insert(db, tx, (size_t)ti, st, out);
    else if (st->kind == MDB_UPDATE || st->kind == MDB_DELETE)
        rc = execute_changes(db, tx, (size_t)ti, st, out);
    else
        fail(&out->error, MDB_SYNTAX, "Unsupported SQL operation");
    if (latest) {
        tx->snap = original;
        pthread_mutex_lock(&db->mutex);
        snapshot_drop(db, latest);
        pthread_mutex_unlock(&db->mutex);
    }
execution_done:
    if (!rc && implicit && st->kind != MDB_SELECT)
        rc = commit_tx(db, tx, out);
    if (implicit) {
        pthread_mutex_lock(&db->mutex);
        tx_drop(db, session->tx);
        session->tx = NULL;
        pthread_mutex_unlock(&db->mutex);
        if (out->commit != MDB_NOT_COMMITTED)
            (void)snprintf(out->state, sizeof out->state, rc ? "RECOVERY_REQUIRED" : "COMMITTED");
    } else if (rc)
        tx->failed = 1;
    else if (st->kind != MDB_SELECT) {
        out->staged = 1;
        (void)snprintf(out->state, sizeof out->state, "STAGED");
    }
finish:
    if (session->tx) {
        out->txid = session->tx->id;
        if (session->tx->failed)
            (void)snprintf(out->state, sizeof out->state, "FAILED");
        else if (!out->staged)
            (void)snprintf(out->state, sizeof out->state, "ACTIVE");
    }
    if (!mdb_healthy(db)) {
        out->recovery_required = 1;
        if (rc && out->commit == MDB_NOT_COMMITTED && out->error.code == MDB_IO)
            out->commit = MDB_UNKNOWN;
    }
    if (rc)
        clear_result_data(out);
    mdb_stmt_free(st);
    return rc;
}

void mdb_get_metrics(mdb_db *db, mdb_metrics *out) {
    if (!db || !out)
        return;
    pthread_mutex_lock(&db->mutex);
    *out = db->metrics;
    pthread_mutex_unlock(&db->mutex);
}

/* Copy metadata while DDL is excluded. Caller frees the result normally. */
int mdb_catalog(mdb_db *db, const char *table, mdb_result *out) {
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&db->mutex);
    int ti = table ? find_table(db, table) : -1, rc = -1;
    if (table && ti < 0) { fail(&out->error, MDB_UNKNOWN_TABLE, "Unknown table"); goto done; }
    out->ncolumns = 1;
    out->columns = calloc(1, sizeof *out->columns);
    out->types = calloc(1, sizeof *out->types);
    if (!out->columns || !out->types) goto memory;
    out->columns[0] = strdup(table ? "definition" : "table");
    if (!out->columns[0]) goto memory;
    out->types[0] = MDB_STRING;
    out->nrows = table ? db->defs[ti].ncols : db->ntables;
    out->rows = calloc(out->nrows ? out->nrows : 1, sizeof *out->rows);
    if (!out->rows) { out->nrows = 0; goto memory; }
    for (size_t i = 0; i < out->nrows; i++) {
        char text[256];
        if (table) {
            column *c = &db->defs[ti].cols[i];
            if (c->type == MDB_INT) (void)snprintf(text, sizeof text, "%s INT%s", c->name, i ? "" : " AUTO");
            else (void)snprintf(text, sizeof text, "%s VARCHAR(%zu)", c->name, c->max);
        } else (void)snprintf(text, sizeof text, "%s", db->defs[i].name);
        out->rows[i].values = calloc(1, sizeof(mdb_value));
        if (!out->rows[i].values) goto memory;
        out->rows[i].values[0].type = MDB_STRING;
        out->rows[i].values[0].s = strdup(text);
        if (!out->rows[i].values[0].s) goto memory;
    }
    rc = 0; goto done;
memory:
    fail(&out->error, MDB_NOMEM, "Metadata allocation failed");
done:
    pthread_mutex_unlock(&db->mutex);
    return rc;
}
