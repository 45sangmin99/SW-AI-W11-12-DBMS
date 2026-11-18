#define _DARWIN_C_SOURCE 1
#include "mdb.h"
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define SAMPLES 200000
#define READ_COUNT 80000
static double now(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}
static void pause_ms(double ms) {
    struct timespec t = {(time_t)(ms/1000), (long)((ms - (long)(ms/1000)*1000)*1e6)};
    while (nanosleep(&t, &t)) {}
}
static int command(mdb_session *s, const char *sql) {
    mdb_result r; int rc = mdb_execute(s, sql, &r);
    if (rc && r.error.code != MDB_BUSY && r.error.code != MDB_FAILED) {
        fprintf(stderr, "%s: %s\n", sql, r.error.message); abort();
    }
    int code = (int)r.error.code;
    mdb_result_free(&r); return rc ? code + 1 : 0;
}
static void ok(mdb_session *s, const char *sql) { assert(!command(s, sql)); }
static int64_t scalar(mdb_session *s, const char *sql) {
    mdb_result r; assert(!mdb_execute(s, sql, &r));
    assert(r.nrows == 1 && r.ncolumns == 1);
    int64_t value = r.rows[0].values[0].i; mdb_result_free(&r); return value;
}
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int ready, go;
    double deadline;
} gate;
typedef struct {
    mdb_db *db;
    gate *gate;
    int index, nthreads, mixed, hot;
    double *read, *write;
    size_t nr, nw, errors, busy_errors;
} worker;
static void *work(void *arg) {
    worker *w = arg; mdb_error e;
    mdb_session *s = mdb_session_open(w->db, &e); assert(s);
    pthread_mutex_lock(&w->gate->mutex);
    w->gate->ready++; pthread_cond_broadcast(&w->gate->cond);
    while (!w->gate->go) pthread_cond_wait(&w->gate->cond, &w->gate->mutex);
    pthread_mutex_unlock(&w->gate->mutex);
    size_t n = 0;
    char sql[128];
    while (w->mixed ? now() < w->gate->deadline : n < READ_COUNT/(size_t)w->nthreads) {
        int writing = w->mixed && w->index == 0;
        int id = w->hot || writing ? 1 : w->index + 1;
        double start = now(); int rc;
        if (writing) {
            snprintf(sql, sizeof sql, "UPDATE t SET value=%zu WHERE id=1", n);
            rc = command(s, sql);
        } else if (w->mixed) {
            rc = command(s, "BEGIN");
            if (!rc) {
                snprintf(sql, sizeof sql, "SELECT value FROM t WHERE id=%d", id);
                rc = command(s, sql);
                if (!rc) { pause_ms(0.5); rc = command(s, "COMMIT"); }
                if (rc) (void)command(s, "ROLLBACK");
            }
        } else {
            snprintf(sql, sizeof sql, "SELECT value FROM t WHERE id=%zu", n % 1000 + 1);
            rc = command(s, sql);
        }
        double us = (now()-start)*1e6;
        if (rc) { w->errors++; if (rc == MDB_BUSY + 1) w->busy_errors++; }
        else if (writing) { assert(w->nw < SAMPLES); w->write[w->nw++] = us; }
        else { assert(w->nr < SAMPLES); w->read[w->nr++] = us; }
        n++;
    }
    mdb_session_close(s); return NULL;
}
static int compare(const void *a, const void *b) {
    double x=*(const double *)a, y=*(const double *)b; return (x>y)-(x<y);
}
static double quantile(double *v, size_t n, int p) {
    if (!n) return 0;
    size_t rank = (n*(size_t)p + 99)/100;
    return v[rank-1];
}
static void report(const char *name, int nt, double elapsed, double *reads, size_t nr,
                   double *writes, size_t nw, size_t errors, size_t busy_errors, int comma) {
    qsort(reads,nr,sizeof *reads,compare); qsort(writes,nw,sizeof *writes,compare);
    printf("%s{\"case\":\"%s\",\"threads\":%d,\"reads\":%zu,\"writes\":%zu,\"errors\":%zu,\"lock_timeouts\":%zu,\"elapsed_ms\":%.6f,\"read_p50_us\":%.6f,\"read_p95_us\":%.6f,\"write_p50_us\":%.6f,\"write_p95_us\":%.6f}",
        comma?",":"",name,nt,nr,nw,errors,busy_errors,elapsed*1000,quantile(reads,nr,50),quantile(reads,nr,95),quantile(writes,nw,50),quantile(writes,nw,95));
}
static void parallel(mdb_db *db, int nt, int mixed, int hot, const char *name, int comma) {
    gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,0,0,0};
    worker w[8]; pthread_t threads[8];
    for (int i=0;i<nt;i++) {
        w[i]=(worker){.db=db,.gate=&g,.index=i,.nthreads=nt,.mixed=mixed,.hot=hot};
        w[i].read=malloc(SAMPLES*sizeof(double)); w[i].write=malloc(SAMPLES*sizeof(double));
        assert(w[i].read && w[i].write);
        assert(!pthread_create(&threads[i],NULL,work,&w[i]));
    }
    pthread_mutex_lock(&g.mutex);
    while (g.ready<nt) pthread_cond_wait(&g.cond,&g.mutex);
    double start=now(); g.deadline=start+1.5; g.go=1;
    pthread_cond_broadcast(&g.cond); pthread_mutex_unlock(&g.mutex);
    size_t nr=0,nw=0,errors=0,busy_errors=0;
    double *reads=malloc((size_t)nt*SAMPLES*sizeof(double)), *writes=malloc((size_t)nt*SAMPLES*sizeof(double));
    assert(reads && writes);
    for(int i=0;i<nt;i++) {
        pthread_join(threads[i],NULL);
        memcpy(reads+nr,w[i].read,w[i].nr*sizeof(double)); nr+=w[i].nr;
        memcpy(writes+nw,w[i].write,w[i].nw*sizeof(double)); nw+=w[i].nw;
        errors+=w[i].errors; busy_errors+=w[i].busy_errors; free(w[i].read); free(w[i].write);
    }
    report(name,nt,now()-start,reads,nr,writes,nw,errors,busy_errors,comma);
    free(reads);free(writes); pthread_cond_destroy(&g.cond);pthread_mutex_destroy(&g.mutex);
}
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int waiting, done;
    mdb_session *writer;
    int value;
    double elapsed;
} observation;
static void waited(void *context) {
    observation *o=context; pthread_mutex_lock(&o->mutex);
    o->waiting=1;pthread_cond_signal(&o->cond);pthread_mutex_unlock(&o->mutex);
}
static void *write_one(void *context) {
    observation *o=context; double start=now();
    char sql[128]; snprintf(sql,sizeof sql,"UPDATE t SET value=%d WHERE id=1",o->value);
    ok(o->writer,sql);ok(o->writer,"COMMIT");
    o->elapsed=(now()-start)*1e6;
    pthread_mutex_lock(&o->mutex);o->done=1;pthread_cond_signal(&o->cond);pthread_mutex_unlock(&o->mutex);
    return NULL;
}
static void long_reader(mdb_db *db, int strict) {
    mdb_error e;
    mdb_session *a=mdb_session_open(db,&e), *b=mdb_session_open(db,&e), *c=mdb_session_open(db,&e);
    assert(a&&b&&c);
    /* S/S must be compatible in the comparison variant, unlike FOR UPDATE. */
    ok(a,"BEGIN");ok(b,"BEGIN");
    (void)scalar(a,"SELECT value FROM t WHERE id=1");
    (void)scalar(b,"SELECT value FROM t WHERE id=1");
    ok(a,"COMMIT");ok(b,"COMMIT");
    double samples[30], reader_samples[30]; double start=now();
    for (int i=0;i<30;i++) {
        observation o={PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,0,0,b,123+i,0};
        double reader_start=now();
        ok(a,"BEGIN");int64_t old=scalar(a,"SELECT value FROM t WHERE id=1");
        ok(b,"BEGIN");mdb_test_lock_hook(db,waited,&o);
        pthread_t thread;assert(!pthread_create(&thread,NULL,write_one,&o));
        pthread_mutex_lock(&o.mutex);
        struct timespec deadline; clock_gettime(CLOCK_REALTIME,&deadline);deadline.tv_sec+=3;
        while (!o.waiting && !o.done) assert(!pthread_cond_timedwait(&o.cond,&o.mutex,&deadline));
        assert(strict ? o.waiting && !o.done : o.done && !o.waiting);
        pthread_mutex_unlock(&o.mutex);
        pause_ms(10); /* Artificial long reader; writer overlap is observed, not assumed. */
        assert(scalar(a,"SELECT value FROM t WHERE id=1")==old);
        ok(a,"COMMIT");reader_samples[i]=(now()-reader_start)*1e6;
        pthread_join(thread,NULL);samples[i]=o.elapsed;
        mdb_test_lock_hook(db,NULL,NULL);
        assert(scalar(c,"SELECT value FROM t WHERE id=1")==123+i);
        pthread_cond_destroy(&o.cond);pthread_mutex_destroy(&o.mutex);
    }
    report("reader_holds_10ms",2,now()-start,reader_samples,30,samples,30,0,0,1);
    mdb_session_close(a);mdb_session_close(b);mdb_session_close(c);
}
int main(int argc,char **argv) {
    assert(argc==3);int strict=strcmp(argv[2],"snapshot")!=0;mdb_error e;
    mdb_db *db=mdb_open(argv[1],&e);assert(db);
    mdb_session *s=mdb_session_open(db,&e);assert(s);
    for(int i=0;i<1000;i++) assert(scalar(s,"SELECT value FROM t WHERE id=1")==0);
    mdb_session_close(s);
    printf("{\"mode\":\"%s\",\"cases\":[",argv[2]);
    parallel(db,1,0,0,"point_read",0);parallel(db,8,0,0,"point_read",1);
    parallel(db,8,1,0,"mixed_disjoint",1);parallel(db,8,1,1,"mixed_hot_row",1);
    long_reader(db,strict);
    assert(mdb_healthy(db));mdb_close(db);puts("]}");
}
