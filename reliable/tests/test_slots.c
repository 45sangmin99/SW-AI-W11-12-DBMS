#include "slots.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef MDB_SLOTS_TEST_ALLOC
static long allocation_budget = -1;
void *mdb_slots_test_calloc(size_t count, size_t size) {
    if (allocation_budget == 0)
        return NULL;
    if (allocation_budget > 0)
        allocation_budget--;
    return calloc(count, size);
}
#endif

typedef struct {
    size_t references;
    unsigned value;
} item;

typedef struct {
    size_t retains;
    size_t releases;
    size_t destroyed;
} tracking;

static void retain_item(void *pointer, void *context) {
    item *value = pointer;
    tracking *track = context;
    value->references++;
    track->retains++;
}

static void release_item(void *pointer, void *context) {
    item *value = pointer;
    tracking *track = context;
    assert(value->references > 0);
    value->references--;
    track->releases++;
}

static void release_heap_item(void *pointer, void *context) {
    item *value = pointer;
    tracking *track = context;
    release_item(pointer, context);
    if (!value->references) {
        track->destroyed++;
        free(value);
    }
}

static uint64_t random_next(uint64_t *state) {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    return *state;
}

static void test_lifetime(void) {
    tracking track = {0};
    mdb_slots *original = mdb_slots_create(retain_item, release_heap_item, &track);
    assert(original);
    item *first = calloc(1, sizeof(*first));
    item *second = calloc(1, sizeof(*second));
    assert(first && second);
    first->value = 100;
    second->value = 200;
    assert(mdb_slots_set(original, 1, first) == 0);
    mdb_slots *snapshot = mdb_slots_clone(original);
    assert(snapshot);
    assert(mdb_slots_set(original, 1, second) == 0);
    assert(((item *)mdb_slots_get(original, 1))->value == 200);
    assert(((item *)mdb_slots_get(snapshot, 1))->value == 100);
    assert(track.destroyed == 0);
    assert(mdb_slots_set(original, 1, NULL) == 0);
    assert(track.destroyed == 1);
    mdb_slots_free(original);
    assert(((item *)mdb_slots_get(snapshot, 1))->value == 100);
    mdb_slots_free(snapshot);
    assert(track.destroyed == 2 && track.retains == track.releases);
}

static void test_sparse_and_shrinking(void) {
    static const size_t indexes[] = {0, 1, 63, 64, 4095, 4096,
                                   262143, 262144, SIZE_MAX - 1, SIZE_MAX};
    item values[sizeof(indexes) / sizeof(indexes[0])] = {{0}};
    tracking track = {0};
    mdb_slots *slots = mdb_slots_create(retain_item, release_item, &track);
    assert(slots);
    for (size_t i = 0; i < sizeof(indexes) / sizeof(indexes[0]); i++) {
        values[i].value = (unsigned)i;
        assert(mdb_slots_set(slots, indexes[i], &values[i]) == 0);
    }
    mdb_slots *snapshot = mdb_slots_clone(slots);
    assert(snapshot);
    for (size_t i = sizeof(indexes) / sizeof(indexes[0]); i > 0; i--) {
        size_t j = i - 1;
        assert(mdb_slots_get(slots, indexes[j]) == &values[j]);
        assert(mdb_slots_set(slots, indexes[j], NULL) == 0);
        assert(!mdb_slots_get(slots, indexes[j]));
        for (size_t k = 0; k < sizeof(indexes) / sizeof(indexes[0]); k++)
            assert(mdb_slots_get(snapshot, indexes[k]) == &values[k]);
    }
    assert(!mdb_slots_get(slots, SIZE_MAX));
    assert(mdb_slots_set(slots, SIZE_MAX, &values[0]) == 0);
    mdb_slots_free(snapshot);
    assert(mdb_slots_get(slots, SIZE_MAX) == &values[0]);
    mdb_slots_free(slots);
    assert(track.retains == track.releases);
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++)
        assert(values[i].references == 0);
}

static void test_random_snapshots(void) {
    enum { COUNT = 4096, VERSIONS = 24, OPERATIONS = 40000 };
    item values[COUNT];
    memset(values, 0, sizeof(values));
    tracking track = {0};
    mdb_slots *slots = mdb_slots_create(retain_item, release_item, &track);
    mdb_slots *snapshots[VERSIONS] = {0};
    void **models[VERSIONS] = {0};
    void *current[COUNT] = {0};
    uint64_t random = UINT64_C(1092873645);
    assert(slots);
    for (size_t operation = 0; operation < OPERATIONS; operation++) {
        size_t index = (size_t)(random_next(&random) % COUNT);
        void *value = random_next(&random) % 4 ? &values[index] : NULL;
        assert(mdb_slots_set(slots, index, value) == 0);
        current[index] = value;
        if (operation % 373 == 0) {
            size_t version = (size_t)(random_next(&random) % VERSIONS);
            mdb_slots_free(snapshots[version]);
            free(models[version]);
            snapshots[version] = mdb_slots_clone(slots);
            models[version] = malloc(sizeof(current));
            assert(snapshots[version] && models[version]);
            memcpy(models[version], current, sizeof(current));
        }
        if (operation % 127 == 0) {
            for (size_t v = 0; v < VERSIONS; v++) {
                if (!snapshots[v])
                    continue;
                for (size_t i = 0; i < COUNT; i++)
                    assert(mdb_slots_get(snapshots[v], i) == models[v][i]);
            }
        }
    }
    for (size_t i = 0; i < COUNT; i++)
        assert(mdb_slots_get(slots, i) == current[i]);
    mdb_slots_free(slots);
    for (size_t v = 0; v < VERSIONS; v++) {
        mdb_slots_free(snapshots[v]);
        free(models[v]);
    }
    assert(track.retains == track.releases);
    for (size_t i = 0; i < COUNT; i++)
        assert(values[i].references == 0);
}

#ifdef MDB_SLOTS_TEST_ALLOC
static void test_allocation_failures(void) {
    const size_t indexes[] = {0, 64, 4096, SIZE_MAX};
    for (unsigned shared = 0; shared < 2; shared++) {
        for (size_t operation = 0; operation < sizeof(indexes) / sizeof(indexes[0]);
             operation++) {
            int succeeded = 0;
            for (long budget = 0; budget < 40 && !succeeded; budget++) {
                allocation_budget = -1;
                tracking track = {0};
                item first = {0}, second = {0};
                mdb_slots *slots = mdb_slots_create(retain_item, release_item, &track);
                assert(slots && mdb_slots_set(slots, 0, &first) == 0);
                mdb_slots *snapshot = shared ? mdb_slots_clone(slots) : NULL;
                assert(!shared || snapshot);
                allocation_budget = budget;
                int result = mdb_slots_set(slots, indexes[operation], &second);
                allocation_budget = -1;
                if (result == 0) {
                    assert(mdb_slots_get(slots, indexes[operation]) == &second);
                    succeeded = 1;
                } else {
                    assert(result == -1);
                    assert(mdb_slots_get(slots, 0) == &first);
                    if (indexes[operation])
                        assert(!mdb_slots_get(slots, indexes[operation]));
                    assert(second.references == 0);
                }
                if (snapshot)
                    assert(mdb_slots_get(snapshot, 0) == &first);
                mdb_slots_free(snapshot);
                mdb_slots_free(slots);
                assert(first.references == 0 && second.references == 0);
                assert(track.retains == track.releases);
            }
            assert(succeeded);
        }
    }
    /* Deletion may also allocate when a snapshot shares the old path. */
    for (size_t operation = 0; operation < sizeof(indexes) / sizeof(indexes[0]);
         operation++) {
        int succeeded = 0;
        for (long budget = 0; budget < 20 && !succeeded; budget++) {
            allocation_budget = -1;
            tracking track = {0};
            item values[sizeof(indexes) / sizeof(indexes[0])] = {{0}};
            mdb_slots *slots = mdb_slots_create(retain_item, release_item, &track);
            assert(slots);
            for (size_t i = 0; i < sizeof(indexes) / sizeof(indexes[0]); i++)
                assert(mdb_slots_set(slots, indexes[i], &values[i]) == 0);
            mdb_slots *snapshot = mdb_slots_clone(slots);
            assert(snapshot);
            allocation_budget = budget;
            int result = mdb_slots_set(slots, indexes[operation], NULL);
            allocation_budget = -1;
            for (size_t i = 0; i < sizeof(indexes) / sizeof(indexes[0]); i++) {
                assert(mdb_slots_get(snapshot, indexes[i]) == &values[i]);
                void *expected = result == 0 && i == operation ? NULL : &values[i];
                assert(mdb_slots_get(slots, indexes[i]) == expected);
            }
            succeeded = result == 0;
            mdb_slots_free(slots);
            mdb_slots_free(snapshot);
            assert(track.retains == track.releases);
        }
        assert(succeeded);
    }
    allocation_budget = 0;
    assert(!mdb_slots_create(NULL, NULL, NULL));
    allocation_budget = -1;
    mdb_slots *slots = mdb_slots_create(NULL, NULL, NULL);
    assert(slots);
    allocation_budget = 0;
    assert(!mdb_slots_clone(slots));
    allocation_budget = -1;
    mdb_slots_free(slots);
}
#endif

int main(void) {
    assert(!mdb_slots_get(NULL, 0));
    assert(!mdb_slots_clone(NULL));
    assert(mdb_slots_set(NULL, 0, NULL) == -1);
    mdb_slots_free(NULL);
    mdb_slots *borrowed = mdb_slots_create(NULL, NULL, NULL);
    assert(borrowed && mdb_slots_set(borrowed, 999, NULL) == 0);
    int value = 42;
    assert(mdb_slots_set(borrowed, 64, &value) == 0);
    assert(mdb_slots_get(borrowed, 64) == &value);
    mdb_slots_free(borrowed);
    test_lifetime();
    test_sparse_and_shrinking();
    test_random_snapshots();
#ifdef MDB_SLOTS_TEST_ALLOC
    test_allocation_failures();
#endif
    puts("slots tests passed");
    return 0;
}
