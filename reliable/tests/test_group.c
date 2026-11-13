#include "group.h"

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef MDB_GROUP_TESTING
static long allocation_budget = -1;
static int allow_allocation(void) {
    if (allocation_budget == 0)
        return 0;
    if (allocation_budget > 0)
        allocation_budget--;
    return 1;
}
void *mdb_group_test_malloc(size_t size) {
    return allow_allocation() ? malloc(size) : NULL;
}
void *mdb_group_test_calloc(size_t count, size_t size) {
    return allow_allocation() ? calloc(count, size) : NULL;
}
void *mdb_group_test_realloc(void *pointer, size_t size) {
    return allow_allocation() ? realloc(pointer, size) : NULL;
}
extern int mdb_group_test_set_count(mdb_group *, size_t, int64_t);
#endif

static mdb_value integer(int64_t value) {
    return (mdb_value){.type = MDB_INT, .i = value};
}
static mdb_value string(char *value) {
    return (mdb_value){.type = MDB_STRING, .s = value};
}
static void add(mdb_group *group, const mdb_value *row) {
    mdb_error error;
    if (mdb_group_add(group, row, &error)) {
        fprintf(stderr, "group add: %s\n", error.message);
        abort();
    }
}
static void assert_order(const mdb_group *group, const int *keys,
                          const int *descending, size_t count,
                          const size_t *expected) {
    mdb_error error;
    size_t *order = NULL;
    assert(!mdb_group_order(group, keys, descending, count, &order, &error));
    for (size_t i = 0; i < mdb_group_size(group); i++)
        assert(order[i] == expected[i]);
    free(order);
}

static void test_empty_and_arguments(void) {
    mdb_error error;
    int position = 0;
    assert(!mdb_group_create(NULL, 1, 10, &error) && error.code == MDB_STATE);
    assert(!mdb_group_create(&position, 0, 10, &error) && error.code == MDB_STATE);
    assert(!mdb_group_create(&position, MDB_COL_MAX + 1, 10, &error));
    assert(!mdb_group_create(&position, 1, 0, &error) && error.code == MDB_LIMIT);
    position = -1;
    assert(!mdb_group_create(&position, 1, 10, &error));
    position = MDB_COL_MAX;
    assert(!mdb_group_create(&position, 1, 10, &error));
    position = 0;
    mdb_group *group = mdb_group_create(&position, 1, 10, &error);
    assert(group && error.code == MDB_OK);
    assert(mdb_group_size(group) == 0);
    assert(!mdb_group_keys(group, 0) && !mdb_group_count(group, 0));
    size_t *order = NULL;
    assert(!mdb_group_order(group, NULL, NULL, 0, &order, &error) && !order);
    mdb_value row = integer(1);
    assert(mdb_group_add(NULL, &row, &error) == -1 && error.code == MDB_STATE);
    assert(mdb_group_add(group, NULL, &error) == -1 && error.code == MDB_STATE);
    assert(mdb_group_order(NULL, NULL, NULL, 0, &order, &error) == -1);
    assert(mdb_group_order(group, NULL, NULL, 0, NULL, &error) == -1);
    int ascending = 0, invalid = -2;
    assert(mdb_group_order(group, &invalid, &ascending, 1, &order, &error) == -1);
    invalid = 1;
    assert(mdb_group_order(group, &invalid, &ascending, 1, &order, &error) == -1);
    invalid = 0;
    ascending = 2;
    assert(mdb_group_order(group, &invalid, &ascending, 1, &order, &error) == -1);
    assert(mdb_group_order(group, NULL, &ascending, 1, &order, &error) == -1);
    assert(mdb_group_order(group, &invalid, NULL, 1, &order, &error) == -1);
    assert(mdb_group_size(NULL) == 0);
    assert(!mdb_group_keys(NULL, 0) && !mdb_group_count(NULL, 0));
    mdb_group_free(group);
    mdb_group_free(NULL);

    position = MDB_COL_MAX - 1;
    group = mdb_group_create(&position, 1, 10, NULL);
    assert(group);
    mdb_value wide[MDB_COL_MAX] = {{0}};
    wide[position] = integer(42);
    add(group, wide);
    assert(mdb_group_keys(group, 0)[0].i == 42);
    mdb_group_free(group);
}

static void test_int_extremes_and_types(void) {
    int position = 0;
    mdb_error error;
    mdb_group *group = mdb_group_create(&position, 1, 10, &error);
    assert(group);
    mdb_value values[] = {integer(INT64_MIN), integer(INT64_MAX), integer(0),
                          integer(INT64_MIN), string("0")};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++)
        add(group, &values[i]);
    assert(mdb_group_size(group) == 4);
    assert(mdb_group_count(group, 0) == 2);
    assert(mdb_group_keys(group, 0)[0].i == INT64_MIN);
    assert(mdb_group_keys(group, 1)[0].i == INT64_MAX);
    assert(mdb_group_keys(group, 2)[0].type == MDB_INT);
    assert(mdb_group_keys(group, 3)[0].type == MDB_STRING);
    const size_t first_seen[] = {0, 1, 2, 3};
    assert_order(group, NULL, NULL, 0, first_seen);
    int key = 0, descending = 0;
    const size_t ascending_order[] = {0, 2, 1, 3};
    assert_order(group, &key, &descending, 1, ascending_order);
    descending = 1;
    const size_t descending_order[] = {3, 1, 2, 0};
    assert_order(group, &key, &descending, 1, descending_order);
    assert(!mdb_group_keys(group, SIZE_MAX) && !mdb_group_count(group, SIZE_MAX));
    mdb_group_free(group);
}

static void test_owned_strings(void) {
    int position = 0;
    mdb_error error;
    mdb_group *group = mdb_group_create(&position, 1, 10, &error);
    assert(group);
    char text[32] = "총웨";
    mdb_value value = string(text);
    add(group, &value);
    strcpy(text, "슈");
    add(group, &value);
    value = string("총웨");
    add(group, &value);
    value = string("");
    add(group, &value);
    value = string("a");
    add(group, &value);
    assert(mdb_group_size(group) == 4 && mdb_group_count(group, 0) == 2);
    assert(!strcmp(mdb_group_keys(group, 0)[0].s, "총웨"));
    assert(!strcmp(mdb_group_keys(group, 1)[0].s, "슈"));
    int key = 0, ascending = 0;
    const size_t expected[] = {2, 3, 1, 0};
    assert_order(group, &key, &ascending, 1, expected);
    char invalid[] = {(char)0xc0, (char)0xaf, 0};
    value = string(invalid);
    assert(mdb_group_add(group, &value, &error) == -1 && error.code == MDB_TYPE);
    value = string(NULL);
    assert(mdb_group_add(group, &value, &error) == -1 && error.code == MDB_TYPE);
    value = (mdb_value){0};
    assert(mdb_group_add(group, &value, NULL) == -1);
    assert(mdb_group_size(group) == 4 && mdb_group_count(group, 0) == 2);
    mdb_group_free(group);
}

static void test_composite_keys(void) {
    int positions[] = {2, 0};
    mdb_error error;
    mdb_group *group = mdb_group_create(positions, 2, 20, &error);
    assert(group);
    char *parts[][2] = {{"ab", "c"}, {"a", "bc"}, {"", "abc"}, {"abc", ""}};
    for (size_t i = 0; i < sizeof(parts) / sizeof(parts[0]); i++) {
        mdb_value row[] = {string(parts[i][1]), integer(999), string(parts[i][0])};
        add(group, row);
    }
    mdb_value duplicate[] = {string("c"), string("unused"), string("ab")};
    add(group, duplicate);
    assert(mdb_group_size(group) == 4 && mdb_group_count(group, 0) == 2);
    for (size_t i = 0; i < sizeof(parts) / sizeof(parts[0]); i++) {
        assert(!strcmp(mdb_group_keys(group, i)[0].s, parts[i][0]));
        assert(!strcmp(mdb_group_keys(group, i)[1].s, parts[i][1]));
    }
    int keys[] = {0, 1}, directions[] = {0, 1};
    const size_t expected[] = {2, 1, 0, 3};
    assert_order(group, keys, directions, 2, expected);
    mdb_group_free(group);

    int mixed_positions[] = {0, 1};
    group = mdb_group_create(mixed_positions, 2, 10, &error);
    assert(group);
    mdb_value rows[][2] = {{integer(1), string("x")}, {integer(2), string("x")},
                            {integer(1), string("y")}, {integer(1), string("x")}};
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++)
        add(group, rows[i]);
    assert(mdb_group_size(group) == 3 && mdb_group_count(group, 0) == 2);
    mdb_group_free(group);
}

static void test_counts_stable_sort_and_limit(void) {
    int position = 0;
    mdb_error error;
    mdb_group *group = mdb_group_create(&position, 1, 4, &error);
    assert(group);
    char *names[] = {"b", "a", "c", "d", "c", "b", "a", "c"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        mdb_value value = string(names[i]);
        add(group, &value);
    }
    int count_key = -1, descending = 1;
    const size_t count_order[] = {2, 0, 1, 3};
    assert_order(group, &count_key, &descending, 1, count_order);
    int keys[] = {-1, 0}, directions[] = {1, 0};
    const size_t tie_order[] = {2, 1, 0, 3};
    assert_order(group, keys, directions, 2, tie_order);
    mdb_value rejected = string("new");
    assert(mdb_group_add(group, &rejected, &error) == -1 && error.code == MDB_LIMIT);
    assert(mdb_group_size(group) == 4 && mdb_group_count(group, 2) == 3);
    mdb_value existing = string("c");
    add(group, &existing);
    assert(mdb_group_count(group, 2) == 4);
    mdb_group_free(group);
}

static void test_hash_growth_and_collisions(void) {
    enum { GROUPS = 2048 };
    int position = 0;
    mdb_error error;
    mdb_group *group = mdb_group_create(&position, 1, GROUPS, &error);
    assert(group);
    for (int pass = 0; pass < 3; pass++)
        for (size_t i = 0; i < GROUPS; i++) {
            mdb_value value = integer((int64_t)((i * 2039) % GROUPS));
            add(group, &value);
        }
    assert(mdb_group_size(group) == GROUPS);
    for (size_t i = 0; i < GROUPS; i++) {
        assert(mdb_group_keys(group, i)[0].i == (int64_t)((i * 2039) % GROUPS));
        assert(mdb_group_count(group, i) == 3);
    }
    int key = 0, ascending = 0;
    size_t *order = NULL;
    assert(!mdb_group_order(group, &key, &ascending, 1, &order, &error));
    for (size_t i = 0; i < GROUPS; i++)
        assert(mdb_group_keys(group, order[i])[0].i == (int64_t)i);
    free(order);
    mdb_group_free(group);
}

#ifdef MDB_GROUP_TESTING
static void test_count_overflow(void) {
    int position = 0;
    mdb_error error;
    mdb_group *group = mdb_group_create(&position, 1, 10, &error);
    assert(group);
    mdb_value value = integer(1);
    add(group, &value);
    assert(!mdb_group_test_set_count(group, 0, INT64_MAX));
    assert(mdb_group_add(group, &value, &error) == -1 && error.code == MDB_LIMIT);
    assert(mdb_group_count(group, 0) == INT64_MAX && mdb_group_size(group) == 1);
    mdb_group_free(group);
}

static void test_allocation_failures(void) {
    int positions[] = {0, 1};
    mdb_error error;
    allocation_budget = 0;
    assert(!mdb_group_create(positions, 2, 100, &error) && error.code == MDB_NOMEM);
    allocation_budget = -1;
    for (size_t initial = 0; initial <= 16; initial += 16) {
        int succeeded = 0;
        for (long budget = 0; budget < 8 && !succeeded; budget++) {
            mdb_group *group = mdb_group_create(positions, 2, 100, &error);
            assert(group);
            for (size_t i = 0; i < initial; i++) {
                char name[32];
                snprintf(name, sizeof(name), "key-%zu", i);
                mdb_value row[] = {string(name), string("second")};
                add(group, row);
            }
            const mdb_value *first = initial ? mdb_group_keys(group, 0) : NULL;
            mdb_value next[] = {string("new key"), string("new second")};
            allocation_budget = budget;
            int result = mdb_group_add(group, next, &error);
            allocation_budget = -1;
            if (result) {
                assert(error.code == MDB_NOMEM);
                assert(mdb_group_size(group) == initial);
            } else {
                succeeded = 1;
                assert(mdb_group_size(group) == initial + 1);
                assert(!strcmp(mdb_group_keys(group, initial)[0].s, "new key"));
            }
            for (size_t i = 0; i < initial; i++) {
                char name[32];
                snprintf(name, sizeof(name), "key-%zu", i);
                assert(!strcmp(mdb_group_keys(group, i)[0].s, name));
                assert(!strcmp(mdb_group_keys(group, i)[1].s, "second"));
                assert(mdb_group_count(group, i) == 1);
            }
            if (initial)
                assert(mdb_group_keys(group, 0) == first);
            mdb_group_free(group);
        }
        assert(succeeded);
    }
    mdb_group *group = mdb_group_create(positions, 2, 100, &error);
    assert(group);
    mdb_value first[] = {integer(2), string("b")};
    mdb_value second[] = {integer(1), string("a")};
    add(group, first);
    add(group, second);
    allocation_budget = 0;
    assert(!mdb_group_add(group, first, &error));
    allocation_budget = -1;
    assert(mdb_group_count(group, 0) == 2);
    int order_key = 0, ascending = 0;
    for (long budget = 0; budget < 3; budget++) {
        size_t *order = NULL;
        allocation_budget = budget;
        int result = mdb_group_order(group, &order_key, &ascending, 1, &order, &error);
        allocation_budget = -1;
        if (budget < 2)
            assert(result == -1 && error.code == MDB_NOMEM && !order);
        else {
            assert(!result && order[0] == 1 && order[1] == 0);
            free(order);
        }
        assert(mdb_group_size(group) == 2 && mdb_group_count(group, 0) == 2);
    }
    mdb_group_free(group);
}
#endif

int main(void) {
    test_empty_and_arguments();
    test_int_extremes_and_types();
    test_owned_strings();
    test_composite_keys();
    test_counts_stable_sort_and_limit();
    test_hash_growth_and_collisions();
#ifdef MDB_GROUP_TESTING
    test_count_overflow();
    test_allocation_failures();
#endif
    puts("PASS GROUP BY: typed composite hash keys, owned UTF-8 strings, COUNT, "
         "limits, stable ordering, collisions and cleanup");
    return 0;
}
