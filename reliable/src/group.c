#include "group.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HASH_INITIAL UINT64_C(1469598103934665603)
#define HASH_PRIME UINT64_C(1099511628211)

#ifdef MDB_GROUP_TESTING
extern void *mdb_group_test_malloc(size_t size);
extern void *mdb_group_test_calloc(size_t count, size_t size);
extern void *mdb_group_test_realloc(void *pointer, size_t size);
#define group_malloc mdb_group_test_malloc
#define group_calloc mdb_group_test_calloc
#define group_realloc mdb_group_test_realloc
#else
#define group_malloc malloc
#define group_calloc calloc
#define group_realloc realloc
#endif

typedef struct {
    mdb_value *keys;
    int64_t count;
    uint64_t hash;
} group_entry;

struct mdb_group {
    int positions[MDB_COL_MAX];
    size_t nkeys, maximum, count, capacity, nbuckets;
    group_entry *entries; /* First appearance order, independent of hash order. */
    size_t *buckets;      /* Entry index + 1; zero denotes an empty bucket. */
};

static void clear_error(mdb_error *error) {
    if (error)
        memset(error, 0, sizeof(*error));
}

static int fail(mdb_error *error, mdb_code code, const char *message) {
    if (error) {
        error->code = code;
        error->offset = 0;
        (void)snprintf(error->message, sizeof(error->message), "%s", message);
    }
    return -1;
}

static uint64_t hash_byte(uint64_t hash, unsigned char byte) {
    return (hash ^ byte) * HASH_PRIME;
}

static uint64_t hash_integer(uint64_t hash, uint64_t value) {
    for (unsigned i = 0; i < 8; i++)
        hash = hash_byte(hash, (unsigned char)(value >> (8U * i)));
    return hash;
}

static int row_hash(const mdb_group *group, const mdb_value *row,
                     uint64_t *result, mdb_error *error) {
    uint64_t hash = HASH_INITIAL;
    for (size_t i = 0; i < group->nkeys; i++) {
        const mdb_value *value = &row[group->positions[i]];
        hash = hash_byte(hash, (unsigned char)value->type);
        if (value->type == MDB_INT)
            hash = hash_integer(hash, (uint64_t)value->i);
        else if (value->type == MDB_STRING) {
            if (!value->s)
                return fail(error, MDB_TYPE, "NULL GROUP BY key is unsupported");
            size_t length = strlen(value->s);
            if (!mdb_utf8(value->s, length))
                return fail(error, MDB_TYPE, "Invalid UTF-8 GROUP BY key");
            /* Include lengths to distinguish ('ab','c') from ('a','bc'). */
            hash = hash_integer(hash, (uint64_t)length);
            for (size_t j = 0; j < length; j++)
                hash = hash_byte(hash, (unsigned char)value->s[j]);
        } else
            return fail(error, MDB_TYPE, "Invalid GROUP BY key type");
    }
#ifdef MDB_GROUP_TEST_COLLISIONS
    hash = 1; /* Exercise full-key comparison despite identical hash values. */
#endif
    *result = hash;
    return 0;
}

static int equal_keys(const mdb_group *group, const mdb_value *keys,
                       const mdb_value *row) {
    for (size_t i = 0; i < group->nkeys; i++) {
        const mdb_value *value = &row[group->positions[i]];
        if (keys[i].type != value->type)
            return 0;
        if (value->type == MDB_INT) {
            if (keys[i].i != value->i)
                return 0;
        } else if (strcmp(keys[i].s, value->s))
            return 0;
    }
    return 1;
}

static void free_keys(mdb_value *keys, size_t count) {
    if (!keys)
        return;
    for (size_t i = 0; i < count; i++)
        mdb_value_clear(&keys[i]);
    free(keys);
}

static mdb_value *copy_keys(const mdb_group *group, const mdb_value *row) {
    mdb_value *keys = group_calloc(group->nkeys, sizeof(*keys));
    if (!keys)
        return NULL;
    for (size_t i = 0; i < group->nkeys; i++) {
        const mdb_value *value = &row[group->positions[i]];
        keys[i].type = value->type;
        if (value->type == MDB_INT)
            keys[i].i = value->i;
        else {
            size_t size = strlen(value->s) + 1;
            keys[i].s = group_malloc(size);
            if (!keys[i].s) {
                free_keys(keys, group->nkeys);
                return NULL;
            }
            memcpy(keys[i].s, value->s, size);
        }
    }
    return keys;
}

static int reserve_entries(mdb_group *group, mdb_error *error) {
    if (group->count < group->capacity)
        return 0;
    size_t capacity = group->capacity ? group->capacity : 16;
    if (capacity <= group->count)
        capacity = capacity > SIZE_MAX / 2 ? group->maximum : capacity * 2;
    if (capacity > group->maximum)
        capacity = group->maximum;
    if (capacity <= group->count || capacity > SIZE_MAX / sizeof(*group->entries))
        return fail(error, MDB_LIMIT, "GROUP BY workspace size overflow");
    group_entry *entries = group_realloc(group->entries, capacity * sizeof(*entries));
    if (!entries)
        return fail(error, MDB_NOMEM, "GROUP BY allocation failed");
    group->entries = entries;
    group->capacity = capacity;
    return 0;
}

static int reserve_buckets(mdb_group *group, mdb_error *error) {
    size_t capacity = group->nbuckets ? group->nbuckets : 16;
    while (group->count + 1 > capacity / 2) {
        if (capacity > SIZE_MAX / 2)
            return fail(error, MDB_LIMIT, "GROUP BY hash table size overflow");
        capacity *= 2;
    }
    if (capacity == group->nbuckets)
        return 0;
    if (capacity > SIZE_MAX / sizeof(*group->buckets))
        return fail(error, MDB_LIMIT, "GROUP BY hash table size overflow");
    size_t *buckets = group_calloc(capacity, sizeof(*buckets));
    if (!buckets)
        return fail(error, MDB_NOMEM, "GROUP BY allocation failed");
    for (size_t i = 0; i < group->count; i++) {
        size_t bucket = (size_t)group->entries[i].hash & (capacity - 1);
        while (buckets[bucket])
            bucket = (bucket + 1) & (capacity - 1);
        buckets[bucket] = i + 1;
    }
    free(group->buckets);
    group->buckets = buckets;
    group->nbuckets = capacity;
    return 0;
}

mdb_group *mdb_group_create(const int *key_positions, size_t nkeys,
                             size_t max_groups, mdb_error *error) {
    clear_error(error);
    if (!key_positions || !nkeys || nkeys > MDB_COL_MAX) {
        fail(error, MDB_STATE, "Invalid GROUP BY key positions");
        return NULL;
    }
    for (size_t i = 0; i < nkeys; i++)
        if (key_positions[i] < 0 || key_positions[i] >= MDB_COL_MAX) {
            fail(error, MDB_STATE, "Invalid GROUP BY source column");
            return NULL;
        }
    if (!max_groups) {
        fail(error, MDB_LIMIT, "GROUP BY group limit must be positive");
        return NULL;
    }
    mdb_group *group = group_calloc(1, sizeof(*group));
    if (!group) {
        fail(error, MDB_NOMEM, "GROUP BY allocation failed");
        return NULL;
    }
    memcpy(group->positions, key_positions, nkeys * sizeof(*key_positions));
    group->nkeys = nkeys;
    group->maximum = max_groups;
    return group;
}

int mdb_group_add(mdb_group *group, const mdb_value *row, mdb_error *error) {
    clear_error(error);
    if (!group || !row)
        return fail(error, MDB_STATE, "Invalid GROUP BY input");
    uint64_t hash;
    if (row_hash(group, row, &hash, error))
        return -1;
    if (group->nbuckets) {
        size_t bucket = (size_t)hash & (group->nbuckets - 1);
        while (group->buckets[bucket]) {
            group_entry *entry = &group->entries[group->buckets[bucket] - 1];
            if (entry->hash == hash && equal_keys(group, entry->keys, row)) {
                if (entry->count == INT64_MAX)
                    return fail(error, MDB_LIMIT, "COUNT(*) exceeds INT64_MAX");
                entry->count++;
                return 0;
            }
            bucket = (bucket + 1) & (group->nbuckets - 1);
        }
    }
    if (group->count >= group->maximum)
        return fail(error, MDB_LIMIT, "GROUP BY workspace group limit reached");
    mdb_value *keys = copy_keys(group, row);
    if (!keys)
        return fail(error, MDB_NOMEM, "GROUP BY allocation failed");
    if (reserve_entries(group, error) || reserve_buckets(group, error)) {
        free_keys(keys, group->nkeys);
        return -1;
    }
    size_t bucket = (size_t)hash & (group->nbuckets - 1);
    while (group->buckets[bucket])
        bucket = (bucket + 1) & (group->nbuckets - 1);
    group->entries[group->count] = (group_entry){.keys = keys, .count = 1, .hash = hash};
    group->buckets[bucket] = ++group->count;
    return 0;
}

size_t mdb_group_size(const mdb_group *group) {
    return group ? group->count : 0;
}

const mdb_value *mdb_group_keys(const mdb_group *group, size_t index) {
    return group && index < group->count ? group->entries[index].keys : NULL;
}

int64_t mdb_group_count(const mdb_group *group, size_t index) {
    return group && index < group->count ? group->entries[index].count : 0;
}

static int value_compare(const mdb_value *a, const mdb_value *b) {
    if (a->type != b->type)
        return (a->type > b->type) - (a->type < b->type);
    if (a->type == MDB_INT)
        return (a->i > b->i) - (a->i < b->i);
    int result = strcmp(a->s, b->s);
    return (result > 0) - (result < 0);
}

static int compare(const mdb_group *group, size_t a, size_t b,
                     const int *keys, const int *descending, size_t norder) {
    for (size_t i = 0; i < norder; i++) {
        int result;
        if (keys[i] < 0) {
            int64_t left = group->entries[a].count;
            int64_t right = group->entries[b].count;
            result = (left > right) - (left < right);
        } else
            result = value_compare(&group->entries[a].keys[keys[i]],
                                   &group->entries[b].keys[keys[i]]);
        if (result)
            return descending[i] ? -result : result;
    }
    return 0;
}

int mdb_group_order(const mdb_group *group, const int *key_indices,
                     const int *descending, size_t norder, size_t **indices,
                     mdb_error *error) {
    clear_error(error);
    if (!indices)
        return fail(error, MDB_STATE, "Invalid GROUP BY ordering output");
    *indices = NULL;
    if (!group || (norder && (!key_indices || !descending)))
        return fail(error, MDB_STATE, "Invalid GROUP BY ordering arguments");
    for (size_t i = 0; i < norder; i++)
        if (key_indices[i] < -1 ||
            (key_indices[i] >= 0 && (size_t)key_indices[i] >= group->nkeys) ||
            (descending[i] != 0 && descending[i] != 1))
            return fail(error, MDB_STATE, "Invalid GROUP BY ordering key");
    if (!group->count)
        return 0;
    if (group->count > SIZE_MAX / sizeof(size_t))
        return fail(error, MDB_LIMIT, "GROUP BY ordering size overflow");
    size_t *order = group_malloc(group->count * sizeof(*order));
    if (!order)
        return fail(error, MDB_NOMEM, "GROUP BY ordering allocation failed");
    for (size_t i = 0; i < group->count; i++)
        order[i] = i;
    if (!norder || group->count < 2) {
        *indices = order;
        return 0;
    }
    size_t *temporary = group_malloc(group->count * sizeof(*temporary));
    if (!temporary) {
        free(order);
        return fail(error, MDB_NOMEM, "GROUP BY ordering allocation failed");
    }
    /* Overflow-safe bottom-up stable merge sort, without a global comparator. */
    for (size_t width = 1; width < group->count;) {
        size_t start = 0;
        while (start < group->count) {
            size_t middle = start + (width < group->count - start ? width : group->count - start);
            size_t end = middle + (width < group->count - middle ? width : group->count - middle);
            size_t left = start, right = middle, output = start;
            while (left < middle && right < end) {
                if (compare(group, order[left], order[right], key_indices,
                            descending, norder) <= 0)
                    temporary[output++] = order[left++];
                else
                    temporary[output++] = order[right++];
            }
            while (left < middle)
                temporary[output++] = order[left++];
            while (right < end)
                temporary[output++] = order[right++];
            start = end;
        }
        memcpy(order, temporary, group->count * sizeof(*order));
        if (width > group->count / 2)
            break;
        width *= 2;
    }
    free(temporary);
    *indices = order;
    return 0;
}

void mdb_group_free(mdb_group *group) {
    if (!group)
        return;
    for (size_t i = 0; i < group->count; i++)
        free_keys(group->entries[i].keys, group->nkeys);
    free(group->entries);
    free(group->buckets);
    free(group);
}

#ifdef MDB_GROUP_TESTING
int mdb_group_test_set_count(mdb_group *group, size_t index, int64_t count) {
    if (!group || index >= group->count || count < 1)
        return -1;
    group->entries[index].count = count;
    return 0;
}
#endif
