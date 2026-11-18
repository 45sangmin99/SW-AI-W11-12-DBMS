#ifndef MDB_GROUP_H
#define MDB_GROUP_H

#include "mdb.h"

typedef struct mdb_group mdb_group;

/* A query-local typed hash aggregator. Key positions are zero-based source
 * column positions, nkeys is 1..MDB_COL_MAX, and max_groups must be positive.
 * Rows passed to add must contain every selected source column. */
mdb_group *mdb_group_create(const int *key_positions, size_t nkeys,
                             size_t max_groups, mdb_error *error);

/* Owns copies of keys only for new groups. Existing keys increment COUNT(*).
 * Failure leaves all group keys and counts unchanged. Serialize all access. */
int mdb_group_add(mdb_group *group, const mdb_value *row, mdb_error *error);
size_t mdb_group_last_index(const mdb_group *group);
size_t mdb_group_size(const mdb_group *group);

/* Borrowed keys remain valid until free. Invalid indexes return NULL / zero. */
const mdb_value *mdb_group_keys(const mdb_group *group, size_t index);
int64_t mdb_group_count(const mdb_group *group, size_t index);

/* Return an owned array of group indexes; free it with free(). Empty input
 * succeeds with *indices=NULL. norder=0 preserves first appearance order.
 * key_indices select grouped keys (0..nkeys-1), or -1 for COUNT(*).
 * descending must contain 0 (ASC) or 1 (DESC) for every order key.
 * Equal sort keys keep first appearance order. Strings compare by bytes;
 * unlike types, if supplied, sort INT before STRING without conversion.
 */
int mdb_group_order(const mdb_group *group, const int *key_indices,
                     const int *descending, size_t norder, size_t **indices,
                     mdb_error *error);
void mdb_group_free(mdb_group *group);

#endif
