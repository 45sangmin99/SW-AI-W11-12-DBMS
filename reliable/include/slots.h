#ifndef MDB_SLOTS_H
#define MDB_SLOTS_H

#include <stddef.h>

typedef struct mdb_slots mdb_slots;
typedef void (*mdb_slots_callback)(void *value, void *context);

/*
 * Persistent, sparse size_t -> void * slots. NULL denotes an empty slot.
 * Each physical leaf retains its non-NULL values through these callbacks;
 * clones share nodes, so cloning need not call retain for every value.
 * Callbacks must not fail or re-enter this container. Either may be NULL.
 * context must remain valid until all clones have been freed.
 *
 * clone/set/free require external serialization. get is safe concurrently
 * with mutations to OTHER clones, provided its own container stays alive
 * and is not mutated. Returned values are borrowed until that snapshot is
 * changed or freed. Reference counters are deliberately non-atomic.
 */
mdb_slots *mdb_slots_create(mdb_slots_callback retain,
                            mdb_slots_callback release, void *context);
mdb_slots *mdb_slots_clone(const mdb_slots *slots);

/* 0 success, -1 allocation/argument failure; failure preserves contents.
 * Setting an existing value to the same pointer (including NULL) is a no-op.
 */
int mdb_slots_set(mdb_slots *slots, size_t index, void *value);
void *mdb_slots_get(const mdb_slots *slots, size_t index);
void mdb_slots_free(mdb_slots *slots);

#endif
