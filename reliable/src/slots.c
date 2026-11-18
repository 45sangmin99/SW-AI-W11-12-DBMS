#include "slots.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

#define RADIX_BITS 6U
#define RADIX_SIZE 64U
#define RADIX_MASK ((size_t)(RADIX_SIZE - 1U))

#ifdef MDB_SLOTS_TEST_ALLOC
extern void *mdb_slots_test_calloc(size_t count, size_t size);
#define slots_calloc mdb_slots_test_calloc
#else
#define slots_calloc calloc
#endif

typedef struct slot_node slot_node;
struct slot_node {
    size_t references;
    uint64_t present;
    union {
        slot_node *children[RADIX_SIZE];
        void *values[RADIX_SIZE];
    } entries;
};

struct mdb_slots {
    slot_node *root;
    unsigned height; /* zero denotes a leaf; each level consumes six bits */
    mdb_slots_callback retain;
    mdb_slots_callback release;
    void *context;
};

static unsigned required_height(size_t index) {
    unsigned height = 0;
    while ((index >>= RADIX_BITS) != 0)
        height++;
    return height;
}

static unsigned position_at(size_t index, unsigned height) {
    /* height * 6 is always strictly smaller than the size_t bit width. */
    return (unsigned)((index >> (height * RADIX_BITS)) & RADIX_MASK);
}

static slot_node *new_node(void) {
    slot_node *node = slots_calloc(1, sizeof(*node));
    if (node)
        node->references = 1;
    return node;
}

static void release_node(const mdb_slots *slots, slot_node *node,
                         unsigned height) {
    if (!node || --node->references != 0)
        return;
    for (unsigned i = 0; i < RADIX_SIZE; i++) {
        if (!(node->present & (UINT64_C(1) << i)))
            continue;
        if (height)
            release_node(slots, node->entries.children[i], height - 1);
        else if (slots->release)
            slots->release(node->entries.values[i], slots->context);
    }
    free(node);
}

static slot_node *copy_node(const mdb_slots *slots, const slot_node *source,
                            unsigned height) {
    slot_node *copy = new_node();
    if (!copy || !source)
        return copy;
    copy->present = source->present;
    for (unsigned i = 0; i < RADIX_SIZE; i++) {
        if (!(source->present & (UINT64_C(1) << i)))
            continue;
        if (height) {
            copy->entries.children[i] = source->entries.children[i];
            copy->entries.children[i]->references++;
        } else {
            copy->entries.values[i] = source->entries.values[i];
            if (slots->retain)
                slots->retain(copy->entries.values[i], slots->context);
        }
    }
    return copy;
}

/*
 * Allocate shared ancestors before descending. Only after the recursive call
 * succeeds do we mutate an exclusive ancestor. Thus no allocation can fail
 * after the first logical mutation, and allocation failure preserves the old
 * tree without allocating an undo log. A copied parent retains its children,
 * which makes its selected child shared and propagates COW down the path.
 *
 * The caller owns the old node reference. A different return pointer owns a
 * new reference, and the caller must release the old one after success.
 */
static slot_node *set_node(const mdb_slots *slots, slot_node *old,
                           unsigned height, size_t index, void *value,
                           int *failed) {
    int copied = !old || old->references > 1;
    slot_node *node = copied ? copy_node(slots, old, height) : old;
    if (!node) {
        *failed = 1;
        return NULL;
    }
    unsigned position = position_at(index, height);
    uint64_t bit = UINT64_C(1) << position;
    if (height) {
        slot_node *child = node->entries.children[position];
        slot_node *replacement = set_node(slots, child, height - 1,
                                          index, value, failed);
        if (*failed) {
            if (copied)
                release_node(slots, node, height);
            return NULL;
        }
        if (replacement != child) {
            release_node(slots, child, height - 1);
            node->entries.children[position] = replacement;
        }
        if (replacement)
            node->present |= bit;
        else
            node->present &= ~bit;
    } else {
        void *previous = node->entries.values[position];
        if (value && slots->retain)
            slots->retain(value, slots->context);
        if (previous && slots->release)
            slots->release(previous, slots->context);
        node->entries.values[position] = value;
        if (value)
            node->present |= bit;
        else
            node->present &= ~bit;
    }
    if (!node->present) {
        if (copied)
            release_node(slots, node, height);
        return NULL;
    }
    return node;
}

mdb_slots *mdb_slots_create(mdb_slots_callback retain,
                            mdb_slots_callback release, void *context) {
    mdb_slots *slots = slots_calloc(1, sizeof(*slots));
    if (!slots)
        return NULL;
    slots->retain = retain;
    slots->release = release;
    slots->context = context;
    return slots;
}

mdb_slots *mdb_slots_clone(const mdb_slots *slots) {
    if (!slots) {
        errno = EINVAL;
        return NULL;
    }
    mdb_slots *copy = slots_calloc(1, sizeof(*copy));
    if (!copy)
        return NULL;
    *copy = *slots;
    if (copy->root)
        copy->root->references++;
    return copy;
}

void *mdb_slots_get(const mdb_slots *slots, size_t index) {
    if (!slots || !slots->root || required_height(index) > slots->height)
        return NULL;
    const slot_node *node = slots->root;
    for (unsigned height = slots->height; height; height--) {
        node = node->entries.children[position_at(index, height)];
        if (!node)
            return NULL;
    }
    return node->entries.values[index & RADIX_MASK];
}

int mdb_slots_set(mdb_slots *slots, size_t index, void *value) {
    if (!slots) {
        errno = EINVAL;
        return -1;
    }
    if (mdb_slots_get(slots, index) == value)
        return 0;
    unsigned height = required_height(index);
    if (height < slots->height)
        height = slots->height;
    slot_node *root = slots->root;
    unsigned staged_height = slots->height;
    int expanded = root && height > staged_height;
    if (expanded) {
        root->references++;
        while (staged_height < height) {
            slot_node *parent = new_node();
            if (!parent) {
                release_node(slots, root, staged_height);
                return -1;
            }
            parent->present = UINT64_C(1);
            parent->entries.children[0] = root;
            root = parent;
            staged_height++;
        }
    }
    int failed = 0;
    slot_node *replacement = set_node(slots, root, height, index, value, &failed);
    if (failed) {
        if (expanded)
            release_node(slots, root, height);
        return -1;
    }
    if (expanded) {
        if (root != replacement)
            release_node(slots, root, height);
        release_node(slots, slots->root, slots->height);
    } else if (slots->root != replacement) {
        release_node(slots, slots->root, slots->height);
    }
    slots->root = replacement;
    slots->height = height;
    /* Remove redundant leading zero digits after deleting a high slot. */
    while (slots->root && slots->height &&
           slots->root->present == UINT64_C(1)) {
        slot_node *parent = slots->root;
        slots->root = parent->entries.children[0];
        slots->root->references++;
        release_node(slots, parent, slots->height);
        slots->height--;
    }
    if (!slots->root)
        slots->height = 0;
    return 0;
}

void mdb_slots_free(mdb_slots *slots) {
    if (!slots)
        return;
    release_node(slots, slots->root, slots->height);
    free(slots);
}
