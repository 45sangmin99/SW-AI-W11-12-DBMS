#ifndef MDB_BINARY_H
#define MDB_BINARY_H

#include "mdb.h"

/* This is a wire format, not the layout of any C structure. All integers are LE.
 * Header: magic[8], schema hash[8], slot size[4], columns[4], checksum[8].
 * Slot: live flag[1], reserved zero[7], checksum[8], fixed-width column payload.
 * STRING payloads contain a byte length[4] followed by capacity bytes. */
#define MDB_BIN_HEADER_SIZE 32U
#define MDB_BIN_SLOT_HEADER 16U

typedef struct {
    mdb_type type;
    size_t capacity; /* STRING UTF-8 byte capacity; zero for INT. */
} mdb_binary_column;

/* A schema must start with INT (the positive automatic ID). Zero means invalid. */
size_t mdb_binary_record_size(const mdb_binary_column *columns, size_t count);
int mdb_binary_header_encode(unsigned char out[MDB_BIN_HEADER_SIZE], uint64_t schema_hash,
                             size_t count, size_t record_size);
int mdb_binary_header_validate(const unsigned char in[MDB_BIN_HEADER_SIZE],
                               uint64_t schema_hash, size_t count, size_t record_size,
                               mdb_error *error);

/* Buffers must contain exactly record_size bytes. Deleted slots retain only ID;
 * values[0] must be supplied, and the remaining values are not inspected.
 * On encode error, the output buffer must not be persisted. */
int mdb_binary_encode(const mdb_binary_column *columns, size_t count, const mdb_value *values,
                      int deleted, unsigned char *out, size_t out_size, mdb_error *error);
/* On success a live row owns an array and strings (mdb_value_clear + free).
 * A deleted slot returns *values=NULL and *deleted=1. On failure *values=NULL. */
int mdb_binary_decode(const mdb_binary_column *columns, size_t count, const unsigned char *in,
                      size_t in_size, mdb_value **values, int *deleted, mdb_error *error);

#endif
