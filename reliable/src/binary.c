#include "binary.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FNV_INIT UINT64_C(1469598103934665603)
#define FNV_PRIME UINT64_C(1099511628211)
static const unsigned char magic[8] = {'M', 'D', 'B', 'B', 'I', 'N', '1', '\n'};

static int fail(mdb_error *error, mdb_code code, const char *message) {
    if (error) {
        error->code = code;
        error->offset = 0;
        (void)snprintf(error->message, sizeof error->message, "%s", message);
    }
    return -1;
}
static void put32(unsigned char *out, uint32_t value) {
    for (unsigned i = 0; i < 4; i++)
        out[i] = (unsigned char)(value >> (8U * i));
}
static void put64(unsigned char *out, uint64_t value) {
    for (unsigned i = 0; i < 8; i++)
        out[i] = (unsigned char)(value >> (8U * i));
}
static uint32_t get32(const unsigned char *in) {
    uint32_t value = 0;
    for (unsigned i = 0; i < 4; i++)
        value |= (uint32_t)in[i] << (8U * i);
    return value;
}
static uint64_t get64(const unsigned char *in) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; i++)
        value |= (uint64_t)in[i] << (8U * i);
    return value;
}
static int64_t signed64(uint64_t value) {
    return value <= INT64_MAX ? (int64_t)value : -1 - (int64_t)(UINT64_MAX - value);
}
static uint64_t hash(uint64_t value, const unsigned char *data, size_t size) {
    for (size_t i = 0; i < size; i++) {
        value ^= data[i];
        value *= FNV_PRIME;
    }
    return value;
}
static uint64_t slot_hash(const unsigned char *data, size_t size) {
    uint64_t value = hash(FNV_INIT, data, 8);
    return hash(value, data + MDB_BIN_SLOT_HEADER, size - MDB_BIN_SLOT_HEADER);
}
static int zeros(const unsigned char *data, size_t size) {
    for (size_t i = 0; i < size; i++)
        if (data[i])
            return 0;
    return 1;
}
size_t mdb_binary_record_size(const mdb_binary_column *columns, size_t count) {
    if (!columns || !count || count > MDB_COL_MAX || columns[0].type != MDB_INT)
        return 0;
    size_t size = MDB_BIN_SLOT_HEADER;
    for (size_t i = 0; i < count; i++) {
        if (columns[i].type == MDB_INT && !columns[i].capacity)
            size += 8;
        else if (columns[i].type == MDB_STRING && columns[i].capacity &&
                 columns[i].capacity <= MDB_TEXT_MAX)
            size += 4 + columns[i].capacity;
        else
            return 0;
    }
    return size;
}
int mdb_binary_header_encode(unsigned char out[MDB_BIN_HEADER_SIZE], uint64_t schema_hash,
                             size_t count, size_t record_size) {
    if (!out || !count || count > MDB_COL_MAX || record_size < MDB_BIN_SLOT_HEADER + 8U ||
        record_size > MDB_BIN_SLOT_HEADER + MDB_COL_MAX * (4U + MDB_TEXT_MAX))
        return -1;
    memset(out, 0, MDB_BIN_HEADER_SIZE);
    memcpy(out, magic, sizeof magic);
    put64(out + 8, schema_hash);
    put32(out + 16, (uint32_t)record_size);
    put32(out + 20, (uint32_t)count);
    put64(out + 24, hash(FNV_INIT, out, 24));
    return 0;
}
int mdb_binary_header_validate(const unsigned char in[MDB_BIN_HEADER_SIZE],
                               uint64_t schema_hash, size_t count, size_t record_size,
                               mdb_error *error) {
    unsigned char expected[MDB_BIN_HEADER_SIZE];
    if (!in || mdb_binary_header_encode(expected, schema_hash, count, record_size))
        return fail(error, MDB_STATE, "Invalid binary header arguments");
    if (memcmp(in, expected, sizeof expected))
        return fail(error, MDB_CORRUPT, "Invalid binary header, version, or schema fingerprint");
    return 0;
}
int mdb_binary_encode(const mdb_binary_column *columns, size_t count, const mdb_value *values,
                      int deleted, unsigned char *out, size_t out_size, mdb_error *error) {
    size_t size = mdb_binary_record_size(columns, count);
    if (!size || !out || !values || out_size != size || (deleted != 0 && deleted != 1))
        return fail(error, MDB_STATE, "Invalid binary encoding arguments");
    if (values[0].type != MDB_INT || values[0].i <= 0)
        return fail(error, MDB_TYPE, "Binary records require a positive INT ID");
    memset(out, 0, size);
    out[0] = deleted ? 0 : 1;
    if (deleted) {
        put64(out + MDB_BIN_SLOT_HEADER, (uint64_t)values[0].i);
    } else {
        size_t offset = MDB_BIN_SLOT_HEADER;
        for (size_t i = 0; i < count; i++) {
            if (values[i].type != columns[i].type)
                return fail(error, MDB_TYPE, "Binary column type mismatch");
            if (columns[i].type == MDB_INT) {
                put64(out + offset, (uint64_t)values[i].i);
                offset += 8;
            } else {
                if (!values[i].s)
                    return fail(error, MDB_TYPE, "NULL STRING is unsupported");
                size_t length = strlen(values[i].s);
                if (length > columns[i].capacity)
                    return fail(error, MDB_TOO_LONG, "STRING exceeds binary slot capacity");
                if (!mdb_utf8(values[i].s, length))
                    return fail(error, MDB_TYPE, "Invalid UTF-8 STRING");
                put32(out + offset, (uint32_t)length);
                offset += 4;
                memcpy(out + offset, values[i].s, length);
                offset += columns[i].capacity;
            }
        }
    }
    put64(out + 8, slot_hash(out, size));
    return 0;
}
int mdb_binary_decode(const mdb_binary_column *columns, size_t count, const unsigned char *in,
                      size_t in_size, mdb_value **values, int *deleted, mdb_error *error) {
    if (values)
        *values = NULL;
    if (deleted)
        *deleted = 0;
    size_t size = mdb_binary_record_size(columns, count);
    if (!size || !in || !values || !deleted)
        return fail(error, MDB_STATE, "Invalid binary decoding arguments");
    if (in_size != size)
        return fail(error, MDB_CORRUPT, "Truncated or oversized binary slot");
    if (in[0] > 1 || !zeros(in + 1, 7) || get64(in + 8) != slot_hash(in, size))
        return fail(error, MDB_CORRUPT, "Invalid binary slot state, reserved bytes, or checksum");
    int64_t id = signed64(get64(in + MDB_BIN_SLOT_HEADER));
    if (id <= 0)
        return fail(error, MDB_CORRUPT, "Invalid binary record ID");
    if (!in[0]) {
        if (!zeros(in + MDB_BIN_SLOT_HEADER + 8, size - MDB_BIN_SLOT_HEADER - 8))
            return fail(error, MDB_CORRUPT, "Invalid deleted-slot padding");
        *deleted = 1;
        return 0;
    }
    mdb_value *result = calloc(count, sizeof *result);
    if (!result)
        return fail(error, MDB_NOMEM, "Allocation failed");
    size_t offset = MDB_BIN_SLOT_HEADER;
    for (size_t i = 0; i < count; i++) {
        result[i].type = columns[i].type;
        if (columns[i].type == MDB_INT) {
            result[i].i = signed64(get64(in + offset));
            offset += 8;
        } else {
            size_t length = get32(in + offset);
            offset += 4;
            if (length > columns[i].capacity ||
                !mdb_utf8((const char *)in + offset, length) ||
                !zeros(in + offset + length, columns[i].capacity - length)) {
                fail(error, MDB_CORRUPT, "Invalid binary STRING length, UTF-8, or padding");
                goto bad;
            }
            result[i].s = malloc(length + 1);
            if (!result[i].s) {
                fail(error, MDB_NOMEM, "Allocation failed");
                goto bad;
            }
            memcpy(result[i].s, in + offset, length);
            result[i].s[length] = '\0';
            offset += columns[i].capacity;
        }
    }
    *values = result;
    return 0;
bad:
    for (size_t i = 0; i < count; i++)
        mdb_value_clear(&result[i]);
    free(result);
    return -1;
}
