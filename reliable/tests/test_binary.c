#include "binary.h"
#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void clear(mdb_value *values, size_t count) {
    if (values) {
        for (size_t i = 0; i < count; i++)
            mdb_value_clear(&values[i]);
        free(values);
    }
}
static void rehash(unsigned char *slot, size_t size) {
    uint64_t h = UINT64_C(1469598103934665603);
    for (size_t i = 0; i < size; i++)
        if (i < 8 || i >= MDB_BIN_SLOT_HEADER) {
            h ^= slot[i];
            h *= UINT64_C(1099511628211);
        }
    for (unsigned i = 0; i < 8; i++)
        slot[8 + i] = (unsigned char)(h >> (i * 8U));
}
static void corrupt(const mdb_binary_column *columns, const unsigned char *slot, size_t size) {
    mdb_value *values = (mdb_value *)(void *)slot;
    int deleted = -1;
    mdb_error error = {0};
    assert(mdb_binary_decode(columns, 3, slot, size, &values, &deleted, &error) < 0);
    assert(!values && error.code == MDB_CORRUPT);
}
int main(void) {
    const mdb_binary_column columns[] = {{MDB_INT, 0}, {MDB_STRING, 80}, {MDB_INT, 0}};
    const size_t size = MDB_BIN_SLOT_HEADER + 8 + 4 + 80 + 8;
    assert(mdb_binary_record_size(columns, 3) == size);
    assert(!mdb_binary_record_size(NULL, 3));
    assert(!mdb_binary_record_size(columns, 0));
    assert(!mdb_binary_record_size(columns, MDB_COL_MAX + 1));
    const mdb_binary_column bad[] = {{MDB_STRING, 80}, {MDB_INT, 0}};
    assert(!mdb_binary_record_size(bad, 2));
    unsigned char header[MDB_BIN_HEADER_SIZE];
    mdb_error error = {0};
    assert(!mdb_binary_header_encode(header, UINT64_C(0x123456789abcdef0), 3, size));
    assert(!memcmp(header, "MDBBIN1\n", 8));
    assert(header[8] == 0xf0 && header[15] == 0x12 && header[16] == size);
    assert(!mdb_binary_header_validate(header, UINT64_C(0x123456789abcdef0), 3, size, &error));
    assert(mdb_binary_header_validate(header, 1, 3, size, &error) < 0);
    assert(mdb_binary_header_validate(header, UINT64_C(0x123456789abcdef0), 2, size, &error) < 0);
    header[31] ^= 1;
    assert(mdb_binary_header_validate(header, UINT64_C(0x123456789abcdef0), 3, size, &error) < 0);
    assert(mdb_binary_header_encode(header, 1, 0, size) < 0);

    unsigned char *slot = malloc(size), *copy = malloc(size);
    assert(slot && copy);
    mdb_value source[] = {{MDB_INT, INT64_MAX, NULL},
                          {MDB_STRING, 0, "총웨,\"quote\"\nsecond\r\nline  "},
                          {MDB_INT, INT64_MIN, NULL}};
    assert(!mdb_binary_encode(columns, 3, source, 0, slot, size, &error));
    assert(slot[0] == 1 && slot[MDB_BIN_SLOT_HEADER] == 0xff);
    mdb_value *values = NULL;
    int deleted;
    assert(!mdb_binary_decode(columns, 3, slot, size, &values, &deleted, &error));
    assert(!deleted && values[0].i == INT64_MAX && values[2].i == INT64_MIN);
    assert(!strcmp(values[1].s, source[1].s));
    clear(values, 3);
    memcpy(copy, slot, size);
    slot[size - 1] ^= 1;
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    corrupt(columns, slot, size - 1);
    slot[1] = 1;
    rehash(slot, size);
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    slot[0] = 2;
    rehash(slot, size);
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    slot[MDB_BIN_SLOT_HEADER + 8] = 81;
    rehash(slot, size);
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    slot[MDB_BIN_SLOT_HEADER + 8 + 4] = 0;
    rehash(slot, size);
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    slot[MDB_BIN_SLOT_HEADER + 8 + 4] = 0xff;
    rehash(slot, size);
    corrupt(columns, slot, size);
    memcpy(slot, copy, size);
    slot[MDB_BIN_SLOT_HEADER + 8 + 4 + 79] = 1;
    rehash(slot, size);
    corrupt(columns, slot, size);

    char maximum[81];
    memset(maximum, 'x', 80);
    maximum[80] = '\0';
    source[1].s = maximum;
    source[2].i = INT64_MAX;
    assert(!mdb_binary_encode(columns, 3, source, 0, slot, size, &error));
    assert(!mdb_binary_decode(columns, 3, slot, size, &values, &deleted, &error));
    assert(!strcmp(values[1].s, maximum) && values[2].i == INT64_MAX);
    clear(values, 3);
    source[1].s = "";
    source[2].i = -1;
    assert(!mdb_binary_encode(columns, 3, source, 0, slot, size, &error));
    assert(!mdb_binary_decode(columns, 3, slot, size, &values, &deleted, &error));
    assert(!strcmp(values[1].s, "") && values[2].i == -1);
    clear(values, 3);
    char too_long[82];
    memset(too_long, 'x', 81);
    too_long[81] = '\0';
    source[1].s = too_long;
    assert(mdb_binary_encode(columns, 3, source, 0, slot, size, &error) < 0);
    assert(error.code == MDB_TOO_LONG);
    source[1].s = "\xc0\xaf";
    assert(mdb_binary_encode(columns, 3, source, 0, slot, size, &error) < 0);
    assert(error.code == MDB_TYPE);
    source[1].type = MDB_INT;
    assert(mdb_binary_encode(columns, 3, source, 0, slot, size, &error) < 0);
    source[1].type = MDB_STRING;
    source[0].i = 0;
    assert(mdb_binary_encode(columns, 3, source, 0, slot, size, &error) < 0);
    source[0].i = 1;
    source[1].s = "ignored on deletion";
    assert(!mdb_binary_encode(columns, 3, source, 1, slot, size, &error));
    assert(slot[0] == 0 && slot[MDB_BIN_SLOT_HEADER] == 1);
    assert(!mdb_binary_decode(columns, 3, slot, size, &values, &deleted, &error));
    assert(deleted && !values);
    slot[size - 1] = 1;
    rehash(slot, size);
    corrupt(columns, slot, size);
    free(slot);
    free(copy);
    puts("PASS binary: fixed widths, LE/int64, UTF-8/escaping, tombstones, checksums, schema, corruption");
    return 0;
}
