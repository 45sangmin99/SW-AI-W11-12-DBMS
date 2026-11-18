/* Query-local aggregation over the same snapshot/overlay scan as SELECT.
 * No global comparator or shared query state. MONTH takes Unix seconds, UTC. */
typedef struct {
    mdb_value values[MDB_COL_MAX];
    long double averages[MDB_COL_MAX];
} aggregate_row;
typedef struct {
    mdb_group *groups;
    aggregate_row *rows;
    size_t n, cap;
    int source[MDB_COL_MAX], key_source[MDB_COL_MAX], projection[MDB_COL_MAX];
    int ordering[MDB_COL_MAX];
    const mdb_stmt *stmt;
} analytics;
static int analytics_needed(const mdb_stmt *st) {
    for (size_t i = 0; i < st->ncolumns; i++)
        if (st->column_counts[i] > 1) return 1;
    for (size_t i = 0; i < st->ngroup_by; i++)
        if (st->group_functions[i]) return 1;
    for (size_t i = 0; i < st->norder; i++)
        if (st->order[i].count > 1) return 1;
    return 0;
}
static int month_value(const mdb_value *v, char *buffer, mdb_value *out, mdb_error *e) {
    if (v->type != MDB_INT) return fail(e, MDB_TYPE, "MONTH requires Unix epoch seconds (INT)");
    /* Limit to years 0001..9999; avoid platform-dependent time_t overflow. */
    if (v->i < INT64_C(-62135596800) || v->i > INT64_C(253402300799))
        return fail(e, MDB_LIMIT, "MONTH timestamp outside years 0001..9999");
    time_t seconds = (time_t)v->i;
    struct tm tm;
    if ((int64_t)seconds != v->i || !gmtime_r(&seconds, &tm))
        return fail(e, MDB_LIMIT, "MONTH timestamp not supported on this platform");
    (void)snprintf(buffer, 16, "%04d-%02d", tm.tm_year + 1900, tm.tm_mon + 1);
    *out = (mdb_value){.type = MDB_STRING, .s = buffer};
    return 0;
}
static int analytics_prepare(mdb_db *db, const tabledef *t, const mdb_stmt *st,
                             analytics *a, mdb_result *out) {
    static const char *names[] = {"", "count", "sum", "avg", "min", "max", "month"};
    a->stmt = st;
    int positions[MDB_COL_MAX];
    for (size_t k = 0; k < st->ngroup_by; k++) {
        a->key_source[k] = find_column(t, st->group_by[k]);
        if (a->key_source[k] < 0) return fail(&out->error, MDB_UNKNOWN_COLUMN, "Unknown GROUP BY column");
        if (st->group_functions[k] && t->cols[a->key_source[k]].type != MDB_INT)
            return fail(&out->error, MDB_TYPE, "MONTH requires INT");
        for (size_t j = 0; j < k; j++)
            if (a->key_source[k] == a->key_source[j] && st->group_functions[k] == st->group_functions[j])
                return fail(&out->error, MDB_COLUMN_COUNT, "Duplicate GROUP BY expression");
        positions[k] = (int)k;
    }
    if (!st->ngroup_by) positions[0] = 0;
    if (st->star) return fail(&out->error, MDB_SYNTAX, "Aggregate projection requires explicit columns");
    out->ncolumns = st->ncolumns;
    out->columns = calloc(out->ncolumns, sizeof *out->columns);
    out->types = calloc(out->ncolumns, sizeof *out->types);
    if (!out->columns || !out->types) return fail(&out->error, MDB_NOMEM, "Aggregation allocation failed");
    for (size_t j = 0; j < st->ncolumns; j++) {
        int f = st->column_counts[j];
        int col = f == 1 ? 0 : find_column(t, st->columns[j]);
        if (col < 0) return fail(&out->error, MDB_UNKNOWN_COLUMN, "Unknown aggregate column");
        a->source[j] = col;
        a->projection[j] = -1;
        if (!f || f == 6) {
            for (size_t k = 0; k < st->ngroup_by; k++)
                if (a->key_source[k] == col && st->group_functions[k] == f) a->projection[j] = (int)k;
            if (a->projection[j] < 0) return fail(&out->error, MDB_SYNTAX, "Nonaggregate expression must appear in GROUP BY");
        }
        if ((f == 2 || f == 3) && t->cols[col].type != MDB_INT)
            return fail(&out->error, MDB_TYPE, "SUM/AVG require INT");
        out->types[j] = f == 3 ? MDB_REAL : f == 6 ? MDB_STRING :
                        (f == 1 || f == 2) ? MDB_INT : t->cols[col].type;
        out->columns[j] = strdup(st->aliases[j] ? st->aliases[j] : f ? names[f] : st->columns[j]);
        if (!out->columns[j]) return fail(&out->error, MDB_NOMEM, "Aggregation allocation failed");
    }
    for (size_t k = 0; k < st->norder; k++) {
        a->ordering[k] = -1;
        for (size_t j = 0; j < st->ncolumns; j++) {
            if ((!st->order[k].count && st->aliases[j] && !strcmp(st->order[k].column, st->aliases[j])) ||
                (st->order[k].count == st->column_counts[j] && !strcmp(st->order[k].column, st->columns[j]))) {
                a->ordering[k] = (int)j; break;
            }
        }
        if (a->ordering[k] < 0) return fail(&out->error, MDB_SYNTAX, "Aggregate ORDER BY must name a projected expression or alias");
    }
    a->groups = mdb_group_create(positions, st->ngroup_by ? st->ngroup_by : 1, db->max_groups, &out->error);
    return a->groups ? 0 : -1;
}
static int analytics_visit(analytics *a, const mdb_value *row, mdb_error *e) {
    const mdb_stmt *st = a->stmt;
    mdb_value keys[MDB_COL_MAX] = {{.type = MDB_INT, .i = 0}};
    char months[MDB_COL_MAX][16];
    for (size_t k = 0; k < st->ngroup_by; k++) {
        keys[k] = row[a->key_source[k]];
        if (st->group_functions[k] && month_value(&keys[k], months[k], &keys[k], e)) return -1;
    }
    if (mdb_group_add(a->groups, keys, e)) return -1;
    size_t index = mdb_group_last_index(a->groups);
    if (index >= a->n) {
        if (grow((void **)&a->rows, &a->cap, index + 1, sizeof *a->rows))
            return fail(e, MDB_NOMEM, "Aggregation allocation failed");
        memset(&a->rows[index], 0, sizeof *a->rows);
        a->n = index + 1;
    }
    aggregate_row *r = &a->rows[index];
    int64_t count = mdb_group_count(a->groups, index);
    for (size_t j = 0; j < st->ncolumns; j++) {
        int f = st->column_counts[j];
        mdb_value *v = &r->values[j];
        const mdb_value *src = &row[a->source[j]];
        if (!f || f == 6) {
            if (count == 1 && mdb_value_copy(v, &keys[a->projection[j]])) return fail(e, MDB_NOMEM, "Aggregation allocation failed");
        } else if (f == 1) *v = (mdb_value){.type = MDB_INT, .i = count};
        else if (f == 2) {
            if ((src->i > 0 && v->i > INT64_MAX - src->i) || (src->i < 0 && v->i < INT64_MIN - src->i))
                return fail(e, MDB_LIMIT, "SUM exceeds int64 range");
            v->type = MDB_INT; v->i += src->i;
        } else if (f == 3) {
            a->rows[index].averages[j] += (long double)src->i;
            char number[64];
            (void)snprintf(number, sizeof number, "%.17g", (double)(r->averages[j] / count));
            char *copy = strdup(number);
            if (!copy) return fail(e, MDB_NOMEM, "AVG allocation failed");
            mdb_value_clear(v);
            *v = (mdb_value){.type = MDB_REAL, .s = copy};
        } else if (count == 1 || (f == 4 ? value_cmp(src, v) < 0 : value_cmp(src, v) > 0)) {
            mdb_value copy = {0};
            if (mdb_value_copy(&copy, src)) return fail(e, MDB_NOMEM, "Aggregation allocation failed");
            mdb_value_clear(v); *v = copy;
        }
    }
    return 0;
}
static int analytics_compare(const analytics *a, size_t l, size_t r) {
    for (size_t k = 0; k < a->stmt->norder; k++) {
        int j = a->ordering[k];
        const mdb_value *x = &a->rows[l].values[j], *y = &a->rows[r].values[j];
        int cmp;
        if (x->type == MDB_REAL) {
            long double lvalue = strtold(x->s, NULL), rvalue = strtold(y->s, NULL);
            cmp = (lvalue > rvalue) - (lvalue < rvalue);
        } else cmp = value_cmp(x, y);
        if (cmp) return a->stmt->order[k].descending ? -cmp : cmp;
    }
    return 0;
}
static int analytics_result(analytics *a, mdb_result *out) {
    const mdb_stmt *st = a->stmt;
    if (!a->n && !st->ngroup_by) {
        a->rows = calloc(1, sizeof *a->rows);
        if (!a->rows) return fail(&out->error, MDB_NOMEM, "Aggregation allocation failed");
        a->n = a->cap = 1;
        for (size_t j = 0; j < st->ncolumns; j++)
            if (st->column_counts[j] == 1) a->rows[0].values[j].type = MDB_INT;
    }
    size_t *order = malloc((a->n ? a->n : 1) * sizeof *order);
    size_t *tmp = malloc((a->n ? a->n : 1) * sizeof *tmp);
    if (!order || !tmp) { free(order); free(tmp); return fail(&out->error, MDB_NOMEM, "Aggregation allocation failed"); }
    for (size_t i = 0; i < a->n; i++) order[i] = i;
    for (size_t width = 1; st->norder && width < a->n;) {
        for (size_t start = 0; start < a->n; start += 2 * width) {
            size_t mid = start + width < a->n ? start + width : a->n;
            size_t end = mid + width < a->n ? mid + width : a->n;
            size_t l = start, r = mid, o = start;
            while (l < mid && r < end) tmp[o++] = analytics_compare(a, order[l], order[r]) <= 0 ? order[l++] : order[r++];
            while (l < mid) tmp[o++] = order[l++];
            while (r < end) tmp[o++] = order[r++];
        }
        memcpy(order, tmp, a->n * sizeof *order);
        if (width > a->n / 2) break;
        width *= 2;
    }
    size_t start = (uint64_t)st->offset < a->n ? (size_t)st->offset : a->n;
    size_t n = a->n - start;
    if (st->limit >= 0 && (uint64_t)st->limit < n) n = (size_t)st->limit;
    int rc = -1;
    if (n > MDB_RESULT_MAX) { fail(&out->error, MDB_LIMIT, "Aggregate result exceeds 10000 rows; use LIMIT"); goto done; }
    out->rows = calloc(n ? n : 1, sizeof *out->rows);
    if (!out->rows) { fail(&out->error, MDB_NOMEM, "Aggregation allocation failed"); goto done; }
    out->nrows = n;
    for (size_t i = 0; i < n; i++) {
        out->rows[i].values = calloc(out->ncolumns, sizeof(mdb_value));
        if (!out->rows[i].values) { fail(&out->error, MDB_NOMEM, "Aggregation allocation failed"); goto done; }
        for (size_t j = 0; j < out->ncolumns; j++)
            if (mdb_value_copy(&out->rows[i].values[j], &a->rows[order[start+i]].values[j])) {
                fail(&out->error, MDB_NOMEM, "Aggregation allocation failed"); goto done;
            }
    }
    rc = 0;
done:
    free(tmp); free(order); return rc;
}
static void analytics_clear(analytics *a) {
    for (size_t i = 0; i < a->n; i++)
        for (size_t j = 0; j < a->stmt->ncolumns; j++) mdb_value_clear(&a->rows[i].values[j]);
    free(a->rows); mdb_group_free(a->groups);
}
