/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: database sessions on an agent
 *
 * The typed layer over the four nosql_* RPCs: every function builds the
 * arguments of one operation as JSON, runs it with rpc_nosql_call() and
 * takes the result apart.
 */

#define TE_LGR_USER     "TAPI NOSQL"

#include "te_config.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"

#include "tapi_nosql.h"
#include "tapi_nosql_rpc.h"

static const char *const cls_names[] = {
    "ok", "connect", "auth", "permission", "tls", "timeout", "invalid",
    "unsupported", "unavailable", "limit", "protocol", "handle", "internal",
    "rpc",
};

/* See description in tapi_nosql.h */
const char *
tapi_nosql_errcls_name(tapi_nosql_errcls cls)
{
    if ((unsigned)cls < sizeof(cls_names) / sizeof(cls_names[0]))
        return cls_names[cls];
    return "unknown";
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_errno(tapi_nosql_errcls cls)
{
    te_errno e;

    switch (cls)
    {
        case TAPI_NOSQL_OK:                 return 0;
        case TAPI_NOSQL_ERR_CONNECT:        e = TE_ECONNREFUSED; break;
        case TAPI_NOSQL_ERR_AUTH:           e = TE_EACCES; break;
        case TAPI_NOSQL_ERR_PERMISSION:     e = TE_EPERM; break;
        case TAPI_NOSQL_ERR_TLS:            e = TE_EPROTO; break;
        case TAPI_NOSQL_ERR_TIMEOUT:        e = TE_ETIMEDOUT; break;
        case TAPI_NOSQL_ERR_INVALID:        e = TE_EINVAL; break;
        case TAPI_NOSQL_ERR_UNSUPPORTED:    e = TE_EOPNOTSUPP; break;
        case TAPI_NOSQL_ERR_UNAVAILABLE:    e = TE_EAGAIN; break;
        case TAPI_NOSQL_ERR_LIMIT:          e = TE_E2BIG; break;
        case TAPI_NOSQL_ERR_PROTOCOL:       e = TE_EBADMSG; break;
        case TAPI_NOSQL_ERR_HANDLE:         e = TE_EBADF; break;
        case TAPI_NOSQL_ERR_RPC:            e = TE_ECORRUPTED; break;
        default:                            e = TE_EFAULT; break;
    }
    return TE_RC(TE_TAPI, e);
}

/* See description in tapi_nosql.h */
json_t *
tapi_nosql_json(const char *text)
{
    json_error_t jerr;
    json_t      *v = json_loads(text, JSON_DECODE_ANY, &jerr);

    if (v == NULL)
        ERROR("Malformed JSON at %d:%d: %s", jerr.line, jerr.column,
              jerr.text);
    return v;
}

/* See description in tapi_nosql.h */
char *
tapi_nosql_json_text(const json_t *v)
{
    return json_dumps(v, JSON_COMPACT | JSON_SORT_KEYS | JSON_ENCODE_ANY |
                         JSON_REAL_PRECISION(17));
}

/* JSON text for the wire: compact, 17 digits, in insertion order. */
static char *
wire(const json_t *v)
{
    return json_dumps(v, JSON_COMPACT | JSON_ENCODE_ANY |
                         JSON_REAL_PRECISION(17));
}

static void
remember(tapi_nosql_conn *conn, const rpc_nosql_err *err, te_errno rc)
{
    free(conn->last_msg);
    if (TE_RC_GET_ERROR(rc) == TE_ECORRUPTED)
    {
        conn->last_cls = TAPI_NOSQL_ERR_RPC;
        conn->last_msg = strdup("the RPC failed");
    }
    else
    {
        conn->last_cls = (tapi_nosql_errcls)err->cls;
        conn->last_msg = strdup(err->msg != NULL ? err->msg : "");
    }
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_available(rcf_rpc_server *rpcs, unsigned int *engines,
                     json_t **info)
{
    char    *json = NULL;
    te_errno rc = rpc_nosql_available(rpcs, engines, &json);

    if (rc == 0 && info != NULL)
        *info = json != NULL ? tapi_nosql_json(json) : NULL;
    free(json);
    return rc;
}

/* See description in tapi_nosql.h */
bool
tapi_nosql_supports(rcf_rpc_server *rpcs, const char *engine)
{
    json_t *info = NULL;
    json_t *list;
    json_t *e;
    size_t  i;
    bool    found = false;

    if (tapi_nosql_available(rpcs, NULL, &info) != 0)
        return false;
    list = json_object_get(info, "engines");
    json_array_foreach(list, i, e)
    {
        const char *name = json_string_value(json_object_get(e, "name"));

        if (name != NULL && strcmp(name, engine) == 0)
            found = true;
    }
    json_decref(info);
    return found;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_open(rcf_rpc_server *rpcs, const char *engine,
                const json_t *config, tapi_nosql_conn **conn,
                tapi_nosql_errcls *cls)
{
    rpc_nosql_err    err = { 0, 0, NULL };
    tapi_nosql_conn *c;
    char            *cfg = config != NULL ? wire(config) : strdup("{}");
    char            *result = NULL;
    int              handle = 0;
    te_errno         rc;

    *conn = NULL;
    rc = rpc_nosql_open(rpcs, engine, cfg, &handle, &result, &err);
    /* The configuration holds the password: wipe it. */
    memset(cfg, 0, strlen(cfg));
    free(cfg);
    if (rc != 0)
    {
        tapi_nosql_errcls c2 = TE_RC_GET_ERROR(rc) == TE_ECORRUPTED
                               ? TAPI_NOSQL_ERR_RPC
                               : (tapi_nosql_errcls)err.cls;

        if (cls != NULL)
            *cls = c2;
        RING("Opening a %s connection failed: %s: %s", engine,
             tapi_nosql_errcls_name(c2), err.msg != NULL ? err.msg : "");
        rpc_nosql_err_free(&err);
        free(result);
        return c2 == TAPI_NOSQL_ERR_RPC ? rc : tapi_nosql_errno(c2);
    }
    if (cls != NULL)
        *cls = TAPI_NOSQL_OK;
    c = calloc(1, sizeof(*c));
    if (c == NULL)
    {
        rpc_nosql_err_free(&err);
        free(result);
        return TE_RC(TE_TAPI, TE_ENOMEM);
    }
    c->rpcs = rpcs;
    c->handle = handle;
    c->engine = strdup(engine);
    c->info = result != NULL ? tapi_nosql_json(result) : json_object();
    rpc_nosql_err_free(&err);
    free(result);
    *conn = c;
    return 0;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_close(tapi_nosql_conn *conn)
{
    rpc_nosql_err err = { 0, 0, NULL };
    te_errno      rc;

    if (conn == NULL)
        return 0;
    rc = rpc_nosql_close(conn->rpcs, conn->handle, &err);
    rpc_nosql_err_free(&err);
    json_decref(conn->info);
    free(conn->engine);
    free(conn->last_msg);
    free(conn);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_call(tapi_nosql_conn *conn, const char *op, const json_t *args,
                json_t **result)
{
    rpc_nosql_err err = { 0, 0, NULL };
    char         *a = args != NULL ? wire(args) : strdup("{}");
    char         *out = NULL;
    te_errno      rc;

    if (result != NULL)
        *result = NULL;
    rc = rpc_nosql_call(conn->rpcs, conn->handle, op, a, &out, &err);
    free(a);
    if (rc != 0)
    {
        remember(conn, &err, rc);
        rpc_nosql_err_free(&err);
        free(out);
        return conn->last_cls == TAPI_NOSQL_ERR_RPC
               ? rc : tapi_nosql_errno(conn->last_cls);
    }
    conn->last_cls = TAPI_NOSQL_OK;
    free(conn->last_msg);
    conn->last_msg = NULL;
    if (result != NULL)
        *result = out != NULL ? tapi_nosql_json(out) : json_object();
    rpc_nosql_err_free(&err);
    free(out);
    return 0;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_callf(tapi_nosql_conn *conn, const char *op, json_t **result,
                 const char *fmt, ...)
{
    json_error_t jerr;
    json_t      *args;
    va_list      ap;
    te_errno     rc;

    va_start(ap, fmt);
    args = json_vpack_ex(&jerr, 0, fmt, ap);
    va_end(ap);
    if (args == NULL)
    {
        ERROR("%s: cannot pack the arguments: %s", op, jerr.text);
        return TE_RC(TE_TAPI, TE_EINVAL);
    }
    rc = tapi_nosql_call(conn, op, args, result);
    json_decref(args);
    return rc;
}

/* See description in tapi_nosql.h */
tapi_nosql_errcls
tapi_nosql_last_cls(const tapi_nosql_conn *conn)
{
    return conn->last_cls;
}

/* See description in tapi_nosql.h */
const char *
tapi_nosql_last_msg(const tapi_nosql_conn *conn)
{
    return conn->last_msg != NULL ? conn->last_msg : "";
}

/* See description in tapi_nosql.h */
bool
tapi_nosql_has_cap(const tapi_nosql_conn *conn, const char *cap)
{
    json_t *caps = json_object_get(conn->info, "caps");
    json_t *v;
    size_t  i;

    json_array_foreach(caps, i, v)
    {
        if (json_is_string(v) && strcmp(json_string_value(v), cap) == 0)
            return true;
    }
    return false;
}

/* See description in tapi_nosql.h */
const char *
tapi_nosql_server_version(const tapi_nosql_conn *conn)
{
    const char *v = json_string_value(json_object_get(conn->info,
                                                      "server_version"));

    return v != NULL ? v : "unknown";
}

/* See description in tapi_nosql.h */
bool
tapi_nosql_tls(const tapi_nosql_conn *conn)
{
    return json_is_true(json_object_get(conn->info, "tls"));
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_ping(tapi_nosql_conn *conn, long long *rtt_us)
{
    json_t  *r = NULL;
    te_errno rc = tapi_nosql_call(conn, "ping", NULL, &r);

    if (rc == 0 && rtt_us != NULL)
        *rtt_us = json_integer_value(json_object_get(r, "rtt_us"));
    json_decref(r);
    return rc;
}

static char *
str_or_null(json_t *v)
{
    return json_is_string(v) ? strdup(json_string_value(v)) : NULL;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_put(tapi_nosql_conn *conn, const char *coll, const char *key,
               const json_t *doc, long ttl_s, char **version)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "put", &r, "{s:s, s:s, s:O, s:I}", "coll",
                          coll, "key", key, "doc", doc, "ttl_s",
                          (json_int_t)ttl_s);
    if (version != NULL)
        *version = rc == 0 ? str_or_null(json_object_get(r, "version")) : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_put_if_absent(tapi_nosql_conn *conn, const char *coll,
                         const char *key, const json_t *doc, long ttl_s,
                         bool *applied)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "put_if", &r, "{s:s, s:s, s:O, s:I, s:s}",
                          "coll", coll, "key", key, "doc", doc, "ttl_s",
                          (json_int_t)ttl_s, "if", "absent");
    if (rc == 0 && applied != NULL)
        *applied = json_is_true(json_object_get(r, "applied"));
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_put_if_version(tapi_nosql_conn *conn, const char *coll,
                          const char *key, const json_t *doc,
                          const char *version, bool *applied,
                          char **new_version)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "put_if", &r, "{s:s, s:s, s:O, s:s, s:s}",
                          "coll", coll, "key", key, "doc", doc, "if",
                          "version", "version", version != NULL ? version : "");
    if (rc == 0 && applied != NULL)
        *applied = json_is_true(json_object_get(r, "applied"));
    if (new_version != NULL)
        *new_version = rc == 0 ? str_or_null(json_object_get(r, "version"))
                               : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_get(tapi_nosql_conn *conn, const char *coll, const char *key,
               json_t **doc, char **version)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "get", &r, "{s:s, s:s}", "coll", coll, "key",
                          key);
    if (doc != NULL)
    {
        *doc = NULL;
        if (rc == 0 && json_is_true(json_object_get(r, "found")))
            *doc = json_incref(json_object_get(r, "doc"));
    }
    if (version != NULL)
        *version = rc == 0 ? str_or_null(json_object_get(r, "version")) : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_delete(tapi_nosql_conn *conn, const char *coll, const char *key,
                  bool *deleted)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "delete", &r, "{s:s, s:s}", "coll", coll,
                          "key", key);
    if (rc == 0 && deleted != NULL)
        *deleted = json_is_true(json_object_get(r, "deleted"));
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_incr(tapi_nosql_conn *conn, const char *coll, const char *key,
                long long by, long long *value)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "incr", &r, "{s:s, s:s, s:I}", "coll", coll,
                          "key", key, "by", (json_int_t)by);
    if (rc == 0 && value != NULL)
        *value = json_integer_value(json_object_get(r, "value"));
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_scan(tapi_nosql_conn *conn, const char *coll, int limit,
                const char *cursor, json_t **keys, char **next)
{
    json_t  *r = NULL;
    json_t  *args = json_pack("{s:s, s:i}", "coll", coll, "limit", limit);
    te_errno rc;

    if (cursor != NULL)
        json_object_set_new(args, "cursor", json_string(cursor));
    rc = tapi_nosql_call(conn, "scan", args, &r);
    json_decref(args);
    if (keys != NULL)
        *keys = rc == 0 ? json_incref(json_object_get(r, "keys")) : NULL;
    if (next != NULL)
        *next = rc == 0 ? str_or_null(json_object_get(r, "cursor")) : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_scan_all(tapi_nosql_conn *conn, const char *coll, int limit,
                    json_t **keys, unsigned int *pages,
                    unsigned int *largest, unsigned int *repeats)
{
    json_t      *seen = json_object();
    json_t      *all = json_array();
    char        *cursor = NULL;
    unsigned int np = 0;
    unsigned int big = 0;
    unsigned int dup = 0;
    te_errno     rc = 0;

    do {
        json_t *page = NULL;
        char   *next = NULL;
        json_t *k;
        size_t  i;

        rc = tapi_nosql_scan(conn, coll, limit, cursor, &page, &next);
        free(cursor);
        cursor = next;
        if (rc != 0)
            break;
        np++;
        if (json_array_size(page) > big)
            big = (unsigned int)json_array_size(page);
        json_array_foreach(page, i, k)
        {
            const char *s = json_string_value(k);

            if (json_object_get(seen, s) != NULL)
            {
                dup++;
                continue;
            }
            json_object_set_new(seen, s, json_true());
            json_array_append(all, k);
        }
        json_decref(page);
    } while (cursor != NULL && np < 1000000);
    free(cursor);
    json_decref(seen);
    if (rc != 0)
    {
        json_decref(all);
        *keys = NULL;
        return rc;
    }
    *keys = all;
    if (pages != NULL)
        *pages = np;
    if (largest != NULL)
        *largest = big;
    if (repeats != NULL)
        *repeats = dup;
    return 0;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_find(tapi_nosql_conn *conn, const char *coll, const json_t *filter,
                int limit, const char *cursor, json_t **docs, char **next)
{
    json_t  *r = NULL;
    json_t  *args = json_pack("{s:s, s:O, s:i}", "coll", coll, "filter",
                              filter, "limit", limit);
    te_errno rc;

    if (cursor != NULL)
        json_object_set_new(args, "cursor", json_string(cursor));
    rc = tapi_nosql_call(conn, "find", args, &r);
    json_decref(args);
    if (docs != NULL)
        *docs = rc == 0 ? json_incref(json_object_get(r, "docs")) : NULL;
    if (next != NULL)
        *next = rc == 0 ? str_or_null(json_object_get(r, "cursor")) : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_find_all(tapi_nosql_conn *conn, const char *coll,
                    const json_t *filter, int limit, json_t **docs,
                    unsigned int *pages)
{
    json_t      *all = json_array();
    char        *cursor = NULL;
    unsigned int np = 0;
    te_errno     rc = 0;

    do {
        json_t *page = NULL;
        char   *next = NULL;

        rc = tapi_nosql_find(conn, coll, filter, limit, cursor, &page, &next);
        free(cursor);
        cursor = next;
        if (rc != 0)
            break;
        np++;
        json_array_extend(all, page);
        json_decref(page);
    } while (cursor != NULL && np < 1000000);
    free(cursor);
    if (rc != 0)
    {
        json_decref(all);
        *docs = NULL;
        return rc;
    }
    *docs = all;
    if (pages != NULL)
        *pages = np;
    return 0;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_count(tapi_nosql_conn *conn, const char *coll,
                 const json_t *filter, long long *n)
{
    json_t  *r = NULL;
    json_t  *args = json_pack("{s:s}", "coll", coll);
    te_errno rc;

    if (filter != NULL)
        json_object_set(args, "filter", (json_t *)filter);
    rc = tapi_nosql_call(conn, "count", args, &r);
    json_decref(args);
    if (rc == 0 && n != NULL)
        *n = json_integer_value(json_object_get(r, "count"));
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_multi(tapi_nosql_conn *conn, const json_t *ops,
                 const char *if_coll, const char *if_key, bool *applied)
{
    json_t  *r = NULL;
    json_t  *args = json_pack("{s:O}", "ops", ops);
    te_errno rc;

    if (if_coll != NULL)
        json_object_set_new(args, "if_absent",
                            json_pack("{s:s, s:s}", "coll", if_coll, "key",
                                      if_key));
    rc = tapi_nosql_call(conn, "multi", args, &r);
    json_decref(args);
    if (rc == 0 && applied != NULL)
        *applied = json_is_true(json_object_get(r, "applied"));
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_drop(tapi_nosql_conn *conn, const char *coll)
{
    return tapi_nosql_callf(conn, "drop", NULL, "{s:s}", "coll", coll);
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_native(tapi_nosql_conn *conn, const json_t *cmd, json_t **reply)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "native", &r, "{s:O}", "cmd", cmd);
    if (reply != NULL)
        *reply = rc == 0 ? json_incref(json_object_get(r, "reply")) : NULL;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_bulk_generate(tapi_nosql_conn *conn, const char *coll, long count,
                         int size, const char *prefix, long seed,
                         long *written, long long *elapsed_us)
{
    json_t  *r = NULL;
    te_errno rc;

    rc = tapi_nosql_callf(conn, "bulk", &r,
                          "{s:s, s:{s:I, s:i, s:s, s:I}}", "coll", coll,
                          "generate", "count", (json_int_t)count, "size",
                          size, "prefix", prefix, "seed", (json_int_t)seed);
    if (written != NULL)
        *written = r != NULL ? (long)json_integer_value(
                                   json_object_get(r, "written")) : 0;
    if (elapsed_us != NULL)
        *elapsed_us = r != NULL ? json_integer_value(
                                      json_object_get(r, "elapsed_us")) : 0;
    json_decref(r);
    return rc;
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_set_timeout(tapi_nosql_conn *conn, int ms)
{
    return tapi_nosql_callf(conn, "set_timeout", NULL, "{s:i}", "ms", ms);
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_info(tapi_nosql_conn *conn, json_t **info)
{
    return tapi_nosql_call(conn, "info", NULL, info);
}

/* See description in tapi_nosql.h */
te_errno
tapi_nosql_registry(rcf_rpc_server *rpcs, json_t **reg)
{
    rpc_nosql_err err = { 0, 0, NULL };
    char         *out = NULL;
    te_errno      rc = rpc_nosql_call(rpcs, 0, "registry", "{}", &out, &err);

    *reg = rc == 0 && out != NULL ? tapi_nosql_json(out) : NULL;
    rpc_nosql_err_free(&err);
    free(out);
    return rc;
}

/* See description in tapi_nosql.h */
json_t *
tapi_nosql_generated_doc(long i, long seed, int size)
{
    /* The same generator as the agent's (ta_nosql_core.c, gen_doc()). */
    char              *payload = malloc((size_t)size + 1);
    unsigned long long x = (unsigned long long)(i + 1) *
                           6364136223846793005ULL + (unsigned long long)seed;
    char               name[32];
    json_t            *doc;
    int                k;

    if (payload == NULL)
        return NULL;
    for (k = 0; k < size; k++)
    {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        payload[k] = (char)('a' + (int)((x >> 33) % 26));
    }
    payload[size] = '\0';
    snprintf(name, sizeof(name), "item-%ld", i);
    doc = json_pack("{s:I, s:s, s:I, s:s}", "i", (json_int_t)i, "name", name,
                    "group", (json_int_t)(i % 10), "payload", payload);
    free(payload);
    return doc;
}
