/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: etcd over its v3 JSON gateway (libcurl)
 *
 * A document is its JSON under the key "/<coll>/<key>" (keys and values
 * travel base64-encoded, as the gateway wants them). Its version is the
 * key's mod_revision; every conditional write is a transaction (txn)
 * comparing it, or comparing create_revision with 0 for write-if-absent.
 * An expiring key is attached to a lease granted for that many seconds.
 * A counter is a compare-and-set loop on the same transaction, so it is
 * exact. Several keys at once is one transaction.
 *
 * With credentials the driver authenticates (auth/authenticate) and
 * sends the token with every request, and authenticates again when the
 * server says the token has expired.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_nosql_int.h"

typedef struct etcd_conn {
    ta_nosql_http http;
    ta_nosql_conf cfg;
} etcd_conn;

static void
etcd_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "libcurl %s (etcd v3 JSON API)", ta_nosql_http_client());
}

static ta_nosql_errcls
grpc_cls(long code, const char *msg)
{
    switch (code)
    {
        case 16:
            return TA_NOSQL_ERR_AUTH;
        case 7:
            return TA_NOSQL_ERR_PERMISSION;
        case 14:
            return TA_NOSQL_ERR_UNAVAILABLE;
        case 4:
            return TA_NOSQL_ERR_TIMEOUT;
        case 8:
            return TA_NOSQL_ERR_LIMIT;
        default:
            break;
    }
    if (msg != NULL && (strstr(msg, "authentication failed") != NULL ||
                        strstr(msg, "user name is empty") != NULL ||
                        strstr(msg, "invalid auth token") != NULL ||
                        strstr(msg, "user name not found") != NULL))
        return TA_NOSQL_ERR_AUTH;
    if (msg != NULL && strstr(msg, "permission denied") != NULL)
        return TA_NOSQL_ERR_PERMISSION;
    if (msg != NULL && strstr(msg, "too large") != NULL)
        return TA_NOSQL_ERR_LIMIT;
    return TA_NOSQL_ERR_INVALID;
}

static int authenticate(etcd_conn *c, ta_nosql_err *err);

/*
 * POST a request to the gateway. A gRPC error in the answer is an error
 * of this call. An expired token is renewed once.
 */
static int
call(etcd_conn *c, const char *path, json_t *body, json_t **reply,
     ta_nosql_err *err)
{
    int attempt;

    for (attempt = 0; attempt < 2; attempt++)
    {
        long        st;
        const char *msg;

        *reply = NULL;
        if (ta_nosql_http_req(&c->http, "POST", path, body, NULL, &st, reply,
                              NULL, err) != 0)
            return -1;
        if (st == 200)
            return 0;
        msg = ta_nosql_json_str(*reply, "error",
                                ta_nosql_json_str(*reply, "message", NULL));
        if (attempt == 0 && c->cfg.user != NULL && msg != NULL &&
            strstr(msg, "invalid auth token") != NULL &&
            authenticate(c, err) == 0)
        {
            json_decref(*reply);
            continue;
        }
        ta_nosql_err_set(err, grpc_cls(ta_nosql_json_int(*reply, "code", -1),
                                       msg),
                         st, "etcd: HTTP %ld: %s", st,
                         msg != NULL ? msg : "error");
        if (st == 401)
            err->cls = TA_NOSQL_ERR_AUTH;
        json_decref(*reply);
        *reply = NULL;
        return -1;
    }
    return -1;
}

static int
authenticate(etcd_conn *c, ta_nosql_err *err)
{
    json_t     *body = json_pack("{s:s, s:s}", "name", c->cfg.user,
                                 "password", c->cfg.password != NULL
                                             ? c->cfg.password : "");
    json_t     *reply = NULL;
    const char *token;
    long        st;
    int         rc;

    free(c->http.bearer);
    c->http.bearer = NULL;
    rc = ta_nosql_http_req(&c->http, "POST", "/v3/auth/authenticate", body,
                           NULL, &st, &reply, NULL, err);
    json_decref(body);
    if (rc != 0)
        return -1;
    token = ta_nosql_json_str(reply, "token", NULL);
    if (st != 200 || token == NULL)
    {
        const char *msg = ta_nosql_json_str(reply, "error",
                                            ta_nosql_json_str(reply, "message",
                                                              "refused"));

        ta_nosql_err_set(err, TA_NOSQL_ERR_AUTH, st, "etcd: %s", msg);
        if (strstr(msg, "authentication is not enabled") != NULL)
        {
            /* Credentials a server without auth does not want: fine. */
            json_decref(reply);
            memset(err, 0, sizeof(*err));
            return 0;
        }
        json_decref(reply);
        return -1;
    }
    c->http.bearer = strdup(token);
    json_decref(reply);
    return 0;
}

static char *
ekey(const char *coll, const char *key)
{
    char *k = key != NULL ? ta_nosql_strdupf("/%s/%s", coll, key)
                          : ta_nosql_strdupf("/%s/", coll);
    char *b = ta_nosql_b64_encode(k, strlen(k));

    free(k);
    return b;
}

/* The end of the range of keys starting with "/<coll>/": "/<coll>0". */
static char *
erange_end(const char *coll)
{
    char *k = ta_nosql_strdupf("/%s0", coll);
    char *b = ta_nosql_b64_encode(k, strlen(k));

    free(k);
    return b;
}

static void etcd_disconnect(void *dc);

static int
etcd_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    etcd_conn *c = calloc(1, sizeof(*c));
    json_t    *reply = NULL;
    json_t    *empty = json_object();

    if (c == NULL)
    {
        json_decref(empty);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &c->cfg);
    if (ta_nosql_http_init(&c->http, cfg, 2379, err) != 0 ||
        (cfg->user != NULL && authenticate(c, err) != 0) ||
        call(c, "/v3/maintenance/status", empty, &reply, err) != 0)
    {
        json_decref(empty);
        etcd_disconnect(c);
        return -1;
    }
    json_decref(empty);
    json_decref(reply);
    *dc = c;
    return 0;
}

static void
etcd_disconnect(void *dc)
{
    etcd_conn *c = dc;

    if (c == NULL)
        return;
    ta_nosql_http_fini(&c->http);
    ta_nosql_conf_free(&c->cfg);
    free(c);
}

static int
etcd_ping(void *dc, ta_nosql_err *err)
{
    json_t *body = json_object();
    json_t *reply = NULL;
    int     rc = call(dc, "/v3/maintenance/status", body, &reply, err);

    json_decref(body);
    json_decref(reply);
    return rc;
}

static int
etcd_info(void *dc, json_t *out, ta_nosql_err *err)
{
    json_t *body = json_object();
    json_t *reply = NULL;

    if (call(dc, "/v3/maintenance/status", body, &reply, err) != 0)
    {
        json_decref(body);
        return -1;
    }
    json_decref(body);
    json_object_set_new(out, "server_version",
                        json_string(ta_nosql_json_str(reply, "version",
                                                      "unknown")));
    if (json_object_get(reply, "dbSize") != NULL)
        json_object_set(out, "db_size", json_object_get(reply, "dbSize"));
    json_decref(reply);
    return 0;
}

static bool
etcd_tls_in_use(void *dc)
{
    return ((etcd_conn *)dc)->http.tls;
}

/* A string-or-number int64 of the gateway (int64 travel as strings). */
static long long
i64(json_t *obj, const char *name)
{
    json_t *v = json_object_get(obj, name);

    if (json_is_string(v))
        return strtoll(json_string_value(v), NULL, 10);
    if (json_is_integer(v))
        return json_integer_value(v);
    return 0;
}

static int
grant(etcd_conn *c, long ttl_s, char **lease, ta_nosql_err *err)
{
    json_t *body = json_pack("{s:I}", "TTL", (json_int_t)ttl_s);
    json_t *reply = NULL;
    int     rc = call(c, "/v3/lease/grant", body, &reply, err);

    json_decref(body);
    if (rc != 0)
        return -1;
    *lease = ta_nosql_strdupf("%lld", i64(reply, "ID"));
    json_decref(reply);
    return 0;
}

static json_t *
put_req(const char *k64, json_t *doc, const char *lease)
{
    char   *json = ta_nosql_json_dump(doc);
    char   *v64 = ta_nosql_b64_encode(json, strlen(json));
    json_t *r = json_pack("{s:s, s:s}", "key", k64, "value", v64);

    if (lease != NULL)
        json_object_set_new(r, "lease", json_string(lease));
    free(json);
    free(v64);
    return r;
}

static int
etcd_put(void *dc, const char *coll, const char *key, json_t *doc,
         long ttl_s, char **ver, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *lease = NULL;
    char      *k64 = ekey(coll, key);
    json_t    *body;
    json_t    *reply = NULL;
    int        rc;

    if (ttl_s > 0 && grant(c, ttl_s, &lease, err) != 0)
    {
        free(k64);
        return -1;
    }
    body = put_req(k64, doc, lease);
    rc = call(c, "/v3/kv/put", body, &reply, err);
    json_decref(body);
    free(k64);
    free(lease);
    if (rc != 0)
        return -1;
    if (ver != NULL)
        *ver = ta_nosql_strdupf("%lld",
                                i64(json_object_get(reply, "header"),
                                    "revision"));
    json_decref(reply);
    return 0;
}

/*
 * One transaction: if the key's @p target ("CREATE" / "MOD") equals
 * @p rev, run @p success. @p applied: did it.
 */
static int
txn(etcd_conn *c, const char *k64, const char *target, const char *field,
    const char *rev, json_t *success, bool *applied, long long *revision,
    ta_nosql_err *err)
{
    json_t *body = json_pack("{s:[{s:s, s:s, s:s, s:s}], s:O}",
                             "compare", "key", k64, "target", target,
                             "result", "EQUAL", field, rev,
                             "success", success);
    json_t *reply = NULL;
    int     rc = call(c, "/v3/kv/txn", body, &reply, err);

    json_decref(body);
    if (rc != 0)
        return -1;
    /* The gateway leaves false booleans out. */
    *applied = json_is_true(json_object_get(reply, "succeeded"));
    if (revision != NULL)
        *revision = i64(json_object_get(reply, "header"), "revision");
    json_decref(reply);
    return 0;
}

static int
etcd_put_if(void *dc, const char *coll, const char *key, json_t *doc,
            long ttl_s, const char *if_ver, bool *applied, char **ver,
            ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *lease = NULL;
    char      *k64 = ekey(coll, key);
    json_t    *success;
    long long  rev = 0;
    int        rc;

    if (ttl_s > 0 && grant(c, ttl_s, &lease, err) != 0)
    {
        free(k64);
        return -1;
    }
    success = json_pack("[{s:o}]", "request_put", put_req(k64, doc, lease));
    rc = if_ver == NULL
         ? txn(c, k64, "CREATE", "create_revision", "0", success, applied,
               &rev, err)
         : txn(c, k64, "MOD", "mod_revision", if_ver, success, applied, &rev,
               err);
    json_decref(success);
    free(k64);
    free(lease);
    if (rc != 0)
        return -1;
    *ver = *applied ? ta_nosql_strdupf("%lld", rev) : NULL;
    return 0;
}

static int
range1(etcd_conn *c, const char *k64, json_t **kv, ta_nosql_err *err,
       json_t **reply)
{
    json_t *body = json_pack("{s:s}", "key", k64);
    int     rc = call(c, "/v3/kv/range", body, reply, err);

    json_decref(body);
    if (rc != 0)
        return -1;
    *kv = json_array_get(json_object_get(*reply, "kvs"), 0);
    return 0;
}

static int
etcd_get(void *dc, const char *coll, const char *key, json_t **doc,
         char **ver, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *k64 = ekey(coll, key);
    json_t    *reply = NULL;
    json_t    *kv = NULL;
    int        rc;

    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    rc = range1(c, k64, &kv, err, &reply);
    free(k64);
    if (rc != 0)
        return -1;
    if (kv != NULL)
    {
        size_t len = 0;
        char  *val = ta_nosql_b64_decode(ta_nosql_json_str(kv, "value", ""),
                                         &len);

        if (val != NULL)
        {
            *doc = ta_nosql_json_load(val, len, err);
            if (*doc == NULL)
            {
                *doc = json_stringn(val, len);
                memset(err, 0, sizeof(*err));
            }
            free(val);
        }
        if (ver != NULL)
            *ver = ta_nosql_strdupf("%lld", i64(kv, "mod_revision"));
    }
    json_decref(reply);
    return 0;
}

static int
etcd_del(void *dc, const char *coll, const char *key, bool *deleted,
         ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *k64 = ekey(coll, key);
    json_t    *body = json_pack("{s:s}", "key", k64);
    json_t    *reply = NULL;
    int        rc = call(c, "/v3/kv/deleterange", body, &reply, err);

    json_decref(body);
    free(k64);
    if (rc != 0)
        return -1;
    *deleted = i64(reply, "deleted") > 0;
    json_decref(reply);
    return 0;
}

static int
etcd_incr(void *dc, const char *coll, const char *key, long long by,
          long long *value, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *k64 = ekey(coll, key);
    int        attempt;

    for (attempt = 0; attempt < 100000; attempt++)
    {
        json_t   *reply = NULL;
        json_t   *kv = NULL;
        json_t   *doc;
        json_t   *success;
        long long cur = 0;
        char      rev[32];
        bool      applied = false;
        int       rc;

        if (range1(c, k64, &kv, err, &reply) != 0)
        {
            free(k64);
            return -1;
        }
        if (kv != NULL)
        {
            size_t len = 0;
            char  *val = ta_nosql_b64_decode(ta_nosql_json_str(kv, "value",
                                                               ""), &len);
            json_t *d = val != NULL ? ta_nosql_json_load(val, len, err) : NULL;

            cur = ta_nosql_json_int(d, "n", 0);
            json_decref(d);
            free(val);
            snprintf(rev, sizeof(rev), "%lld", i64(kv, "mod_revision"));
        }
        json_decref(reply);
        if (by == 0)
        {
            *value = cur;
            free(k64);
            return 0;
        }
        doc = json_pack("{s:I}", "n", (json_int_t)(cur + by));
        success = json_pack("[{s:o}]", "request_put", put_req(k64, doc, NULL));
        json_decref(doc);
        rc = kv == NULL
             ? txn(c, k64, "CREATE", "create_revision", "0", success,
                   &applied, NULL, err)
             : txn(c, k64, "MOD", "mod_revision", rev, success, &applied,
                   NULL, err);
        json_decref(success);
        if (rc != 0)
        {
            free(k64);
            return -1;
        }
        if (applied)
        {
            *value = cur + by;
            free(k64);
            return 0;
        }
    }
    free(k64);
    ta_nosql_err_set(err, TA_NOSQL_ERR_UNAVAILABLE, 0,
                     "etcd: the counter kept changing under 100000 attempts");
    return -1;
}

static int
etcd_scan(void *dc, const char *coll, int limit, const char *cursor,
          json_t *keys, char **next, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *start;
    char      *end = erange_end(coll);
    json_t    *body;
    json_t    *reply = NULL;
    json_t    *kvs;
    json_t    *kv;
    size_t     plen = strlen(coll) + 2;
    size_t     i;
    int        rc;

    *next = NULL;
    if (cursor != NULL)
    {
        /* Strictly after the cursor: the cursor with a NUL appended. */
        char  *k = ta_nosql_strdupf("/%s/%s", coll, cursor);
        size_t n = strlen(k);
        char  *kk = malloc(n + 1);

        memcpy(kk, k, n);
        kk[n] = '\0';
        start = ta_nosql_b64_encode(kk, n + 1);
        free(k);
        free(kk);
    }
    else
    {
        start = ekey(coll, NULL);
    }
    body = json_pack("{s:s, s:s, s:i, s:b, s:s, s:s}", "key", start,
                     "range_end", end, "limit", limit, "keys_only", 1,
                     "sort_order", "ASCEND", "sort_target", "KEY");
    rc = call(c, "/v3/kv/range", body, &reply, err);
    json_decref(body);
    free(start);
    free(end);
    if (rc != 0)
        return -1;
    kvs = json_object_get(reply, "kvs");
    json_array_foreach(kvs, i, kv)
    {
        size_t len = 0;
        char  *k = ta_nosql_b64_decode(ta_nosql_json_str(kv, "key", ""), &len);

        if (k != NULL && len > plen)
            json_array_append_new(keys, json_stringn(k + plen, len - plen));
        free(k);
    }
    if (json_is_true(json_object_get(reply, "more")) &&
        json_array_size(keys) > 0)
        *next = strdup(json_string_value(json_array_get(keys,
                                         json_array_size(keys) - 1)));
    json_decref(reply);
    return 0;
}

static int
etcd_count(void *dc, const char *coll, json_t *filter, long long *n,
           ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *start = ekey(coll, NULL);
    char      *end = erange_end(coll);
    json_t    *body = json_pack("{s:s, s:s, s:b}", "key", start, "range_end",
                                end, "count_only", 1);
    json_t    *reply = NULL;
    int        rc;

    (void)filter;
    rc = call(c, "/v3/kv/range", body, &reply, err);
    json_decref(body);
    free(start);
    free(end);
    if (rc != 0)
        return -1;
    *n = i64(reply, "count");
    json_decref(reply);
    return 0;
}

static int
etcd_multi(void *dc, json_t *ops, const char *if_coll, const char *if_key,
           bool *applied, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    json_t    *success = json_array();
    json_t    *body;
    json_t    *reply = NULL;
    size_t     i;
    json_t    *op;
    int        rc;

    json_array_foreach(ops, i, op)
    {
        char *k64 = ekey(ta_nosql_json_str(op, "coll", ""),
                         ta_nosql_json_str(op, "key", ""));

        if (strcmp(ta_nosql_json_str(op, "op", ""), "put") == 0)
            json_array_append_new(success, json_pack("{s:o}", "request_put",
                put_req(k64, json_object_get(op, "doc"), NULL)));
        else
            json_array_append_new(success,
                json_pack("{s:{s:s}}", "request_delete_range", "key", k64));
        free(k64);
    }
    if (if_coll != NULL)
    {
        char *k64 = ekey(if_coll, if_key);

        rc = txn(c, k64, "CREATE", "create_revision", "0", success, applied,
                 NULL, err);
        free(k64);
        json_decref(success);
        return rc;
    }
    body = json_pack("{s:o}", "success", success);
    rc = call(c, "/v3/kv/txn", body, &reply, err);
    json_decref(body);
    if (rc != 0)
        return -1;
    /* No compare: the success branch always runs. */
    *applied = true;
    json_decref(reply);
    return 0;
}

static int
etcd_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    etcd_conn *c = dc;
    char      *start = ekey(coll, NULL);
    char      *end = erange_end(coll);
    json_t    *body = json_pack("{s:s, s:s}", "key", start, "range_end", end);
    json_t    *reply = NULL;
    int        rc = call(c, "/v3/kv/deleterange", body, &reply, err);

    json_decref(body);
    json_decref(reply);
    free(start);
    free(end);
    return rc;
}

static int
etcd_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    etcd_conn  *c = dc;
    const char *method = ta_nosql_json_str(cmd, "method", "POST");
    const char *path = ta_nosql_json_str(cmd, "path", NULL);
    json_t     *body = json_object_get(cmd, "body");
    json_t     *r = NULL;
    char       *raw = NULL;
    long        st;

    if (path == NULL || path[0] != '/')
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "etcd: 'cmd' needs 'path' (starting with '/') and "
                         "takes 'method' and 'body'");
        return -1;
    }
    if (ta_nosql_http_req(&c->http, method, path, body, NULL, &st, &r, &raw,
                          err) != 0)
        return -1;
    *reply = json_pack("{s:I, s:o}", "status", (json_int_t)st, "body",
                       r != NULL ? r : json_string(raw != NULL ? raw : ""));
    free(raw);
    return 0;
}

static int
etcd_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    (void)err;
    ((etcd_conn *)dc)->http.timeout_ms = ms;
    return 0;
}

static int
etcd_audit(const ta_nosql_conf *cfg, void *dc, json_t *args,
           json_t *findings, json_t *checked, ta_nosql_err *err)
{
    static const char *const open_paths[][2] = {
        { "/version",  "the server version" },
        { "/metrics",  "the Prometheus metrics (cluster internals)" },
    };
    ta_nosql_conf anon;
    ta_nosql_http h;
    ta_nosql_err  cerr;
    char          subject[300];
    size_t        i;

    (void)dc;
    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    ta_nosql_conf_copy(cfg, &anon);
    free(anon.user);
    free(anon.password);
    anon.user = NULL;
    anon.password = NULL;
    memset(&cerr, 0, sizeof(cerr));
    if (ta_nosql_http_init(&h, &anon, 2379, &cerr) != 0)
    {
        ta_nosql_conf_free(&anon);
        return 0;
    }
    for (i = 0; i < sizeof(open_paths) / sizeof(open_paths[0]); i++)
    {
        char check[64];
        long st = 0;

        snprintf(check, sizeof(check), "etcd.open%s", open_paths[i][0]);
        json_array_append_new(checked, json_string(check));
        if (ta_nosql_http_req(&h, "GET", open_paths[i][0], NULL, NULL, &st,
                              NULL, NULL, &cerr) == 0 && st == 200)
        {
            ta_nosql_finding(findings, check, TA_NOSQL_SEV_LOW, subject,
                             "GET %s with no credentials returns %s",
                             open_paths[i][0], open_paths[i][1]);
        }
    }
    ta_nosql_http_fini(&h);
    ta_nosql_conf_free(&anon);
    return 0;
}

static const char *const etcd_default_creds[] = {
    "root:root", "root:password", "root:etcd", NULL
};

const ta_nosql_driver ta_nosql_etcd_driver = {
    .engine = TA_NOSQL_ENGINE_ETCD,
    .name = "etcd",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_COUNT | TA_NOSQL_CAP_TTL |
            TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT | TA_NOSQL_CAP_INCR |
            TA_NOSQL_CAP_INCR_EXACT | TA_NOSQL_CAP_MULTI |
            TA_NOSQL_CAP_MULTI_IF | TA_NOSQL_CAP_DURABLE |
            TA_NOSQL_CAP_TLS | TA_NOSQL_CAP_AUTH,
    .default_port = 2379,
    .default_creds = etcd_default_creds,
    .client_version = etcd_client_version,
    .connect = etcd_connect,
    .disconnect = etcd_disconnect,
    .ping = etcd_ping,
    .info = etcd_info,
    .tls_in_use = etcd_tls_in_use,
    .put = etcd_put,
    .put_if = etcd_put_if,
    .get = etcd_get,
    .del = etcd_del,
    .incr = etcd_incr,
    .scan = etcd_scan,
    .count = etcd_count,
    .multi = etcd_multi,
    .drop = etcd_drop,
    .native = etcd_native,
    .set_timeout = etcd_set_timeout,
    .audit = etcd_audit,
};
