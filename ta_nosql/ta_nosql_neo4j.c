/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: Neo4j over its HTTP Cypher API (libcurl)
 *
 * A collection is a node label; a document is one node with the
 * properties _key (unique: a constraint per label, made on first use),
 * _v (version), _json (the document exactly as written) and, so a query
 * can compare them, a copy of each top-level field Neo4j can hold as a
 * property (a scalar or an array of scalars). The document comes back
 * from _json, so it is the same whatever Neo4j could index. Every
 * statement takes its values as parameters, never in the Cypher text.
 *
 * A counter takes the node's write lock before it reads (SET n._lock),
 * which makes "SET n.n = n.n + 1" exact under concurrency; the plain
 * read-then-write is not. A conditional multi-write is an explicit
 * transaction: the condition is read in it and the transaction is
 * rolled back when it fails.
 *
 * Neo4j has no expiring nodes.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ta_nosql_int.h"

typedef struct neo_conn {
    ta_nosql_http http;
    char         *db;
    json_t       *ready;    /**< Labels with their key constraint. */
    ta_nosql_conf cfg;
} neo_conn;

static void
neo_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "libcurl %s (Neo4j HTTP API)", ta_nosql_http_client());
}

static ta_nosql_errcls
code_cls(const char *code)
{
    if (strstr(code, "Security.Unauthorized") != NULL ||
        strstr(code, "Security.AuthenticationRateLimit") != NULL ||
        strstr(code, "Security.CredentialsExpired") != NULL)
        return TA_NOSQL_ERR_AUTH;
    if (strstr(code, "Security.Forbidden") != NULL ||
        strstr(code, "Security.AuthorizationExpired") != NULL)
        return TA_NOSQL_ERR_PERMISSION;
    if (strstr(code, "TransientError") != NULL ||
        strstr(code, "DatabaseUnavailable") != NULL)
        return TA_NOSQL_ERR_UNAVAILABLE;
    if (strstr(code, "TransactionTimedOut") != NULL)
        return TA_NOSQL_ERR_TIMEOUT;
    if (strstr(code, "ConstraintValidationFailed") != NULL)
        return TA_NOSQL_ERR_INVALID;
    if (strstr(code, "ClientError") != NULL)
        return TA_NOSQL_ERR_INVALID;
    return TA_NOSQL_ERR_PROTOCOL;
}

static bool
is_constraint_violation(const ta_nosql_err *err)
{
    return strstr(err->msg, "ConstraintValidationFailed") != NULL;
}

/*
 * POST statements to @p path (a transaction endpoint). @p results gets
 * the "results" array (borrowed from @p reply).
 */
static int
post(neo_conn *c, const char *path, json_t *statements, json_t **reply,
     ta_nosql_err *err)
{
    json_t *body = json_pack("{s:O}", "statements", statements);
    json_t *errors;
    long    st;
    int     rc;

    *reply = NULL;
    rc = ta_nosql_http_req(&c->http, "POST", path, body, NULL, &st, reply,
                           NULL, err);
    json_decref(body);
    if (rc != 0)
        return -1;
    if (st == 401)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_AUTH, st,
                         "neo4j: HTTP 401: %s",
                         ta_nosql_json_str(json_array_get(
                             json_object_get(*reply, "errors"), 0),
                             "message", "unauthorized"));
        json_decref(*reply);
        *reply = NULL;
        return -1;
    }
    errors = json_object_get(*reply, "errors");
    if (json_array_size(errors) > 0)
    {
        json_t     *e = json_array_get(errors, 0);
        const char *code = ta_nosql_json_str(e, "code", "");

        ta_nosql_err_set(err, code_cls(code), st, "neo4j: %s: %s", code,
                         ta_nosql_json_str(e, "message", ""));
        json_decref(*reply);
        *reply = NULL;
        return -1;
    }
    if (st != 200 && st != 201)
    {
        ta_nosql_err_set(err, ta_nosql_http_cls(st), st, "neo4j: HTTP %ld",
                         st);
        json_decref(*reply);
        *reply = NULL;
        return -1;
    }
    return 0;
}

/* How often a transaction that hit a transient error is run again. */
#define NEO_RETRIES 10

/*
 * One statement in an auto-commit transaction: its "data" rows. A
 * transient error (a deadlock between concurrent transactions, a
 * leader switch) is retried with a growing pause, as the official
 * drivers do for a transaction they manage: the statement had no effect.
 */
static int
cypher(neo_conn *c, const char *stmt, json_t *params, json_t **reply,
       json_t **rows, ta_nosql_err *err)
{
    char   *path = ta_nosql_strdupf("/db/%s/tx/commit", c->db);
    json_t *sts = json_pack("[{s:s, s:o}]", "statement", stmt,
                            "parameters", params != NULL ? json_incref(params)
                                                         : json_object());
    int     rc;
    int     attempt;

    for (attempt = 0; ; attempt++)
    {
        rc = post(c, path, sts, reply, err);
        if (rc == 0 || attempt >= NEO_RETRIES ||
            strstr(err->msg, "TransientError") == NULL)
            break;
        usleep((useconds_t)(5000 * (attempt + 1)));
    }
    json_decref(sts);
    free(path);
    if (rc == 0 && rows != NULL)
        *rows = json_object_get(json_array_get(json_object_get(*reply,
                                                               "results"), 0),
                                "data");
    return rc;
}

static int
ensure_label(neo_conn *c, const char *coll, ta_nosql_err *err)
{
    char   *stmt;
    json_t *reply = NULL;
    int     rc;

    if (json_object_get(c->ready, coll) != NULL)
        return 0;
    stmt = ta_nosql_strdupf("CREATE CONSTRAINT `tsf_nosql_%s_key` IF NOT "
                            "EXISTS FOR (n:`%s`) REQUIRE n._key IS UNIQUE",
                            coll, coll);
    rc = cypher(c, stmt, NULL, &reply, NULL, err);
    free(stmt);
    json_decref(reply);
    if (rc != 0)
        return -1;
    json_object_set_new(c->ready, coll, json_true());
    return 0;
}

static void neo_disconnect(void *dc);

static int
neo_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    neo_conn *c = calloc(1, sizeof(*c));
    json_t   *reply = NULL;

    if (c == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &c->cfg);
    c->db = strdup(cfg->database != NULL ? cfg->database : "neo4j");
    c->ready = json_object();
    if (ta_nosql_http_init(&c->http, cfg, 7474, err) != 0 ||
        cypher(c, "RETURN 1", NULL, &reply, NULL, err) != 0)
    {
        neo_disconnect(c);
        return -1;
    }
    json_decref(reply);
    *dc = c;
    return 0;
}

static void
neo_disconnect(void *dc)
{
    neo_conn *c = dc;

    if (c == NULL)
        return;
    ta_nosql_http_fini(&c->http);
    json_decref(c->ready);
    free(c->db);
    ta_nosql_conf_free(&c->cfg);
    free(c);
}

static int
neo_ping(void *dc, ta_nosql_err *err)
{
    json_t *reply = NULL;
    int     rc = cypher(dc, "RETURN 1", NULL, &reply, NULL, err);

    json_decref(reply);
    return rc;
}

static int
neo_info(void *dc, json_t *out, ta_nosql_err *err)
{
    neo_conn *c = dc;
    json_t   *reply = NULL;
    json_t   *rows = NULL;
    json_t   *row;

    if (cypher(c, "CALL dbms.components() YIELD versions, edition "
                  "RETURN versions[0], edition", NULL, &reply, &rows,
               err) != 0)
        return -1;
    row = json_object_get(json_array_get(rows, 0), "row");
    json_object_set_new(out, "server_version",
                        json_string(json_is_string(json_array_get(row, 0))
                                    ? json_string_value(json_array_get(row, 0))
                                    : "unknown"));
    if (json_is_string(json_array_get(row, 1)))
        json_object_set_new(out, "edition", json_copy(json_array_get(row, 1)));
    json_object_set_new(out, "database", json_string(c->db));
    json_decref(reply);
    return 0;
}

static bool
neo_tls_in_use(void *dc)
{
    return ((neo_conn *)dc)->http.tls;
}

static bool
scalar(json_t *v)
{
    return json_is_string(v) || json_is_integer(v) || json_is_real(v) ||
           json_is_boolean(v);
}

/* The node's properties for a document. */
static json_t *
props_of(const char *key, const char *ver, json_t *doc)
{
    json_t     *p = json_object();
    char       *json = ta_nosql_json_dump(doc);
    const char *k;
    json_t     *v;

    json_object_foreach(doc, k, v)
    {
        if (scalar(v))
        {
            json_object_set(p, k, v);
        }
        else if (json_is_array(v) && json_array_size(v) > 0)
        {
            size_t  i;
            json_t *e;
            bool    same = true;
            int     t = json_typeof(json_array_get(v, 0));

            json_array_foreach(v, i, e)
            {
                int et = json_typeof(e);

                if (!scalar(e) ||
                    !(et == t || (json_is_boolean(e) &&
                                  (t == JSON_TRUE || t == JSON_FALSE))))
                    same = false;
            }
            if (same)
                json_object_set(p, k, v);
        }
    }
    json_object_set_new(p, "_key", json_string(key));
    json_object_set_new(p, "_v", json_string(ver));
    json_object_set_new(p, "_json", json_string(json));
    free(json);
    return p;
}

static int
neo_write(neo_conn *c, const char *coll, const char *key, json_t *doc,
          int mode, const char *if_ver, bool *applied, char **ver,
          ta_nosql_err *err)
{
    char   *nv = ta_nosql_new_version();
    json_t *params;
    json_t *reply = NULL;
    json_t *rows = NULL;
    char   *stmt;
    int     rc;

    if (ensure_label(c, coll, err) != 0)
    {
        free(nv);
        return -1;
    }
    params = json_pack("{s:s, s:o}", "k", key, "p", props_of(key, nv, doc));
    if (mode == 0)
        stmt = ta_nosql_strdupf("MERGE (n:`%s` {_key: $k}) SET n = $p "
                                "RETURN 1", coll);
    else if (mode == 1)
        stmt = ta_nosql_strdupf("CREATE (n:`%s`) SET n = $p RETURN 1", coll);
    else
    {
        json_object_set_new(params, "v", json_string(if_ver));
        stmt = ta_nosql_strdupf("MATCH (n:`%s` {_key: $k}) WHERE n._v = $v "
                                "SET n = $p RETURN 1", coll);
    }
    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    if (rc != 0 && mode == 1 && is_constraint_violation(err))
    {
        memset(err, 0, sizeof(*err));
        *applied = false;
        free(nv);
        if (ver != NULL)
            *ver = NULL;
        return 0;
    }
    if (rc != 0)
    {
        free(nv);
        return -1;
    }
    if (applied != NULL)
        *applied = json_array_size(rows) > 0;
    json_decref(reply);
    if (ver != NULL && (applied == NULL || *applied))
        *ver = nv;
    else
        free(nv);
    return 0;
}

static int
neo_put(void *dc, const char *coll, const char *key, json_t *doc, long ttl_s,
        char **ver, ta_nosql_err *err)
{
    (void)ttl_s;
    return neo_write(dc, coll, key, doc, 0, NULL, NULL, ver, err);
}

static int
neo_put_if(void *dc, const char *coll, const char *key, json_t *doc,
           long ttl_s, const char *if_ver, bool *applied, char **ver,
           ta_nosql_err *err)
{
    (void)ttl_s;
    return neo_write(dc, coll, key, doc, if_ver == NULL ? 1 : 2, if_ver,
                     applied, ver, err);
}

/* A node's document: _json, or the counter for a node made by incr. */
static json_t *
doc_of(json_t *json_text, json_t *n)
{
    if (json_is_string(json_text))
    {
        ta_nosql_err e;

        return ta_nosql_json_load(json_string_value(json_text),
                                  json_string_length(json_text), &e);
    }
    return json_pack("{s:O}", "n", json_is_integer(n) ? n : json_integer(0));
}

static int
neo_get(void *dc, const char *coll, const char *key, json_t **doc,
        char **ver, ta_nosql_err *err)
{
    neo_conn *c = dc;
    char     *stmt = ta_nosql_strdupf("MATCH (n:`%s` {_key: $k}) "
                                      "RETURN n._json, n._v, n.n", coll);
    json_t   *params = json_pack("{s:s}", "k", key);
    json_t   *reply = NULL;
    json_t   *rows = NULL;
    json_t   *row;
    int       rc;

    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    if (rc != 0)
        return -1;
    row = json_object_get(json_array_get(rows, 0), "row");
    if (row != NULL)
    {
        *doc = doc_of(json_array_get(row, 0), json_array_get(row, 2));
        if (ver != NULL && json_is_string(json_array_get(row, 1)))
            *ver = strdup(json_string_value(json_array_get(row, 1)));
    }
    json_decref(reply);
    return 0;
}

static int
neo_del(void *dc, const char *coll, const char *key, bool *deleted,
        ta_nosql_err *err)
{
    neo_conn *c = dc;
    char     *stmt = ta_nosql_strdupf("MATCH (n:`%s` {_key: $k}) WITH n "
                                      "DETACH DELETE n RETURN count(*)", coll);
    json_t   *params = json_pack("{s:s}", "k", key);
    json_t   *reply = NULL;
    json_t   *rows = NULL;
    int       rc;

    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    if (rc != 0)
        return -1;
    *deleted = json_integer_value(json_array_get(json_object_get(
                   json_array_get(rows, 0), "row"), 0)) > 0;
    json_decref(reply);
    return 0;
}

static int
neo_incr(void *dc, const char *coll, const char *key, long long by,
         long long *value, ta_nosql_err *err)
{
    neo_conn *c = dc;
    char     *nv = ta_nosql_new_version();
    char     *stmt;
    json_t   *params;
    json_t   *reply = NULL;
    json_t   *rows = NULL;
    int       rc;

    if (ensure_label(c, coll, err) != 0)
    {
        free(nv);
        return -1;
    }
    /*
     * SET n._lock first: the transaction then holds the node's write
     * lock, and the read of n.n below cannot interleave with another
     * transaction's increment.
     */
    stmt = ta_nosql_strdupf("MERGE (n:`%s` {_key: $k}) "
                            "SET n._lock = true "
                            "WITH n "
                            "SET n.n = coalesce(n.n, 0) + $by, n._v = $v "
                            "REMOVE n._lock, n._json "
                            "RETURN n.n", coll);
    params = json_pack("{s:s, s:I, s:s}", "k", key, "by", (json_int_t)by,
                       "v", nv);
    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    free(nv);
    if (rc != 0)
        return -1;
    *value = json_integer_value(json_array_get(json_object_get(
                 json_array_get(rows, 0), "row"), 0));
    json_decref(reply);
    return 0;
}

/* WHERE clauses of a filter: n[$f0] = $v0 AND ... */
static char *
where_of(json_t *filter, json_t *params, const char *cursor)
{
    size_t      cap = 256;
    char       *w = calloc(1, cap);
    size_t      len = 0;
    const char *k;
    json_t     *v;
    int         i = 0;

    if (w == NULL)
        return NULL;
    len += (size_t)snprintf(w, cap, "WHERE ($cur IS NULL OR n._key > $cur)");
    json_object_set_new(params, "cur", cursor != NULL ? json_string(cursor)
                                                      : json_null());
    if (filter != NULL)
    {
        json_object_foreach(filter, k, v)
        {
            char fname[16];
            char vname[16];
            char part[64];
            size_t plen;

            snprintf(fname, sizeof(fname), "f%d", i);
            snprintf(vname, sizeof(vname), "v%d", i);
            plen = (size_t)snprintf(part, sizeof(part),
                                    " AND n[$%s] = $%s", fname, vname);
            if (len + plen + 1 > cap)
            {
                char *p;

                cap = (len + plen + 1) * 2;
                p = realloc(w, cap);
                if (p == NULL)
                {
                    free(w);
                    return NULL;
                }
                w = p;
            }
            memcpy(w + len, part, plen + 1);
            len += plen;
            json_object_set_new(params, fname, json_string(k));
            json_object_set(params, vname, v);
            i++;
        }
    }
    return w;
}

static int
neo_page(neo_conn *c, const char *coll, json_t *filter, int limit,
         const char *cursor, json_t *out, bool docs, char **next,
         ta_nosql_err *err)
{
    json_t *params = json_object();
    char   *where = where_of(filter, params, cursor);
    char   *stmt;
    json_t *reply = NULL;
    json_t *rows = NULL;
    json_t *r;
    size_t  i;
    int     rc;

    *next = NULL;
    stmt = ta_nosql_strdupf("MATCH (n:`%s`) %s RETURN n._key, n._json, n.n "
                            "ORDER BY n._key LIMIT $lim", coll, where);
    free(where);
    json_object_set_new(params, "lim", json_integer(limit));
    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    if (rc != 0)
        return -1;
    json_array_foreach(rows, i, r)
    {
        json_t *row = json_object_get(r, "row");
        json_t *key = json_array_get(row, 0);

        if (!json_is_string(key))
            continue;
        if (docs)
            json_array_append_new(out, json_pack("{s:O, s:o}", "key", key,
                "doc", doc_of(json_array_get(row, 1),
                              json_array_get(row, 2))));
        else
            json_array_append(out, key);
    }
    if ((int)json_array_size(rows) == limit)
    {
        json_t *last = json_array_get(json_object_get(
            json_array_get(rows, json_array_size(rows) - 1), "row"), 0);

        if (json_is_string(last))
            *next = strdup(json_string_value(last));
    }
    json_decref(reply);
    return 0;
}

static int
neo_scan(void *dc, const char *coll, int limit, const char *cursor,
         json_t *keys, char **next, ta_nosql_err *err)
{
    return neo_page(dc, coll, NULL, limit, cursor, keys, false, next, err);
}

static int
neo_find(void *dc, const char *coll, json_t *filter, int limit,
         const char *cursor, json_t *docs, char **next, ta_nosql_err *err)
{
    return neo_page(dc, coll, filter, limit, cursor, docs, true, next, err);
}

static int
neo_count(void *dc, const char *coll, json_t *filter, long long *n,
          ta_nosql_err *err)
{
    neo_conn *c = dc;
    json_t   *params = json_object();
    char     *where = where_of(filter, params, NULL);
    char     *stmt = ta_nosql_strdupf("MATCH (n:`%s`) %s RETURN count(n)",
                                      coll, where);
    json_t   *reply = NULL;
    json_t   *rows = NULL;
    int       rc;

    free(where);
    rc = cypher(c, stmt, params, &reply, &rows, err);
    free(stmt);
    json_decref(params);
    if (rc != 0)
        return -1;
    *n = json_integer_value(json_array_get(json_object_get(
             json_array_get(rows, 0), "row"), 0));
    json_decref(reply);
    return 0;
}

/* The statements of a multi-write. */
static json_t *
multi_statements(json_t *ops)
{
    json_t *sts = json_array();
    size_t  i;
    json_t *op;

    json_array_foreach(ops, i, op)
    {
        const char *coll = ta_nosql_json_str(op, "coll", "");
        const char *key = ta_nosql_json_str(op, "key", "");
        char       *stmt;

        if (strcmp(ta_nosql_json_str(op, "op", ""), "put") == 0)
        {
            char *nv = ta_nosql_new_version();

            stmt = ta_nosql_strdupf("MERGE (n:`%s` {_key: $k}) SET n = $p",
                                    coll);
            json_array_append_new(sts, json_pack("{s:s, s:{s:s, s:o}}",
                "statement", stmt, "parameters", "k", key,
                "p", props_of(key, nv, json_object_get(op, "doc"))));
            free(nv);
        }
        else
        {
            stmt = ta_nosql_strdupf("MATCH (n:`%s` {_key: $k}) DETACH DELETE n",
                                    coll);
            json_array_append_new(sts, json_pack("{s:s, s:{s:s}}",
                "statement", stmt, "parameters", "k", key));
        }
        free(stmt);
    }
    return sts;
}

static int
neo_multi(void *dc, json_t *ops, const char *if_coll, const char *if_key,
          bool *applied, ta_nosql_err *err)
{
    neo_conn   *c = dc;
    json_t     *sts;
    json_t     *reply = NULL;
    char       *path;
    size_t      i;
    json_t     *op;
    int         rc;

    *applied = false;
    json_array_foreach(ops, i, op)
    {
        if (strcmp(ta_nosql_json_str(op, "op", ""), "put") == 0 &&
            ensure_label(c, ta_nosql_json_str(op, "coll", ""), err) != 0)
            return -1;
    }
    sts = multi_statements(ops);
    if (if_coll == NULL)
    {
        path = ta_nosql_strdupf("/db/%s/tx/commit", c->db);
        rc = post(c, path, sts, &reply, err);
        free(path);
        json_decref(sts);
        json_decref(reply);
        if (rc == 0)
            *applied = true;
        return rc;
    }
    else
    {
        /* Open a transaction, read the condition in it, then commit or
         * roll back. */
        char       *stmt = ta_nosql_strdupf("MATCH (n:`%s` {_key: $k}) "
                                            "RETURN count(n)", if_coll);
        json_t     *check = json_pack("[{s:s, s:{s:s}}]", "statement", stmt,
                                      "parameters", "k", if_key);
        const char *commit;
        char       *tx = NULL;
        long long   present;

        free(stmt);
        path = ta_nosql_strdupf("/db/%s/tx", c->db);
        rc = post(c, path, check, &reply, err);
        free(path);
        json_decref(check);
        if (rc != 0)
        {
            json_decref(sts);
            return -1;
        }
        commit = ta_nosql_json_str(reply, "commit", NULL);
        if (commit == NULL || strstr(commit, "/db/") == NULL)
        {
            json_decref(reply);
            json_decref(sts);
            ta_nosql_err_set(err, TA_NOSQL_ERR_PROTOCOL, 0,
                             "neo4j: no transaction in the answer");
            return -1;
        }
        tx = strdup(strstr(commit, "/db/"));
        present = json_integer_value(json_array_get(json_object_get(
            json_array_get(json_object_get(json_array_get(
                json_object_get(reply, "results"), 0), "data"), 0),
            "row"), 0));
        json_decref(reply);
        reply = NULL;
        if (present > 0)
        {
            long  st;
            char *txp = strndup(tx, strlen(tx) - strlen("/commit"));

            ta_nosql_http_req(&c->http, "DELETE", txp, NULL, NULL, &st, NULL,
                              NULL, err);
            free(txp);
            free(tx);
            json_decref(sts);
            memset(err, 0, sizeof(*err));
            return 0;
        }
        rc = post(c, tx, sts, &reply, err);
        free(tx);
        json_decref(sts);
        json_decref(reply);
        if (rc == 0)
            *applied = true;
        return rc;
    }
}

static int
neo_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    neo_conn *c = dc;
    char     *stmt = ta_nosql_strdupf("MATCH (n:`%s`) DETACH DELETE n", coll);
    json_t   *reply = NULL;
    int       rc;

    rc = cypher(c, stmt, NULL, &reply, NULL, err);
    free(stmt);
    json_decref(reply);
    return rc;
}

static int
neo_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    neo_conn   *c = dc;
    const char *q = ta_nosql_json_str(cmd, "cypher", NULL);
    json_t     *params = json_object_get(cmd, "params");
    json_t     *r = NULL;
    json_t     *rows = NULL;
    json_t     *res;
    json_t     *out_rows;
    json_t     *row;
    size_t      i;

    if (q == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "neo4j: 'cmd' needs 'cypher' and takes 'params'");
        return -1;
    }
    if (cypher(c, q, json_is_object(params) ? params : NULL, &r, &rows,
               err) != 0)
        return -1;
    res = json_array_get(json_object_get(r, "results"), 0);
    out_rows = json_array();
    json_array_foreach(rows, i, row)
        json_array_append(out_rows, json_object_get(row, "row"));
    *reply = json_pack("{s:O, s:o}", "columns",
                       json_object_get(res, "columns") != NULL
                       ? json_object_get(res, "columns") : json_array(),
                       "rows", out_rows);
    json_decref(r);
    return 0;
}

static int
neo_bulk(void *dc, const char *coll, json_t *items, ta_nosql_err *err)
{
    neo_conn *c = dc;
    json_t   *list = json_array();
    json_t   *params;
    json_t   *reply = NULL;
    char     *stmt;
    size_t    i;
    json_t   *it;
    int       rc;

    if (ensure_label(c, coll, err) != 0)
    {
        json_decref(list);
        return -1;
    }
    json_array_foreach(items, i, it)
    {
        const char *key = json_string_value(json_object_get(it, "key"));
        char       *nv = ta_nosql_new_version();

        json_array_append_new(list, json_pack("{s:s, s:o}", "k", key, "p",
            props_of(key, nv, json_object_get(it, "doc"))));
        free(nv);
    }
    stmt = ta_nosql_strdupf("UNWIND $items AS it MERGE (n:`%s` {_key: it.k}) "
                            "SET n = it.p", coll);
    params = json_pack("{s:o}", "items", list);
    rc = cypher(c, stmt, params, &reply, NULL, err);
    free(stmt);
    json_decref(params);
    json_decref(reply);
    return rc;
}

static int
neo_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    (void)err;
    ((neo_conn *)dc)->http.timeout_ms = ms;
    return 0;
}

static int
neo_audit(const ta_nosql_conf *cfg, void *dc, json_t *args, json_t *findings,
          json_t *checked, ta_nosql_err *err)
{
    ta_nosql_conf anon;
    ta_nosql_http h;
    ta_nosql_err  cerr;
    json_t       *reply = NULL;
    long          st = 0;
    char          subject[300];

    (void)dc;
    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    json_array_append_new(checked, json_string("neo4j.version-disclosed"));
    /* The discovery document, asked for with no credentials. */
    ta_nosql_conf_copy(cfg, &anon);
    free(anon.user);
    free(anon.password);
    anon.user = NULL;
    anon.password = NULL;
    memset(&cerr, 0, sizeof(cerr));
    if (ta_nosql_http_init(&h, &anon, 7474, &cerr) == 0)
    {
        if (ta_nosql_http_req(&h, "GET", "/", NULL, NULL, &st, &reply, NULL,
                              &cerr) == 0 && st == 200 &&
            ta_nosql_json_str(reply, "neo4j_version", NULL) != NULL)
        {
            ta_nosql_finding(findings, "neo4j.version-disclosed",
                             TA_NOSQL_SEV_LOW, subject,
                             "an unauthenticated GET / reports the server "
                             "version (%s) and edition",
                             ta_nosql_json_str(reply, "neo4j_version", ""));
        }
        json_decref(reply);
        ta_nosql_http_fini(&h);
    }
    ta_nosql_conf_free(&anon);
    return 0;
}

static const char *const neo_default_creds[] = {
    "neo4j:neo4j", "neo4j:password", "neo4j:neo4jneo4j", "neo4j:test", NULL
};

const ta_nosql_driver ta_nosql_neo4j_driver = {
    .engine = TA_NOSQL_ENGINE_NEO4J,
    .name = "neo4j",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_FIND | TA_NOSQL_CAP_COUNT |
            TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT | TA_NOSQL_CAP_INCR |
            TA_NOSQL_CAP_INCR_EXACT | TA_NOSQL_CAP_MULTI |
            TA_NOSQL_CAP_MULTI_IF | TA_NOSQL_CAP_DURABLE |
            TA_NOSQL_CAP_TLS | TA_NOSQL_CAP_AUTH,
    .default_port = 7474,
    .default_creds = neo_default_creds,
    .client_version = neo_client_version,
    .connect = neo_connect,
    .disconnect = neo_disconnect,
    .ping = neo_ping,
    .info = neo_info,
    .tls_in_use = neo_tls_in_use,
    .put = neo_put,
    .put_if = neo_put_if,
    .get = neo_get,
    .del = neo_del,
    .incr = neo_incr,
    .scan = neo_scan,
    .find = neo_find,
    .count = neo_count,
    .multi = neo_multi,
    .drop = neo_drop,
    .native = neo_native,
    .bulk = neo_bulk,
    .set_timeout = neo_set_timeout,
    .audit = neo_audit,
};
