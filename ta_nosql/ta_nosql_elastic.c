/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: Elasticsearch over its REST API (libcurl)
 *
 * A collection is an index (named "<index_prefix><coll>"), made on
 * first write. A stored document has three parts:
 *  - "_nosql_doc": the document itself, in an object mapped with
 *    "enabled": false - kept in _source exactly as sent and never parsed
 *    into the mapping, so any JSON object reads back as written (dynamic
 *    mapping alone refuses many: 1e308 as a "float", an array mixing
 *    numbers and objects, a field that is a string in one document and
 *    an object in the next);
 *  - "_nosql_key": the key, a keyword, so pages can be cut by key with
 *    search_after (Elasticsearch does not sort on _id);
 *  - a copy of each top-level field that is a scalar or an array of
 *    scalars, for find and count: strings mapped as keyword (exact
 *    match), numbers as long or double, malformed values ignored rather
 *    than refusing the document.
 * A document's version
 * is "<_seq_no>:<_primary_term>", the pair optimistic concurrency
 * control compares (if_seq_no / if_primary_term); write-if-absent is
 * the _create endpoint.
 *
 * Reads by key are real time. Searches see what the last refresh made
 * visible, so scan, find and count refresh the index first unless the
 * configuration says "refresh_before_query": false. A counter is a
 * scripted update retried on version conflicts (retry_on_conflict), and
 * returns the value its own update made. There are no expiring
 * documents and no atomic multi-document write (_bulk is per document).
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_nosql_int.h"

typedef struct es_conn {
    ta_nosql_http http;
    char         *prefix;
    bool          refresh_before_query;
    json_t       *ready;    /**< Indices known to exist. */
    ta_nosql_conf cfg;
} es_conn;

#define KEY_FIELD   "_nosql_key"
#define DOC_FIELD   "_nosql_doc"

static bool
scalar(json_t *v)
{
    return json_is_string(v) || json_is_integer(v) || json_is_real(v) ||
           json_is_boolean(v);
}

/* The stored form of a document (see the file comment). */
static json_t *
stored(const char *key, json_t *doc)
{
    json_t     *s = json_object();
    const char *k;
    json_t     *v;

    json_object_foreach(doc, k, v)
    {
        if (scalar(v))
        {
            json_object_set(s, k, v);
        }
        else if (json_is_array(v) && json_array_size(v) > 0)
        {
            size_t  i;
            json_t *e;
            bool    same = true;
            bool    str = json_is_string(json_array_get(v, 0));

            json_array_foreach(v, i, e)
            {
                if (!scalar(e) || json_is_string(e) != str)
                    same = false;
            }
            if (same)
                json_object_set(s, k, v);
        }
    }
    json_object_set_new(s, KEY_FIELD, json_string(key));
    json_object_set(s, DOC_FIELD, doc);
    return s;
}

static void
es_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "libcurl %s (Elasticsearch REST)",
             ta_nosql_http_client());
}

/* The JSON error of an answer, as an error. */
static void
es_http_err(long status, json_t *reply, const char *raw, ta_nosql_err *err)
{
    json_t     *e = json_object_get(reply, "error");
    const char *type = ta_nosql_json_str(e, "type", NULL);
    const char *reason = ta_nosql_json_str(e, "reason", NULL);

    if (json_is_string(e))
        reason = json_string_value(e);
    ta_nosql_err_set(err, ta_nosql_http_cls(status), status,
                     "elasticsearch: HTTP %ld%s%s%s%s", status,
                     type != NULL ? ": " : "", type != NULL ? type : "",
                     reason != NULL ? ": " : "",
                     reason != NULL ? reason
                     : (raw != NULL && reply == NULL ? raw : ""));
    if (type != NULL && strcmp(type, "version_conflict_engine_exception") == 0)
        err->cls = TA_NOSQL_ERR_INVALID;
}

static char *
index_of(es_conn *c, const char *coll)
{
    return ta_nosql_strdupf("%s%s", c->prefix, coll);
}

static int
es_req(es_conn *c, const char *method, const char *path, const json_t *body,
       long *status, json_t **reply, ta_nosql_err *err)
{
    return ta_nosql_http_req(&c->http, method, path, body, NULL, status, reply,
                             NULL, err);
}

static int
ensure_index(es_conn *c, const char *coll, ta_nosql_err *err)
{
    char   *idx;
    char   *path;
    json_t *body;
    json_t *reply = NULL;
    long    st;
    int     rc;

    if (json_object_get(c->ready, coll) != NULL)
        return 0;
    idx = index_of(c, coll);
    path = ta_nosql_strdupf("/%s", idx);
    /*
     * Strings longer than ignore_above are kept in _source but not
     * indexed: Lucene refuses a term over 32766 bytes, and a long
     * string would otherwise make the whole document fail.
     */
    body = json_pack("{s:{s:b}, s:{s:[{s:{s:s, s:{s:s, s:i}}},"
                     " {s:{s:s, s:{s:s}}}], s:{s:{s:s}, s:{s:s, s:b}}}}",
                     "settings", "index.mapping.ignore_malformed", 1,
                     "mappings",
                         "dynamic_templates",
                             "strings", "match_mapping_type", "string",
                                 "mapping", "type", "keyword",
                                 "ignore_above", 8191,
                             "reals", "match_mapping_type", "double",
                                 "mapping", "type", "double",
                         "properties",
                             KEY_FIELD, "type", "keyword",
                             DOC_FIELD, "type", "object", "enabled", 0);
    rc = es_req(c, "PUT", path, body, &st, &reply, err);
    json_decref(body);
    free(path);
    free(idx);
    if (rc != 0)
        return -1;
    if (st != 200 && !(st == 400 &&
                       strstr(ta_nosql_json_str(json_object_get(reply, "error"),
                                                "type", ""),
                              "already_exists") != NULL))
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    json_decref(reply);
    json_object_set_new(c->ready, coll, json_true());
    return 0;
}

static int
refresh(es_conn *c, const char *coll, ta_nosql_err *err)
{
    char   *idx = index_of(c, coll);
    char   *path = ta_nosql_strdupf("/%s/_refresh", idx);
    json_t *reply = NULL;
    long    st;
    int     rc;

    rc = es_req(c, "POST", path, NULL, &st, &reply, err);
    free(path);
    free(idx);
    json_decref(reply);
    if (rc != 0)
        return -1;
    /* A missing index has nothing to refresh. */
    return 0;
}

static void es_disconnect(void *dc);

static int
es_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    es_conn *c = calloc(1, sizeof(*c));
    json_t  *reply = NULL;
    long     st;

    if (c == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &c->cfg);
    c->prefix = strdup(ta_nosql_json_str(cfg->raw, "index_prefix", ""));
    c->refresh_before_query = ta_nosql_json_bool(cfg->raw,
                                                 "refresh_before_query", true);
    c->ready = json_object();
    if (ta_nosql_http_init(&c->http, cfg, 9200, err) != 0)
    {
        es_disconnect(c);
        return -1;
    }
    if (es_req(c, "GET", "/", NULL, &st, &reply, err) != 0)
    {
        es_disconnect(c);
        return -1;
    }
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        es_disconnect(c);
        return -1;
    }
    json_decref(reply);
    *dc = c;
    return 0;
}

static void
es_disconnect(void *dc)
{
    es_conn *c = dc;

    if (c == NULL)
        return;
    ta_nosql_http_fini(&c->http);
    json_decref(c->ready);
    free(c->prefix);
    ta_nosql_conf_free(&c->cfg);
    free(c);
}

static int
es_ping(void *dc, ta_nosql_err *err)
{
    json_t *reply = NULL;
    long    st;

    if (es_req(dc, "GET", "/", NULL, &st, &reply, err) != 0)
        return -1;
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    json_decref(reply);
    return 0;
}

static int
es_info(void *dc, json_t *out, ta_nosql_err *err)
{
    json_t *reply = NULL;
    long    st;

    if (es_req(dc, "GET", "/", NULL, &st, &reply, err) != 0)
        return -1;
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    json_object_set_new(out, "server_version",
                        json_string(ta_nosql_json_str(
                            json_object_get(reply, "version"), "number",
                            "unknown")));
    json_object_set_new(out, "cluster",
                        json_string(ta_nosql_json_str(reply, "cluster_name",
                                                      "")));
    json_decref(reply);
    return 0;
}

static bool
es_tls_in_use(void *dc)
{
    return ((es_conn *)dc)->http.tls;
}

static char *
version_of(json_t *reply)
{
    return ta_nosql_strdupf("%lld:%lld",
                            ta_nosql_json_int(reply, "_seq_no", -1),
                            ta_nosql_json_int(reply, "_primary_term", -1));
}

static int
es_write(es_conn *c, const char *coll, const char *key, json_t *doc,
         int mode, const char *if_ver, bool *applied, char **ver,
         ta_nosql_err *err)
{
    char   *idx = index_of(c, coll);
    char   *ek = ta_nosql_http_escape(key);
    char   *path;
    json_t *body = stored(key, doc);
    json_t *reply = NULL;
    long    st;
    int     rc;

    if (ensure_index(c, coll, err) != 0)
    {
        free(idx);
        free(ek);
        json_decref(body);
        return -1;
    }
    if (mode == 1)
    {
        path = ta_nosql_strdupf("/%s/_create/%s", idx, ek);
    }
    else if (mode == 2)
    {
        long long seq = -1;
        long long term = -1;

        if (sscanf(if_ver, "%lld:%lld", &seq, &term) != 2)
        {
            free(idx);
            free(ek);
            json_decref(body);
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "elasticsearch: a version is <seq_no>:"
                             "<primary_term>");
            return -1;
        }
        path = ta_nosql_strdupf("/%s/_doc/%s?if_seq_no=%lld"
                                "&if_primary_term=%lld", idx, ek, seq, term);
    }
    else
    {
        path = ta_nosql_strdupf("/%s/_doc/%s", idx, ek);
    }
    rc = es_req(c, "PUT", path, body, &st, &reply, err);
    free(path);
    free(idx);
    free(ek);
    json_decref(body);
    if (rc != 0)
        return -1;
    if (st == 409 && mode != 0)
    {
        *applied = false;
        if (ver != NULL)
            *ver = NULL;
        json_decref(reply);
        return 0;
    }
    if (st != 200 && st != 201)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    if (applied != NULL)
        *applied = true;
    if (ver != NULL)
        *ver = version_of(reply);
    json_decref(reply);
    return 0;
}

static int
es_put(void *dc, const char *coll, const char *key, json_t *doc, long ttl_s,
       char **ver, ta_nosql_err *err)
{
    (void)ttl_s;
    return es_write(dc, coll, key, doc, 0, NULL, NULL, ver, err);
}

static int
es_put_if(void *dc, const char *coll, const char *key, json_t *doc,
          long ttl_s, const char *if_ver, bool *applied, char **ver,
          ta_nosql_err *err)
{
    (void)ttl_s;
    return es_write(dc, coll, key, doc, if_ver == NULL ? 1 : 2, if_ver,
                    applied, ver, err);
}

/* The caller's document of a stored source: _nosql_doc, or for a
 * counter made by incr, {"n": n}. */
static json_t *
unstore(json_t *source)
{
    json_t *d = json_object_get(source, DOC_FIELD);

    if (json_is_object(d))
        return json_deep_copy(d);
    return json_pack("{s:I}", "n",
                     (json_int_t)ta_nosql_json_int(source, "n", 0));
}

static int
es_get(void *dc, const char *coll, const char *key, json_t **doc,
       char **ver, ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx = index_of(c, coll);
    char    *ek = ta_nosql_http_escape(key);
    char    *path = ta_nosql_strdupf("/%s/_doc/%s", idx, ek);
    json_t  *reply = NULL;
    long     st;
    int      rc;

    free(idx);
    free(ek);
    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    rc = es_req(c, "GET", path, NULL, &st, &reply, err);
    free(path);
    if (rc != 0)
        return -1;
    if (st == 404)
    {
        json_decref(reply);
        return 0;
    }
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    if (json_is_true(json_object_get(reply, "found")))
    {
        *doc = unstore(json_object_get(reply, "_source"));
        if (ver != NULL)
            *ver = version_of(reply);
    }
    json_decref(reply);
    return 0;
}

static int
es_del(void *dc, const char *coll, const char *key, bool *deleted,
       ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx = index_of(c, coll);
    char    *ek = ta_nosql_http_escape(key);
    char    *path = ta_nosql_strdupf("/%s/_doc/%s", idx, ek);
    json_t  *reply = NULL;
    long     st;
    int      rc;

    free(idx);
    free(ek);
    rc = es_req(c, "DELETE", path, NULL, &st, &reply, err);
    free(path);
    if (rc != 0)
        return -1;
    if (st == 404)
    {
        *deleted = false;
        json_decref(reply);
        return 0;
    }
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    *deleted = strcmp(ta_nosql_json_str(reply, "result", ""), "deleted") == 0;
    json_decref(reply);
    return 0;
}

static int
es_incr(void *dc, const char *coll, const char *key, long long by,
        long long *value, ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx;
    char    *ek;
    char    *path;
    json_t  *body;
    json_t  *reply = NULL;
    long     st;
    int      rc;

    if (by == 0)
    {
        /* The counter is the top-level "n" of the source. */
        char   *i0 = index_of(c, coll);
        char   *k0 = ta_nosql_http_escape(key);
        char   *p0 = ta_nosql_strdupf("/%s/_doc/%s?_source=n", i0, k0);

        free(i0);
        free(k0);
        rc = es_req(c, "GET", p0, NULL, &st, &reply, err);
        free(p0);
        if (rc != 0)
            return -1;
        if (st != 200 && st != 404)
        {
            es_http_err(st, reply, NULL, err);
            json_decref(reply);
            return -1;
        }
        *value = ta_nosql_json_int(json_object_get(reply, "_source"), "n", 0);
        json_decref(reply);
        return 0;
    }
    if (ensure_index(c, coll, err) != 0)
        return -1;
    idx = index_of(c, coll);
    ek = ta_nosql_http_escape(key);
    path = ta_nosql_strdupf("/%s/_update/%s?retry_on_conflict=1000"
                            "&_source=n", idx, ek);
    free(idx);
    free(ek);
    body = json_pack("{s:{s:s, s:s, s:{s:I}}, s:{s:I, s:s}}",
                     "script",
                         "lang", "painless",
                         "source", "if (ctx._source.n == null) "
                                   "{ ctx._source.n = params.by } "
                                   "else { ctx._source.n += params.by }",
                         "params", "by", (json_int_t)by,
                     "upsert", "n", (json_int_t)by, KEY_FIELD, key);
    rc = es_req(c, "POST", path, body, &st, &reply, err);
    json_decref(body);
    free(path);
    if (rc != 0)
        return -1;
    if (st != 200 && st != 201)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    *value = ta_nosql_json_int(json_object_get(json_object_get(reply, "get"),
                                               "_source"), "n", 0);
    json_decref(reply);
    return 0;
}

static json_t *
query_of(json_t *filter)
{
    json_t     *must = json_array();
    const char *k;
    json_t     *v;

    if (filter != NULL)
    {
        json_object_foreach(filter, k, v)
        {
            /* Strings are keywords here: term is an exact comparison. */
            json_array_append_new(must, json_pack("{s:{s:O}}", "term", k, v));
        }
    }
    return json_pack("{s:{s:o}}", "bool", "filter", must);
}

static int
es_page(es_conn *c, const char *coll, json_t *filter, int limit,
        const char *cursor, json_t *out, bool docs, char **next,
        ta_nosql_err *err)
{
    char   *idx = index_of(c, coll);
    char   *path = ta_nosql_strdupf("/%s/_search", idx);
    json_t *body;
    json_t *reply = NULL;
    json_t *hits;
    json_t *hit;
    size_t  i;
    long    st;
    int     rc;

    free(idx);
    *next = NULL;
    if (c->refresh_before_query && refresh(c, coll, err) != 0)
    {
        free(path);
        return -1;
    }
    body = json_pack("{s:i, s:o, s:[{s:s}], s:b}", "size", limit,
                     "query", query_of(filter), "sort", KEY_FIELD, "asc",
                     "_source", docs);
    if (cursor != NULL)
        json_object_set_new(body, "search_after", json_pack("[s]", cursor));
    rc = es_req(c, "POST", path, body, &st, &reply, err);
    json_decref(body);
    free(path);
    if (rc != 0)
        return -1;
    if (st == 404)
    {
        json_decref(reply);
        return 0;
    }
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    hits = json_object_get(json_object_get(reply, "hits"), "hits");
    json_array_foreach(hits, i, hit)
    {
        const char *id = ta_nosql_json_str(hit, "_id", "");

        if (docs)
            json_array_append_new(out, json_pack("{s:s, s:o}", "key", id,
                "doc", unstore(json_object_get(hit, "_source"))));
        else
            json_array_append_new(out, json_string(id));
    }
    if ((int)json_array_size(hits) == limit && limit > 0)
    {
        json_t *last = json_array_get(json_object_get(
            json_array_get(hits, json_array_size(hits) - 1), "sort"), 0);

        if (json_is_string(last))
            *next = strdup(json_string_value(last));
    }
    json_decref(reply);
    return 0;
}

static int
es_scan(void *dc, const char *coll, int limit, const char *cursor,
        json_t *keys, char **next, ta_nosql_err *err)
{
    return es_page(dc, coll, NULL, limit, cursor, keys, false, next, err);
}

static int
es_find(void *dc, const char *coll, json_t *filter, int limit,
        const char *cursor, json_t *docs, char **next, ta_nosql_err *err)
{
    return es_page(dc, coll, filter, limit, cursor, docs, true, next, err);
}

static int
es_count(void *dc, const char *coll, json_t *filter, long long *n,
         ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx = index_of(c, coll);
    char    *path = ta_nosql_strdupf("/%s/_count", idx);
    json_t  *body = json_pack("{s:o}", "query", query_of(filter));
    json_t  *reply = NULL;
    long     st;
    int      rc;

    free(idx);
    if (c->refresh_before_query && refresh(c, coll, err) != 0)
    {
        free(path);
        json_decref(body);
        return -1;
    }
    rc = es_req(c, "POST", path, body, &st, &reply, err);
    json_decref(body);
    free(path);
    if (rc != 0)
        return -1;
    if (st == 404)
    {
        *n = 0;
        json_decref(reply);
        return 0;
    }
    if (st != 200)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    *n = ta_nosql_json_int(reply, "count", 0);
    json_decref(reply);
    return 0;
}

static int
es_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx = index_of(c, coll);
    char    *path = ta_nosql_strdupf("/%s", idx);
    json_t  *reply = NULL;
    long     st;
    int      rc;

    free(idx);
    rc = es_req(c, "DELETE", path, NULL, &st, &reply, err);
    free(path);
    json_object_del(c->ready, coll);
    if (rc != 0)
        return -1;
    if (st != 200 && st != 404)
    {
        es_http_err(st, reply, NULL, err);
        json_decref(reply);
        return -1;
    }
    json_decref(reply);
    return 0;
}

static int
es_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    es_conn    *c = dc;
    const char *method = ta_nosql_json_str(cmd, "method", "GET");
    const char *path = ta_nosql_json_str(cmd, "path", NULL);
    json_t     *body = json_object_get(cmd, "body");
    json_t     *r = NULL;
    char       *raw = NULL;
    long        st;

    if (path == NULL || path[0] != '/')
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "elasticsearch: 'cmd' needs 'path' (starting with "
                         "'/') and takes 'method' and 'body'");
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
es_bulk(void *dc, const char *coll, json_t *items, ta_nosql_err *err)
{
    es_conn *c = dc;
    char    *idx;
    char    *path;
    size_t   cap = 65536;
    size_t   len = 0;
    char    *nd = malloc(cap);
    size_t   i;
    json_t  *it;
    json_t  *reply = NULL;
    long     st;
    int      rc;

    if (nd == NULL || ensure_index(c, coll, err) != 0)
    {
        free(nd);
        if (nd == NULL)
            ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM,
                             "out of memory");
        return -1;
    }
    idx = index_of(c, coll);
    json_array_foreach(items, i, it)
    {
        const char *key = json_string_value(json_object_get(it, "key"));
        json_t     *action = json_pack("{s:{s:s, s:s}}", "index", "_index",
                                       idx, "_id", key);
        json_t     *doc = stored(key, json_object_get(it, "doc"));
        char       *a;
        char       *d;
        size_t      need;

        a = ta_nosql_json_dump(action);
        d = ta_nosql_json_dump(doc);
        need = strlen(a) + strlen(d) + 3;
        while (len + need + 1 > cap)
        {
            char *p = realloc(nd, cap * 2);

            if (p == NULL)
                break;
            nd = p;
            cap *= 2;
        }
        if (len + need + 1 <= cap)
            len += (size_t)sprintf(nd + len, "%s\n%s\n", a, d);
        free(a);
        free(d);
        json_decref(action);
        json_decref(doc);
    }
    nd[len] = '\0';
    free(idx);
    path = strdup("/_bulk");
    rc = ta_nosql_http_req_text(&c->http, "POST", path, nd,
                                "application/x-ndjson", &st, &reply, NULL,
                                err);
    free(path);
    free(nd);
    if (rc != 0)
        return -1;
    if (st != 200 || json_is_true(json_object_get(reply, "errors")))
    {
        if (st == 200)
        {
            json_t *its = json_object_get(reply, "items");
            json_t *first = NULL;

            json_array_foreach(its, i, it)
            {
                json_t *e = json_object_get(json_object_get(it, "index"),
                                            "error");

                if (e != NULL)
                {
                    first = e;
                    break;
                }
            }
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "elasticsearch: bulk: %s: %s",
                             ta_nosql_json_str(first, "type", "?"),
                             ta_nosql_json_str(first, "reason", "?"));
        }
        else
        {
            es_http_err(st, reply, NULL, err);
        }
        json_decref(reply);
        return -1;
    }
    json_decref(reply);
    return refresh(c, coll, err);
}

static int
es_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    (void)err;
    ((es_conn *)dc)->http.timeout_ms = ms;
    return 0;
}

static int
es_audit(const ta_nosql_conf *cfg, void *dc, json_t *args, json_t *findings,
         json_t *checked, ta_nosql_err *err)
{
    es_conn     *c = dc;
    json_t      *reply = NULL;
    json_t      *roles;
    size_t       i;
    json_t      *role;
    long         st;
    char         subject[300];
    ta_nosql_err cerr;

    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    json_array_append_new(checked, json_string("elasticsearch.superuser"));
    if (c == NULL || cfg->user == NULL)
        return 0;
    memset(&cerr, 0, sizeof(cerr));
    if (es_req(c, "GET", "/_security/_authenticate", NULL, &st, &reply,
               &cerr) != 0 || st != 200)
    {
        json_decref(reply);
        return 0;
    }
    roles = json_object_get(reply, "roles");
    json_array_foreach(roles, i, role)
    {
        if (json_is_string(role) &&
            strcmp(json_string_value(role), "superuser") == 0)
            ta_nosql_finding(findings, "elasticsearch.superuser",
                             TA_NOSQL_SEV_MEDIUM, subject,
                             "the account '%s' holds the superuser role",
                             cfg->user);
    }
    json_decref(reply);
    return 0;
}

static const char *const es_default_creds[] = {
    "elastic:changeme", "elastic:elastic", "elastic:password", NULL
};

const ta_nosql_driver ta_nosql_elastic_driver = {
    .engine = TA_NOSQL_ENGINE_ELASTIC,
    .name = "elasticsearch",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_FIND | TA_NOSQL_CAP_COUNT |
            TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT | TA_NOSQL_CAP_INCR |
            TA_NOSQL_CAP_INCR_EXACT | TA_NOSQL_CAP_DURABLE |
            TA_NOSQL_CAP_TLS | TA_NOSQL_CAP_AUTH,
    .default_port = 9200,
    .default_creds = es_default_creds,
    .client_version = es_client_version,
    .connect = es_connect,
    .disconnect = es_disconnect,
    .ping = es_ping,
    .info = es_info,
    .tls_in_use = es_tls_in_use,
    .put = es_put,
    .put_if = es_put_if,
    .get = es_get,
    .del = es_del,
    .incr = es_incr,
    .scan = es_scan,
    .find = es_find,
    .count = es_count,
    .drop = es_drop,
    .native = es_native,
    .bulk = es_bulk,
    .set_timeout = es_set_timeout,
    .audit = es_audit,
};
