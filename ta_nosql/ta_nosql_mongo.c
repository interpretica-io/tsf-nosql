/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: MongoDB over libmongoc
 *
 * A collection is a MongoDB collection of the configured database
 * ("tsf" by default); a document is stored with _id = the key, _v = its
 * version and, when it expires, _exp = the expiry time under a TTL
 * index. Those three fields are stripped on the way out. JSON and BSON
 * are converted here, field by field, not through extended JSON, so a
 * document reads back with the same types it was written with (an
 * integer stays an integer, 1.0 stays a double) and a field that looks
 * like extended JSON ({"$numberLong": ..}) stays an object.
 *
 * Expiry is the server's: MongoDB removes expired documents in a
 * background pass (every 60 s by default, ttlMonitorSleepSecs), and a
 * document is readable until that pass runs.
 *
 * Several keys at once ("multi") is a multi-document transaction, which
 * MongoDB allows on a replica set (one member is enough) and refuses on
 * a standalone server: there the operation fails as unsupported.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mongoc/mongoc.h>

#include "ta_nosql_int.h"

typedef struct mongo_conn {
    mongoc_client_t *client;
    mongoc_uri_t    *uri;
    ta_nosql_conf    cfg;
    char            *db;
    json_t          *ttl_ready;     /**< Collections with a TTL index. */
} mongo_conn;

static pthread_once_t mongo_once = PTHREAD_ONCE_INIT;

static void
mongo_init(void)
{
    mongoc_init();
    /* A driver never logs: errors come back as errors. */
    mongoc_log_set_handler(NULL, NULL);
}

static void
mongo_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "libmongoc %s", mongoc_get_version());
}

/* ---- JSON <-> BSON --------------------------------------------------- */

static bool json_to_bson(json_t *obj, bson_t *b);

static bool
append_value(bson_t *b, const char *k, json_t *v)
{
    switch (json_typeof(v))
    {
        case JSON_OBJECT:
        {
            bson_t child;
            bool   ok;

            if (!bson_append_document_begin(b, k, -1, &child))
                return false;
            ok = json_to_bson(v, &child);
            return bson_append_document_end(b, &child) && ok;
        }
        case JSON_ARRAY:
        {
            bson_t child;
            size_t i;
            char   idx[24];

            if (!bson_append_array_begin(b, k, -1, &child))
                return false;
            for (i = 0; i < json_array_size(v); i++)
            {
                snprintf(idx, sizeof(idx), "%zu", i);
                if (!append_value(&child, idx, json_array_get(v, i)))
                    return false;
            }
            return bson_append_array_end(b, &child);
        }
        case JSON_STRING:
            return bson_append_utf8(b, k, -1, json_string_value(v),
                                    (int)json_string_length(v));
        case JSON_INTEGER:
        {
            json_int_t i = json_integer_value(v);

            if (i >= INT32_MIN && i <= INT32_MAX)
                return bson_append_int32(b, k, -1, (int32_t)i);
            return bson_append_int64(b, k, -1, (int64_t)i);
        }
        case JSON_REAL:
            return bson_append_double(b, k, -1, json_real_value(v));
        case JSON_TRUE:
            return bson_append_bool(b, k, -1, true);
        case JSON_FALSE:
            return bson_append_bool(b, k, -1, false);
        case JSON_NULL:
            return bson_append_null(b, k, -1);
    }
    return false;
}

static bool
json_to_bson(json_t *obj, bson_t *b)
{
    const char *k;
    json_t     *v;

    json_object_foreach(obj, k, v)
    {
        if (!append_value(b, k, v))
            return false;
    }
    return true;
}

static json_t *iter_doc(bson_iter_t *it, bool array);

static json_t *
iter_value(bson_iter_t *it)
{
    switch (bson_iter_type(it))
    {
        case BSON_TYPE_UTF8:
        {
            uint32_t    len;
            const char *s = bson_iter_utf8(it, &len);

            return json_stringn(s, len);
        }
        case BSON_TYPE_INT32:
            return json_integer(bson_iter_int32(it));
        case BSON_TYPE_INT64:
            return json_integer(bson_iter_int64(it));
        case BSON_TYPE_DOUBLE:
            return json_real(bson_iter_double(it));
        case BSON_TYPE_BOOL:
            return json_boolean(bson_iter_bool(it));
        case BSON_TYPE_NULL:
        case BSON_TYPE_UNDEFINED:
            return json_null();
        case BSON_TYPE_DOCUMENT:
        case BSON_TYPE_ARRAY:
        {
            bson_iter_t child;

            if (!bson_iter_recurse(it, &child))
                return json_null();
            return iter_doc(&child, bson_iter_type(it) == BSON_TYPE_ARRAY);
        }
        case BSON_TYPE_OID:
        {
            char oid[25];

            bson_oid_to_string(bson_iter_oid(it), oid);
            return json_pack("{s:s}", "$oid", oid);
        }
        case BSON_TYPE_DATE_TIME:
            return json_pack("{s:I}", "$date",
                             (json_int_t)bson_iter_date_time(it));
        case BSON_TYPE_TIMESTAMP:
        {
            uint32_t t;
            uint32_t i;

            bson_iter_timestamp(it, &t, &i);
            return json_pack("{s:{s:I, s:I}}", "$timestamp", "t",
                             (json_int_t)t, "i", (json_int_t)i);
        }
        case BSON_TYPE_DECIMAL128:
        {
            bson_decimal128_t d;
            char              s[BSON_DECIMAL128_STRING];

            bson_iter_decimal128(it, &d);
            bson_decimal128_to_string(&d, s);
            return json_pack("{s:s}", "$numberDecimal", s);
        }
        case BSON_TYPE_BINARY:
        {
            bson_subtype_t sub;
            uint32_t       len;
            const uint8_t *data;
            char          *b64;
            json_t        *o;

            bson_iter_binary(it, &sub, &len, &data);
            b64 = ta_nosql_b64_encode(data, len);
            o = json_pack("{s:s}", "$binary", b64 != NULL ? b64 : "");
            free(b64);
            return o;
        }
        default:
            return json_pack("{s:i}", "$type", (int)bson_iter_type(it));
    }
}

static json_t *
iter_doc(bson_iter_t *it, bool array)
{
    json_t *out = array ? json_array() : json_object();

    while (bson_iter_next(it))
    {
        json_t *v = iter_value(it);

        if (array)
            json_array_append_new(out, v);
        else
            json_object_set_new(out, bson_iter_key(it), v);
    }
    return out;
}

static json_t *
bson_to_json(const bson_t *b)
{
    bson_iter_t it;

    if (!bson_iter_init(&it, b))
        return json_object();
    return iter_doc(&it, false);
}

/* ---- errors ---------------------------------------------------------- */

static void
mongo_err(const bson_error_t *e, ta_nosql_err *err)
{
    ta_nosql_errcls cls = TA_NOSQL_ERR_INVALID;

    if (e->domain == MONGOC_ERROR_CLIENT &&
        e->code == MONGOC_ERROR_CLIENT_AUTHENTICATE)
        cls = TA_NOSQL_ERR_AUTH;
    else if (strstr(e->message, "Authentication failed") != NULL ||
             strstr(e->message, "auth error") != NULL)
        cls = TA_NOSQL_ERR_AUTH;
    else if (e->domain == MONGOC_ERROR_STREAM ||
             e->domain == MONGOC_ERROR_SERVER_SELECTION)
    {
        if (strstr(e->message, "TLS") != NULL ||
            strstr(e->message, "SSL") != NULL ||
            strstr(e->message, "certificate") != NULL)
            cls = TA_NOSQL_ERR_TLS;
        else if (strstr(e->message, "timed out") != NULL ||
                 strstr(e->message, "timeout") != NULL)
            cls = TA_NOSQL_ERR_TIMEOUT;
        else
            cls = TA_NOSQL_ERR_CONNECT;
    }
    else if (e->domain == MONGOC_ERROR_SERVER ||
             e->domain == MONGOC_ERROR_WRITE_CONCERN ||
             e->domain == MONGOC_ERROR_QUERY ||
             e->domain == MONGOC_ERROR_COLLECTION ||
             e->domain == MONGOC_ERROR_COMMAND)
    {
        switch (e->code)
        {
            case 13:        /* Unauthorized */
                cls = strstr(e->message, "requires authentication") != NULL
                      ? TA_NOSQL_ERR_AUTH : TA_NOSQL_ERR_PERMISSION;
                break;
            case 18:        /* AuthenticationFailed */
                cls = TA_NOSQL_ERR_AUTH;
                break;
            case 50:        /* MaxTimeMSExpired */
            case 262:       /* ExceededTimeLimit */
                cls = TA_NOSQL_ERR_TIMEOUT;
                break;
            case 20:        /* IllegalOperation (transactions on a
                               standalone server) */
            case 263:       /* OperationNotSupportedInTransaction */
                cls = TA_NOSQL_ERR_UNSUPPORTED;
                break;
            case 91:        /* ShutdownInProgress */
            case 189:       /* PrimarySteppedDown */
            case 10107:     /* NotWritablePrimary */
            case 13435:     /* NotPrimaryNoSecondaryOk */
            case 11600:     /* InterruptedAtShutdown */
                cls = TA_NOSQL_ERR_UNAVAILABLE;
                break;
            case 10334:     /* BSONObjectTooLarge */
            case 17419:
                cls = TA_NOSQL_ERR_LIMIT;
                break;
            default:
                cls = TA_NOSQL_ERR_INVALID;
                break;
        }
        if (strstr(e->message, "Transaction numbers are only allowed") != NULL)
            cls = TA_NOSQL_ERR_UNSUPPORTED;
    }
    else if (e->domain == MONGOC_ERROR_TRANSACTION)
    {
        cls = TA_NOSQL_ERR_UNSUPPORTED;
    }
    ta_nosql_err_set(err, cls, (long)e->code, "mongodb: %s", e->message);
}

/* ---- connection ------------------------------------------------------ */

static int
make_client(mongo_conn *mc, ta_nosql_err *err)
{
    bson_error_t e;

    if (mc->client != NULL)
        mongoc_client_destroy(mc->client);
    mongoc_uri_set_option_as_int32(mc->uri, MONGOC_URI_SOCKETTIMEOUTMS,
                                   mc->cfg.timeout_ms);
    mongoc_uri_set_option_as_int32(mc->uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS,
                                   mc->cfg.connect_timeout_ms);
    mc->client = mongoc_client_new_from_uri_with_error(mc->uri, &e);
    if (mc->client == NULL)
    {
        mongo_err(&e, err);
        return -1;
    }
    mongoc_client_set_error_api(mc->client, MONGOC_ERROR_API_VERSION_2);
    mongoc_client_set_appname(mc->client, "tsf-nosql");
    return 0;
}

static int mongo_ping(void *dc, ta_nosql_err *err);
static void mongo_disconnect(void *dc);

static int
mongo_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    mongo_conn  *mc = calloc(1, sizeof(*mc));
    bson_error_t e;
    const char  *rs = ta_nosql_json_str(cfg->raw, "replica_set", NULL);
    const char  *auth_db = ta_nosql_json_str(cfg->raw, "auth_db", "admin");
    char        *uri_text;

    pthread_once(&mongo_once, mongo_init);
    if (mc == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &mc->cfg);
    mc->db = strdup(cfg->database != NULL ? cfg->database : "tsf");
    mc->ttl_ready = json_object();
    uri_text = ta_nosql_strdupf("mongodb://%s%s%s:%d/",
                                strchr(cfg->host, ':') != NULL ? "[" : "",
                                cfg->host,
                                strchr(cfg->host, ':') != NULL ? "]" : "",
                                cfg->port);
    mc->uri = mongoc_uri_new_with_error(uri_text, &e);
    free(uri_text);
    if (mc->uri == NULL)
    {
        mongo_err(&e, err);
        err->cls = TA_NOSQL_ERR_INVALID;
        mongo_disconnect(mc);
        return -1;
    }
    /* Credentials through the API: no escaping, nothing in a URI text. */
    if (cfg->user != NULL)
    {
        mongoc_uri_set_username(mc->uri, cfg->user);
        mongoc_uri_set_password(mc->uri, cfg->password != NULL
                                         ? cfg->password : "");
        mongoc_uri_set_auth_source(mc->uri, auth_db);
    }
    mongoc_uri_set_option_as_int32(mc->uri, MONGOC_URI_CONNECTTIMEOUTMS,
                                   cfg->connect_timeout_ms);
    if (rs != NULL)
        mongoc_uri_set_option_as_utf8(mc->uri, MONGOC_URI_REPLICASET, rs);
    else
        mongoc_uri_set_option_as_bool(mc->uri, MONGOC_URI_DIRECTCONNECTION,
                                      true);
    if (cfg->tls)
    {
        mongoc_uri_set_option_as_bool(mc->uri, MONGOC_URI_TLS, true);
        if (cfg->tls_ca != NULL)
            mongoc_uri_set_option_as_utf8(mc->uri, MONGOC_URI_TLSCAFILE,
                                          cfg->tls_ca);
        if (!cfg->tls_verify)
            mongoc_uri_set_option_as_bool(mc->uri,
                                          MONGOC_URI_TLSALLOWINVALIDCERTIFICATES,
                                          true);
    }
    if (make_client(mc, err) != 0 || mongo_ping(mc, err) != 0)
    {
        mongo_disconnect(mc);
        return -1;
    }
    *dc = mc;
    return 0;
}

static void
mongo_disconnect(void *dc)
{
    mongo_conn *mc = dc;

    if (mc == NULL)
        return;
    if (mc->client != NULL)
        mongoc_client_destroy(mc->client);
    if (mc->uri != NULL)
        mongoc_uri_destroy(mc->uri);
    json_decref(mc->ttl_ready);
    free(mc->db);
    ta_nosql_conf_free(&mc->cfg);
    free(mc);
}

static int
run_cmd(mongo_conn *mc, const char *db, const bson_t *cmd, bson_t *reply,
        ta_nosql_err *err)
{
    bson_error_t e;
    bson_t       local;
    bool         ok;

    ok = mongoc_client_command_simple(mc->client, db, cmd, NULL,
                                      reply != NULL ? reply : &local, &e);
    if (reply == NULL)
        bson_destroy(&local);
    if (!ok)
    {
        mongo_err(&e, err);
        return -1;
    }
    return 0;
}

static int
mongo_ping(void *dc, ta_nosql_err *err)
{
    bson_t *cmd = BCON_NEW("ping", BCON_INT32(1));
    int     rc = run_cmd(dc, "admin", cmd, NULL, err);

    bson_destroy(cmd);
    return rc;
}

static int
mongo_info(void *dc, json_t *out, ta_nosql_err *err)
{
    mongo_conn *mc = dc;
    bson_t     *cmd = BCON_NEW("buildInfo", BCON_INT32(1));
    bson_t      reply;
    bson_iter_t it;

    if (run_cmd(mc, "admin", cmd, &reply, err) != 0)
    {
        bson_destroy(cmd);
        return -1;
    }
    bson_destroy(cmd);
    if (bson_iter_init_find(&it, &reply, "version") && BSON_ITER_HOLDS_UTF8(&it))
        json_object_set_new(out, "server_version",
                            json_string(bson_iter_utf8(&it, NULL)));
    bson_destroy(&reply);

    cmd = BCON_NEW("hello", BCON_INT32(1));
    if (run_cmd(mc, "admin", cmd, &reply, err) == 0)
    {
        if (bson_iter_init_find(&it, &reply, "setName") &&
            BSON_ITER_HOLDS_UTF8(&it))
            json_object_set_new(out, "replica_set",
                                json_string(bson_iter_utf8(&it, NULL)));
        bson_destroy(&reply);
    }
    bson_destroy(cmd);
    json_object_set_new(out, "database", json_string(mc->db));
    memset(err, 0, sizeof(*err));
    return 0;
}

static bool
mongo_tls_in_use(void *dc)
{
    return ((mongo_conn *)dc)->cfg.tls;
}

static mongoc_collection_t *
coll_of(mongo_conn *mc, const char *coll)
{
    return mongoc_client_get_collection(mc->client, mc->db, coll);
}

/* The TTL index of a collection, made once per connection. */
static int
ensure_ttl(mongo_conn *mc, const char *coll, ta_nosql_err *err)
{
    bson_t *cmd;
    int     rc;

    if (json_object_get(mc->ttl_ready, coll) != NULL)
        return 0;
    cmd = BCON_NEW("createIndexes", BCON_UTF8(coll),
                   "indexes", "[", "{",
                       "key", "{", "_exp", BCON_INT32(1), "}",
                       "name", BCON_UTF8("tsf_nosql_ttl"),
                       "expireAfterSeconds", BCON_INT32(0),
                   "}", "]");
    rc = run_cmd(mc, mc->db, cmd, NULL, err);
    bson_destroy(cmd);
    if (rc == 0)
        json_object_set_new(mc->ttl_ready, coll, json_true());
    return rc;
}

/* The stored form: _id, _v, _exp?, then the document's fields. */
static bson_t *
stored(const char *key, const char *ver, json_t *doc, long ttl_s)
{
    bson_t *b = bson_new();

    BSON_APPEND_UTF8(b, "_id", key);
    BSON_APPEND_UTF8(b, "_v", ver);
    if (ttl_s > 0)
    {
        struct timespec ts;

        clock_gettime(CLOCK_REALTIME, &ts);
        BSON_APPEND_DATE_TIME(b, "_exp",
                              ((int64_t)ts.tv_sec + ttl_s) * 1000 +
                              ts.tv_nsec / 1000000);
    }
    if (!json_to_bson(doc, b))
    {
        bson_destroy(b);
        return NULL;
    }
    return b;
}

static int
mongo_write(mongo_conn *mc, const char *coll, const char *key, json_t *doc,
            long ttl_s, int mode, const char *if_ver, bool *applied,
            char **ver, ta_nosql_err *err)
{
    mongoc_collection_t *c;
    bson_t              *sel = NULL;
    bson_t              *b;
    bson_t              *opts = NULL;
    bson_t               reply;
    bson_error_t         e;
    char                *nv = ta_nosql_new_version();
    bool                 ok;
    int                  rc = 0;

    if (ttl_s > 0 && ensure_ttl(mc, coll, err) != 0)
    {
        free(nv);
        return -1;
    }
    b = stored(key, nv, doc, ttl_s);
    if (b == NULL)
    {
        free(nv);
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "mongodb: the document does not fit BSON");
        return -1;
    }
    c = coll_of(mc, coll);
    if (mode == 1)
    {
        /* Absent: an insert, refused on a duplicate _id. */
        ok = mongoc_collection_insert_one(c, b, NULL, &reply, &e);
        if (!ok && e.code == 11000)
        {
            *applied = false;
            ok = true;
            memset(&e, 0, sizeof(e));
            free(nv);
            nv = NULL;
        }
        else if (ok)
        {
            *applied = true;
        }
    }
    else
    {
        sel = mode == 2 ? BCON_NEW("_id", BCON_UTF8(key), "_v", BCON_UTF8(if_ver))
                        : BCON_NEW("_id", BCON_UTF8(key));
        opts = mode == 2 ? NULL : BCON_NEW("upsert", BCON_BOOL(true));
        ok = mongoc_collection_replace_one(c, sel, b, opts, &reply, &e);
        if (ok && mode == 2)
        {
            bson_iter_t it;

            *applied = bson_iter_init_find(&it, &reply, "matchedCount") &&
                       bson_iter_as_int64(&it) > 0;
            if (!*applied)
            {
                free(nv);
                nv = NULL;
            }
        }
    }
    if (!ok)
    {
        mongo_err(&e, err);
        rc = -1;
    }
    bson_destroy(&reply);
    if (sel != NULL)
        bson_destroy(sel);
    if (opts != NULL)
        bson_destroy(opts);
    bson_destroy(b);
    mongoc_collection_destroy(c);
    if (rc == 0 && ver != NULL)
        *ver = nv;
    else
        free(nv);
    return rc;
}

static int
mongo_put(void *dc, const char *coll, const char *key, json_t *doc,
          long ttl_s, char **ver, ta_nosql_err *err)
{
    return mongo_write(dc, coll, key, doc, ttl_s, 0, NULL, NULL, ver, err);
}

static int
mongo_put_if(void *dc, const char *coll, const char *key, json_t *doc,
             long ttl_s, const char *if_ver, bool *applied, char **ver,
             ta_nosql_err *err)
{
    return mongo_write(dc, coll, key, doc, ttl_s, if_ver == NULL ? 1 : 2,
                       if_ver, applied, ver, err);
}

/* A stored document as the caller's: the bookkeeping fields removed. */
static json_t *
unstore(const bson_t *b, char **ver)
{
    json_t *d = bson_to_json(b);

    if (ver != NULL)
    {
        const char *v = ta_nosql_json_str(d, "_v", NULL);

        *ver = v != NULL ? strdup(v) : NULL;
    }
    json_object_del(d, "_id");
    json_object_del(d, "_v");
    json_object_del(d, "_exp");
    return d;
}

static int
mongo_get(void *dc, const char *coll, const char *key, json_t **doc,
          char **ver, ta_nosql_err *err)
{
    mongo_conn          *mc = dc;
    mongoc_collection_t *c = coll_of(mc, coll);
    bson_t              *filter = BCON_NEW("_id", BCON_UTF8(key));
    bson_t              *opts = BCON_NEW("limit", BCON_INT64(1));
    mongoc_cursor_t     *cur;
    const bson_t        *b;
    bson_error_t         e;
    int                  rc = 0;

    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    cur = mongoc_collection_find_with_opts(c, filter, opts, NULL);
    if (mongoc_cursor_next(cur, &b))
        *doc = unstore(b, ver);
    if (mongoc_cursor_error(cur, &e))
    {
        mongo_err(&e, err);
        json_decref(*doc);
        *doc = NULL;
        rc = -1;
    }
    mongoc_cursor_destroy(cur);
    bson_destroy(filter);
    bson_destroy(opts);
    mongoc_collection_destroy(c);
    return rc;
}

static int
mongo_del(void *dc, const char *coll, const char *key, bool *deleted,
          ta_nosql_err *err)
{
    mongo_conn          *mc = dc;
    mongoc_collection_t *c = coll_of(mc, coll);
    bson_t              *sel = BCON_NEW("_id", BCON_UTF8(key));
    bson_t               reply;
    bson_error_t         e;
    bson_iter_t          it;
    int                  rc = 0;

    if (!mongoc_collection_delete_one(c, sel, NULL, &reply, &e))
    {
        mongo_err(&e, err);
        rc = -1;
    }
    else
    {
        *deleted = bson_iter_init_find(&it, &reply, "deletedCount") &&
                   bson_iter_as_int64(&it) > 0;
    }
    bson_destroy(&reply);
    bson_destroy(sel);
    mongoc_collection_destroy(c);
    return rc;
}

static int
mongo_incr(void *dc, const char *coll, const char *key, long long by,
           long long *value, ta_nosql_err *err)
{
    mongo_conn                 *mc = dc;
    mongoc_collection_t        *c = coll_of(mc, coll);
    mongoc_find_and_modify_opts_t *fam = mongoc_find_and_modify_opts_new();
    bson_t                     *q = BCON_NEW("_id", BCON_UTF8(key));
    char                       *nv = ta_nosql_new_version();
    bson_t                     *upd = BCON_NEW("$inc", "{", "n", BCON_INT64(by),
                                               "}", "$set", "{", "_v",
                                               BCON_UTF8(nv), "}");
    bson_t                      reply;
    bson_error_t                e;
    bson_iter_t                 it;
    bson_iter_t                 val;
    int                         rc = 0;

    mongoc_find_and_modify_opts_set_update(fam, upd);
    mongoc_find_and_modify_opts_set_flags(fam,
        MONGOC_FIND_AND_MODIFY_UPSERT | MONGOC_FIND_AND_MODIFY_RETURN_NEW);
    if (!mongoc_collection_find_and_modify_with_opts(c, q, fam, &reply, &e))
    {
        mongo_err(&e, err);
        rc = -1;
    }
    else if (bson_iter_init_find(&it, &reply, "value") &&
             BSON_ITER_HOLDS_DOCUMENT(&it) &&
             bson_iter_recurse(&it, &val) && bson_iter_find(&val, "n"))
    {
        *value = bson_iter_as_int64(&val);
    }
    else
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_PROTOCOL, 0,
                         "mongodb: findAndModify returned no counter");
        rc = -1;
    }
    bson_destroy(&reply);
    bson_destroy(q);
    bson_destroy(upd);
    free(nv);
    mongoc_find_and_modify_opts_destroy(fam);
    mongoc_collection_destroy(c);
    return rc;
}

/* A query: the filter's fields compared with $eq, so a value that is an
 * operator object stays a value. */
static bson_t *
query_of(json_t *filter, const char *cursor)
{
    bson_t     *q = bson_new();
    const char *k;
    json_t     *v;

    if (filter != NULL)
    {
        json_object_foreach(filter, k, v)
        {
            bson_t child;

            bson_append_document_begin(q, k, -1, &child);
            append_value(&child, "$eq", v);
            bson_append_document_end(q, &child);
        }
    }
    if (cursor != NULL)
    {
        bson_t child;

        bson_append_document_begin(q, "_id", -1, &child);
        BSON_APPEND_UTF8(&child, "$gt", cursor);
        bson_append_document_end(q, &child);
    }
    return q;
}

static int
page(mongo_conn *mc, const char *coll, json_t *filter, int limit,
     const char *cursor, json_t *out, bool docs, char **next,
     ta_nosql_err *err)
{
    mongoc_collection_t *c = coll_of(mc, coll);
    bson_t              *q = query_of(filter, cursor);
    bson_t              *opts = docs
        ? BCON_NEW("sort", "{", "_id", BCON_INT32(1), "}",
                   "limit", BCON_INT64(limit))
        : BCON_NEW("sort", "{", "_id", BCON_INT32(1), "}",
                   "limit", BCON_INT64(limit),
                   "projection", "{", "_id", BCON_INT32(1), "}");
    mongoc_cursor_t     *cur = mongoc_collection_find_with_opts(c, q, opts,
                                                                NULL);
    const bson_t        *b;
    bson_error_t         e;
    bson_iter_t          it;
    char                *last = NULL;
    int                  n = 0;
    int                  rc = 0;

    *next = NULL;
    while (mongoc_cursor_next(cur, &b))
    {
        if (!bson_iter_init_find(&it, b, "_id") || !BSON_ITER_HOLDS_UTF8(&it))
            continue;
        free(last);
        last = strdup(bson_iter_utf8(&it, NULL));
        if (docs)
            json_array_append_new(out, json_pack("{s:s, s:o}", "key", last,
                                                 "doc", unstore(b, NULL)));
        else
            json_array_append_new(out, json_string(last));
        n++;
    }
    if (mongoc_cursor_error(cur, &e))
    {
        mongo_err(&e, err);
        rc = -1;
    }
    else if (n == limit && last != NULL)
    {
        *next = last;
        last = NULL;
    }
    free(last);
    mongoc_cursor_destroy(cur);
    bson_destroy(q);
    bson_destroy(opts);
    mongoc_collection_destroy(c);
    return rc;
}

static int
mongo_scan(void *dc, const char *coll, int limit, const char *cursor,
           json_t *keys, char **next, ta_nosql_err *err)
{
    return page(dc, coll, NULL, limit, cursor, keys, false, next, err);
}

static int
mongo_find(void *dc, const char *coll, json_t *filter, int limit,
           const char *cursor, json_t *docs, char **next, ta_nosql_err *err)
{
    return page(dc, coll, filter, limit, cursor, docs, true, next, err);
}

static int
mongo_count(void *dc, const char *coll, json_t *filter, long long *n,
            ta_nosql_err *err)
{
    mongo_conn          *mc = dc;
    mongoc_collection_t *c = coll_of(mc, coll);
    bson_t              *q = query_of(filter, NULL);
    bson_error_t         e;
    int64_t              cnt;

    cnt = mongoc_collection_count_documents(c, q, NULL, NULL, NULL, &e);
    bson_destroy(q);
    mongoc_collection_destroy(c);
    if (cnt < 0)
    {
        mongo_err(&e, err);
        return -1;
    }
    *n = cnt;
    return 0;
}

static int
mongo_multi(void *dc, json_t *ops, const char *if_coll, const char *if_key,
            bool *applied, ta_nosql_err *err)
{
    mongo_conn              *mc = dc;
    mongoc_client_session_t *s;
    bson_t                   opts = BSON_INITIALIZER;
    bson_error_t             e;
    size_t                   i;
    json_t                  *op;
    int                      rc = 0;

    *applied = false;
    s = mongoc_client_start_session(mc->client, NULL, &e);
    if (s == NULL)
    {
        mongo_err(&e, err);
        return -1;
    }
    if (!mongoc_client_session_start_transaction(s, NULL, &e))
    {
        mongo_err(&e, err);
        mongoc_client_session_destroy(s);
        return -1;
    }
    if (!mongoc_client_session_append(s, &opts, &e))
    {
        mongo_err(&e, err);
        rc = -1;
        goto abort;
    }
    if (if_coll != NULL)
    {
        mongoc_collection_t *c = coll_of(mc, if_coll);
        bson_t              *q = BCON_NEW("_id", BCON_UTF8(if_key));
        int64_t              cnt;

        cnt = mongoc_collection_count_documents(c, q, &opts, NULL, NULL, &e);
        bson_destroy(q);
        mongoc_collection_destroy(c);
        if (cnt < 0)
        {
            mongo_err(&e, err);
            rc = -1;
            goto abort;
        }
        if (cnt > 0)
            goto abort;
    }
    json_array_foreach(ops, i, op)
    {
        const char          *kind = ta_nosql_json_str(op, "op", "");
        const char          *key = ta_nosql_json_str(op, "key", "");
        mongoc_collection_t *c = coll_of(mc, ta_nosql_json_str(op, "coll", ""));
        bson_t              *sel = BCON_NEW("_id", BCON_UTF8(key));
        bool                 ok;

        if (strcmp(kind, "put") == 0)
        {
            char   *nv = ta_nosql_new_version();
            bson_t *b = stored(key, nv, json_object_get(op, "doc"), 0);
            bson_t  o = BSON_INITIALIZER;

            bson_copy_to_excluding_noinit(&opts, &o, "", NULL);
            BSON_APPEND_BOOL(&o, "upsert", true);
            ok = b != NULL &&
                 mongoc_collection_replace_one(c, sel, b, &o, NULL, &e);
            bson_destroy(&o);
            if (b != NULL)
                bson_destroy(b);
            free(nv);
        }
        else
        {
            ok = mongoc_collection_delete_one(c, sel, &opts, NULL, &e);
        }
        bson_destroy(sel);
        mongoc_collection_destroy(c);
        if (!ok)
        {
            mongo_err(&e, err);
            rc = -1;
            goto abort;
        }
    }
    if (!mongoc_client_session_commit_transaction(s, NULL, &e))
    {
        mongo_err(&e, err);
        rc = -1;
    }
    else
    {
        *applied = true;
    }
    bson_destroy(&opts);
    mongoc_client_session_destroy(s);
    return rc;

abort:
    mongoc_client_session_abort_transaction(s, NULL);
    bson_destroy(&opts);
    mongoc_client_session_destroy(s);
    return rc;
}

static int
mongo_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    mongo_conn          *mc = dc;
    mongoc_collection_t *c = coll_of(mc, coll);
    bson_error_t         e;
    int                  rc = 0;

    if (!mongoc_collection_drop(c, &e) && e.code != 26 &&
        strstr(e.message, "ns not found") == NULL)
    {
        mongo_err(&e, err);
        rc = -1;
    }
    mongoc_collection_destroy(c);
    json_object_del(mc->ttl_ready, coll);
    return rc;
}

static int
mongo_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    mongo_conn *mc = dc;
    json_t     *command = json_object_get(cmd, "command");
    const char *db = ta_nosql_json_str(cmd, "db", mc->db);
    bson_t     *b;
    bson_t      r;
    int         rc;

    if (!json_is_object(command) || json_object_size(command) == 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "mongodb: 'cmd' needs 'command', a non-empty object "
                         "(its first field names the command)");
        return -1;
    }
    b = bson_new();
    if (!json_to_bson(command, b))
    {
        bson_destroy(b);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "mongodb: the command does not convert to BSON");
        return -1;
    }
    rc = run_cmd(mc, db, b, &r, err);
    bson_destroy(b);
    if (rc == 0)
        *reply = bson_to_json(&r);
    bson_destroy(&r);
    return rc;
}

static int
mongo_bulk(void *dc, const char *coll, json_t *items, ta_nosql_err *err)
{
    mongo_conn               *mc = dc;
    mongoc_collection_t      *c = coll_of(mc, coll);
    bson_t                   *bo = BCON_NEW("ordered", BCON_BOOL(false));
    mongoc_bulk_operation_t  *bulk = mongoc_collection_create_bulk_operation_with_opts(c, bo);
    bson_t                    reply;
    bson_error_t              e;
    size_t                    i;
    json_t                   *it;
    int                       rc = 0;

    json_array_foreach(items, i, it)
    {
        const char *key = json_string_value(json_object_get(it, "key"));
        char       *nv = ta_nosql_new_version();
        bson_t     *b = stored(key, nv, json_object_get(it, "doc"), 0);
        bson_t     *sel = BCON_NEW("_id", BCON_UTF8(key));
        bson_t     *o = BCON_NEW("upsert", BCON_BOOL(true));

        if (b != NULL)
            mongoc_bulk_operation_replace_one_with_opts(bulk, sel, b, o, NULL);
        bson_destroy(o);
        bson_destroy(sel);
        if (b != NULL)
            bson_destroy(b);
        free(nv);
    }
    if (!mongoc_bulk_operation_execute(bulk, &reply, &e))
    {
        mongo_err(&e, err);
        rc = -1;
    }
    bson_destroy(&reply);
    mongoc_bulk_operation_destroy(bulk);
    bson_destroy(bo);
    mongoc_collection_destroy(c);
    return rc;
}

static int
mongo_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    mongo_conn *mc = dc;

    mc->cfg.timeout_ms = ms;
    return make_client(mc, err);
}

static int
mongo_audit(const ta_nosql_conf *cfg, void *dc, json_t *args,
            json_t *findings, json_t *checked, ta_nosql_err *err)
{
    mongo_conn *mc = dc;
    bson_t     *cmd;
    bson_t      reply;
    json_t     *r;
    json_t     *roles;
    size_t      i;
    json_t     *role;
    char        subject[300];
    ta_nosql_err cerr;

    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    json_array_append_new(checked, json_string("mongodb.superuser"));
    if (mc == NULL || cfg->user == NULL)
        return 0;
    memset(&cerr, 0, sizeof(cerr));
    cmd = BCON_NEW("connectionStatus", BCON_INT32(1));
    if (run_cmd(mc, "admin", cmd, &reply, &cerr) != 0)
    {
        bson_destroy(cmd);
        return 0;
    }
    bson_destroy(cmd);
    r = bson_to_json(&reply);
    bson_destroy(&reply);
    roles = json_object_get(json_object_get(r, "authInfo"),
                            "authenticatedUserRoles");
    json_array_foreach(roles, i, role)
    {
        const char *name = ta_nosql_json_str(role, "role", "");

        if (strcmp(name, "root") == 0 || strcmp(name, "__system") == 0 ||
            strcmp(name, "userAdminAnyDatabase") == 0 ||
            strcmp(name, "dbAdminAnyDatabase") == 0 ||
            strcmp(name, "clusterAdmin") == 0)
        {
            ta_nosql_finding(findings, "mongodb.superuser",
                             TA_NOSQL_SEV_MEDIUM, subject,
                             "the account '%s' holds the server-wide role "
                             "'%s'", cfg->user, name);
        }
    }
    json_decref(r);
    return 0;
}

static void
mongo_thread_init(void)
{
    pthread_once(&mongo_once, mongo_init);
}

static const char *const mongo_default_creds[] = {
    "admin:admin", "root:root", "admin:password", "root:example",
    "mongo:mongo", NULL
};

const ta_nosql_driver ta_nosql_mongo_driver = {
    .engine = TA_NOSQL_ENGINE_MONGODB,
    .name = "mongodb",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_FIND | TA_NOSQL_CAP_COUNT |
            TA_NOSQL_CAP_TTL | TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT |
            TA_NOSQL_CAP_INCR | TA_NOSQL_CAP_INCR_EXACT |
            TA_NOSQL_CAP_MULTI | TA_NOSQL_CAP_MULTI_IF |
            TA_NOSQL_CAP_DURABLE | TA_NOSQL_CAP_TLS | TA_NOSQL_CAP_AUTH,
    .default_port = 27017,
    .default_creds = mongo_default_creds,
    .client_version = mongo_client_version,
    .connect = mongo_connect,
    .disconnect = mongo_disconnect,
    .ping = mongo_ping,
    .info = mongo_info,
    .tls_in_use = mongo_tls_in_use,
    .put = mongo_put,
    .put_if = mongo_put_if,
    .get = mongo_get,
    .del = mongo_del,
    .incr = mongo_incr,
    .scan = mongo_scan,
    .find = mongo_find,
    .count = mongo_count,
    .multi = mongo_multi,
    .drop = mongo_drop,
    .native = mongo_native,
    .bulk = mongo_bulk,
    .set_timeout = mongo_set_timeout,
    .audit = mongo_audit,
    .thread_init = mongo_thread_init,
};
