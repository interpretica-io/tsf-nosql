/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: Apache Cassandra over the DataStax C driver
 *
 * A collection is a table of the configured keyspace ("tsf" by default,
 * made with SimpleStrategy and one replica when missing):
 * <coll> (k text PRIMARY KEY, doc text, v text) for documents and
 * <coll>__n (k text PRIMARY KEY, n counter) for counters - Cassandra
 * keeps counters in tables of their own. Both are created on first use.
 *
 * The conditional writes are lightweight transactions (IF NOT EXISTS,
 * IF v = ?), which Cassandra runs through Paxos; "multi" is a logged
 * batch, atomic but without a condition across partitions, so the
 * conditional form is not offered. A counter update returns nothing, so
 * incr reads the counter after the update: under concurrency the value
 * returned may include other clients' increments (no "incr_exact").
 *
 * A key read from a table or a keyspace that does not exist yet is
 * simply absent.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cassandra.h>

#include "ta_nosql_int.h"

typedef struct cass_conn {
    CassCluster *cluster;
    CassSession *session;
    CassSsl     *ssl;
    ta_nosql_conf cfg;
    char        *ks;
    json_t      *ready;     /**< Tables known to exist. */
    bool         ks_ready;
} cass_conn;

static void
cass_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "DataStax C/C++ driver %d.%d.%d", CASS_VERSION_MAJOR,
             CASS_VERSION_MINOR, CASS_VERSION_PATCH);
}

static ta_nosql_errcls
code_cls(CassError rc, const char *msg)
{
    switch (rc)
    {
        case CASS_ERROR_SERVER_BAD_CREDENTIALS:
            return TA_NOSQL_ERR_AUTH;
        case CASS_ERROR_SERVER_UNAUTHORIZED:
            return TA_NOSQL_ERR_PERMISSION;
        case CASS_ERROR_LIB_REQUEST_TIMED_OUT:
        case CASS_ERROR_SERVER_READ_TIMEOUT:
        case CASS_ERROR_SERVER_WRITE_TIMEOUT:
            return TA_NOSQL_ERR_TIMEOUT;
        case CASS_ERROR_SERVER_UNAVAILABLE:
        case CASS_ERROR_SERVER_OVERLOADED:
        case CASS_ERROR_SERVER_IS_BOOTSTRAPPING:
            return TA_NOSQL_ERR_UNAVAILABLE;
        case CASS_ERROR_SERVER_SYNTAX_ERROR:
        case CASS_ERROR_SERVER_INVALID_QUERY:
        case CASS_ERROR_SERVER_ALREADY_EXISTS:
        case CASS_ERROR_LIB_BAD_PARAMS:
            return TA_NOSQL_ERR_INVALID;
        case CASS_ERROR_SSL_INVALID_CERT:
        case CASS_ERROR_SSL_PROTOCOL_ERROR:
        case CASS_ERROR_SSL_NO_PEER_CERT:
        case CASS_ERROR_SSL_INVALID_PEER_CERT:
        case CASS_ERROR_SSL_IDENTITY_MISMATCH:
        case CASS_ERROR_SSL_CLOSED:
            return TA_NOSQL_ERR_TLS;
        case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE:
        case CASS_ERROR_LIB_UNABLE_TO_CONNECT:
        case CASS_ERROR_LIB_UNABLE_TO_INIT:
        case CASS_ERROR_LIB_UNABLE_TO_CLOSE:
            if (msg != NULL &&
                (strstr(msg, "redentials") != NULL ||
                 strstr(msg, "uthentication") != NULL ||
                 strstr(msg, "Provided username") != NULL))
                return TA_NOSQL_ERR_AUTH;
            if (msg != NULL && (strstr(msg, "SSL") != NULL ||
                                strstr(msg, "TLS") != NULL))
                return TA_NOSQL_ERR_TLS;
            return TA_NOSQL_ERR_CONNECT;
        default:
            return TA_NOSQL_ERR_PROTOCOL;
    }
}

static void
future_err(CassFuture *f, ta_nosql_err *err)
{
    const char *msg = NULL;
    size_t      len = 0;
    CassError   rc = cass_future_error_code(f);
    char        m[400];

    cass_future_error_message(f, &msg, &len);
    snprintf(m, sizeof(m), "%.*s", (int)len, msg != NULL ? msg : "");
    ta_nosql_err_set(err, code_cls(rc, m), (long)rc, "cassandra: %s: %s",
                     cass_error_desc(rc), m);
}

/* A table or keyspace that is not there yet: a read finds nothing. */
static bool
missing_schema(const ta_nosql_err *err)
{
    return err->cls == TA_NOSQL_ERR_INVALID &&
           (strstr(err->msg, "unconfigured table") != NULL ||
            strstr(err->msg, "does not exist") != NULL ||
            strstr(err->msg, "not exist") != NULL);
}

static const CassResult *
run(cass_conn *cc, CassStatement *st, ta_nosql_err *err)
{
    CassFuture       *f;
    const CassResult *r = NULL;

    cass_statement_set_request_timeout(st, (cass_uint64_t)cc->cfg.timeout_ms);
    f = cass_session_execute(cc->session, st);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK)
    {
        future_err(f, err);
        /*
         * On a session that is up, "no hosts available" means every host
         * the policy tried refused or failed the request - with too few
         * replicas up, each answers Unavailable and the driver moves on.
         * That is the replicas' unavailability, not a connection failure
         * (connecting reports its own errors).
         */
        if (cass_future_error_code(f) == CASS_ERROR_LIB_NO_HOSTS_AVAILABLE)
            err->cls = TA_NOSQL_ERR_UNAVAILABLE;
    }
    else
    {
        r = cass_future_get_result(f);
    }
    cass_future_free(f);
    cass_statement_free(st);
    return r;
}

static int
run_simple(cass_conn *cc, const char *cql, ta_nosql_err *err)
{
    const CassResult *r = run(cc, cass_statement_new(cql, 0), err);

    if (r == NULL)
        return -1;
    cass_result_free(r);
    return 0;
}

static int
ensure_tables(cass_conn *cc, const char *coll, ta_nosql_err *err)
{
    char *cql;
    int   rc;

    if (json_object_get(cc->ready, coll) != NULL)
        return 0;
    if (!cc->ks_ready &&
        ta_nosql_json_bool(cc->cfg.raw, "create_keyspace", true))
    {
        cql = ta_nosql_strdupf("CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH "
                               "replication = {'class': 'SimpleStrategy', "
                               "'replication_factor': %lld}", cc->ks,
                               ta_nosql_json_int(cc->cfg.raw,
                                                 "replication_factor", 1));
        rc = run_simple(cc, cql, err);
        free(cql);
        if (rc != 0)
            return -1;
        cc->ks_ready = true;
    }
    cql = ta_nosql_strdupf("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" "
                           "(k text PRIMARY KEY, doc text, v text)",
                           cc->ks, coll);
    rc = run_simple(cc, cql, err);
    free(cql);
    if (rc != 0)
        return -1;
    cql = ta_nosql_strdupf("CREATE TABLE IF NOT EXISTS \"%s\".\"%s__n\" "
                           "(k text PRIMARY KEY, n counter)", cc->ks, coll);
    rc = run_simple(cc, cql, err);
    free(cql);
    if (rc != 0)
        return -1;
    json_object_set_new(cc->ready, coll, json_true());
    return 0;
}

static void cass_disconnect(void *dc);

static int
cass_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    cass_conn  *cc = calloc(1, sizeof(*cc));
    CassFuture *f;

    if (cc == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &cc->cfg);
    cc->ks = strdup(cfg->database != NULL ? cfg->database : "tsf");
    cc->ready = json_object();
    cc->cluster = cass_cluster_new();
    cc->session = cass_session_new();
    cass_log_set_level(CASS_LOG_DISABLED);
    /* host may list several nodes: "10.0.0.1,10.0.0.2". */
    cass_cluster_set_contact_points(cc->cluster, cfg->host);
    cass_cluster_set_port(cc->cluster, cfg->port);
    cass_cluster_set_connect_timeout(cc->cluster,
                                     (unsigned)cfg->connect_timeout_ms);
    cass_cluster_set_request_timeout(cc->cluster, (unsigned)cfg->timeout_ms);
    {
        /* "consistency": the level of every request (default LOCAL_ONE,
         * the driver's); a cluster test sets ONE / QUORUM / ALL. */
        static const struct {
            const char      *name;
            CassConsistency  level;
        } levels[] = {
            { "ANY", CASS_CONSISTENCY_ANY },
            { "ONE", CASS_CONSISTENCY_ONE },
            { "TWO", CASS_CONSISTENCY_TWO },
            { "THREE", CASS_CONSISTENCY_THREE },
            { "QUORUM", CASS_CONSISTENCY_QUORUM },
            { "ALL", CASS_CONSISTENCY_ALL },
            { "LOCAL_QUORUM", CASS_CONSISTENCY_LOCAL_QUORUM },
            { "LOCAL_ONE", CASS_CONSISTENCY_LOCAL_ONE },
        };
        const char *want = ta_nosql_json_str(cfg->raw, "consistency", NULL);
        size_t      i;

        if (want != NULL)
        {
            for (i = 0; i < sizeof(levels) / sizeof(levels[0]); i++)
            {
                if (strcmp(levels[i].name, want) == 0)
                    break;
            }
            if (i == sizeof(levels) / sizeof(levels[0]))
            {
                ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                                 "cassandra: unknown consistency '%s'", want);
                cass_disconnect(cc);
                return -1;
            }
            cass_cluster_set_consistency(cc->cluster, levels[i].level);
        }
    }
    cass_cluster_set_num_threads_io(cc->cluster, 1);
    cass_cluster_set_core_connections_per_host(cc->cluster, 1);
    /* Fail fast instead of retrying a host that is not there. */
    cass_cluster_set_constant_reconnect(cc->cluster, 500);
    if (cfg->user != NULL)
        cass_cluster_set_credentials(cc->cluster, cfg->user,
                                     cfg->password != NULL ? cfg->password
                                                           : "");
    if (cfg->tls)
    {
        cc->ssl = cass_ssl_new();
        cass_ssl_set_verify_flags(cc->ssl, cfg->tls_verify
                                  ? CASS_SSL_VERIFY_PEER_CERT |
                                    CASS_SSL_VERIFY_PEER_IDENTITY
                                  : CASS_SSL_VERIFY_NONE);
        if (cfg->tls_ca != NULL)
        {
            FILE  *fp = fopen(cfg->tls_ca, "r");
            char  *pem = NULL;
            size_t n = 0;

            if (fp != NULL)
            {
                pem = malloc(65536);
                if (pem != NULL)
                {
                    n = fread(pem, 1, 65535, fp);
                    pem[n] = '\0';
                }
                fclose(fp);
            }
            if (pem == NULL || n == 0 ||
                cass_ssl_add_trusted_cert(cc->ssl, pem) != CASS_OK)
            {
                free(pem);
                ta_nosql_err_set(err, TA_NOSQL_ERR_TLS, 0,
                                 "cassandra: cannot load the CA file %s",
                                 cfg->tls_ca);
                cass_disconnect(cc);
                return -1;
            }
            free(pem);
        }
        cass_cluster_set_ssl(cc->cluster, cc->ssl);
    }
    f = cass_session_connect(cc->session, cc->cluster);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK)
    {
        future_err(f, err);
        cass_future_free(f);
        cass_disconnect(cc);
        return -1;
    }
    cass_future_free(f);
    *dc = cc;
    return 0;
}

static void
cass_disconnect(void *dc)
{
    cass_conn *cc = dc;

    if (cc == NULL)
        return;
    if (cc->session != NULL)
    {
        CassFuture *f = cass_session_close(cc->session);

        cass_future_wait(f);
        cass_future_free(f);
        cass_session_free(cc->session);
    }
    if (cc->cluster != NULL)
        cass_cluster_free(cc->cluster);
    if (cc->ssl != NULL)
        cass_ssl_free(cc->ssl);
    json_decref(cc->ready);
    free(cc->ks);
    ta_nosql_conf_free(&cc->cfg);
    free(cc);
}

static char *
col_text(const CassRow *row, size_t i)
{
    const CassValue *v = cass_row_get_column(row, i);
    const char      *s;
    size_t           len;

    if (v == NULL || cass_value_is_null(v) ||
        cass_value_get_string(v, &s, &len) != CASS_OK)
        return NULL;
    return strndup(s, len);
}

static int
cass_ping(void *dc, ta_nosql_err *err)
{
    return run_simple(dc, "SELECT release_version FROM system.local", err);
}

static int
cass_info(void *dc, json_t *out, ta_nosql_err *err)
{
    const CassResult *r = run(dc, cass_statement_new(
        "SELECT release_version, cluster_name FROM system.local", 0), err);
    const CassRow    *row;

    if (r == NULL)
        return -1;
    row = cass_result_first_row(r);
    if (row != NULL)
    {
        char *ver = col_text(row, 0);
        char *cluster = col_text(row, 1);

        json_object_set_new(out, "server_version",
                            json_string(ver != NULL ? ver : "unknown"));
        if (cluster != NULL)
            json_object_set_new(out, "cluster", json_string(cluster));
        free(ver);
        free(cluster);
    }
    json_object_set_new(out, "keyspace", json_string(((cass_conn *)dc)->ks));
    cass_result_free(r);
    return 0;
}

static bool
cass_tls_in_use(void *dc)
{
    return ((cass_conn *)dc)->cfg.tls;
}

/* [applied] of a lightweight transaction's answer. */
static bool
lwt_applied(const CassResult *r)
{
    const CassRow *row = cass_result_first_row(r);
    cass_bool_t    b = cass_false;

    if (row != NULL)
        cass_value_get_bool(cass_row_get_column(row, 0), &b);
    return b == cass_true;
}

static int
cass_write(cass_conn *cc, const char *coll, const char *key, json_t *doc,
           long ttl_s, int mode, const char *if_ver, bool *applied,
           char **ver, ta_nosql_err *err)
{
    char             *json = ta_nosql_json_dump(doc);
    char             *nv = ta_nosql_new_version();
    char             *cql;
    char              ttl[48] = "";
    CassStatement    *st;
    const CassResult *r;

    if (ensure_tables(cc, coll, err) != 0)
    {
        free(json);
        free(nv);
        return -1;
    }
    if (ttl_s > 0)
        snprintf(ttl, sizeof(ttl), " USING TTL %ld", ttl_s);
    if (mode == 0)
    {
        cql = ta_nosql_strdupf("INSERT INTO \"%s\".\"%s\" (k, doc, v) VALUES "
                               "(?, ?, ?)%s", cc->ks, coll, ttl);
        st = cass_statement_new(cql, 3);
        cass_statement_bind_string(st, 0, key);
        cass_statement_bind_string(st, 1, json);
        cass_statement_bind_string(st, 2, nv);
    }
    else if (mode == 1)
    {
        cql = ta_nosql_strdupf("INSERT INTO \"%s\".\"%s\" (k, doc, v) VALUES "
                               "(?, ?, ?) IF NOT EXISTS%s", cc->ks, coll, ttl);
        st = cass_statement_new(cql, 3);
        cass_statement_bind_string(st, 0, key);
        cass_statement_bind_string(st, 1, json);
        cass_statement_bind_string(st, 2, nv);
    }
    else
    {
        cql = ta_nosql_strdupf("UPDATE \"%s\".\"%s\"%s SET doc = ?, v = ? "
                               "WHERE k = ? IF v = ?", cc->ks, coll, ttl);
        st = cass_statement_new(cql, 4);
        cass_statement_bind_string(st, 0, json);
        cass_statement_bind_string(st, 1, nv);
        cass_statement_bind_string(st, 2, key);
        cass_statement_bind_string(st, 3, if_ver);
    }
    free(cql);
    free(json);
    r = run(cc, st, err);
    if (r == NULL)
    {
        free(nv);
        return -1;
    }
    if (applied != NULL)
        *applied = lwt_applied(r);
    cass_result_free(r);
    if (ver != NULL && (applied == NULL || *applied))
        *ver = nv;
    else
        free(nv);
    return 0;
}

static int
cass_put(void *dc, const char *coll, const char *key, json_t *doc,
         long ttl_s, char **ver, ta_nosql_err *err)
{
    return cass_write(dc, coll, key, doc, ttl_s, 0, NULL, NULL, ver, err);
}

static int
cass_put_if(void *dc, const char *coll, const char *key, json_t *doc,
            long ttl_s, const char *if_ver, bool *applied, char **ver,
            ta_nosql_err *err)
{
    return cass_write(dc, coll, key, doc, ttl_s, if_ver == NULL ? 1 : 2,
                      if_ver, applied, ver, err);
}

static int
cass_get(void *dc, const char *coll, const char *key, json_t **doc,
         char **ver, ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    char             *cql = ta_nosql_strdupf("SELECT doc, v FROM \"%s\".\"%s\" "
                                             "WHERE k = ?", cc->ks, coll);
    CassStatement    *st = cass_statement_new(cql, 1);
    const CassResult *r;
    const CassRow    *row;
    int               rc = 0;

    free(cql);
    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    cass_statement_bind_string(st, 0, key);
    r = run(cc, st, err);
    if (r == NULL)
    {
        if (missing_schema(err))
        {
            memset(err, 0, sizeof(*err));
            return 0;
        }
        return -1;
    }
    row = cass_result_first_row(r);
    if (row != NULL)
    {
        char *json = col_text(row, 0);

        if (json != NULL)
        {
            *doc = ta_nosql_json_load(json, strlen(json), err);
            if (*doc == NULL)
            {
                err->cls = TA_NOSQL_ERR_PROTOCOL;
                rc = -1;
            }
            free(json);
            if (ver != NULL && rc == 0)
                *ver = col_text(row, 1);
        }
    }
    cass_result_free(r);
    return rc;
}

static int
cass_del(void *dc, const char *coll, const char *key, bool *deleted,
         ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    char             *cql = ta_nosql_strdupf("DELETE FROM \"%s\".\"%s\" WHERE "
                                             "k = ? IF EXISTS", cc->ks, coll);
    CassStatement    *st = cass_statement_new(cql, 1);
    const CassResult *r;

    free(cql);
    cass_statement_bind_string(st, 0, key);
    r = run(cc, st, err);
    if (r == NULL)
    {
        if (missing_schema(err))
        {
            memset(err, 0, sizeof(*err));
            *deleted = false;
            return 0;
        }
        return -1;
    }
    *deleted = lwt_applied(r);
    cass_result_free(r);
    return 0;
}

static int
cass_incr(void *dc, const char *coll, const char *key, long long by,
          long long *value, ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    char             *cql;
    CassStatement    *st;
    const CassResult *r;
    const CassRow    *row;

    if (ensure_tables(cc, coll, err) != 0)
        return -1;
    if (by != 0)
    {
        cql = ta_nosql_strdupf("UPDATE \"%s\".\"%s__n\" SET n = n + ? "
                               "WHERE k = ?", cc->ks, coll);
        st = cass_statement_new(cql, 2);
        free(cql);
        cass_statement_bind_int64(st, 0, by);
        cass_statement_bind_string(st, 1, key);
        r = run(cc, st, err);
        if (r == NULL)
            return -1;
        cass_result_free(r);
    }
    cql = ta_nosql_strdupf("SELECT n FROM \"%s\".\"%s__n\" WHERE k = ?",
                           cc->ks, coll);
    st = cass_statement_new(cql, 1);
    free(cql);
    cass_statement_bind_string(st, 0, key);
    r = run(cc, st, err);
    if (r == NULL)
        return -1;
    *value = 0;
    row = cass_result_first_row(r);
    if (row != NULL)
    {
        cass_int64_t n = 0;

        cass_value_get_int64(cass_row_get_column(row, 0), &n);
        *value = n;
    }
    cass_result_free(r);
    return 0;
}

static int
cass_scan(void *dc, const char *coll, int limit, const char *cursor,
          json_t *keys, char **next, ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    char             *cql = ta_nosql_strdupf("SELECT k FROM \"%s\".\"%s\"",
                                             cc->ks, coll);
    CassStatement    *st = cass_statement_new(cql, 0);
    const CassResult *r;
    CassIterator     *it;

    free(cql);
    *next = NULL;
    cass_statement_set_paging_size(st, limit);
    if (cursor != NULL)
    {
        size_t len;
        char  *state = ta_nosql_b64_decode(cursor, &len);

        if (state == NULL)
        {
            cass_statement_free(st);
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "cassandra: bad cursor");
            return -1;
        }
        cass_statement_set_paging_state_token(st, state, len);
        free(state);
    }
    r = run(cc, st, err);
    if (r == NULL)
    {
        if (missing_schema(err))
        {
            memset(err, 0, sizeof(*err));
            return 0;
        }
        return -1;
    }
    it = cass_iterator_from_result(r);
    while (cass_iterator_next(it))
    {
        char *k = col_text(cass_iterator_get_row(it), 0);

        if (k != NULL)
            json_array_append_new(keys, json_string(k));
        free(k);
    }
    cass_iterator_free(it);
    if (cass_result_has_more_pages(r))
    {
        const char *state;
        size_t      len;

        if (cass_result_paging_state_token(r, &state, &len) == CASS_OK)
            *next = ta_nosql_b64_encode(state, len);
    }
    cass_result_free(r);
    return 0;
}

static int
cass_count(void *dc, const char *coll, json_t *filter, long long *n,
           ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    char             *cql = ta_nosql_strdupf("SELECT COUNT(*) FROM "
                                             "\"%s\".\"%s\"", cc->ks, coll);
    const CassResult *r;
    const CassRow    *row;
    cass_int64_t      v = 0;

    (void)filter;
    r = run(cc, cass_statement_new(cql, 0), err);
    free(cql);
    if (r == NULL)
    {
        if (missing_schema(err))
        {
            memset(err, 0, sizeof(*err));
            *n = 0;
            return 0;
        }
        return -1;
    }
    row = cass_result_first_row(r);
    if (row != NULL)
        cass_value_get_int64(cass_row_get_column(row, 0), &v);
    *n = v;
    cass_result_free(r);
    return 0;
}

static int
cass_multi(void *dc, json_t *ops, const char *if_coll, const char *if_key,
           bool *applied, ta_nosql_err *err)
{
    cass_conn  *cc = dc;
    CassBatch  *batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    CassFuture *f;
    size_t      i;
    json_t     *op;
    int         rc = 0;

    (void)if_coll;
    (void)if_key;
    *applied = false;
    json_array_foreach(ops, i, op)
    {
        const char    *coll = ta_nosql_json_str(op, "coll", "");
        const char    *key = ta_nosql_json_str(op, "key", "");
        CassStatement *st;
        char          *cql;

        if (ensure_tables(cc, coll, err) != 0)
        {
            cass_batch_free(batch);
            return -1;
        }
        if (strcmp(ta_nosql_json_str(op, "op", ""), "put") == 0)
        {
            char *json = ta_nosql_json_dump(json_object_get(op, "doc"));
            char *nv = ta_nosql_new_version();

            cql = ta_nosql_strdupf("INSERT INTO \"%s\".\"%s\" (k, doc, v) "
                                   "VALUES (?, ?, ?)", cc->ks, coll);
            st = cass_statement_new(cql, 3);
            cass_statement_bind_string(st, 0, key);
            cass_statement_bind_string(st, 1, json);
            cass_statement_bind_string(st, 2, nv);
            free(json);
            free(nv);
        }
        else
        {
            cql = ta_nosql_strdupf("DELETE FROM \"%s\".\"%s\" WHERE k = ?",
                                   cc->ks, coll);
            st = cass_statement_new(cql, 1);
            cass_statement_bind_string(st, 0, key);
        }
        free(cql);
        cass_batch_add_statement(batch, st);
        cass_statement_free(st);
    }
    cass_batch_set_request_timeout(batch, (cass_uint64_t)cc->cfg.timeout_ms);
    f = cass_session_execute_batch(cc->session, batch);
    cass_future_wait(f);
    if (cass_future_error_code(f) != CASS_OK)
    {
        future_err(f, err);
        rc = -1;
    }
    else
    {
        *applied = true;
    }
    cass_future_free(f);
    cass_batch_free(batch);
    return rc;
}

static int
cass_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    cass_conn *cc = dc;
    char      *cql;
    int        rc;

    cql = ta_nosql_strdupf("DROP TABLE IF EXISTS \"%s\".\"%s\"", cc->ks, coll);
    rc = run_simple(cc, cql, err);
    free(cql);
    if (rc == 0)
    {
        cql = ta_nosql_strdupf("DROP TABLE IF EXISTS \"%s\".\"%s__n\"",
                               cc->ks, coll);
        rc = run_simple(cc, cql, err);
        free(cql);
    }
    json_object_del(cc->ready, coll);
    if (rc != 0 && missing_schema(err))
    {
        memset(err, 0, sizeof(*err));
        rc = 0;
    }
    return rc;
}

static json_t *value_json(const CassValue *v);

static json_t *
collection_json(const CassValue *v, bool map)
{
    CassIterator *it = map ? cass_iterator_from_map(v)
                           : cass_iterator_from_collection(v);
    json_t       *out = map ? json_object() : json_array();

    if (it == NULL)
        return json_null();
    while (cass_iterator_next(it))
    {
        if (map)
        {
            json_t *k = value_json(cass_iterator_get_map_key(it));
            char   *ks = json_is_string(k) ? strdup(json_string_value(k))
                                           : ta_nosql_json_dump(k);

            json_object_set_new(out, ks, value_json(
                                    cass_iterator_get_map_value(it)));
            free(ks);
            json_decref(k);
        }
        else
        {
            json_array_append_new(out, value_json(cass_iterator_get_value(it)));
        }
    }
    cass_iterator_free(it);
    return out;
}

static json_t *
value_json(const CassValue *v)
{
    if (v == NULL || cass_value_is_null(v))
        return json_null();
    switch (cass_value_type(v))
    {
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR:
        {
            const char *s;
            size_t      len;

            cass_value_get_string(v, &s, &len);
            return json_stringn(s, len);
        }
        case CASS_VALUE_TYPE_INT:
        {
            cass_int32_t i;

            cass_value_get_int32(v, &i);
            return json_integer(i);
        }
        case CASS_VALUE_TYPE_SMALL_INT:
        {
            cass_int16_t i;

            cass_value_get_int16(v, &i);
            return json_integer(i);
        }
        case CASS_VALUE_TYPE_TINY_INT:
        {
            cass_int8_t i;

            cass_value_get_int8(v, &i);
            return json_integer(i);
        }
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_TIMESTAMP:
        {
            cass_int64_t i;

            cass_value_get_int64(v, &i);
            return json_integer(i);
        }
        case CASS_VALUE_TYPE_DOUBLE:
        {
            cass_double_t d;

            cass_value_get_double(v, &d);
            return json_real(d);
        }
        case CASS_VALUE_TYPE_FLOAT:
        {
            cass_float_t d;

            cass_value_get_float(v, &d);
            return json_real(d);
        }
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t b;

            cass_value_get_bool(v, &b);
            return json_boolean(b == cass_true);
        }
        case CASS_VALUE_TYPE_UUID:
        case CASS_VALUE_TYPE_TIMEUUID:
        {
            CassUuid u;
            char     s[CASS_UUID_STRING_LENGTH];

            cass_value_get_uuid(v, &u);
            cass_uuid_string(u, s);
            return json_string(s);
        }
        case CASS_VALUE_TYPE_INET:
        {
            CassInet i;
            char     s[CASS_INET_STRING_LENGTH];

            cass_value_get_inet(v, &i);
            cass_inet_string(i, s);
            return json_string(s);
        }
        case CASS_VALUE_TYPE_BLOB:
        {
            const cass_byte_t *b;
            size_t             len;
            char              *s;
            json_t            *o;

            cass_value_get_bytes(v, &b, &len);
            s = ta_nosql_b64_encode(b, len);
            o = json_string(s != NULL ? s : "");
            free(s);
            return o;
        }
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
            return collection_json(v, false);
        case CASS_VALUE_TYPE_MAP:
            return collection_json(v, true);
        default:
            return json_string("<unsupported type>");
    }
}

static int
cass_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    const char       *cql = ta_nosql_json_str(cmd, "cql", NULL);
    json_t           *params = json_object_get(cmd, "params");
    size_t            np = json_array_size(params);
    CassStatement    *st;
    const CassResult *r;
    CassIterator     *it;
    json_t           *cols = json_array();
    json_t           *rows = json_array();
    size_t            i;
    size_t            ncol;

    if (cql == NULL)
    {
        json_decref(cols);
        json_decref(rows);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "cassandra: 'cmd' needs 'cql'");
        return -1;
    }
    st = cass_statement_new(cql, np);
    for (i = 0; i < np; i++)
    {
        json_t *p = json_array_get(params, i);

        if (json_is_string(p))
            cass_statement_bind_string(st, i, json_string_value(p));
        else if (json_is_integer(p))
            cass_statement_bind_int64(st, i, json_integer_value(p));
        else if (json_is_real(p))
            cass_statement_bind_double(st, i, json_real_value(p));
        else if (json_is_boolean(p))
            cass_statement_bind_bool(st, i, json_is_true(p) ? cass_true
                                                            : cass_false);
        else
            cass_statement_bind_null(st, i);
    }
    r = run(cc, st, err);
    if (r == NULL)
    {
        json_decref(cols);
        json_decref(rows);
        return -1;
    }
    ncol = cass_result_column_count(r);
    for (i = 0; i < ncol; i++)
    {
        const char *name;
        size_t      len;

        cass_result_column_name(r, i, &name, &len);
        json_array_append_new(cols, json_stringn(name, len));
    }
    it = cass_iterator_from_result(r);
    while (cass_iterator_next(it))
    {
        const CassRow *row = cass_iterator_get_row(it);
        json_t        *jr = json_array();

        for (i = 0; i < ncol; i++)
            json_array_append_new(jr, value_json(cass_row_get_column(row, i)));
        json_array_append_new(rows, jr);
    }
    cass_iterator_free(it);
    cass_result_free(r);
    *reply = json_pack("{s:o, s:o}", "columns", cols, "rows", rows);
    return 0;
}

static int
cass_bulk(void *dc, const char *coll, json_t *items, ta_nosql_err *err)
{
    cass_conn   *cc = dc;
    char        *cql;
    size_t       i;
    size_t       n = json_array_size(items);
    CassFuture **fs;
    json_t      *it;
    int          rc = 0;

    if (ensure_tables(cc, coll, err) != 0)
        return -1;
    cql = ta_nosql_strdupf("INSERT INTO \"%s\".\"%s\" (k, doc, v) VALUES "
                           "(?, ?, ?)", cc->ks, coll);
    fs = calloc(n, sizeof(*fs));
    if (fs == NULL)
    {
        free(cql);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    /* Concurrent single-partition inserts: the way to load Cassandra
     * (a batch across partitions is slower, not faster). */
    json_array_foreach(items, i, it)
    {
        CassStatement *st = cass_statement_new(cql, 3);
        char          *json = ta_nosql_json_dump(json_object_get(it, "doc"));
        char          *nv = ta_nosql_new_version();

        cass_statement_bind_string(st, 0,
                                   json_string_value(json_object_get(it,
                                                                     "key")));
        cass_statement_bind_string(st, 1, json);
        cass_statement_bind_string(st, 2, nv);
        cass_statement_set_request_timeout(st,
                                           (cass_uint64_t)cc->cfg.timeout_ms);
        fs[i] = cass_session_execute(cc->session, st);
        cass_statement_free(st);
        free(json);
        free(nv);
    }
    for (i = 0; i < n; i++)
    {
        cass_future_wait(fs[i]);
        if (cass_future_error_code(fs[i]) != CASS_OK && rc == 0)
        {
            future_err(fs[i], err);
            rc = -1;
        }
        cass_future_free(fs[i]);
    }
    free(fs);
    free(cql);
    return rc;
}

static int
cass_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    (void)err;
    ((cass_conn *)dc)->cfg.timeout_ms = ms;
    return 0;
}

static int
cass_audit(const ta_nosql_conf *cfg, void *dc, json_t *args,
           json_t *findings, json_t *checked, ta_nosql_err *err)
{
    cass_conn        *cc = dc;
    CassStatement    *st;
    const CassResult *r;
    const CassRow    *row;
    char              subject[300];
    ta_nosql_err      cerr;

    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    json_array_append_new(checked, json_string("cassandra.superuser"));
    if (cc == NULL || cfg->user == NULL)
        return 0;
    memset(&cerr, 0, sizeof(cerr));
    st = cass_statement_new("SELECT is_superuser FROM system_auth.roles "
                            "WHERE role = ?", 1);
    cass_statement_bind_string(st, 0, cfg->user);
    r = run(cc, st, &cerr);
    if (r == NULL)
        return 0;
    row = cass_result_first_row(r);
    if (row != NULL)
    {
        cass_bool_t b = cass_false;

        cass_value_get_bool(cass_row_get_column(row, 0), &b);
        if (b == cass_true)
            ta_nosql_finding(findings, "cassandra.superuser",
                             TA_NOSQL_SEV_MEDIUM, subject,
                             "the account '%s' is a superuser", cfg->user);
    }
    cass_result_free(r);
    return 0;
}

static const char *const cass_default_creds[] = {
    "cassandra:cassandra", NULL
};

const ta_nosql_driver ta_nosql_cassandra_driver = {
    .engine = TA_NOSQL_ENGINE_CASSANDRA,
    .name = "cassandra",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_COUNT | TA_NOSQL_CAP_TTL |
            TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT | TA_NOSQL_CAP_INCR |
            TA_NOSQL_CAP_MULTI | TA_NOSQL_CAP_DURABLE | TA_NOSQL_CAP_TLS |
            TA_NOSQL_CAP_AUTH,
    .default_port = 9042,
    .default_creds = cass_default_creds,
    .client_version = cass_client_version,
    .connect = cass_connect,
    .disconnect = cass_disconnect,
    .ping = cass_ping,
    .info = cass_info,
    .tls_in_use = cass_tls_in_use,
    .put = cass_put,
    .put_if = cass_put_if,
    .get = cass_get,
    .del = cass_del,
    .incr = cass_incr,
    .scan = cass_scan,
    .count = cass_count,
    .multi = cass_multi,
    .drop = cass_drop,
    .native = cass_native,
    .bulk = cass_bulk,
    .set_timeout = cass_set_timeout,
    .audit = cass_audit,
};
