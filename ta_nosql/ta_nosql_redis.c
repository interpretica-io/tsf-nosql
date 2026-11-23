/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: Redis over hiredis
 *
 * A document is a string value "<version>\n<json>" under the key
 * "<coll>:<key>": SET replaces it and its expiry in one command, SET NX
 * is the write-if-absent, and a compare-and-set is a server-side Lua
 * script that compares the version prefix, so nothing races. A counter
 * is a plain integer string under the same kind of key (INCRBY).
 *
 * A connection that failed (a timeout, the server gone) is reconnected,
 * authenticated and pointed at its database again before the next
 * command, so one failure does not kill a handle.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <hiredis/hiredis.h>
#ifdef TA_NOSQL_HAVE_REDIS_SSL
#include <hiredis/hiredis_ssl.h>
#endif

#include "ta_nosql_int.h"

typedef struct redis_conn {
    redisContext  *ctx;
    ta_nosql_conf  cfg;
    bool           tls;
#ifdef TA_NOSQL_HAVE_REDIS_SSL
    redisSSLContext *ssl;
#endif
    int            db;
} redis_conn;

/* Compare-and-set: KEYS[1] key, ARGV[1] expected version, ARGV[2] new
 * value, ARGV[3] TTL (0: none). 1 if written. */
static const char cas_script[] =
    "local cur = redis.call('GET', KEYS[1]) "
    "if not cur then return 0 end "
    "local nl = string.find(cur, '\\n', 1, true) "
    "if not nl or string.sub(cur, 1, nl - 1) ~= ARGV[1] then return 0 end "
    "if tonumber(ARGV[3]) > 0 then "
    "  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]) "
    "else redis.call('SET', KEYS[1], ARGV[2]) end "
    "return 1";

static void
redis_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "hiredis %d.%d.%d%s", HIREDIS_MAJOR, HIREDIS_MINOR,
             HIREDIS_PATCH,
#ifdef TA_NOSQL_HAVE_REDIS_SSL
             " (TLS)"
#else
             ""
#endif
             );
}

static ta_nosql_errcls
reply_cls(const char *msg)
{
    if (strncmp(msg, "NOAUTH", 6) == 0 || strncmp(msg, "WRONGPASS", 9) == 0 ||
        strstr(msg, "invalid password") != NULL ||
        strstr(msg, "invalid username-password") != NULL)
        return TA_NOSQL_ERR_AUTH;
    if (strncmp(msg, "NOPERM", 6) == 0)
        return TA_NOSQL_ERR_PERMISSION;
    if (strncmp(msg, "LOADING", 7) == 0 || strncmp(msg, "BUSY", 4) == 0 ||
        strncmp(msg, "MASTERDOWN", 10) == 0 ||
        strncmp(msg, "CLUSTERDOWN", 11) == 0)
        return TA_NOSQL_ERR_UNAVAILABLE;
    if (strncmp(msg, "OOM", 3) == 0)
        return TA_NOSQL_ERR_LIMIT;
    if (strncmp(msg, "READONLY", 8) == 0)
        return TA_NOSQL_ERR_PERMISSION;
    if (strstr(msg, "unknown command") != NULL)
        return TA_NOSQL_ERR_UNSUPPORTED;
    return TA_NOSQL_ERR_INVALID;
}

static void
ctx_err(redisContext *c, ta_nosql_err *err)
{
    ta_nosql_errcls cls = TA_NOSQL_ERR_CONNECT;

    switch (c->err)
    {
        case REDIS_ERR_IO:
            cls = (errno == EAGAIN || errno == ETIMEDOUT ||
                   errno == EWOULDBLOCK) ? TA_NOSQL_ERR_TIMEOUT
                                         : TA_NOSQL_ERR_CONNECT;
            break;
#ifdef REDIS_ERR_TIMEOUT
        case REDIS_ERR_TIMEOUT:
            cls = TA_NOSQL_ERR_TIMEOUT;
            break;
#endif
        case REDIS_ERR_EOF:
            cls = TA_NOSQL_ERR_CONNECT;
            break;
        case REDIS_ERR_PROTOCOL:
            cls = TA_NOSQL_ERR_PROTOCOL;
            break;
        default:
            break;
    }
    ta_nosql_err_set(err, cls, c->err, "redis: %s", c->errstr);
}

/*
 * Run a command; NULL with @p err on any failure, including an error
 * reply. The caller frees the reply.
 */
static redisReply *
cmd_argv(redis_conn *rc, int argc, const char **argv, const size_t *lens,
         ta_nosql_err *err);

static int
session_setup(redis_conn *rc, ta_nosql_err *err)
{
    redisReply *r;

    if (rc->cfg.password != NULL || rc->cfg.user != NULL)
    {
        const char *argv[3];
        size_t      lens[3];
        int         argc = 0;

        argv[argc] = "AUTH";
        lens[argc++] = 4;
        if (rc->cfg.user != NULL)
        {
            argv[argc] = rc->cfg.user;
            lens[argc++] = strlen(rc->cfg.user);
        }
        argv[argc] = rc->cfg.password != NULL ? rc->cfg.password : "";
        lens[argc] = strlen(argv[argc]);
        argc++;
        r = redisCommandArgv(rc->ctx, argc, argv, lens);
        if (r == NULL)
        {
            ctx_err(rc->ctx, err);
            return -1;
        }
        if (r->type == REDIS_REPLY_ERROR)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_AUTH, 0, "redis: %s", r->str);
            freeReplyObject(r);
            return -1;
        }
        freeReplyObject(r);
    }
    if (rc->db != 0)
    {
        r = redisCommand(rc->ctx, "SELECT %d", rc->db);
        if (r == NULL)
        {
            ctx_err(rc->ctx, err);
            return -1;
        }
        if (r->type == REDIS_REPLY_ERROR)
        {
            ta_nosql_err_set(err, reply_cls(r->str), 0, "redis: %s", r->str);
            freeReplyObject(r);
            return -1;
        }
        freeReplyObject(r);
    }
    return 0;
}

static int
open_ctx(redis_conn *rc, ta_nosql_err *err)
{
    redisOptions   opt;
    struct timeval cto = {
        .tv_sec = rc->cfg.connect_timeout_ms / 1000,
        .tv_usec = (rc->cfg.connect_timeout_ms % 1000) * 1000,
    };
    struct timeval to = {
        .tv_sec = rc->cfg.timeout_ms / 1000,
        .tv_usec = (rc->cfg.timeout_ms % 1000) * 1000,
    };

    memset(&opt, 0, sizeof(opt));
    REDIS_OPTIONS_SET_TCP(&opt, rc->cfg.host, rc->cfg.port);
    opt.connect_timeout = &cto;
    opt.command_timeout = &to;
    rc->ctx = redisConnectWithOptions(&opt);
    if (rc->ctx == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0,
                         "redis: cannot allocate a context");
        return -1;
    }
    if (rc->ctx->err != 0)
    {
        ctx_err(rc->ctx, err);
        return -1;
    }
#ifdef TA_NOSQL_HAVE_REDIS_SSL
    if (rc->cfg.tls)
    {
        if (redisInitiateSSLWithContext(rc->ctx, rc->ssl) != REDIS_OK)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_TLS, rc->ctx->err,
                             "redis: TLS: %s", rc->ctx->errstr);
            return -1;
        }
        rc->tls = true;
    }
#endif
    return session_setup(rc, err);
}

/* Reconnect a context a failure left unusable. */
static int
ensure(redis_conn *rc, ta_nosql_err *err)
{
    if (rc->ctx != NULL && rc->ctx->err == 0)
        return 0;
    if (rc->ctx != NULL)
        redisFree(rc->ctx);
    rc->ctx = NULL;
    return open_ctx(rc, err);
}

static redisReply *
cmd_argv(redis_conn *rc, int argc, const char **argv, const size_t *lens,
         ta_nosql_err *err)
{
    redisReply *r;

    if (ensure(rc, err) != 0)
        return NULL;
    r = redisCommandArgv(rc->ctx, argc, argv, lens);
    if (r == NULL)
    {
        ctx_err(rc->ctx, err);
        return NULL;
    }
    if (r->type == REDIS_REPLY_ERROR)
    {
        ta_nosql_err_set(err, reply_cls(r->str), 0, "redis: %s", r->str);
        freeReplyObject(r);
        return NULL;
    }
    return r;
}

static char *
full_key(const char *coll, const char *key)
{
    return ta_nosql_strdupf("%s:%s", coll, key);
}

static void redis_disconnect(void *dc);

static int
redis_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    redis_conn *rc = calloc(1, sizeof(*rc));

    if (rc == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &rc->cfg);
    if (cfg->database != NULL)
        rc->db = atoi(cfg->database);
#ifdef TA_NOSQL_HAVE_REDIS_SSL
    if (cfg->tls)
    {
        redisSSLContextError serr = REDIS_SSL_CTX_NONE;
        redisSSLOptions      sopt;

        memset(&sopt, 0, sizeof(sopt));
        sopt.cacert_filename = cfg->tls_ca;
        sopt.server_name = ta_nosql_json_str(cfg->raw, "tls_server_name",
                                             cfg->host);
        sopt.verify_mode = cfg->tls_verify ? REDIS_SSL_VERIFY_PEER
                                           : REDIS_SSL_VERIFY_NONE;
        rc->ssl = redisCreateSSLContextWithOptions(&sopt, &serr);
        if (rc->ssl == NULL)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_TLS, serr, "redis: TLS: %s",
                             redisSSLContextGetError(serr));
            redis_disconnect(rc);
            return -1;
        }
    }
#endif
    if (open_ctx(rc, err) != 0)
    {
        redis_disconnect(rc);
        return -1;
    }
    *dc = rc;
    return 0;
}

static void
redis_disconnect(void *dc)
{
    redis_conn *rc = dc;

    if (rc == NULL)
        return;
    if (rc->ctx != NULL)
        redisFree(rc->ctx);
#ifdef TA_NOSQL_HAVE_REDIS_SSL
    if (rc->ssl != NULL)
        redisFreeSSLContext(rc->ssl);
#endif
    ta_nosql_conf_free(&rc->cfg);
    free(rc);
}

static int
redis_ping(void *dc, ta_nosql_err *err)
{
    const char  *argv[] = { "PING" };
    const size_t lens[] = { 4 };
    redisReply *r = cmd_argv(dc, 1, argv, lens, err);

    if (r == NULL)
        return -1;
    freeReplyObject(r);
    return 0;
}

/* A field of INFO's "key:value" lines. */
static char *
info_field(const char *info, const char *name)
{
    size_t      n = strlen(name);
    const char *p = info;

    while (p != NULL && *p != '\0')
    {
        if (strncmp(p, name, n) == 0 && p[n] == ':')
        {
            const char *e = strpbrk(p + n + 1, "\r\n");

            return strndup(p + n + 1, e != NULL ? (size_t)(e - (p + n + 1))
                                                : strlen(p + n + 1));
        }
        p = strchr(p, '\n');
        if (p != NULL)
            p++;
    }
    return NULL;
}

static int
redis_info(void *dc, json_t *out, ta_nosql_err *err)
{
    const char  *argv[] = { "INFO", "server" };
    const size_t lens[] = { 4, 6 };
    redisReply *r = cmd_argv(dc, 2, argv, lens, err);
    char       *v;

    if (r == NULL)
        return -1;
    if (r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_VERB)
    {
        v = info_field(r->str, "redis_version");
        json_object_set_new(out, "server_version",
                            json_string(v != NULL ? v : "unknown"));
        free(v);
        v = info_field(r->str, "redis_mode");
        if (v != NULL)
            json_object_set_new(out, "mode", json_string(v));
        free(v);
    }
    freeReplyObject(r);
    return 0;
}

static bool
redis_tls_in_use(void *dc)
{
    return ((redis_conn *)dc)->tls;
}

static char *
encode_value(const char *ver, json_t *doc)
{
    char *json = ta_nosql_json_dump(doc);
    char *v;

    if (json == NULL)
        return NULL;
    v = ta_nosql_strdupf("%s\n%s", ver, json);
    free(json);
    return v;
}

static int
redis_put_common(redis_conn *rc, const char *coll, const char *key,
                 json_t *doc, long ttl_s, bool nx, bool *applied, char **ver,
                 ta_nosql_err *err)
{
    char       *k = full_key(coll, key);
    char       *nv = ta_nosql_new_version();
    char       *val = encode_value(nv, doc);
    char        ttl[32];
    const char *argv[6];
    size_t      lens[6];
    int         argc = 0;
    redisReply *r;

    argv[argc] = "SET";
    lens[argc++] = 3;
    argv[argc] = k;
    lens[argc++] = strlen(k);
    argv[argc] = val;
    lens[argc++] = strlen(val);
    if (ttl_s > 0)
    {
        snprintf(ttl, sizeof(ttl), "%ld", ttl_s);
        argv[argc] = "EX";
        lens[argc++] = 2;
        argv[argc] = ttl;
        lens[argc++] = strlen(ttl);
    }
    if (nx)
    {
        argv[argc] = "NX";
        lens[argc++] = 2;
    }
    r = cmd_argv(rc, argc, argv, lens, err);
    free(k);
    free(val);
    if (r == NULL)
    {
        free(nv);
        return -1;
    }
    if (applied != NULL)
        *applied = r->type != REDIS_REPLY_NIL;
    freeReplyObject(r);
    if (ver != NULL && (applied == NULL || *applied))
        *ver = nv;
    else
        free(nv);
    return 0;
}

static int
redis_put(void *dc, const char *coll, const char *key, json_t *doc,
          long ttl_s, char **ver, ta_nosql_err *err)
{
    return redis_put_common(dc, coll, key, doc, ttl_s, false, NULL, ver, err);
}

static int
redis_put_if(void *dc, const char *coll, const char *key, json_t *doc,
             long ttl_s, const char *if_ver, bool *applied, char **ver,
             ta_nosql_err *err)
{
    redis_conn *rc = dc;
    char       *k;
    char       *nv;
    char       *val;
    char        ttl[32];
    const char *argv[7];
    size_t      lens[7];
    redisReply *r;

    if (if_ver == NULL)
        return redis_put_common(rc, coll, key, doc, ttl_s, true, applied, ver,
                                err);

    k = full_key(coll, key);
    nv = ta_nosql_new_version();
    val = encode_value(nv, doc);
    snprintf(ttl, sizeof(ttl), "%ld", ttl_s > 0 ? ttl_s : 0L);
    argv[0] = "EVAL";
    argv[1] = cas_script;
    argv[2] = "1";
    argv[3] = k;
    argv[4] = if_ver;
    argv[5] = val;
    argv[6] = ttl;
    for (int i = 0; i < 7; i++)
        lens[i] = strlen(argv[i]);
    r = cmd_argv(rc, 7, argv, lens, err);
    free(k);
    free(val);
    if (r == NULL)
    {
        free(nv);
        return -1;
    }
    *applied = r->type == REDIS_REPLY_INTEGER && r->integer == 1;
    freeReplyObject(r);
    if (*applied)
        *ver = nv;
    else
        free(nv);
    return 0;
}

/* Split "<version>\n<json>". */
static int
decode_value(const char *s, size_t len, json_t **doc, char **ver,
             ta_nosql_err *err)
{
    const char *nl = memchr(s, '\n', len);
    json_t     *d;

    if (nl == NULL)
    {
        /* Not written by this module (a counter, a native SET). */
        d = ta_nosql_json_load(s, len, err);
        if (d == NULL)
            d = json_stringn(s, len);
        *doc = d;
        if (ver != NULL)
            *ver = NULL;
        return 0;
    }
    d = ta_nosql_json_load(nl + 1, len - (size_t)(nl + 1 - s), err);
    if (d == NULL)
    {
        err->cls = TA_NOSQL_ERR_PROTOCOL;
        return -1;
    }
    *doc = d;
    if (ver != NULL)
        *ver = strndup(s, (size_t)(nl - s));
    return 0;
}

static int
redis_get(void *dc, const char *coll, const char *key, json_t **doc,
          char **ver, ta_nosql_err *err)
{
    char       *k = full_key(coll, key);
    const char  *argv[] = { "GET", k };
    const size_t lens[] = { 3, strlen(k) };
    redisReply *r = cmd_argv(dc, 2, argv, lens, err);
    int         rc = 0;

    free(k);
    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    if (r == NULL)
        return -1;
    if (r->type == REDIS_REPLY_STRING)
        rc = decode_value(r->str, r->len, doc, ver, err);
    freeReplyObject(r);
    return rc;
}

static int
redis_del(void *dc, const char *coll, const char *key, bool *deleted,
          ta_nosql_err *err)
{
    char       *k = full_key(coll, key);
    const char  *argv[] = { "DEL", k };
    const size_t lens[] = { 3, strlen(k) };
    redisReply *r = cmd_argv(dc, 2, argv, lens, err);

    free(k);
    if (r == NULL)
        return -1;
    *deleted = r->type == REDIS_REPLY_INTEGER && r->integer > 0;
    freeReplyObject(r);
    return 0;
}

static int
redis_incr(void *dc, const char *coll, const char *key, long long by,
           long long *value, ta_nosql_err *err)
{
    char       *k = full_key(coll, key);
    char        num[32];
    const char *argv[3];
    size_t      lens[3];
    redisReply *r;

    snprintf(num, sizeof(num), "%lld", by);
    argv[0] = "INCRBY";
    argv[1] = k;
    argv[2] = num;
    lens[0] = 6;
    lens[1] = strlen(k);
    lens[2] = strlen(num);
    r = cmd_argv(dc, 3, argv, lens, err);
    free(k);
    if (r == NULL)
        return -1;
    *value = r->integer;
    freeReplyObject(r);
    return 0;
}

static int
redis_scan(void *dc, const char *coll, int limit, const char *cursor,
           json_t *keys, char **next, ta_nosql_err *err)
{
    char       *match = ta_nosql_strdupf("%s:*", coll);
    char        count[16];
    char        cur[32];
    size_t      plen = strlen(coll) + 1;
    int         rounds;

    snprintf(count, sizeof(count), "%d", limit);
    snprintf(cur, sizeof(cur), "%s", cursor != NULL ? cursor : "0");
    *next = NULL;
    /*
     * SCAN may answer a round with nothing while the cursor moves on;
     * go on until there is something to return or the scan is over.
     * COUNT is a hint: a page can be a little larger than the limit.
     */
    for (rounds = 0; rounds < 10000; rounds++)
    {
        const char *argv[] = { "SCAN", cur, "MATCH", match, "COUNT", count };
        size_t      lens[6];
        redisReply *r;
        size_t      i;
        int         j;

        for (j = 0; j < 6; j++)
            lens[j] = strlen(argv[j]);
        r = cmd_argv(dc, 6, argv, lens, err);
        if (r == NULL)
        {
            free(match);
            return -1;
        }
        if (r->type != REDIS_REPLY_ARRAY || r->elements != 2)
        {
            freeReplyObject(r);
            free(match);
            ta_nosql_err_set(err, TA_NOSQL_ERR_PROTOCOL, 0,
                             "redis: unexpected SCAN reply");
            return -1;
        }
        snprintf(cur, sizeof(cur), "%s", r->element[0]->str);
        for (i = 0; i < r->element[1]->elements; i++)
        {
            redisReply *e = r->element[1]->element[i];

            if (e->len > plen)
                json_array_append_new(keys, json_stringn(e->str + plen,
                                                         e->len - plen));
        }
        freeReplyObject(r);
        if (strcmp(cur, "0") == 0 || json_array_size(keys) > 0)
            break;
    }
    free(match);
    if (strcmp(cur, "0") != 0)
        *next = strdup(cur);
    return 0;
}

static int
redis_multi(void *dc, json_t *ops, const char *if_coll, const char *if_key,
            bool *applied, ta_nosql_err *err)
{
    redis_conn *rc = dc;
    redisReply *r;
    char       *wk = NULL;
    size_t      i;
    json_t     *op;
    int         queued = 0;

    if (ensure(rc, err) != 0)
        return -1;
    *applied = false;
    if (if_coll != NULL)
    {
        const char *argv[2];
        size_t      lens[2];

        wk = full_key(if_coll, if_key);
        argv[0] = "WATCH";
        argv[1] = wk;
        lens[0] = 5;
        lens[1] = strlen(wk);
        r = cmd_argv(rc, 2, argv, lens, err);
        if (r == NULL)
            goto fail;
        freeReplyObject(r);
        argv[0] = "EXISTS";
        lens[0] = 6;
        r = cmd_argv(rc, 2, argv, lens, err);
        if (r == NULL)
            goto fail;
        if (r->integer > 0)
        {
            freeReplyObject(r);
            r = redisCommand(rc->ctx, "UNWATCH");
            if (r != NULL)
                freeReplyObject(r);
            free(wk);
            return 0;
        }
        freeReplyObject(r);
    }
    r = redisCommand(rc->ctx, "MULTI");
    if (r == NULL)
    {
        ctx_err(rc->ctx, err);
        goto fail;
    }
    freeReplyObject(r);
    json_array_foreach(ops, i, op)
    {
        const char *kind = ta_nosql_json_str(op, "op", "");
        char       *k = full_key(ta_nosql_json_str(op, "coll", ""),
                                 ta_nosql_json_str(op, "key", ""));

        if (strcmp(kind, "put") == 0)
        {
            char *nv = ta_nosql_new_version();
            char *val = encode_value(nv, json_object_get(op, "doc"));

            redisAppendCommand(rc->ctx, "SET %s %s", k, val);
            free(nv);
            free(val);
        }
        else
        {
            redisAppendCommand(rc->ctx, "DEL %s", k);
        }
        free(k);
        queued++;
    }
    redisAppendCommand(rc->ctx, "EXEC");
    for (int q = 0; q < queued; q++)
    {
        if (redisGetReply(rc->ctx, (void **)&r) != REDIS_OK)
        {
            ctx_err(rc->ctx, err);
            goto fail;
        }
        freeReplyObject(r);
    }
    if (redisGetReply(rc->ctx, (void **)&r) != REDIS_OK)
    {
        ctx_err(rc->ctx, err);
        goto fail;
    }
    if (r->type == REDIS_REPLY_ERROR)
    {
        ta_nosql_err_set(err, reply_cls(r->str), 0, "redis: %s", r->str);
        freeReplyObject(r);
        goto fail;
    }
    /* A nil EXEC: the watched key changed between the check and now. */
    *applied = r->type == REDIS_REPLY_ARRAY;
    freeReplyObject(r);
    free(wk);
    return 0;

fail:
    free(wk);
    return -1;
}

static int
redis_drop(void *dc, const char *coll, ta_nosql_err *err)
{
    char   *cursor = NULL;
    json_t *keys;

    do {
        size_t  i;
        json_t *k;
        char   *next = NULL;

        keys = json_array();
        if (redis_scan(dc, coll, 1000, cursor, keys, &next, err) != 0)
        {
            json_decref(keys);
            free(cursor);
            return -1;
        }
        free(cursor);
        cursor = next;
        json_array_foreach(keys, i, k)
        {
            bool deleted;

            if (redis_del(dc, coll, json_string_value(k), &deleted, err) != 0)
            {
                json_decref(keys);
                free(cursor);
                return -1;
            }
        }
        json_decref(keys);
    } while (cursor != NULL);
    return 0;
}

static json_t *
reply_json(redisReply *r)
{
    json_t *a;
    size_t  i;

    switch (r->type)
    {
        case REDIS_REPLY_STRING:
        case REDIS_REPLY_STATUS:
        case REDIS_REPLY_VERB:
        case REDIS_REPLY_BIGNUM:
            return json_stringn(r->str, r->len);
        case REDIS_REPLY_INTEGER:
            return json_integer(r->integer);
        case REDIS_REPLY_DOUBLE:
            return json_real(r->dval);
        case REDIS_REPLY_BOOL:
            return json_boolean(r->integer != 0);
        case REDIS_REPLY_NIL:
            return json_null();
        case REDIS_REPLY_MAP:
            a = json_object();
            for (i = 0; i + 1 < r->elements; i += 2)
            {
                char *k = r->element[i]->str != NULL
                          ? strndup(r->element[i]->str, r->element[i]->len)
                          : ta_nosql_strdupf("%lld", r->element[i]->integer);

                json_object_set_new(a, k, reply_json(r->element[i + 1]));
                free(k);
            }
            return a;
        case REDIS_REPLY_ARRAY:
        case REDIS_REPLY_SET:
        case REDIS_REPLY_PUSH:
            a = json_array();
            for (i = 0; i < r->elements; i++)
                json_array_append_new(a, reply_json(r->element[i]));
            return a;
        default:
            return json_null();
    }
}

static int
redis_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    json_t      *argv_j = json_object_get(cmd, "argv");
    size_t       n = json_array_size(argv_j);
    const char **argv;
    size_t      *lens;
    char       **own;
    redisReply  *r;
    size_t       i;

    if (!json_is_array(argv_j) || n == 0 || n > 4096)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "redis: 'cmd' needs 'argv', an array of 1 to 4096 "
                         "words");
        return -1;
    }
    argv = calloc(n, sizeof(*argv));
    lens = calloc(n, sizeof(*lens));
    own = calloc(n, sizeof(*own));
    for (i = 0; i < n; i++)
    {
        json_t *w = json_array_get(argv_j, i);

        if (json_is_string(w))
        {
            argv[i] = json_string_value(w);
            lens[i] = json_string_length(w);
        }
        else
        {
            own[i] = ta_nosql_json_dump(w);
            argv[i] = own[i];
            lens[i] = strlen(own[i]);
        }
    }
    r = cmd_argv(dc, (int)n, argv, lens, err);
    for (i = 0; i < n; i++)
        free(own[i]);
    free(own);
    free(argv);
    free(lens);
    if (r == NULL)
        return -1;
    *reply = reply_json(r);
    freeReplyObject(r);
    return 0;
}

static int
redis_bulk(void *dc, const char *coll, json_t *items, ta_nosql_err *err)
{
    redis_conn *rc = dc;
    size_t      i;
    json_t     *it;
    size_t      n = json_array_size(items);
    int         rcode = 0;

    if (ensure(rc, err) != 0)
        return -1;
    json_array_foreach(items, i, it)
    {
        char *k = full_key(coll, json_string_value(json_object_get(it, "key")));
        char *nv = ta_nosql_new_version();
        char *val = encode_value(nv, json_object_get(it, "doc"));

        redisAppendCommand(rc->ctx, "SET %s %s", k, val);
        free(k);
        free(nv);
        free(val);
    }
    for (i = 0; i < n; i++)
    {
        redisReply *r;

        if (redisGetReply(rc->ctx, (void **)&r) != REDIS_OK)
        {
            ctx_err(rc->ctx, err);
            return -1;
        }
        if (r->type == REDIS_REPLY_ERROR && rcode == 0)
        {
            ta_nosql_err_set(err, reply_cls(r->str), 0, "redis: %s", r->str);
            rcode = -1;
        }
        freeReplyObject(r);
    }
    return rcode;
}

static int
redis_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    redis_conn    *rc = dc;
    struct timeval tv = { .tv_sec = ms / 1000, .tv_usec = (ms % 1000) * 1000 };

    rc->cfg.timeout_ms = ms;
    if (ensure(rc, err) != 0)
        return -1;
    if (redisSetTimeout(rc->ctx, tv) != REDIS_OK)
    {
        ctx_err(rc->ctx, err);
        return -1;
    }
    return 0;
}

static int
redis_audit(const ta_nosql_conf *cfg, void *dc, json_t *args,
            json_t *findings, json_t *checked, ta_nosql_err *err)
{
    redis_conn *rc = dc;
    char        subject[300];
    const char  *argv[] = { "CONFIG", "GET", "protected-mode" };
    const size_t lens[] = { 6, 3, 14 };
    redisReply *r;
    ta_nosql_err cerr;

    (void)args;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d", cfg->host, cfg->port);
    json_array_append_new(checked, json_string("redis.config-access"));
    if (rc == NULL)
        return 0;
    memset(&cerr, 0, sizeof(cerr));
    r = cmd_argv(rc, 3, argv, lens, &cerr);
    if (r == NULL)
        return 0;
    /*
     * The account the application connects as can reconfigure the
     * server: CONFIG SET dir + dbfilename is the classic way from a
     * Redis login to a file written anywhere the server can write.
     */
    ta_nosql_finding(findings, "redis.config-access", TA_NOSQL_SEV_MEDIUM,
                     subject,
                     "the account can run CONFIG (server administration: "
                     "changing persistence paths, disabling protections)");
    json_array_append_new(checked, json_string("redis.protected-mode"));
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 2 &&
        r->element[1]->str != NULL && strcmp(r->element[1]->str, "no") == 0 &&
        cfg->password == NULL)
    {
        ta_nosql_finding(findings, "redis.protected-mode",
                         TA_NOSQL_SEV_MEDIUM, subject,
                         "protected-mode is off and no password is set: the "
                         "server takes commands from any address it is "
                         "reachable on");
    }
    freeReplyObject(r);
    return 0;
}

static const char *const redis_default_creds[] = {
    "default:foobared", "default:redis", "default:password", "default:admin",
    NULL
};

const ta_nosql_driver ta_nosql_redis_driver = {
    .engine = TA_NOSQL_ENGINE_REDIS,
    .name = "redis",
    .caps = TA_NOSQL_CAP_SCAN | TA_NOSQL_CAP_TTL | TA_NOSQL_CAP_CAS |
            TA_NOSQL_CAP_ABSENT | TA_NOSQL_CAP_INCR |
            TA_NOSQL_CAP_INCR_EXACT | TA_NOSQL_CAP_MULTI |
            TA_NOSQL_CAP_MULTI_IF | TA_NOSQL_CAP_DURABLE |
#ifdef TA_NOSQL_HAVE_REDIS_SSL
            TA_NOSQL_CAP_TLS |
#endif
            TA_NOSQL_CAP_AUTH,
    .default_port = 6379,
    .default_creds = redis_default_creds,
    .client_version = redis_client_version,
    .connect = redis_connect,
    .disconnect = redis_disconnect,
    .ping = redis_ping,
    .info = redis_info,
    .tls_in_use = redis_tls_in_use,
    .put = redis_put,
    .put_if = redis_put_if,
    .get = redis_get,
    .del = redis_del,
    .incr = redis_incr,
    .scan = redis_scan,
    .multi = redis_multi,
    .drop = redis_drop,
    .native = redis_native,
    .bulk = redis_bulk,
    .set_timeout = redis_set_timeout,
    .audit = redis_audit,
};
