/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql driver: Memcached over libmemcached
 *
 * A document is its JSON under "<coll>:<key>"; its version is the
 * server's CAS unique, so a compare-and-set is the protocol's own "cas"
 * command and write-if-absent is "add". A counter is the server's
 * incr/decr on a decimal value (made with "add 0" first, as incr does
 * not create a key). Memcached keeps everything in memory: nothing
 * survives a restart, and it cannot list its keys, so there is no scan,
 * find, count, multi or drop. The driver has no SASL and no TLS.
 *
 * Its audit sends one "stats" request over UDP: a server that answers
 * is a reflector, and the ratio of answer to request bytes is what an
 * attacker spoofing the victim's address would get.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <libmemcached/memcached.h>

#include "ta_nosql_int.h"

typedef struct mc_conn {
    memcached_st *mc;
    ta_nosql_conf cfg;
} mc_conn;

static void
mc_client_version(char *buf, size_t len)
{
    snprintf(buf, len, "libmemcached %s", memcached_lib_version());
}

static ta_nosql_errcls
rc_cls(memcached_return_t rc)
{
    switch (rc)
    {
        case MEMCACHED_TIMEOUT:
            return TA_NOSQL_ERR_TIMEOUT;
        case MEMCACHED_CONNECTION_FAILURE:
        case MEMCACHED_CONNECTION_SOCKET_CREATE_FAILURE:
        case MEMCACHED_HOST_LOOKUP_FAILURE:
        case MEMCACHED_ERRNO:
        case MEMCACHED_WRITE_FAILURE:
        case MEMCACHED_READ_FAILURE:
        case MEMCACHED_UNKNOWN_READ_FAILURE:
        case MEMCACHED_NO_SERVERS:
            return TA_NOSQL_ERR_CONNECT;
        case MEMCACHED_SERVER_MARKED_DEAD:
        case MEMCACHED_SERVER_TEMPORARILY_DISABLED:
        case MEMCACHED_SERVER_ERROR:
            return TA_NOSQL_ERR_UNAVAILABLE;
        case MEMCACHED_E2BIG:
        case MEMCACHED_MEMORY_ALLOCATION_FAILURE:
            return TA_NOSQL_ERR_LIMIT;
        case MEMCACHED_AUTH_FAILURE:
        case MEMCACHED_AUTH_PROBLEM:
            return TA_NOSQL_ERR_AUTH;
        case MEMCACHED_BAD_KEY_PROVIDED:
        case MEMCACHED_CLIENT_ERROR:
            return TA_NOSQL_ERR_INVALID;
        case MEMCACHED_PROTOCOL_ERROR:
            return TA_NOSQL_ERR_PROTOCOL;
        default:
            return TA_NOSQL_ERR_PROTOCOL;
    }
}

static void
mc_err(mc_conn *c, memcached_return_t rc, ta_nosql_err *err)
{
    ta_nosql_err_set(err, rc_cls(rc), (long)rc, "memcached: %s",
                     memcached_last_error_message(c->mc) != NULL
                     ? memcached_last_error_message(c->mc)
                     : memcached_strerror(c->mc, rc));
}

static char *
mkey(const char *coll, const char *key, ta_nosql_err *err)
{
    char  *k = ta_nosql_strdupf("%s:%s", coll, key);
    size_t i;

    if (k == NULL)
        return NULL;
    if (strlen(k) > 250)
    {
        free(k);
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "memcached: a key is at most 250 bytes");
        return NULL;
    }
    for (i = 0; k[i] != '\0'; i++)
    {
        if ((unsigned char)k[i] <= ' ' || k[i] == 0x7f)
        {
            free(k);
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "memcached: keys may not hold spaces or control "
                             "characters");
            return NULL;
        }
    }
    return k;
}

static void mc_disconnect(void *dc);

static int
mc_connect(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err)
{
    mc_conn           *c;
    memcached_return_t rc;

    if (cfg->user != NULL || cfg->password != NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "memcached: this driver has no SASL authentication");
        return -1;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
        return -1;
    }
    ta_nosql_conf_copy(cfg, &c->cfg);
    c->mc = memcached_create(NULL);
    if (c->mc == NULL)
    {
        mc_disconnect(c);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0,
                         "memcached: cannot create a client");
        return -1;
    }
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_SUPPORT_CAS, 1);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT,
                           (uint64_t)cfg->connect_timeout_ms);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_POLL_TIMEOUT,
                           (uint64_t)cfg->timeout_ms);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_RCV_TIMEOUT,
                           (uint64_t)cfg->timeout_ms * 1000);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_SND_TIMEOUT,
                           (uint64_t)cfg->timeout_ms * 1000);
    /* Retry a server marked down after a second, not the default two. */
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_RETRY_TIMEOUT, 1);
    rc = memcached_server_add(c->mc, cfg->host, (in_port_t)cfg->port);
    if (rc == MEMCACHED_SUCCESS)
        rc = memcached_version(c->mc);
    if (rc != MEMCACHED_SUCCESS)
    {
        mc_err(c, rc, err);
        mc_disconnect(c);
        return -1;
    }
    *dc = c;
    return 0;
}

static void
mc_disconnect(void *dc)
{
    mc_conn *c = dc;

    if (c == NULL)
        return;
    if (c->mc != NULL)
        memcached_free(c->mc);
    ta_nosql_conf_free(&c->cfg);
    free(c);
}

static int
mc_ping(void *dc, ta_nosql_err *err)
{
    mc_conn           *c = dc;
    memcached_return_t rc = memcached_version(c->mc);

    if (rc != MEMCACHED_SUCCESS)
    {
        mc_err(c, rc, err);
        return -1;
    }
    return 0;
}

static int
mc_info(void *dc, json_t *out, ta_nosql_err *err)
{
    mc_conn                   *c = dc;
    const memcached_instance_st *s;

    if (mc_ping(dc, err) != 0)
        return -1;
    s = memcached_server_instance_by_position(c->mc, 0);
    if (s != NULL)
    {
        char v[64];

        snprintf(v, sizeof(v), "%u.%u.%u",
                 (unsigned)memcached_server_major_version(s),
                 (unsigned)memcached_server_minor_version(s),
                 (unsigned)memcached_server_micro_version(s));
        json_object_set_new(out, "server_version", json_string(v));
    }
    return 0;
}

static bool
mc_tls_in_use(void *dc)
{
    (void)dc;
    return false;
}

static int
mc_put(void *dc, const char *coll, const char *key, json_t *doc,
       long ttl_s, char **ver, ta_nosql_err *err)
{
    mc_conn           *c = dc;
    char              *k = mkey(coll, key, err);
    char              *v;
    memcached_return_t rc;

    if (k == NULL)
        return -1;
    v = ta_nosql_json_dump(doc);
    rc = memcached_set(c->mc, k, strlen(k), v, strlen(v),
                       (time_t)(ttl_s > 0 ? ttl_s : 0), 0);
    free(k);
    free(v);
    if (ver != NULL)
        *ver = NULL;
    if (rc != MEMCACHED_SUCCESS)
    {
        mc_err(c, rc, err);
        return -1;
    }
    return 0;
}

static int
mc_put_if(void *dc, const char *coll, const char *key, json_t *doc,
          long ttl_s, const char *if_ver, bool *applied, char **ver,
          ta_nosql_err *err)
{
    mc_conn           *c = dc;
    char              *k = mkey(coll, key, err);
    char              *v;
    memcached_return_t rc;

    if (k == NULL)
        return -1;
    v = ta_nosql_json_dump(doc);
    if (if_ver == NULL)
    {
        rc = memcached_add(c->mc, k, strlen(k), v, strlen(v),
                           (time_t)(ttl_s > 0 ? ttl_s : 0), 0);
    }
    else
    {
        char    *end = NULL;
        uint64_t cas = strtoull(if_ver, &end, 10);

        if (end == NULL || *end != '\0')
        {
            free(k);
            free(v);
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "memcached: a version is a CAS number");
            return -1;
        }
        rc = memcached_cas(c->mc, k, strlen(k), v, strlen(v),
                           (time_t)(ttl_s > 0 ? ttl_s : 0), 0, cas);
    }
    free(k);
    free(v);
    *ver = NULL;
    if (rc == MEMCACHED_SUCCESS)
    {
        *applied = true;
        return 0;
    }
    if (rc == MEMCACHED_NOTSTORED || rc == MEMCACHED_DATA_EXISTS ||
        rc == MEMCACHED_NOTFOUND)
    {
        *applied = false;
        return 0;
    }
    mc_err(c, rc, err);
    return -1;
}

static int
mc_get(void *dc, const char *coll, const char *key, json_t **doc,
       char **ver, ta_nosql_err *err)
{
    mc_conn           *c = dc;
    char              *k = mkey(coll, key, err);
    const char        *keys[1];
    size_t             lens[1];
    memcached_return_t rc;
    memcached_result_st *res;
    int                ret = 0;

    *doc = NULL;
    if (ver != NULL)
        *ver = NULL;
    if (k == NULL)
        return -1;
    keys[0] = k;
    lens[0] = strlen(k);
    rc = memcached_mget(c->mc, keys, lens, 1);
    if (rc != MEMCACHED_SUCCESS)
    {
        free(k);
        mc_err(c, rc, err);
        return -1;
    }
    res = memcached_fetch_result(c->mc, NULL, &rc);
    if (res != NULL)
    {
        const char *val = memcached_result_value(res);
        size_t      vlen = memcached_result_length(res);

        *doc = ta_nosql_json_load(val, vlen, err);
        if (*doc == NULL)
        {
            *doc = json_stringn(val, vlen);
            memset(err, 0, sizeof(*err));
        }
        if (ver != NULL)
            *ver = ta_nosql_strdupf("%llu", (unsigned long long)
                                    memcached_result_cas(res));
        memcached_result_free(res);
        /* Drain the end of the mget. */
        while ((res = memcached_fetch_result(c->mc, NULL, &rc)) != NULL)
            memcached_result_free(res);
    }
    else if (rc != MEMCACHED_END && rc != MEMCACHED_NOTFOUND &&
             rc != MEMCACHED_SUCCESS)
    {
        mc_err(c, rc, err);
        ret = -1;
    }
    free(k);
    return ret;
}

static int
mc_del(void *dc, const char *coll, const char *key, bool *deleted,
       ta_nosql_err *err)
{
    mc_conn           *c = dc;
    char              *k = mkey(coll, key, err);
    memcached_return_t rc;

    if (k == NULL)
        return -1;
    rc = memcached_delete(c->mc, k, strlen(k), 0);
    free(k);
    if (rc == MEMCACHED_SUCCESS || rc == MEMCACHED_NOTFOUND)
    {
        *deleted = rc == MEMCACHED_SUCCESS;
        return 0;
    }
    mc_err(c, rc, err);
    return -1;
}

static int
mc_incr(void *dc, const char *coll, const char *key, long long by,
        long long *value, ta_nosql_err *err)
{
    mc_conn           *c = dc;
    char              *k = mkey(coll, key, err);
    uint64_t           v = 0;
    memcached_return_t rc;
    int                attempt;

    if (k == NULL)
        return -1;
    for (attempt = 0; attempt < 2; attempt++)
    {
        if (by >= 0)
            rc = memcached_increment(c->mc, k, strlen(k), (uint32_t)by, &v);
        else
            rc = memcached_decrement(c->mc, k, strlen(k), (uint32_t)(-by), &v);
        if (rc != MEMCACHED_NOTFOUND)
            break;
        /* incr does not create a key: make it at 0 (losing the race to
         * another client's add is fine, its 0 is as good). */
        rc = memcached_add(c->mc, k, strlen(k), "0", 1, 0, 0);
        if (rc != MEMCACHED_SUCCESS && rc != MEMCACHED_NOTSTORED &&
            rc != MEMCACHED_DATA_EXISTS)
            break;
    }
    free(k);
    if (rc != MEMCACHED_SUCCESS)
    {
        mc_err(c, rc, err);
        return -1;
    }
    *value = (long long)v;
    return 0;
}

static memcached_return_t
stat_cb(const memcached_instance_st *s, const char *key, size_t klen,
        const char *value, size_t vlen, void *ctx)
{
    json_t *out = ctx;
    char   *k = strndup(key, klen);

    (void)s;
    json_object_set_new(out, k, json_stringn(value, vlen));
    free(k);
    return MEMCACHED_SUCCESS;
}

static int
mc_native(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err)
{
    mc_conn           *c = dc;
    const char        *stats = ta_nosql_json_str(cmd, "stats", NULL);
    json_t            *out;
    memcached_return_t rc;

    if (stats == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "memcached: 'cmd' takes 'stats' (\"\", \"items\", "
                         "\"settings\", \"slabs\")");
        return -1;
    }
    out = json_object();
    rc = memcached_stat_execute(c->mc, *stats != '\0' ? stats : NULL, stat_cb,
                                out);
    if (rc != MEMCACHED_SUCCESS)
    {
        json_decref(out);
        mc_err(c, rc, err);
        return -1;
    }
    *reply = out;
    return 0;
}

static int
mc_set_timeout(void *dc, int ms, ta_nosql_err *err)
{
    mc_conn *c = dc;

    (void)err;
    c->cfg.timeout_ms = ms;
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_POLL_TIMEOUT,
                           (uint64_t)ms);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_RCV_TIMEOUT,
                           (uint64_t)ms * 1000);
    memcached_behavior_set(c->mc, MEMCACHED_BEHAVIOR_SND_TIMEOUT,
                           (uint64_t)ms * 1000);
    return 0;
}

/*
 * One UDP "stats": bytes sent, bytes received within @p wait_ms (0 when
 * nothing came back), and datagrams.
 */
static int
udp_stats(const char *host, int port, int wait_ms, size_t *sent,
          size_t *got, int *datagrams)
{
    static const unsigned char req[] = {
        0x13, 0x37,     /* request id */
        0x00, 0x00,     /* sequence */
        0x00, 0x01,     /* datagrams in the request */
        0x00, 0x00,     /* reserved */
        's', 't', 'a', 't', 's', '\r', '\n',
    };
    struct addrinfo  hints;
    struct addrinfo *ai = NULL;
    char             pstr[16];
    unsigned char    buf[65536];
    int              s;
    int64_t          deadline;

    *sent = 0;
    *got = 0;
    *datagrams = 0;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_DGRAM;
    snprintf(pstr, sizeof(pstr), "%d", port);
    if (getaddrinfo(host, pstr, &hints, &ai) != 0 || ai == NULL)
        return -1;
    s = socket(ai->ai_family, SOCK_DGRAM, 0);
    if (s < 0)
    {
        freeaddrinfo(ai);
        return -1;
    }
    if (sendto(s, req, sizeof(req), 0, ai->ai_addr, ai->ai_addrlen) !=
        (ssize_t)sizeof(req))
    {
        close(s);
        freeaddrinfo(ai);
        return -1;
    }
    freeaddrinfo(ai);
    *sent = sizeof(req);
    deadline = ta_nosql_now_us() + (int64_t)wait_ms * 1000;
    for (;;)
    {
        struct pollfd pfd = { .fd = s, .events = POLLIN };
        int64_t       left = (deadline - ta_nosql_now_us()) / 1000;
        ssize_t       n;

        if (left <= 0 || poll(&pfd, 1, (int)left) <= 0)
            break;
        n = recv(s, buf, sizeof(buf), 0);
        if (n <= 0)
            break;
        *got += (size_t)n;
        (*datagrams)++;
    }
    close(s);
    return 0;
}

static int
mc_audit(const ta_nosql_conf *cfg, void *dc, json_t *args,
         json_t *findings, json_t *checked, ta_nosql_err *err)
{
    int    port = (int)ta_nosql_json_int(args, "udp_port", cfg->port);
    char   subject[300];
    size_t sent;
    size_t got;
    int    dgrams;

    (void)dc;
    (void)err;
    snprintf(subject, sizeof(subject), "%s:%d/udp", cfg->host, port);
    json_array_append_new(checked, json_string("memcached.udp-reflection"));
    if (udp_stats(cfg->host, port, 1500, &sent, &got, &dgrams) != 0)
    {
        ta_nosql_finding(findings, "memcached.udp-not-assessed",
                         TA_NOSQL_SEV_INFO, subject,
                         "could not send a UDP probe");
        return 0;
    }
    if (got > 0)
    {
        ta_nosql_finding(findings, "memcached.udp-reflection",
                         TA_NOSQL_SEV_HIGH, subject,
                         "UDP is enabled: a %zu-byte request drew %zu bytes "
                         "in %d datagrams (amplification x%.1f) - a "
                         "reflector for spoofed traffic",
                         sent, got, dgrams, (double)got / (double)sent);
    }
    return 0;
}

const ta_nosql_driver ta_nosql_memcached_driver = {
    .engine = TA_NOSQL_ENGINE_MEMCACHED,
    .name = "memcached",
    .caps = TA_NOSQL_CAP_TTL | TA_NOSQL_CAP_CAS | TA_NOSQL_CAP_ABSENT |
            TA_NOSQL_CAP_INCR | TA_NOSQL_CAP_INCR_EXACT,
    .default_port = 11211,
    .default_creds = NULL,
    .client_version = mc_client_version,
    .connect = mc_connect,
    .disconnect = mc_disconnect,
    .ping = mc_ping,
    .info = mc_info,
    .tls_in_use = mc_tls_in_use,
    .put = mc_put,
    .put_if = mc_put_if,
    .get = mc_get,
    .del = mc_del,
    .incr = mc_incr,
    .native = mc_native,
    .set_timeout = mc_set_timeout,
    .audit = mc_audit,
};
