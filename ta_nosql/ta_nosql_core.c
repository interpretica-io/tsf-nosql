/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql core: handles, argument checks, operations
 *
 * Every operation goes through ta_nosql_call(): the arguments are
 * parsed and checked here, once, so a driver sees a valid collection
 * name, a key of sane length and a document that is a JSON object
 * without the module's reserved fields; the driver's answer is put into
 * the result object here, so every engine answers in the same shape.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/random.h>
#endif

#include "ta_nosql_int.h"

/** How many connections one process may hold. */
#define TA_NOSQL_SLOTS  255

/** One connection. */
typedef struct ta_nosql_slot {
    unsigned int           gen;     /**< 0: free; else the generation. */
    const ta_nosql_driver *drv;     /**< Its driver. */
    void                  *dc;      /**< The driver's connection. */
    ta_nosql_conf          cfg;     /**< What it was opened with. */
    unsigned long          ops;     /**< Operations run on it. */
} ta_nosql_slot;

static ta_nosql_slot   slots[TA_NOSQL_SLOTS];
static pthread_mutex_t reg_lock = PTHREAD_MUTEX_INITIALIZER;

static const ta_nosql_driver *const drivers[] = {
#ifdef TA_NOSQL_HAVE_REDIS
    &ta_nosql_redis_driver,
#endif
#ifdef TA_NOSQL_HAVE_MONGODB
    &ta_nosql_mongo_driver,
#endif
#ifdef TA_NOSQL_HAVE_CASSANDRA
    &ta_nosql_cassandra_driver,
#endif
#ifdef TA_NOSQL_HAVE_CURL
    &ta_nosql_elastic_driver,
    &ta_nosql_neo4j_driver,
#endif
#ifdef TA_NOSQL_HAVE_MEMCACHED
    &ta_nosql_memcached_driver,
#endif
#ifdef TA_NOSQL_HAVE_CURL
    &ta_nosql_etcd_driver,
#endif
    NULL
};

static const struct {
    unsigned int bit;
    const char  *name;
} engine_names[] = {
    { TA_NOSQL_ENGINE_REDIS,     "redis" },
    { TA_NOSQL_ENGINE_MONGODB,   "mongodb" },
    { TA_NOSQL_ENGINE_CASSANDRA, "cassandra" },
    { TA_NOSQL_ENGINE_ELASTIC,   "elasticsearch" },
    { TA_NOSQL_ENGINE_NEO4J,     "neo4j" },
    { TA_NOSQL_ENGINE_MEMCACHED, "memcached" },
    { TA_NOSQL_ENGINE_ETCD,      "etcd" },
};

static const struct {
    unsigned int bit;
    const char  *name;
} cap_names[] = {
    { TA_NOSQL_CAP_SCAN,        "scan" },
    { TA_NOSQL_CAP_FIND,        "find" },
    { TA_NOSQL_CAP_COUNT,       "count" },
    { TA_NOSQL_CAP_TTL,         "ttl" },
    { TA_NOSQL_CAP_CAS,         "cas" },
    { TA_NOSQL_CAP_ABSENT,      "absent" },
    { TA_NOSQL_CAP_INCR,        "incr" },
    { TA_NOSQL_CAP_INCR_EXACT,  "incr_exact" },
    { TA_NOSQL_CAP_MULTI,       "multi" },
    { TA_NOSQL_CAP_MULTI_IF,    "multi_if" },
    { TA_NOSQL_CAP_DURABLE,     "durable" },
    { TA_NOSQL_CAP_TLS,         "tls" },
    { TA_NOSQL_CAP_AUTH,        "auth" },
};

/* See description in ta_nosql_core.h */
unsigned int
ta_nosql_engine_by_name(const char *name)
{
    size_t i;

    if (name == NULL)
        return 0;
    for (i = 0; i < sizeof(engine_names) / sizeof(engine_names[0]); i++)
    {
        if (strcmp(engine_names[i].name, name) == 0)
            return engine_names[i].bit;
    }
    if (strcmp(name, "elastic") == 0)
        return TA_NOSQL_ENGINE_ELASTIC;
    if (strcmp(name, "mongo") == 0)
        return TA_NOSQL_ENGINE_MONGODB;
    return 0;
}

/* See description in ta_nosql_core.h */
const char *
ta_nosql_engine_name(unsigned int engine)
{
    size_t i;

    for (i = 0; i < sizeof(engine_names) / sizeof(engine_names[0]); i++)
    {
        if (engine_names[i].bit == engine)
            return engine_names[i].name;
    }
    return NULL;
}

/* See description in ta_nosql_int.h */
const ta_nosql_driver *
ta_nosql_driver_by_engine(unsigned int engine)
{
    size_t i;

    for (i = 0; drivers[i] != NULL; i++)
    {
        if (drivers[i]->engine == engine)
            return drivers[i];
    }
    return NULL;
}

/* See description in ta_nosql_int.h */
json_t *
ta_nosql_caps_json(unsigned int caps)
{
    json_t *a = json_array();
    size_t  i;

    for (i = 0; i < sizeof(cap_names) / sizeof(cap_names[0]); i++)
    {
        if ((caps & cap_names[i].bit) != 0)
            json_array_append_new(a, json_string(cap_names[i].name));
    }
    return a;
}

/* See description in ta_nosql_core.h */
int
ta_nosql_available(unsigned int *engines, char **json)
{
    json_t      *list = json_array();
    json_t      *out;
    unsigned int mask = 0;
    size_t       i;

    for (i = 0; drivers[i] != NULL; i++)
    {
        char client[128] = "";

        drivers[i]->client_version(client, sizeof(client));
        mask |= drivers[i]->engine;
        json_array_append_new(list,
            json_pack("{s:s, s:s, s:o, s:i}", "name", drivers[i]->name,
                      "client", client,
                      "caps", ta_nosql_caps_json(drivers[i]->caps),
                      "default_port", drivers[i]->default_port));
    }
    out = json_pack("{s:o}", "engines", list);
    *engines = mask;
    *json = ta_nosql_json_dump(out);
    json_decref(out);
    return *json == NULL ? -1 : 0;
}

/* See description in ta_nosql_int.h */
void
ta_nosql_err_scrub(ta_nosql_err *err, const ta_nosql_conf *cfg)
{
    if (err != NULL && cfg != NULL)
        ta_nosql_scrub(err->msg, cfg->password);
}

/*
 * A failure that took (nearly) the whole deadline was the deadline,
 * whatever the client library called it: several report a timed-out
 * read as a plain I/O error.
 */
static void
classify_timeout(ta_nosql_err *err, int64_t elapsed_us, int timeout_ms)
{
    if (err->cls == TA_NOSQL_ERR_CONNECT || err->cls == TA_NOSQL_ERR_PROTOCOL ||
        err->cls == TA_NOSQL_ERR_UNAVAILABLE)
    {
        if (timeout_ms > 0 && elapsed_us >= (int64_t)timeout_ms * 900)
            err->cls = TA_NOSQL_ERR_TIMEOUT;
    }
}

/* See description in ta_nosql_int.h */
int
ta_nosql_connect_checked(const ta_nosql_driver *drv, const ta_nosql_conf *cfg,
                         void **dc, ta_nosql_err *err)
{
    int64_t t0 = ta_nosql_now_us();
    int     rc;

    if (cfg->tls && (drv->caps & TA_NOSQL_CAP_TLS) == 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "the %s driver cannot use TLS", drv->name);
        return -1;
    }
    rc = drv->connect(cfg, dc, err);
    if (rc != 0)
    {
        classify_timeout(err, ta_nosql_now_us() - t0,
                         cfg->connect_timeout_ms);
        ta_nosql_err_scrub(err, cfg);
    }
    return rc;
}

static void
random_bytes(void *buf, size_t len)
{
#if defined(__linux__)
    if (getrandom(buf, len, 0) == (ssize_t)len)
        return;
#else
    if (getentropy(buf, len) == 0)
        return;
#endif
    {
        unsigned char *p = buf;
        size_t         i;

        srand((unsigned int)(ta_nosql_now_us() ^ getpid()));
        for (i = 0; i < len; i++)
            p[i] = (unsigned char)rand();
    }
}

static unsigned int
new_gen(void)
{
    unsigned int g;

    do {
        random_bytes(&g, sizeof(g));
        g &= 0x3fffffu;
    } while (g == 0);
    return g;
}

/* Find a handle's slot; NULL with an error if there is none. */
static ta_nosql_slot *
slot_of(int handle, ta_nosql_err *err)
{
    unsigned int idx = (unsigned int)handle & 0xffu;
    unsigned int gen = (unsigned int)handle >> 8;
    ta_nosql_slot *s = NULL;

    pthread_mutex_lock(&reg_lock);
    if (handle > 0 && idx >= 1 && idx <= TA_NOSQL_SLOTS &&
        slots[idx - 1].gen != 0 && slots[idx - 1].gen == gen)
        s = &slots[idx - 1];
    pthread_mutex_unlock(&reg_lock);
    if (s == NULL)
        ta_nosql_err_set(err, TA_NOSQL_ERR_HANDLE, 0,
                         "no connection with handle %d", handle);
    return s;
}

static json_t *
info_json(ta_nosql_slot *s, ta_nosql_err *err)
{
    json_t *out = json_object();
    char    client[128] = "";

    s->drv->client_version(client, sizeof(client));
    json_object_set_new(out, "engine", json_string(s->drv->name));
    json_object_set_new(out, "client", json_string(client));
    json_object_set_new(out, "host", json_string(s->cfg.host));
    json_object_set_new(out, "port",
                        json_integer(s->cfg.port != 0 ? s->cfg.port
                                                     : s->drv->default_port));
    json_object_set_new(out, "user", s->cfg.user != NULL
                                     ? json_string(s->cfg.user)
                                     : json_null());
    json_object_set_new(out, "tls", json_boolean(s->drv->tls_in_use != NULL &&
                                                 s->drv->tls_in_use(s->dc)));
    json_object_set_new(out, "caps", ta_nosql_caps_json(s->drv->caps));
    if (s->drv->info != NULL && s->drv->info(s->dc, out, err) != 0)
    {
        json_decref(out);
        return NULL;
    }
    return out;
}

/* See description in ta_nosql_core.h */
int
ta_nosql_open(const char *engine, const char *config, int *handle,
              char **result, ta_nosql_err *err)
{
    const ta_nosql_driver *drv;
    ta_nosql_conf          cfg;
    json_t                *raw;
    json_t                *info;
    void                  *dc = NULL;
    unsigned int           i;
    unsigned int           bit = ta_nosql_engine_by_name(engine);

    memset(err, 0, sizeof(*err));
    *handle = 0;
    if (result != NULL)
        *result = NULL;
    if (bit == 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0, "unknown engine '%s'",
                         engine != NULL ? engine : "(null)");
        return -1;
    }
    drv = ta_nosql_driver_by_engine(bit);
    if (drv == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "the agent has no %s driver", engine);
        return -1;
    }
    raw = ta_nosql_json_load(config != NULL && *config != '\0' ? config : "{}",
                             config != NULL && *config != '\0'
                             ? strlen(config) : 2, err);
    if (raw == NULL)
        return -1;
    if (ta_nosql_conf_parse(raw, &cfg, err) != 0)
    {
        json_decref(raw);
        return -1;
    }
    json_decref(raw);
    if (cfg.port == 0)
        cfg.port = drv->default_port;

    if (ta_nosql_connect_checked(drv, &cfg, &dc, err) != 0)
    {
        ta_nosql_conf_free(&cfg);
        return -1;
    }

    pthread_mutex_lock(&reg_lock);
    for (i = 0; i < TA_NOSQL_SLOTS && slots[i].gen != 0; i++)
        ;
    if (i == TA_NOSQL_SLOTS)
    {
        pthread_mutex_unlock(&reg_lock);
        drv->disconnect(dc);
        ta_nosql_conf_free(&cfg);
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "too many connections (%d)", TA_NOSQL_SLOTS);
        return -1;
    }
    slots[i].gen = new_gen();
    slots[i].drv = drv;
    slots[i].dc = dc;
    slots[i].cfg = cfg;
    slots[i].ops = 0;
    *handle = (int)((slots[i].gen << 8) | (i + 1));
    pthread_mutex_unlock(&reg_lock);

    if (result != NULL)
    {
        info = info_json(&slots[i], err);
        if (info == NULL)
        {
            /* The server answered the connection but not the question:
             * still a connection, report it without the server's view. */
            info = json_pack("{s:s}", "engine", drv->name);
            memset(err, 0, sizeof(*err));
        }
        json_object_set_new(info, "handle", json_integer(*handle));
        *result = ta_nosql_json_dump(info);
        ta_nosql_scrub(*result, cfg.password);
        json_decref(info);
    }
    return 0;
}

static void
slot_free(ta_nosql_slot *s)
{
    s->drv->disconnect(s->dc);
    ta_nosql_conf_free(&s->cfg);
    memset(s, 0, sizeof(*s));
}

/* See description in ta_nosql_core.h */
int
ta_nosql_close(int handle, ta_nosql_err *err)
{
    ta_nosql_slot *s;

    memset(err, 0, sizeof(*err));
    s = slot_of(handle, err);
    if (s == NULL)
        return -1;
    pthread_mutex_lock(&reg_lock);
    slot_free(s);
    pthread_mutex_unlock(&reg_lock);
    return 0;
}

/* See description in ta_nosql_core.h */
void
ta_nosql_close_all(void)
{
    unsigned int i;

    pthread_mutex_lock(&reg_lock);
    for (i = 0; i < TA_NOSQL_SLOTS; i++)
    {
        if (slots[i].gen != 0)
            slot_free(&slots[i]);
    }
    pthread_mutex_unlock(&reg_lock);
}

/* ---- argument checks ------------------------------------------------ */

static const char *
arg_coll(json_t *args, ta_nosql_err *err)
{
    const char *coll = ta_nosql_json_str(args, "coll", NULL);

    if (!ta_nosql_coll_ok(coll))
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'coll' must be a lower-case identifier "
                         "([a-z][a-z0-9_]*, at most 48 characters)");
        return NULL;
    }
    return coll;
}

static const char *
arg_key(json_t *args, const char *name, ta_nosql_err *err)
{
    const char *key = ta_nosql_json_str(args, name, NULL);
    size_t      len;

    if (key == NULL || (len = strlen(key)) == 0 || len > TA_NOSQL_KEY_MAX)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'%s' must be a string of 1 to %d bytes", name,
                         TA_NOSQL_KEY_MAX);
        return NULL;
    }
    return key;
}

static json_t *
arg_doc(json_t *args, ta_nosql_err *err)
{
    json_t     *doc = json_object_get(args, "doc");
    const char *field;
    json_t     *v;
    char       *enc;
    size_t      len;

    if (!json_is_object(doc))
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'doc' must be a JSON object");
        return NULL;
    }
    json_object_foreach(doc, field, v)
    {
        if (field[0] == '_' || field[0] == '\0' || strchr(field, '.') != NULL ||
            field[0] == '$')
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "document field '%s': top-level names may not "
                             "be empty, start with '_' or '$' (reserved) or "
                             "contain '.'", field);
            return NULL;
        }
    }
    enc = ta_nosql_json_dump(doc);
    len = enc != NULL ? strlen(enc) : 0;
    free(enc);
    if (len > TA_NOSQL_DOC_MAX)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "document of %zu bytes is over %d", len,
                         TA_NOSQL_DOC_MAX);
        return NULL;
    }
    return doc;
}

static int
arg_limit(json_t *args, int *limit, ta_nosql_err *err)
{
    long long l = ta_nosql_json_int(args, "limit", 100);

    if (l <= 0 || l > TA_NOSQL_PAGE_MAX)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "'limit' must be 1 to %d", TA_NOSQL_PAGE_MAX);
        return -1;
    }
    *limit = (int)l;
    return 0;
}

static json_t *
arg_filter(json_t *args, bool required, ta_nosql_err *err)
{
    json_t     *f = json_object_get(args, "filter");
    const char *field;
    json_t     *v;

    if (f == NULL && !required)
        return NULL;
    if (!json_is_object(f))
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'filter' must be a JSON object");
        return NULL;
    }
    json_object_foreach(f, field, v)
    {
        if (!(json_is_string(v) || json_is_integer(v) || json_is_boolean(v)))
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "filter field '%s': a filter compares with a "
                             "string, an integer or a boolean", field);
            return NULL;
        }
        if (field[0] == '_' || field[0] == '$' || field[0] == '\0')
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "filter field '%s' is reserved", field);
            return NULL;
        }
    }
    return f;
}

static bool
need(const ta_nosql_driver *drv, unsigned int cap, const void *fn,
     const char *what, ta_nosql_err *err)
{
    if (fn != NULL && (cap == 0 || (drv->caps & cap) != 0))
        return true;
    ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0, "%s does not %s",
                     drv->name, what);
    return false;
}

/* ---- the operations ------------------------------------------------- */

static int
op_put(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *key;
    json_t     *doc;
    long        ttl = (long)ta_nosql_json_int(args, "ttl_s", 0);
    char       *ver = NULL;

    if (coll == NULL || (key = arg_key(args, "key", err)) == NULL ||
        (doc = arg_doc(args, err)) == NULL)
        return -1;
    if (ttl > 0 && !need(s->drv, TA_NOSQL_CAP_TTL, s->drv->put,
                         "expire entries", err))
        return -1;
    if (s->drv->put(s->dc, coll, key, doc, ttl, &ver, err) != 0)
        return -1;
    json_object_set_new(out, "version", ver != NULL ? json_string(ver)
                                                    : json_null());
    free(ver);
    return 0;
}

static int
op_put_if(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *key;
    const char *cond = ta_nosql_json_str(args, "if", NULL);
    const char *ver_in = NULL;
    json_t     *doc;
    long        ttl = (long)ta_nosql_json_int(args, "ttl_s", 0);
    bool        applied = false;
    char       *ver = NULL;

    if (coll == NULL || (key = arg_key(args, "key", err)) == NULL ||
        (doc = arg_doc(args, err)) == NULL)
        return -1;
    if (cond != NULL && strcmp(cond, "absent") == 0)
    {
        if (!need(s->drv, TA_NOSQL_CAP_ABSENT, s->drv->put_if,
                  "write only when a key is absent", err))
            return -1;
    }
    else if (cond != NULL && strcmp(cond, "version") == 0)
    {
        ver_in = ta_nosql_json_str(args, "version", NULL);
        if (ver_in == NULL || *ver_in == '\0')
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "'if': 'version' needs a 'version' to compare");
            return -1;
        }
        if (!need(s->drv, TA_NOSQL_CAP_CAS, s->drv->put_if,
                  "compare and set", err))
            return -1;
    }
    else
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'if' must be \"absent\" or \"version\"");
        return -1;
    }
    if (ttl > 0 && (s->drv->caps & TA_NOSQL_CAP_TTL) == 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "%s does not expire entries", s->drv->name);
        return -1;
    }
    if (s->drv->put_if(s->dc, coll, key, doc, ttl, ver_in, &applied, &ver,
                       err) != 0)
        return -1;
    json_object_set_new(out, "applied", json_boolean(applied));
    json_object_set_new(out, "version", ver != NULL ? json_string(ver)
                                                    : json_null());
    free(ver);
    return 0;
}

static int
op_get(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *key;
    json_t     *doc = NULL;
    char       *ver = NULL;

    if (coll == NULL || (key = arg_key(args, "key", err)) == NULL)
        return -1;
    if (s->drv->get(s->dc, coll, key, &doc, &ver, err) != 0)
        return -1;
    json_object_set_new(out, "found", json_boolean(doc != NULL));
    json_object_set_new(out, "doc", doc != NULL ? doc : json_null());
    json_object_set_new(out, "version", ver != NULL ? json_string(ver)
                                                    : json_null());
    free(ver);
    return 0;
}

static int
op_delete(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *key;
    bool        deleted = false;

    if (coll == NULL || (key = arg_key(args, "key", err)) == NULL)
        return -1;
    if (s->drv->del(s->dc, coll, key, &deleted, err) != 0)
        return -1;
    json_object_set_new(out, "deleted", json_boolean(deleted));
    return 0;
}

static int
op_incr(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *key;
    long long   by = ta_nosql_json_int(args, "by", 1);
    long long   value = 0;

    if (coll == NULL || (key = arg_key(args, "key", err)) == NULL)
        return -1;
    if (!need(s->drv, TA_NOSQL_CAP_INCR, s->drv->incr, "keep counters", err))
        return -1;
    if (s->drv->incr(s->dc, coll, key, by, &value, err) != 0)
        return -1;
    json_object_set_new(out, "value", json_integer(value));
    return 0;
}

static int
op_scan(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *cursor = ta_nosql_json_str(args, "cursor", NULL);
    json_t     *keys;
    char       *next = NULL;
    int         limit;

    if (coll == NULL || arg_limit(args, &limit, err) != 0)
        return -1;
    if (!need(s->drv, TA_NOSQL_CAP_SCAN, s->drv->scan, "list keys", err))
        return -1;
    keys = json_array();
    if (s->drv->scan(s->dc, coll, limit, cursor, keys, &next, err) != 0)
    {
        json_decref(keys);
        return -1;
    }
    json_object_set_new(out, "keys", keys);
    json_object_set_new(out, "cursor", next != NULL ? json_string(next)
                                                    : json_null());
    free(next);
    return 0;
}

static int
op_find(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    const char *cursor = ta_nosql_json_str(args, "cursor", NULL);
    json_t     *filter;
    json_t     *docs;
    char       *next = NULL;
    int         limit;

    if (coll == NULL || arg_limit(args, &limit, err) != 0 ||
        (filter = arg_filter(args, true, err)) == NULL)
        return -1;
    if (!need(s->drv, TA_NOSQL_CAP_FIND, s->drv->find,
              "query by field", err))
        return -1;
    docs = json_array();
    if (s->drv->find(s->dc, coll, filter, limit, cursor, docs, &next,
                     err) != 0)
    {
        json_decref(docs);
        return -1;
    }
    json_object_set_new(out, "docs", docs);
    json_object_set_new(out, "cursor", next != NULL ? json_string(next)
                                                    : json_null());
    free(next);
    return 0;
}

static int
op_count(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    json_t     *filter;
    long long   n = 0;

    if (coll == NULL)
        return -1;
    filter = arg_filter(args, false, err);
    if (filter == NULL && json_object_get(args, "filter") != NULL)
        return -1;
    if (!need(s->drv, TA_NOSQL_CAP_COUNT, s->drv->count, "count", err))
        return -1;
    if (filter != NULL && json_object_size(filter) > 0 &&
        (s->drv->caps & TA_NOSQL_CAP_FIND) == 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "%s does not count by field", s->drv->name);
        return -1;
    }
    if (s->drv->count(s->dc, coll, filter, &n, err) != 0)
        return -1;
    json_object_set_new(out, "count", json_integer(n));
    return 0;
}

static int
op_multi(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    json_t     *ops = json_object_get(args, "ops");
    json_t     *cond = json_object_get(args, "if_absent");
    const char *if_coll = NULL;
    const char *if_key = NULL;
    bool        applied = false;
    size_t      i;
    json_t     *op;

    if (!json_is_array(ops) || json_array_size(ops) == 0 ||
        json_array_size(ops) > 100)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'ops' must be an array of 1 to 100 operations");
        return -1;
    }
    json_array_foreach(ops, i, op)
    {
        const char *kind = ta_nosql_json_str(op, "op", "");

        if (arg_coll(op, err) == NULL || arg_key(op, "key", err) == NULL)
            return -1;
        if (strcmp(kind, "put") == 0)
        {
            if (arg_doc(op, err) == NULL)
                return -1;
        }
        else if (strcmp(kind, "delete") != 0)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "operation %zu: 'op' must be \"put\" or "
                             "\"delete\"", i);
            return -1;
        }
    }
    if (cond != NULL)
    {
        if ((if_coll = arg_coll(cond, err)) == NULL ||
            (if_key = arg_key(cond, "key", err)) == NULL)
            return -1;
        if (!need(s->drv, TA_NOSQL_CAP_MULTI_IF, s->drv->multi,
                  "write several keys under a condition", err))
            return -1;
    }
    else if (!need(s->drv, TA_NOSQL_CAP_MULTI, s->drv->multi,
                   "write several keys atomically", err))
        return -1;
    if (s->drv->multi(s->dc, ops, if_coll, if_key, &applied, err) != 0)
        return -1;
    json_object_set_new(out, "applied", json_boolean(applied));
    return 0;
}

static int
op_drop(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);

    (void)out;
    if (coll == NULL)
        return -1;
    if (!need(s->drv, 0, s->drv->drop, "drop a collection", err))
        return -1;
    return s->drv->drop(s->dc, coll, err);
}

static int
op_native(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    json_t *cmd = json_object_get(args, "cmd");
    json_t *reply = NULL;

    if (cmd == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0, "'cmd' is missing");
        return -1;
    }
    if (!need(s->drv, 0, s->drv->native, "take native commands", err))
        return -1;
    if (s->drv->native(s->dc, cmd, &reply, err) != 0)
        return -1;
    json_object_set_new(out, "reply", reply != NULL ? reply : json_null());
    return 0;
}

/*
 * The documents bulk writes when asked to generate them: deterministic
 * in (seed, i), so a test knows each one without having sent it.
 */
static json_t *
gen_doc(long long i, long long seed, int size)
{
    char              *payload = malloc((size_t)size + 1);
    unsigned long long x = (unsigned long long)(i + 1) *
                           6364136223846793005ULL +
                           (unsigned long long)seed;
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
    doc = json_pack("{s:I, s:s, s:I, s:s}", "i", (json_int_t)i,
                    "name", "", "group", (json_int_t)(i % 10),
                    "payload", payload);
    if (doc != NULL)
    {
        char name[32];

        snprintf(name, sizeof(name), "item-%lld", i);
        json_object_set_new(doc, "name", json_string(name));
    }
    free(payload);
    return doc;
}

static int
bulk_flush(ta_nosql_slot *s, const char *coll, json_t *items,
           ta_nosql_err *err)
{
    size_t  i;
    json_t *it;

    if (json_array_size(items) == 0)
        return 0;
    if (s->drv->bulk != NULL)
        return s->drv->bulk(s->dc, coll, items, err);
    json_array_foreach(items, i, it)
    {
        if (s->drv->put(s->dc, coll, json_string_value(json_object_get(it,
                                                                      "key")),
                        json_object_get(it, "doc"), 0, NULL, err) != 0)
            return -1;
    }
    return 0;
}

static int
op_bulk(ta_nosql_slot *s, json_t *args, json_t *out, ta_nosql_err *err)
{
    const char *coll = arg_coll(args, err);
    json_t     *items = json_object_get(args, "items");
    json_t     *gen = json_object_get(args, "generate");
    long long   batch = ta_nosql_json_int(args, "batch", 500);
    long long   written = 0;
    int64_t     t0;
    json_t     *chunk;

    if (coll == NULL)
        return -1;
    if (batch < 1 || batch > 10000)
        batch = 500;
    chunk = json_array();
    t0 = ta_nosql_now_us();
    if (json_is_array(items))
    {
        size_t  i;
        json_t *it;

        json_array_foreach(items, i, it)
        {
            if (arg_key(it, "key", err) == NULL || arg_doc(it, err) == NULL)
                goto fail;
            json_array_append(chunk, it);
            if ((long long)json_array_size(chunk) >= batch)
            {
                if (bulk_flush(s, coll, chunk, err) != 0)
                    goto fail;
                written += (long long)json_array_size(chunk);
                json_array_clear(chunk);
            }
        }
    }
    else if (json_is_object(gen))
    {
        long long   count = ta_nosql_json_int(gen, "count", 0);
        long long   seed = ta_nosql_json_int(gen, "seed", 1);
        long long   size = ta_nosql_json_int(gen, "size", 16);
        const char *prefix = ta_nosql_json_str(gen, "prefix", "k");
        long long   i;

        if (count < 0 || count > 10000000 || size < 0 || size > 1048576 ||
            strlen(prefix) > 64)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                             "'generate': count up to 10000000, size up to "
                             "1 MiB, prefix up to 64 bytes");
            goto fail;
        }
        for (i = 0; i < count; i++)
        {
            char    key[96];
            json_t *doc = gen_doc(i, seed, (int)size);

            if (doc == NULL)
            {
                ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM,
                                 "out of memory");
                goto fail;
            }
            snprintf(key, sizeof(key), "%s%lld", prefix, i);
            json_array_append_new(chunk, json_pack("{s:s, s:o}", "key", key,
                                                   "doc", doc));
            if ((long long)json_array_size(chunk) >= batch)
            {
                if (bulk_flush(s, coll, chunk, err) != 0)
                    goto fail;
                written += (long long)json_array_size(chunk);
                json_array_clear(chunk);
            }
        }
    }
    else
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "bulk needs 'items' or 'generate'");
        goto fail;
    }
    if (bulk_flush(s, coll, chunk, err) != 0)
        goto fail;
    written += (long long)json_array_size(chunk);
    json_decref(chunk);
    json_object_set_new(out, "written", json_integer(written));
    json_object_set_new(out, "elapsed_us",
                        json_integer(ta_nosql_now_us() - t0));
    return 0;

fail:
    json_object_set_new(out, "written", json_integer(written));
    json_decref(chunk);
    return -1;
}

static int
op_set_timeout(ta_nosql_slot *s, json_t *args, json_t *out,
               ta_nosql_err *err)
{
    long long ms = ta_nosql_json_int(args, "ms", 0);

    (void)out;
    if (ms <= 0 || ms > 3600000)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'ms' must be 1 to 3600000");
        return -1;
    }
    if (!need(s->drv, 0, s->drv->set_timeout, "change its deadline", err))
        return -1;
    if (s->drv->set_timeout(s->dc, (int)ms, err) != 0)
        return -1;
    s->cfg.timeout_ms = (int)ms;
    return 0;
}

static int
op_registry(json_t *out)
{
    json_t      *list = json_array();
    unsigned int i;

    pthread_mutex_lock(&reg_lock);
    for (i = 0; i < TA_NOSQL_SLOTS; i++)
    {
        if (slots[i].gen != 0)
            json_array_append_new(list,
                json_pack("{s:i, s:s, s:I}",
                          "handle", (int)((slots[i].gen << 8) | (i + 1)),
                          "engine", slots[i].drv->name,
                          "ops", (json_int_t)slots[i].ops));
    }
    pthread_mutex_unlock(&reg_lock);
    json_object_set_new(out, "open", json_integer((json_int_t)
                                                  json_array_size(list)));
    json_object_set_new(out, "connections", list);
    json_object_set_new(out, "pid", json_integer(getpid()));
    return 0;
}

/* The audit of a server named in the arguments rather than a handle. */
static int
op_audit_unbound(json_t *args, json_t *out, ta_nosql_err *err)
{
    const char            *engine = ta_nosql_json_str(args, "engine", NULL);
    unsigned int           bit = ta_nosql_engine_by_name(engine);
    const ta_nosql_driver *drv = ta_nosql_driver_by_engine(bit);
    ta_nosql_conf          cfg;
    int                    rc;

    if (drv == NULL)
    {
        ta_nosql_err_set(err, bit == 0 ? TA_NOSQL_ERR_INVALID
                                       : TA_NOSQL_ERR_UNSUPPORTED, 0,
                         "no %s driver", engine != NULL ? engine : "(none)");
        return -1;
    }
    if (ta_nosql_conf_parse(json_object_get(args, "config"), &cfg, err) != 0)
        return -1;
    if (cfg.port == 0)
        cfg.port = drv->default_port;
    rc = ta_nosql_audit_run(drv, &cfg, args, out, err);
    ta_nosql_err_scrub(err, &cfg);
    ta_nosql_conf_free(&cfg);
    return rc;
}

typedef int (*op_fn)(ta_nosql_slot *s, json_t *args, json_t *out,
                     ta_nosql_err *err);

static const struct {
    const char *name;
    op_fn       fn;
} ops_table[] = {
    { "put",         op_put },
    { "put_if",      op_put_if },
    { "get",         op_get },
    { "delete",      op_delete },
    { "incr",        op_incr },
    { "scan",        op_scan },
    { "find",        op_find },
    { "count",       op_count },
    { "multi",       op_multi },
    { "drop",        op_drop },
    { "native",      op_native },
    { "bulk",        op_bulk },
    { "set_timeout", op_set_timeout },
};

/* See description in ta_nosql_core.h */
int
ta_nosql_call(int handle, const char *op, const char *args_text,
              char **result, ta_nosql_err *err)
{
    ta_nosql_slot *s = NULL;
    json_t        *args;
    json_t        *out;
    int64_t        t0;
    int            rc = -1;
    size_t         i;

    memset(err, 0, sizeof(*err));
    *result = NULL;
    if (op == NULL)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0, "no operation");
        return -1;
    }
    args = ta_nosql_json_load(args_text != NULL && *args_text != '\0'
                              ? args_text : "{}",
                              args_text != NULL && *args_text != '\0'
                              ? strlen(args_text) : 2, err);
    if (args == NULL)
        return -1;
    if (!json_is_object(args))
    {
        json_decref(args);
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "the arguments are not a JSON object");
        return -1;
    }
    out = json_object();

    if (strcmp(op, "registry") == 0)
    {
        rc = op_registry(out);
        goto done;
    }
    if (handle == 0 && strcmp(op, "audit") == 0)
    {
        rc = op_audit_unbound(args, out, err);
        goto done;
    }

    s = slot_of(handle, err);
    if (s == NULL)
        goto done;
    s->ops++;
    t0 = ta_nosql_now_us();

    if (strcmp(op, "ping") == 0)
    {
        rc = s->drv->ping(s->dc, err);
        if (rc == 0)
            json_object_set_new(out, "rtt_us",
                                json_integer(ta_nosql_now_us() - t0));
    }
    else if (strcmp(op, "info") == 0)
    {
        json_t *info = info_json(s, err);

        if (info != NULL)
        {
            json_decref(out);
            out = info;
            rc = 0;
        }
    }
    else if (strcmp(op, "load") == 0)
    {
        rc = ta_nosql_load_run(s->drv, &s->cfg, args, out, err);
    }
    else if (strcmp(op, "audit") == 0)
    {
        rc = ta_nosql_audit_run(s->drv, &s->cfg, args, out, err);
    }
    else
    {
        for (i = 0; i < sizeof(ops_table) / sizeof(ops_table[0]); i++)
        {
            if (strcmp(ops_table[i].name, op) == 0)
            {
                rc = ops_table[i].fn(s, args, out, err);
                break;
            }
        }
        if (i == sizeof(ops_table) / sizeof(ops_table[0]))
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "unknown operation '%s'", op);
    }
    if (rc != 0)
    {
        classify_timeout(err, ta_nosql_now_us() - t0, s->cfg.timeout_ms);
        ta_nosql_err_scrub(err, &s->cfg);
    }

done:
    if (rc == 0)
    {
        *result = ta_nosql_json_dump(out);
        if (*result == NULL)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM,
                             "cannot encode the result");
            rc = -1;
        }
        else if (s != NULL)
        {
            ta_nosql_scrub(*result, s->cfg.password);
        }
    }
    else if (err->cls == TA_NOSQL_OK)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, 0,
                         "operation '%s' failed with no reason given", op);
    }
    json_decref(out);
    json_decref(args);
    return rc;
}
