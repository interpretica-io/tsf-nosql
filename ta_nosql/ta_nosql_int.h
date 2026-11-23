/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql internals: the driver seam and the helpers
 *
 * ta_nosql_core.c owns the handles, parses and checks the arguments of
 * an operation and formats its result; a driver is one translation unit
 * that maps the operations below onto one engine. A driver is compiled
 * in only when its client library is found, and nothing outside its
 * unit names that library. An operation a driver leaves NULL is one the
 * engine does not do: the core answers TA_NOSQL_ERR_UNSUPPORTED.
 *
 * Every operation returns 0 on success and -1 on failure with @c err
 * filled. A driver never logs.
 *
 * The data model every driver keeps:
 *  - a collection (lower-case identifier) is a key prefix, a table, a
 *    collection, an index or a node label, whatever the engine has;
 *  - a document is a JSON object stored under a key in a collection;
 *    it reads back equal (jansson json_equal()) to what was put;
 *  - a version is an opaque token a get returns and a conditional put
 *    compares; it changes on every write of the key;
 *  - a counter is an integer under a key, changed by incr only.
 */

#ifndef __TA_NOSQL_INT_H__
#define __TA_NOSQL_INT_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <jansson.h>

#include "ta_nosql_core.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Largest page a scan or find returns. */
#define TA_NOSQL_PAGE_MAX       1000
/** Longest key accepted. */
#define TA_NOSQL_KEY_MAX        200
/** Largest document accepted (encoded). */
#define TA_NOSQL_DOC_MAX        (8 * 1024 * 1024)

/** A connection's parameters, parsed from the configuration JSON. */
typedef struct ta_nosql_conf {
    char   *host;           /**< Server address. */
    int     port;           /**< Port (0: the engine's default). */
    char   *user;           /**< User name or @c NULL. */
    char   *password;       /**< Password or @c NULL. */
    char   *database;       /**< Database / keyspace / index prefix. */
    bool    tls;            /**< Use TLS. */
    char   *tls_ca;         /**< CA file to trust, or @c NULL. */
    bool    tls_verify;     /**< Verify the server certificate. */
    int     timeout_ms;     /**< Deadline of one operation. */
    int     connect_timeout_ms; /**< Deadline of a connection. */
    json_t *raw;            /**< The whole configuration (extras). */
} ta_nosql_conf;

/** A database driver: an engine's properties and operations. */
typedef struct ta_nosql_driver {
    unsigned int engine;        /**< TA_NOSQL_ENGINE_* bit. */
    const char  *name;          /**< "redis", "mongodb", ... */
    unsigned int caps;          /**< TA_NOSQL_CAP_* of the engine. */
    int          default_port;  /**< Port when the configuration has 0. */
    /**
     * Well-known credentials ("user:password", NULL-terminated) the
     * audit tries when asked to; NULL: none.
     */
    const char *const *default_creds;

    /** Write the client library's name and version. */
    void (*client_version)(char *buf, size_t len);

    /** Open a connection; @p dc receives the driver's object. */
    int (*connect)(const ta_nosql_conf *cfg, void **dc, ta_nosql_err *err);
    /** Close a connection. */
    void (*disconnect)(void *dc);
    /** One round trip. */
    int (*ping)(void *dc, ta_nosql_err *err);
    /**
     * Add to @p out what the server says about itself:
     * "server_version" at least.
     */
    int (*info)(void *dc, json_t *out, ta_nosql_err *err);
    /** Is the connection encrypted? */
    bool (*tls_in_use)(void *dc);

    /**
     * Store @p doc under @p key. @p ttl_s > 0: expire after that many
     * seconds. @p ver receives the new version (malloc'ed) if the engine
     * gives one cheaply, else NULL.
     */
    int (*put)(void *dc, const char *coll, const char *key, json_t *doc,
               long ttl_s, char **ver, ta_nosql_err *err);
    /**
     * Store @p doc only if @p key is absent (@p if_ver NULL) or still at
     * version @p if_ver. Not applying is a result (@p applied false),
     * not an error.
     */
    int (*put_if)(void *dc, const char *coll, const char *key, json_t *doc,
                  long ttl_s, const char *if_ver, bool *applied, char **ver,
                  ta_nosql_err *err);
    /** Read @p key: @p doc NULL when absent; @p ver its version. */
    int (*get)(void *dc, const char *coll, const char *key, json_t **doc,
               char **ver, ta_nosql_err *err);
    /** Remove @p key; @p deleted says whether it was there. */
    int (*del)(void *dc, const char *coll, const char *key, bool *deleted,
               ta_nosql_err *err);
    /** Add @p by to the counter @p key (0 if absent); @p value after. */
    int (*incr)(void *dc, const char *coll, const char *key, long long by,
                long long *value, ta_nosql_err *err);
    /**
     * One page of a collection's keys, appended to the array @p keys,
     * from @p cursor (NULL: the start); @p next NULL at the end.
     */
    int (*scan)(void *dc, const char *coll, int limit, const char *cursor,
                json_t *keys, char **next, ta_nosql_err *err);
    /**
     * One page of the documents whose top-level fields equal those of
     * @p filter (an object of scalars), appended to @p docs as
     * {"key":..,"doc":..}.
     */
    int (*find)(void *dc, const char *coll, json_t *filter, int limit,
                const char *cursor, json_t *docs, char **next,
                ta_nosql_err *err);
    /** How many documents (matching @p filter, if not NULL). */
    int (*count)(void *dc, const char *coll, json_t *filter, long long *n,
                 ta_nosql_err *err);
    /**
     * Apply @p ops (an array of {"op":"put"|"delete","coll","key","doc"})
     * all or none. With @p if_coll / @p if_key set, only if that key is
     * absent; @p applied says whether they were applied.
     */
    int (*multi)(void *dc, json_t *ops, const char *if_coll,
                 const char *if_key, bool *applied, ta_nosql_err *err);
    /** Remove a whole collection. */
    int (*drop)(void *dc, const char *coll, ta_nosql_err *err);
    /** The engine's own command language; @p reply is new JSON. */
    int (*native)(void *dc, json_t *cmd, json_t **reply, ta_nosql_err *err);
    /**
     * Store many documents (an array of {"key","doc"}) in as few round
     * trips as the engine allows. NULL: the core puts them one by one.
     */
    int (*bulk)(void *dc, const char *coll, json_t *items, ta_nosql_err *err);
    /** Change the deadline of an operation. */
    int (*set_timeout)(void *dc, int ms, ta_nosql_err *err);

    /**
     * Engine-specific security checks. Append findings
     * ({"check","severity","subject","detail"}) to @p findings and the
     * names of the checks run to @p checked. @p dc is a connection made
     * with @p cfg, or NULL when the configuration could not connect.
     */
    int (*audit)(const ta_nosql_conf *cfg, void *dc, json_t *args,
                 json_t *findings, json_t *checked, ta_nosql_err *err);

    /** Per-thread set-up of the client library (load threads). */
    void (*thread_init)(void);
} ta_nosql_driver;

#ifdef TA_NOSQL_HAVE_REDIS
extern const ta_nosql_driver ta_nosql_redis_driver;
#endif
#ifdef TA_NOSQL_HAVE_MONGODB
extern const ta_nosql_driver ta_nosql_mongo_driver;
#endif
#ifdef TA_NOSQL_HAVE_CASSANDRA
extern const ta_nosql_driver ta_nosql_cassandra_driver;
#endif
#ifdef TA_NOSQL_HAVE_MEMCACHED
extern const ta_nosql_driver ta_nosql_memcached_driver;
#endif
#ifdef TA_NOSQL_HAVE_CURL
extern const ta_nosql_driver ta_nosql_elastic_driver;
extern const ta_nosql_driver ta_nosql_neo4j_driver;
extern const ta_nosql_driver ta_nosql_etcd_driver;
#endif

/* ---- helpers (ta_nosql_util.c) -------------------------------------- */

/** Fill an error. */
extern void ta_nosql_err_set(ta_nosql_err *err, ta_nosql_errcls cls,
                             long native, const char *fmt, ...)
                             __attribute__((format(printf, 4, 5)));

/** Encode JSON the one way the module does (compact, 17 digits). */
extern char *ta_nosql_json_dump(const json_t *v);

/** Parse JSON text; NULL with @p err (INVALID) on a syntax error. */
extern json_t *ta_nosql_json_load(const char *text, size_t len,
                                  ta_nosql_err *err);

/** A string member, or @p dflt. */
extern const char *ta_nosql_json_str(const json_t *obj, const char *name,
                                     const char *dflt);

/** An integer member, or @p dflt. */
extern long long ta_nosql_json_int(const json_t *obj, const char *name,
                                   long long dflt);

/** A boolean member, or @p dflt. */
extern bool ta_nosql_json_bool(const json_t *obj, const char *name,
                               bool dflt);

/** malloc'ed printf. */
extern char *ta_nosql_strdupf(const char *fmt, ...)
                              __attribute__((format(printf, 1, 2)));

/** Base64 (standard alphabet, padded); malloc'ed. */
extern char *ta_nosql_b64_encode(const void *data, size_t len);

/** Decode base64; malloc'ed, NUL-terminated, length in @p len. */
extern char *ta_nosql_b64_decode(const char *text, size_t *len);

/** Monotonic microseconds. */
extern int64_t ta_nosql_now_us(void);

/** A new version token unique within the process and time. */
extern char *ta_nosql_new_version(void);

/** Is @p s a collection name (^[a-z][a-z0-9_]{0,47}$)? */
extern bool ta_nosql_coll_ok(const char *s);

/** Remove every occurrence of @p secret from @p buf (in place). */
extern void ta_nosql_scrub(char *buf, const char *secret);

/** Copy a configuration (deep). */
extern int ta_nosql_conf_copy(const ta_nosql_conf *src, ta_nosql_conf *dst);

/** Parse a configuration object. */
extern int ta_nosql_conf_parse(json_t *raw, ta_nosql_conf *cfg,
                               ta_nosql_err *err);

/** Free a configuration's strings. */
extern void ta_nosql_conf_free(ta_nosql_conf *cfg);

/** Add a finding to an audit's array. */
extern void ta_nosql_finding(json_t *findings, const char *check,
                             int severity, const char *subject,
                             const char *fmt, ...)
                             __attribute__((format(printf, 5, 6)));

/** @name Severities of a finding (the tsf-cybersec scale)
 * @{ */
#define TA_NOSQL_SEV_INFO       0
#define TA_NOSQL_SEV_LOW        1
#define TA_NOSQL_SEV_MEDIUM     2
#define TA_NOSQL_SEV_HIGH       3
#define TA_NOSQL_SEV_CRITICAL   4
/** @} */

/* ---- the HTTP client of the HTTP engines (ta_nosql_http.c) ---------- */

#ifdef TA_NOSQL_HAVE_CURL
/** One HTTP endpoint: a base URL and how to authenticate. */
typedef struct ta_nosql_http {
    void   *curl;           /**< CURL easy handle. */
    char   *base;           /**< "http(s)://host:port". */
    char   *user;           /**< Basic user, or NULL. */
    char   *password;       /**< Basic password, or NULL. */
    char   *bearer;         /**< "Authorization:" value, or NULL. */
    char   *ca;             /**< CA file, or NULL. */
    bool    verify;         /**< Verify the server. */
    bool    tls;            /**< https. */
    int     timeout_ms;     /**< Request deadline. */
    int     connect_timeout_ms; /**< Connect deadline. */
} ta_nosql_http;

/** The libcurl version ("8.5.0"). */
extern const char *ta_nosql_http_client(void);

/** Set up an endpoint from a configuration. */
extern int ta_nosql_http_init(ta_nosql_http *h, const ta_nosql_conf *cfg,
                              int default_port, ta_nosql_err *err);

/** Tear an endpoint down. */
extern void ta_nosql_http_fini(ta_nosql_http *h);

/**
 * One request. @p body NULL: none. @p status receives the HTTP status;
 * @p reply the body parsed (NULL when empty or not JSON, then @p raw
 * has it if asked for). A transport failure is an error (CONNECT, TLS,
 * TIMEOUT); any HTTP status is a success of this call.
 */
extern int ta_nosql_http_req(ta_nosql_http *h, const char *method,
                             const char *path, const json_t *body,
                             const char *content_type, long *status,
                             json_t **reply, char **raw, ta_nosql_err *err);

/** The same with a body that is already text (NDJSON and the like). */
extern int ta_nosql_http_req_text(ta_nosql_http *h, const char *method,
                                  const char *path, const char *body,
                                  const char *content_type, long *status,
                                  json_t **reply, char **raw,
                                  ta_nosql_err *err);

/** Escape a path segment (RFC 3986 unreserved kept); malloc'ed. */
extern char *ta_nosql_http_escape(const char *s);

/** The error class of an HTTP status (401 AUTH, 403 PERMISSION ...). */
extern ta_nosql_errcls ta_nosql_http_cls(long status);
#endif

/* ---- shared by the core's own units --------------------------------- */

/** The driver of an engine bit; NULL when not compiled in. */
extern const ta_nosql_driver *ta_nosql_driver_by_engine(unsigned int engine);

/** The capability names of a bitmask, as a new JSON array. */
extern json_t *ta_nosql_caps_json(unsigned int caps);

/**
 * Run a load ("load" operation, ta_nosql_load.c): threads of their own
 * connections made from @p cfg hammer one engine; @p out receives the
 * counts and the latency distribution.
 */
extern int ta_nosql_load_run(const ta_nosql_driver *drv,
                             const ta_nosql_conf *cfg, json_t *args,
                             json_t *out, ta_nosql_err *err);

/**
 * Run the security checks ("audit" operation, ta_nosql_audit.c) of the
 * server @p cfg names; @p out receives "findings" and "checked".
 */
extern int ta_nosql_audit_run(const ta_nosql_driver *drv,
                              const ta_nosql_conf *cfg, json_t *args,
                              json_t *out, ta_nosql_err *err);

/**
 * Connect, then scrub and classify a failure the way the core does for
 * every operation (the load and the audit connect on their own).
 */
extern int ta_nosql_connect_checked(const ta_nosql_driver *drv,
                                    const ta_nosql_conf *cfg, void **dc,
                                    ta_nosql_err *err);

/** Scrub @p err of the configuration's password. */
extern void ta_nosql_err_scrub(ta_nosql_err *err, const ta_nosql_conf *cfg);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TA_NOSQL_INT_H__ */
