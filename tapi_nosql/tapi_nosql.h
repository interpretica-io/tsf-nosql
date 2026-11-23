/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: database sessions on an agent
 *
 * A test talks to a NoSQL database through an agent: the connection
 * lives in the agent's RPC server, which links the engine's own client
 * library (hiredis, libmongoc, the DataStax C driver, libmemcached,
 * libcurl for the HTTP engines), and the test drives it with RPCs. The
 * engines: Redis, MongoDB, Cassandra, Elasticsearch, Neo4j, Memcached
 * and etcd.
 *
 * One model for all of them: a collection (a lower-case identifier)
 * holds documents (JSON objects) under string keys. A document reads
 * back equal to what was written, whatever the engine stores it as.
 * What an engine cannot do natively it does not pretend to do: such an
 * operation fails with TE_EOPNOTSUPP (class TAPI_NOSQL_ERR_UNSUPPORTED),
 * and tapi_nosql_has_cap() says beforehand. Values are jansson json_t.
 *
 * Every function returns a status code. An agent-side failure is mapped
 * from its class (see tapi_nosql_errno()), and the class and message of
 * the last failure on a connection stay in it (tapi_nosql_last_cls(),
 * tapi_nosql_last_msg()), so a test can tell a timeout from a refused
 * login without parsing text. The password of a configuration never
 * appears in a log, a result or an error message.
 *
 * Authorized use only: test servers you own or are engaged to test.
 */

#ifndef __TAPI_NOSQL_H__
#define __TAPI_NOSQL_H__

#include <stdbool.h>

#include <jansson.h>

#include "te_errno.h"
#include "rcf_rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/** @name Engines (bits of tapi_nosql_available()), as on the agent
 * @{ */
#define TAPI_NOSQL_ENGINE_REDIS     0x01u
#define TAPI_NOSQL_ENGINE_MONGODB   0x02u
#define TAPI_NOSQL_ENGINE_CASSANDRA 0x04u
#define TAPI_NOSQL_ENGINE_ELASTIC   0x08u
#define TAPI_NOSQL_ENGINE_NEO4J     0x10u
#define TAPI_NOSQL_ENGINE_MEMCACHED 0x20u
#define TAPI_NOSQL_ENGINE_ETCD      0x40u
/** @} */

/** Error classes, as on the agent (TA_NOSQL_ERR_*). */
typedef enum tapi_nosql_errcls {
    TAPI_NOSQL_OK = 0,
    TAPI_NOSQL_ERR_CONNECT,     /**< Cannot reach the server. */
    TAPI_NOSQL_ERR_AUTH,        /**< Credentials refused. */
    TAPI_NOSQL_ERR_PERMISSION,  /**< Authenticated, not allowed. */
    TAPI_NOSQL_ERR_TLS,         /**< TLS failed. */
    TAPI_NOSQL_ERR_TIMEOUT,     /**< A deadline passed. */
    TAPI_NOSQL_ERR_INVALID,     /**< Bad request. */
    TAPI_NOSQL_ERR_UNSUPPORTED, /**< The engine cannot do that. */
    TAPI_NOSQL_ERR_UNAVAILABLE, /**< Up but not serving. */
    TAPI_NOSQL_ERR_LIMIT,       /**< A size or count limit. */
    TAPI_NOSQL_ERR_PROTOCOL,    /**< Unexpected answer. */
    TAPI_NOSQL_ERR_HANDLE,      /**< No such connection. */
    TAPI_NOSQL_ERR_INTERNAL,    /**< Out of memory, a bug. */
    TAPI_NOSQL_ERR_RPC,         /**< The RPC itself failed (engine side
                                     only, never from the agent). */
} tapi_nosql_errcls;

/** A connection to a database, held by an RPC server on an agent. */
typedef struct tapi_nosql_conn {
    rcf_rpc_server   *rpcs;     /**< The RPC server holding it. */
    int               handle;   /**< Its handle there. */
    char             *engine;   /**< Engine name. */
    json_t           *info;     /**< What nosql_open returned: engine,
                                     server_version, tls, caps, ... */
    tapi_nosql_errcls last_cls; /**< Class of the last failure. */
    char             *last_msg; /**< Its message (or NULL). */
} tapi_nosql_conn;

/** Stable name of an error class ("timeout", "auth", ...). */
extern const char *tapi_nosql_errcls_name(tapi_nosql_errcls cls);

/** The te_errno a class maps to (TE_ETIMEDOUT for a timeout, ...). */
extern te_errno tapi_nosql_errno(tapi_nosql_errcls cls);

/**
 * Which engines the agent's RPC server has.
 *
 * @param[in]  rpcs     RPC server.
 * @param[out] engines  TAPI_NOSQL_ENGINE_* bits (may be NULL).
 * @param[out] info     {"engines":[{name, client, caps}]} (may be NULL).
 *
 * @return Status code.
 */
extern te_errno tapi_nosql_available(rcf_rpc_server *rpcs,
                                     unsigned int *engines, json_t **info);

/** Does the agent have a driver for @p engine? (false on any error) */
extern bool tapi_nosql_supports(rcf_rpc_server *rpcs, const char *engine);

/**
 * Open a connection.
 *
 * @param[in]  rpcs    RPC server that will hold it.
 * @param[in]  engine  "redis", "mongodb", "cassandra", "elasticsearch",
 *                     "neo4j", "memcached" or "etcd".
 * @param[in]  config  host, port, user, password, database, tls (true
 *                     or {enabled, ca, verify}), timeout_ms,
 *                     connect_timeout_ms, and engine extras
 *                     (replica_set, auth_db, index_prefix, ...).
 * @param[out] conn    The connection (tapi_nosql_close() it).
 * @param[out] cls     Class of a failure (may be NULL).
 *
 * @return Status code.
 */
extern te_errno tapi_nosql_open(rcf_rpc_server *rpcs, const char *engine,
                                const json_t *config, tapi_nosql_conn **conn,
                                tapi_nosql_errcls *cls);

/** Close a connection and free it (NULL is fine). */
extern te_errno tapi_nosql_close(tapi_nosql_conn *conn);

/**
 * Run any operation (see the README for the list).
 *
 * @param[in]  conn    Connection.
 * @param[in]  op      Operation name.
 * @param[in]  args    Its arguments (NULL: none).
 * @param[out] result  Its result (may be NULL); json_decref() it.
 *
 * @return Status code.
 */
extern te_errno tapi_nosql_call(tapi_nosql_conn *conn, const char *op,
                                const json_t *args, json_t **result);

/**
 * The same with the arguments packed in place: a json_pack() format
 * and its values.
 */
extern te_errno tapi_nosql_callf(tapi_nosql_conn *conn, const char *op,
                                 json_t **result, const char *fmt, ...);

/** Class of the last failure on @p conn. */
extern tapi_nosql_errcls tapi_nosql_last_cls(const tapi_nosql_conn *conn);

/** Message of the last failure on @p conn ("" if none). */
extern const char *tapi_nosql_last_msg(const tapi_nosql_conn *conn);

/** Does the engine do @p cap ("scan", "find", "ttl", "cas", ...)? */
extern bool tapi_nosql_has_cap(const tapi_nosql_conn *conn, const char *cap);

/** The server's version as the server reported it. */
extern const char *tapi_nosql_server_version(const tapi_nosql_conn *conn);

/** Is the connection encrypted? */
extern bool tapi_nosql_tls(const tapi_nosql_conn *conn);

/** One round trip; its time. */
extern te_errno tapi_nosql_ping(tapi_nosql_conn *conn, long long *rtt_us);

/**
 * Store @p doc under @p key.
 *
 * @param ttl_s    Expire after that many seconds (0: never).
 * @param version  The new version, if the engine gives one cheaply
 *                 (may be NULL; free() it).
 */
extern te_errno tapi_nosql_put(tapi_nosql_conn *conn, const char *coll,
                               const char *key, const json_t *doc,
                               long ttl_s, char **version);

/** Store @p doc only if @p key is absent; @p applied says if it did. */
extern te_errno tapi_nosql_put_if_absent(tapi_nosql_conn *conn,
                                         const char *coll, const char *key,
                                         const json_t *doc, long ttl_s,
                                         bool *applied);

/**
 * Store @p doc only if @p key is still at @p version (compare and set).
 * @p new_version may be NULL.
 */
extern te_errno tapi_nosql_put_if_version(tapi_nosql_conn *conn,
                                          const char *coll, const char *key,
                                          const json_t *doc,
                                          const char *version, bool *applied,
                                          char **new_version);

/**
 * Read @p key. @p doc is NULL when the key is absent (that is not an
 * error); @p version may be NULL.
 */
extern te_errno tapi_nosql_get(tapi_nosql_conn *conn, const char *coll,
                               const char *key, json_t **doc,
                               char **version);

/** Remove @p key; @p deleted (may be NULL) says if it was there. */
extern te_errno tapi_nosql_delete(tapi_nosql_conn *conn, const char *coll,
                                  const char *key, bool *deleted);

/** Add @p by to a counter (0 reads it); @p value after (may be NULL). */
extern te_errno tapi_nosql_incr(tapi_nosql_conn *conn, const char *coll,
                                const char *key, long long by,
                                long long *value);

/**
 * One page of a collection's keys.
 *
 * @param cursor  NULL for the first page, then the previous @p next.
 * @param keys    Array of strings (may be NULL).
 * @param next    Where the next page starts; NULL at the end (may be
 *                NULL: the first page only).
 */
extern te_errno tapi_nosql_scan(tapi_nosql_conn *conn, const char *coll,
                                int limit, const char *cursor,
                                json_t **keys, char **next);

/**
 * Every key of a collection, page by page. @p keys holds each key once
 * (an engine may return a key twice while it scans: @p repeats counts
 * those). @p pages and @p largest (the largest page) may be NULL.
 */
extern te_errno tapi_nosql_scan_all(tapi_nosql_conn *conn, const char *coll,
                                    int limit, json_t **keys,
                                    unsigned int *pages, unsigned int *largest,
                                    unsigned int *repeats);

/**
 * One page of the documents whose top-level fields equal those of
 * @p filter (strings, integers, booleans). @p docs is an array of
 * {"key":..,"doc":..}.
 */
extern te_errno tapi_nosql_find(tapi_nosql_conn *conn, const char *coll,
                                const json_t *filter, int limit,
                                const char *cursor, json_t **docs,
                                char **next);

/** Every matching document, page by page. */
extern te_errno tapi_nosql_find_all(tapi_nosql_conn *conn, const char *coll,
                                    const json_t *filter, int limit,
                                    json_t **docs, unsigned int *pages);

/** How many documents (matching @p filter, NULL: all). */
extern te_errno tapi_nosql_count(tapi_nosql_conn *conn, const char *coll,
                                 const json_t *filter, long long *n);

/**
 * Several writes, all or none. @p ops is an array of
 * {"op":"put"|"delete","coll","key","doc"}. With @p if_coll / @p if_key
 * set, only if that key is absent.
 */
extern te_errno tapi_nosql_multi(tapi_nosql_conn *conn, const json_t *ops,
                                 const char *if_coll, const char *if_key,
                                 bool *applied);

/** Remove a collection. */
extern te_errno tapi_nosql_drop(tapi_nosql_conn *conn, const char *coll);

/**
 * The engine's own command language (see the README for each engine's
 * form: an argv for Redis, a command document for MongoDB, CQL, Cypher,
 * an HTTP request for Elasticsearch and etcd, "stats" for Memcached).
 */
extern te_errno tapi_nosql_native(tapi_nosql_conn *conn, const json_t *cmd,
                                  json_t **reply);

/**
 * Write @p count generated documents ("<prefix><i>" ->
 * {"i", "name": "item-<i>", "group": i % 10, "payload": @p size letters
 * from @p seed}) in batches.
 */
extern te_errno tapi_nosql_bulk_generate(tapi_nosql_conn *conn,
                                         const char *coll, long count,
                                         int size, const char *prefix,
                                         long seed, long *written,
                                         long long *elapsed_us);

/** The deadline of every further operation. */
extern te_errno tapi_nosql_set_timeout(tapi_nosql_conn *conn, int ms);

/** What the server says about itself (engine, version, tls, caps). */
extern te_errno tapi_nosql_info(tapi_nosql_conn *conn, json_t **info);

/** The connections the RPC server holds: {"open": n, "connections"}. */
extern te_errno tapi_nosql_registry(rcf_rpc_server *rpcs, json_t **reg);

/**
 * Generated document @p i as tapi_nosql_bulk_generate() writes it: a
 * test knows what it should read without having sent it.
 */
extern json_t *tapi_nosql_generated_doc(long i, long seed, int size);

/** A document from JSON text (NULL on a syntax error). */
extern json_t *tapi_nosql_json(const char *text);

/** JSON text of @p v, compact, keys sorted (free() it). */
extern char *tapi_nosql_json_text(const json_t *v);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_NOSQL_H__ */
