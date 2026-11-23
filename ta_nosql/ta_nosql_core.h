/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL database sessions: the agent-side core
 *
 * Plain C with no TE header, so it builds and is tested on its own (see
 * ci/). ta_nosql.c is the TE glue on top of it; the RPC server calls it
 * in-process. Nothing is spawned: every engine is reached over its own
 * client library (hiredis, libmongoc, the DataStax C driver,
 * libmemcached) or, for the engines whose interface is HTTP
 * (Elasticsearch, Neo4j, etcd), over libcurl.
 *
 * The interface is uniform: a connection is opened from a JSON
 * configuration and gets an integer handle; every operation is named by
 * a string and takes and returns a JSON object. The documents a test
 * stores are JSON, whatever the engine keeps them as, so a value put
 * into Redis and the same value put into MongoDB read back equal.
 *
 * A handle belongs to the process that opened it: it carries a random
 * generation, so a handle from an RPC server that has since restarted
 * is refused rather than mistaken for a new connection.
 *
 * Secrets: the password of a configuration is never written into a
 * result or an error message. Messages are scrubbed of it before they
 * leave the core, whatever the client library put in them.
 */

#ifndef __TA_NOSQL_CORE_H__
#define __TA_NOSQL_CORE_H__

#ifdef __cplusplus
extern "C" {
#endif

/** @name Engines (bits)
 * @{ */
#define TA_NOSQL_ENGINE_REDIS       0x01u   /**< Redis / Valkey. */
#define TA_NOSQL_ENGINE_MONGODB     0x02u   /**< MongoDB. */
#define TA_NOSQL_ENGINE_CASSANDRA   0x04u   /**< Apache Cassandra. */
#define TA_NOSQL_ENGINE_ELASTIC     0x08u   /**< Elasticsearch. */
#define TA_NOSQL_ENGINE_NEO4J       0x10u   /**< Neo4j. */
#define TA_NOSQL_ENGINE_MEMCACHED   0x20u   /**< Memcached. */
#define TA_NOSQL_ENGINE_ETCD        0x40u   /**< etcd (v3 API). */
/** @} */

/** @name Capabilities (bits): what an engine does natively
 * @{ */
#define TA_NOSQL_CAP_SCAN       0x0001u /**< List a collection's keys. */
#define TA_NOSQL_CAP_FIND       0x0002u /**< Equality query on fields. */
#define TA_NOSQL_CAP_COUNT      0x0004u /**< Count a collection. */
#define TA_NOSQL_CAP_TTL        0x0008u /**< Entries that expire. */
#define TA_NOSQL_CAP_CAS        0x0010u /**< Write if unchanged. */
#define TA_NOSQL_CAP_ABSENT     0x0020u /**< Write if absent. */
#define TA_NOSQL_CAP_INCR       0x0040u /**< Atomic counter. */
#define TA_NOSQL_CAP_INCR_EXACT 0x0080u /**< The counter returns the value
                                             its own increment made. */
#define TA_NOSQL_CAP_MULTI      0x0100u /**< Atomic multi-key write. */
#define TA_NOSQL_CAP_MULTI_IF   0x0200u /**< ... under a condition. */
#define TA_NOSQL_CAP_DURABLE    0x0400u /**< Data survives a restart. */
#define TA_NOSQL_CAP_TLS        0x0800u /**< The driver can use TLS. */
#define TA_NOSQL_CAP_AUTH       0x1000u /**< The driver authenticates. */
/** @} */

/** Error classes: what kind of failure, whatever the engine. */
typedef enum ta_nosql_errcls {
    TA_NOSQL_OK = 0,            /**< No error. */
    TA_NOSQL_ERR_CONNECT,       /**< Cannot reach the server. */
    TA_NOSQL_ERR_AUTH,          /**< Credentials refused. */
    TA_NOSQL_ERR_PERMISSION,    /**< Authenticated, not allowed. */
    TA_NOSQL_ERR_TLS,           /**< TLS failed (handshake, trust). */
    TA_NOSQL_ERR_TIMEOUT,       /**< A deadline passed. */
    TA_NOSQL_ERR_INVALID,       /**< Bad request (arguments, query). */
    TA_NOSQL_ERR_UNSUPPORTED,   /**< The engine cannot do that. */
    TA_NOSQL_ERR_UNAVAILABLE,   /**< Server up but not serving. */
    TA_NOSQL_ERR_LIMIT,         /**< A size or count limit. */
    TA_NOSQL_ERR_PROTOCOL,      /**< Unexpected answer. */
    TA_NOSQL_ERR_HANDLE,        /**< No such handle (or stale). */
    TA_NOSQL_ERR_INTERNAL,      /**< Out of memory, a bug. */
} ta_nosql_errcls;

/** A failure: its class, the engine's own code, a message. */
typedef struct ta_nosql_err {
    ta_nosql_errcls cls;    /**< Error class. */
    long            native; /**< Engine / library code, 0 if none. */
    char            msg[512];   /**< Message, scrubbed of secrets. */
} ta_nosql_err;

/** Stable name of an error class ("timeout", "auth", ...). */
extern const char *ta_nosql_errcls_name(ta_nosql_errcls cls);

/** Engine bit of a name ("redis", "mongodb", ...); 0 if unknown. */
extern unsigned int ta_nosql_engine_by_name(const char *name);

/** Name of an engine bit; @c NULL if unknown. */
extern const char *ta_nosql_engine_name(unsigned int engine);

/**
 * The engines compiled in, with their client libraries and
 * capabilities, as a JSON object:
 * {"engines":[{"name":..,"client":..,"caps":[..]}, ..]}.
 *
 * @param[out] engines  Bitmask of the engines compiled in.
 * @param[out] json     The description; free() it.
 *
 * @return 0, or -1 when out of memory.
 */
extern int ta_nosql_available(unsigned int *engines, char **json);

/**
 * Open a connection.
 *
 * @param[in]  engine  Engine name.
 * @param[in]  config  JSON object: host, port, user, password, database,
 *                     tls {enabled, ca, verify}, timeout_ms,
 *                     connect_timeout_ms, and engine extras.
 * @param[out] handle  The connection's handle.
 * @param[out] result  JSON: server version, TLS in use, caps; free() it.
 * @param[out] err     The failure.
 *
 * @return 0 or -1.
 */
extern int ta_nosql_open(const char *engine, const char *config,
                         int *handle, char **result, ta_nosql_err *err);

/**
 * Close a connection.
 *
 * @return 0, or -1 with @c TA_NOSQL_ERR_HANDLE.
 */
extern int ta_nosql_close(int handle, ta_nosql_err *err);

/**
 * Run an operation.
 *
 * @param[in]  handle  The connection, or 0 for an operation that needs
 *                     none ("registry", "audit" with an engine and a
 *                     configuration in @p args).
 * @param[in]  op      Operation name (see the README for the list).
 * @param[in]  args    JSON object of arguments ("{}" or @c NULL: none).
 * @param[out] result  JSON object; free() it. Set on success only.
 * @param[out] err     The failure.
 *
 * @return 0 or -1.
 */
extern int ta_nosql_call(int handle, const char *op, const char *args,
                         char **result, ta_nosql_err *err);

/** Close every connection (the RPC server is going away). */
extern void ta_nosql_close_all(void);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TA_NOSQL_CORE_H__ */
