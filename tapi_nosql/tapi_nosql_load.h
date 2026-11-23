/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: load on the agent, and its numbers as MI charts
 *
 * tapi_nosql_load() runs the "load" operation: threads on the agent,
 * each with a connection of its own made with the configuration of
 * @p conn, run one mode and record every operation's latency:
 *  - put, get: documents over a key space;
 *  - incr: one counter (final value, distinct values returned);
 *  - rmw: read-modify-write with no condition, the known-bad control
 *    of a lost-update test (updates lost);
 *  - absent: write-if-absent of one key (writes applied: one);
 *  - cas: compare-and-set of one document holding {"n"} (applied,
 *    conflicts, final n, lost = applied - final);
 *  - torn: thread 0 writes keys a and b together, the others read b
 *    then a (torn: half-applied multi-writes seen);
 *  - scan_churn: thread 0 scans the collection @p passes times while
 *    the others write and delete keys "x<n>" (missing: the @p stable
 *    keys "s<n>" a pass did not see).
 * Every result has the counts, the errors by class and the exact
 * latency percentiles.
 *
 * tapi_nosql_load_mi() logs a result as a TE MI measurement artifact
 * with a line graph of the latency percentiles, so the HTML log and
 * Bublik draw it.
 */

#ifndef __TAPI_NOSQL_LOAD_H__
#define __TAPI_NOSQL_LOAD_H__

#include "te_errno.h"

#include "tapi_nosql.h"

#ifdef __cplusplus
extern "C" {
#endif

/** What to run. */
typedef struct tapi_nosql_load_args {
    const char *mode;       /**< "put", "get", "incr" or "rmw". */
    const char *coll;       /**< Collection. */
    const char *key;        /**< incr / rmw: the one key. */
    const char *prefix;     /**< put / get: key prefix ("k"). */
    long        keys;       /**< put / get: key space (1000). */
    int         size;       /**< put: payload bytes (64). */
    int         threads;    /**< Threads (1). */
    long        ops;        /**< Operations per thread (1000). */
    long        think_us;   /**< rmw: pause between read and write. */
    long        passes;     /**< scan_churn: full scans (5). */
    long        stable;     /**< scan_churn: keys s0.. written before. */
} tapi_nosql_load_args;

/** What came out. */
typedef struct tapi_nosql_load_result {
    long long ops;          /**< Operations attempted. */
    long long errors;       /**< ... that failed. */
    double    throughput;   /**< Operations per second. */
    long long elapsed_us;   /**< Wall time. */
    long long lat_min;      /**< Latency, us. */
    long long lat_p50;
    long long lat_p90;
    long long lat_p99;
    long long lat_p999;
    long long lat_max;
    double    lat_mean;
    long long final;        /**< incr / rmw: value after the load. */
    long long succeeded;    /**< incr / rmw: operations that succeeded. */
    long long distinct;     /**< incr: distinct values returned. */
    long long lost;         /**< rmw, cas: updates lost. */
    long long applied;      /**< absent, cas: writes applied. */
    long long conflicts;    /**< cas: writes refused. */
    long long torn;         /**< torn: half-applied writes seen. */
    long long reads;        /**< torn: read pairs. */
    long long missing;      /**< scan_churn: stable keys not seen. */
    long long passes;       /**< scan_churn: passes made. */
    json_t   *raw;          /**< The whole answer (errors_by_class,
                                 first_error); json_decref() it. */
} tapi_nosql_load_result;

/** Run a load. */
extern te_errno tapi_nosql_load(tapi_nosql_conn *conn,
                                const tapi_nosql_load_args *args,
                                tapi_nosql_load_result *result);

/** Release what a result holds. */
extern void tapi_nosql_load_result_free(tapi_nosql_load_result *result);

/**
 * Log a load result as an MI artifact: throughput, error count and the
 * latency percentiles, with a line graph of latency over percentile.
 *
 * @param name    Series name (engine, mode, ...).
 * @param result  The result.
 */
extern void tapi_nosql_load_mi(const char *name,
                               const tapi_nosql_load_result *result);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_NOSQL_LOAD_H__ */
