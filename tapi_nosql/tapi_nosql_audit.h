/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: what a stranger can do to a database server
 *
 * The audit runs on the agent, from the outside, with connections of
 * its own. Checks every engine gets:
 * - @c nosql.auth.none (HIGH) - a client with no credentials is served
 *   (a read of an absent key is answered instead of refused);
 * - @c nosql.auth.default (HIGH) - a well-known default credential
 *   logs in; tried only when asked (@p default_creds), being a login
 *   attempt;
 * - @c nosql.tls.plaintext (MEDIUM) - the server takes unencrypted
 *   connections;
 * and the engine's own:
 * - @c redis.config-access (MEDIUM), @c redis.protected-mode (MEDIUM);
 * - @c mongodb.superuser, @c cassandra.superuser,
 *   @c elasticsearch.superuser (MEDIUM) - the account given is a
 *   server-wide administrator;
 * - @c memcached.udp-reflection (HIGH) - UDP answers, with the
 *   amplification factor in the detail;
 * - @c neo4j.version-disclosed, @c etcd.open/version,
 *   @c etcd.open/metrics (LOW) - served to anyone.
 * A check that could not decide reports @c *.not-assessed (INFO).
 *
 * Findings carry a stable check identifier and a subject (host:port),
 * the tsf-cybersec model; tapi_nosql_cybersec.h moves a report into a
 * tapi_cybersec_report for a suite that builds tsf-cybersec. tapi_nosql
 * itself does not depend on it. No finding carries a password.
 *
 * Authorized use only: run it against servers you own or are engaged
 * to test.
 */

#ifndef __TAPI_NOSQL_AUDIT_H__
#define __TAPI_NOSQL_AUDIT_H__

#include <stdbool.h>

#include "te_errno.h"
#include "rcf_rpc.h"

#include "tapi_nosql.h"

#ifdef __cplusplus
extern "C" {
#endif

/** How much a finding matters (the scale of tsf-cybersec). */
typedef enum tapi_nosql_severity {
    TAPI_NOSQL_SEV_INFO = 0,    /**< Worth recording, not acting on. */
    TAPI_NOSQL_SEV_LOW,         /**< Needs something else to go wrong. */
    TAPI_NOSQL_SEV_MEDIUM,      /**< Makes an attack easier. */
    TAPI_NOSQL_SEV_HIGH,        /**< A missing defence. */
    TAPI_NOSQL_SEV_CRITICAL,    /**< Something is wrong right now. */
} tapi_nosql_severity;

/** One thing a check found. */
typedef struct tapi_nosql_finding {
    tapi_nosql_severity severity;   /**< How much it matters. */
    char               *check;      /**< Stable identifier. */
    char               *subject;    /**< What it is about ("host:port"). */
    char               *detail;     /**< Free text for the log. */
} tapi_nosql_finding;

/** Everything the checks found. */
typedef struct tapi_nosql_report {
    tapi_nosql_finding *items;      /**< Findings, in the order found. */
    unsigned int        n;          /**< How many. */
    char               *engine;     /**< Engine audited. */
    char               *subject;    /**< Server audited. */
} tapi_nosql_report;

/** An empty report. */
#define TAPI_NOSQL_REPORT_INIT { NULL, 0, NULL, NULL }

/**
 * Audit a server named by a configuration (no connection needed: the
 * audit makes its own).
 *
 * @param rpcs           RPC server on the agent that reaches the server.
 * @param engine         Engine name.
 * @param config         Configuration of the account to audit with.
 * @param default_creds  Also try the engine's well-known credentials.
 * @param report         Filled with the findings (tapi_nosql_report_free
 *                       it).
 *
 * @return Status code.
 */
extern te_errno tapi_nosql_audit(rcf_rpc_server *rpcs, const char *engine,
                                 const json_t *config, bool default_creds,
                                 tapi_nosql_report *report);

/** Count the findings of at least @p min_severity. */
extern unsigned int tapi_nosql_report_count(const tapi_nosql_report *report,
                                            tapi_nosql_severity min_severity);

/** Does the report have a finding of @p check? */
extern bool tapi_nosql_report_has(const tapi_nosql_report *report,
                                  const char *check);

/** The finding of @p check, or NULL. */
extern const tapi_nosql_finding *tapi_nosql_report_get(
                                    const tapi_nosql_report *report,
                                    const char *check);

/** Log every finding (RING), most severe first. */
extern void tapi_nosql_report_log(const tapi_nosql_report *report);

/**
 * The checks that found something at or above @p min_severity, sorted,
 * comma-separated: a stable string for a verdict (free() it).
 */
extern char *tapi_nosql_report_checks(const tapi_nosql_report *report,
                                      tapi_nosql_severity min_severity);

/** Release a report. */
extern void tapi_nosql_report_free(tapi_nosql_report *report);

/** Spell a severity out. */
extern const char *tapi_nosql_severity2str(tapi_nosql_severity severity);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_NOSQL_AUDIT_H__ */
