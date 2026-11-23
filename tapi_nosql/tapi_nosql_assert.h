/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: assertions on what a database holds
 *
 * Every tapi_nosql_assert_*() returns 0 when the property holds. When it
 * does not, the function logs what it found (expected against actual),
 * emits a TE verdict whose text is stable - it names the property and
 * the collection and key, never a value that changes between runs, so
 * TRC can match it - and returns TE_EFAIL. Any other status means the
 * check itself could not be made.
 */

#ifndef __TAPI_NOSQL_ASSERT_H__
#define __TAPI_NOSQL_ASSERT_H__

#include "te_errno.h"

#include "tapi_nosql.h"

#ifdef __cplusplus
extern "C" {
#endif

/** @p key holds exactly @p expected (json_equal()). */
extern te_errno tapi_nosql_assert_doc(tapi_nosql_conn *conn, const char *coll,
                                      const char *key,
                                      const json_t *expected);

/** @p key is absent. */
extern te_errno tapi_nosql_assert_absent(tapi_nosql_conn *conn,
                                         const char *coll, const char *key);

/** The collection (or what matches @p filter) has @p expected documents. */
extern te_errno tapi_nosql_assert_count(tapi_nosql_conn *conn,
                                        const char *coll,
                                        const json_t *filter,
                                        long long expected);

/**
 * The collection's keys are exactly @p expected (an array of strings),
 * in any order, each once.
 */
extern te_errno tapi_nosql_assert_keys(tapi_nosql_conn *conn,
                                       const char *coll,
                                       const json_t *expected);

/** The counter @p key is @p expected. */
extern te_errno tapi_nosql_assert_counter(tapi_nosql_conn *conn,
                                          const char *coll, const char *key,
                                          long long expected);

/**
 * @p key becomes absent within @p timeout_ms (an expiry), checked every
 * @p poll_ms; @p elapsed_ms (may be NULL) is when it was seen gone.
 */
extern te_errno tapi_nosql_assert_eventually_absent(tapi_nosql_conn *conn,
                                                    const char *coll,
                                                    const char *key,
                                                    int timeout_ms,
                                                    int poll_ms,
                                                    int *elapsed_ms);

/**
 * The last operation on @p conn failed with class @p expected (used
 * after a call a test expects to fail).
 */
extern te_errno tapi_nosql_assert_failed_as(const tapi_nosql_conn *conn,
                                            te_errno rc,
                                            tapi_nosql_errcls expected,
                                            const char *what);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_NOSQL_ASSERT_H__ */
