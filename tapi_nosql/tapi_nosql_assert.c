/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: assertions on what a database holds
 */

#define TE_LGR_USER     "TAPI NOSQL"

#include "te_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"
#include "tapi_test_log.h"

#include "tapi_nosql_assert.h"

/* JSON text for a log line, cut (the logger takes no "%.*s"). */
static const char *
show(const json_t *v, char *buf, size_t size)
{
    char *s = v != NULL ? tapi_nosql_json_text(v) : strdup("(absent)");

    snprintf(buf, size, "%s", s != NULL ? s : "?");
    if (s != NULL && strlen(s) >= size && size > 4)
        memcpy(buf + size - 4, "...", 4);
    free(s);
    return buf;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_doc(tapi_nosql_conn *conn, const char *coll,
                      const char *key, const json_t *expected)
{
    json_t  *doc = NULL;
    char     a[512];
    char     b[512];
    te_errno rc = tapi_nosql_get(conn, coll, key, &doc, NULL);

    if (rc != 0)
        return rc;
    if (doc == NULL || !json_equal(doc, (json_t *)expected))
    {
        ERROR("%s/%s: expected %s, found %s", coll, key,
              show(expected, a, sizeof(a)), show(doc, b, sizeof(b)));
        ERROR_VERDICT("Document %s/%s is not what was expected", coll, key);
        json_decref(doc);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    json_decref(doc);
    return 0;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_absent(tapi_nosql_conn *conn, const char *coll,
                         const char *key)
{
    json_t  *doc = NULL;
    char     b[512];
    te_errno rc = tapi_nosql_get(conn, coll, key, &doc, NULL);

    if (rc != 0)
        return rc;
    if (doc != NULL)
    {
        ERROR("%s/%s: expected absent, found %s", coll, key,
              show(doc, b, sizeof(b)));
        ERROR_VERDICT("Document %s/%s is present but should be absent", coll,
                      key);
        json_decref(doc);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    return 0;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_count(tapi_nosql_conn *conn, const char *coll,
                        const json_t *filter, long long expected)
{
    long long n = -1;
    te_errno  rc = tapi_nosql_count(conn, coll, filter, &n);

    if (rc != 0)
        return rc;
    if (n != expected)
    {
        ERROR("%s: expected %lld documents, found %lld", coll, expected, n);
        ERROR_VERDICT("Collection %s does not hold the expected number of "
                      "documents", coll);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    return 0;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_keys(tapi_nosql_conn *conn, const char *coll,
                       const json_t *expected)
{
    json_t      *keys = NULL;
    json_t      *want = json_object();
    json_t      *k;
    size_t       i;
    unsigned int missing = 0;
    unsigned int extra = 0;
    te_errno     rc = tapi_nosql_scan_all(conn, coll, 500, &keys, NULL, NULL,
                                          NULL);

    if (rc != 0)
    {
        json_decref(want);
        return rc;
    }
    json_array_foreach(expected, i, k)
        json_object_set_new(want, json_string_value(k), json_false());
    json_array_foreach(keys, i, k)
    {
        const char *s = json_string_value(k);
        json_t     *w = json_object_get(want, s);

        if (w == NULL)
        {
            if (extra++ < 5)
                ERROR("%s: unexpected key '%s'", coll, s);
        }
        else
        {
            json_object_set_new(want, s, json_true());
        }
    }
    {
        const char *s;
        json_t     *seen;

        json_object_foreach(want, s, seen)
        {
            if (!json_is_true(seen) && missing++ < 5)
                ERROR("%s: missing key '%s'", coll, s);
        }
    }
    json_decref(want);
    json_decref(keys);
    if (missing != 0 || extra != 0)
    {
        ERROR("%s: %u keys missing, %u unexpected", coll, missing, extra);
        ERROR_VERDICT("Collection %s does not hold the expected keys", coll);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    return 0;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_counter(tapi_nosql_conn *conn, const char *coll,
                          const char *key, long long expected)
{
    long long v = 0;
    te_errno  rc = tapi_nosql_incr(conn, coll, key, 0, &v);

    if (rc != 0)
        return rc;
    if (v != expected)
    {
        ERROR("%s/%s: counter expected %lld, found %lld", coll, key,
              expected, v);
        ERROR_VERDICT("Counter %s/%s does not hold the expected value", coll,
                      key);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    return 0;
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_eventually_absent(tapi_nosql_conn *conn, const char *coll,
                                    const char *key, int timeout_ms,
                                    int poll_ms, int *elapsed_ms)
{
    int waited = 0;

    if (poll_ms <= 0)
        poll_ms = 100;
    for (;;)
    {
        json_t  *doc = NULL;
        te_errno rc = tapi_nosql_get(conn, coll, key, &doc, NULL);

        if (rc != 0)
            return rc;
        if (doc == NULL)
        {
            if (elapsed_ms != NULL)
                *elapsed_ms = waited;
            return 0;
        }
        json_decref(doc);
        if (waited >= timeout_ms)
            break;
        usleep((useconds_t)poll_ms * 1000);
        waited += poll_ms;
    }
    ERROR("%s/%s: still present after %d ms", coll, key, waited);
    ERROR_VERDICT("Document %s/%s did not expire in time", coll, key);
    if (elapsed_ms != NULL)
        *elapsed_ms = waited;
    return TE_RC(TE_TAPI, TE_EFAIL);
}

/* See description in tapi_nosql_assert.h */
te_errno
tapi_nosql_assert_failed_as(const tapi_nosql_conn *conn, te_errno rc,
                            tapi_nosql_errcls expected, const char *what)
{
    if (rc == 0)
    {
        ERROR_VERDICT("%s succeeded but should have failed as %s", what,
                      tapi_nosql_errcls_name(expected));
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    if (tapi_nosql_last_cls(conn) != expected)
    {
        ERROR("%s failed as %s (%s), expected %s", what,
              tapi_nosql_errcls_name(tapi_nosql_last_cls(conn)),
              tapi_nosql_last_msg(conn), tapi_nosql_errcls_name(expected));
        ERROR_VERDICT("%s failed with an unexpected class of error", what);
        return TE_RC(TE_TAPI, TE_EFAIL);
    }
    RING("%s failed as expected: %s: %s", what,
         tapi_nosql_errcls_name(expected), tapi_nosql_last_msg(conn));
    return 0;
}
