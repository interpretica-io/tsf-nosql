/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Contract tests of the ta_nosql core against live servers
 *
 * Every operation on every engine the build has and the environment
 * names a server for, with the expectation taken from the engine's
 * capabilities: an operation an engine claims must work exactly, one it
 * does not claim must fail as "unsupported". Run by ci/core-test.sh
 * against the servers of tsf-nosql-ts/scripts/nosql-servers.sh.
 *
 * A target is given by an environment variable TSF_NOSQL_<NAME> holding
 * "<engine> <config JSON>"; ci/core-test.sh sets them for the suite's
 * servers. A target that is not set is skipped.
 */

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include <jansson.h>

#include "ta_nosql_core.h"

static int failures;
static int checks;
static const char *cur_target = "";

#define CHECK(cond, ...) \
    do {                                                                    \
        checks++;                                                           \
        if (!(cond))                                                        \
        {                                                                   \
            failures++;                                                     \
            printf("FAIL [%s] %s:%d: ", cur_target, __FILE__, __LINE__);    \
            printf(__VA_ARGS__);                                            \
            printf("\n");                                                   \
        }                                                                   \
    } while (0)

/* Run an operation; the result parsed (NULL on failure, @p cls set). */
static json_t *
op(int h, const char *name, ta_nosql_errcls *cls, char *msg, size_t mlen,
   const char *fmt, ...)
{
    char        *args = NULL;
    char        *out = NULL;
    ta_nosql_err err;
    va_list      ap;
    json_t      *r = NULL;

    va_start(ap, fmt);
    if (vasprintf(&args, fmt, ap) < 0)
        args = NULL;
    va_end(ap);
    if (ta_nosql_call(h, name, args, &out, &err) == 0)
    {
        r = json_loads(out, JSON_DECODE_ANY, NULL);
        if (cls != NULL)
            *cls = TA_NOSQL_OK;
    }
    else
    {
        if (cls != NULL)
            *cls = err.cls;
        if (msg != NULL)
            snprintf(msg, mlen, "%s", err.msg);
    }
    free(out);
    free(args);
    return r;
}

/* An operation that must succeed. */
static json_t *
must(int h, const char *name, const char *fmt, ...)
{
    char        *args = NULL;
    char        *out = NULL;
    ta_nosql_err err;
    va_list      ap;
    json_t      *r = NULL;

    va_start(ap, fmt);
    if (vasprintf(&args, fmt, ap) < 0)
        args = NULL;
    va_end(ap);
    if (ta_nosql_call(h, name, args, &out, &err) == 0)
        r = json_loads(out, JSON_DECODE_ANY, NULL);
    else
        CHECK(false, "%s %s: %s: %s", name, args,
              ta_nosql_errcls_name(err.cls), err.msg);
    free(out);
    free(args);
    return r;
}

static bool
has_cap(json_t *caps, const char *cap)
{
    size_t  i;
    json_t *v;

    json_array_foreach(caps, i, v)
    {
        if (strcmp(json_string_value(v), cap) == 0)
            return true;
    }
    return false;
}

static void
t_crud(int h, const char *coll)
{
    static const char *docs[] = {
        "{\"name\":\"alice\",\"age\":30}",
        "{\"i64max\":9223372036854775807,\"i64min\":-9223372036854775808,"
        "\"i32\":2147483647,\"zero\":0}",
        "{\"real\":0.1,\"big\":1.7976931348623157e308,\"tiny\":5e-324,"
        "\"one\":1.0,\"neg\":-2.5}",
        "{\"text\":\"κόσμε 🙂 \\\" \\\\ \\n \\t\",\"empty\":\"\"}",
        "{\"nested\":{\"a\":{\"b\":{\"c\":[1,2,{\"d\":null}]}}},"
        "\"arr\":[],\"obj\":{}}",
        "{\"t\":true,\"f\":false,\"n\":null,\"mixed\":[1,\"a\",true,null,2.5]}",
        "{\"ops\":{\"$numberLong\":\"5\"},\"dollar\":{\"$gt\":1}}",
        "{}",
    };
    size_t i;

    for (i = 0; i < sizeof(docs) / sizeof(docs[0]); i++)
    {
        json_t *want = json_loads(docs[i], 0, NULL);
        json_t *r;
        json_t *g;
        char    key[32];

        snprintf(key, sizeof(key), "doc%zu", i);
        r = must(h, "put", "{\"coll\":\"%s\",\"key\":\"%s\",\"doc\":%s}", coll,
                 key, docs[i]);
        json_decref(r);
        g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"%s\"}", coll, key);
        CHECK(g != NULL && json_is_true(json_object_get(g, "found")),
              "get %s: not found", key);
        if (g != NULL)
        {
            char *got = json_dumps(json_object_get(g, "doc"),
                                   JSON_SORT_KEYS | JSON_COMPACT |
                                   JSON_REAL_PRECISION(17));

            CHECK(json_equal(json_object_get(g, "doc"), want),
                  "doc %zu reads back different: %s", i, got);
            free(got);
        }
        json_decref(g);
        json_decref(want);
    }

    /* Overwrite replaces; delete removes; absent reads as not found. */
    json_decref(must(h, "put", "{\"coll\":\"%s\",\"key\":\"x\","
                     "\"doc\":{\"v\":1}}", coll));
    json_decref(must(h, "put", "{\"coll\":\"%s\",\"key\":\"x\","
                     "\"doc\":{\"w\":2}}", coll));
    {
        json_t *g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"x\"}", coll);
        json_t *want = json_pack("{s:i}", "w", 2);

        CHECK(g != NULL && json_equal(json_object_get(g, "doc"), want),
              "overwrite did not replace the document");
        json_decref(want);
        json_decref(g);
    }
    {
        json_t *d = must(h, "delete", "{\"coll\":\"%s\",\"key\":\"x\"}", coll);
        json_t *d2 = must(h, "delete", "{\"coll\":\"%s\",\"key\":\"x\"}", coll);
        json_t *g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"x\"}", coll);

        CHECK(d != NULL && json_is_true(json_object_get(d, "deleted")),
              "delete of a present key: deleted is not true");
        CHECK(d2 != NULL && json_is_false(json_object_get(d2, "deleted")),
              "delete of an absent key: deleted is not false");
        CHECK(g != NULL && json_is_false(json_object_get(g, "found")),
              "a deleted key is still found");
        json_decref(d);
        json_decref(d2);
        json_decref(g);
    }
    {
        json_t *g = must(h, "get", "{\"coll\":\"nosuchcoll_zz\","
                         "\"key\":\"nothing\"}");

        CHECK(g != NULL && json_is_false(json_object_get(g, "found")),
              "a key of a collection never written is found");
        json_decref(g);
    }
    /* Keys that need escaping somewhere. */
    {
        static const char *keys[] = { "a/b", "a%20b", "ключ", "x?y=z&w",
                                      "dot.dot", "colon:colon" };
        size_t k;

        for (k = 0; k < sizeof(keys) / sizeof(keys[0]); k++)
        {
            json_t *g;

            json_decref(must(h, "put", "{\"coll\":\"%s\",\"key\":\"%s\","
                             "\"doc\":{\"k\":%zu}}", coll, keys[k], k));
            g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"%s\"}", coll,
                     keys[k]);
            CHECK(g != NULL && json_integer_value(json_object_get(
                      json_object_get(g, "doc"), "k")) == (json_int_t)k,
                  "key '%s' does not round-trip", keys[k]);
            json_decref(g);
        }
    }
}

static void
t_conditional(int h, const char *coll, json_t *caps)
{
    ta_nosql_errcls cls;
    char            msg[512];
    json_t         *r;

    r = op(h, "put_if", &cls, msg, sizeof(msg),
           "{\"coll\":\"%s\",\"key\":\"once\",\"if\":\"absent\","
           "\"doc\":{\"n\":1}}", coll);
    if (!has_cap(caps, "absent"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "put_if absent without the capability: %s",
              ta_nosql_errcls_name(cls));
    }
    else
    {
        json_t *r2;

        CHECK(r != NULL && json_is_true(json_object_get(r, "applied")),
              "first put_if absent not applied: %s", msg);
        r2 = must(h, "put_if", "{\"coll\":\"%s\",\"key\":\"once\","
                  "\"if\":\"absent\",\"doc\":{\"n\":2}}", coll);
        CHECK(r2 != NULL && json_is_false(json_object_get(r2, "applied")),
              "second put_if absent applied");
        json_decref(r2);
        r2 = must(h, "get", "{\"coll\":\"%s\",\"key\":\"once\"}", coll);
        CHECK(r2 != NULL && json_integer_value(json_object_get(
                  json_object_get(r2, "doc"), "n")) == 1,
              "the refused put_if absent changed the document");
        json_decref(r2);
    }
    json_decref(r);

    if (!has_cap(caps, "cas"))
        return;
    {
        json_t     *g;
        json_t     *ok;
        json_t     *stale;
        const char *v1;
        char       *v1c;

        json_decref(must(h, "put", "{\"coll\":\"%s\",\"key\":\"cas\","
                         "\"doc\":{\"n\":1}}", coll));
        g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"cas\"}", coll);
        v1 = json_string_value(json_object_get(g, "version"));
        CHECK(v1 != NULL, "get returns no version on a CAS engine");
        if (v1 == NULL)
        {
            json_decref(g);
            return;
        }
        v1c = strdup(v1);
        json_decref(g);
        ok = must(h, "put_if", "{\"coll\":\"%s\",\"key\":\"cas\","
                  "\"if\":\"version\",\"version\":\"%s\",\"doc\":{\"n\":2}}",
                  coll, v1c);
        CHECK(ok != NULL && json_is_true(json_object_get(ok, "applied")),
              "CAS with the current version not applied");
        stale = must(h, "put_if", "{\"coll\":\"%s\",\"key\":\"cas\","
                     "\"if\":\"version\",\"version\":\"%s\","
                     "\"doc\":{\"n\":3}}", coll, v1c);
        CHECK(stale != NULL && json_is_false(json_object_get(stale, "applied")),
              "CAS with a stale version applied");
        g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"cas\"}", coll);
        CHECK(g != NULL && json_integer_value(json_object_get(
                  json_object_get(g, "doc"), "n")) == 2,
              "CAS result is not the applied write");
        CHECK(g != NULL && json_string_value(json_object_get(g, "version"))
              != NULL && strcmp(json_string_value(json_object_get(g,
                                "version")), v1c) != 0,
              "the version did not change on a write");
        json_decref(g);
        json_decref(ok);
        json_decref(stale);
        free(v1c);
    }
}

static void
t_incr(int h, const char *coll, json_t *caps)
{
    ta_nosql_errcls cls;
    json_t         *r;

    r = op(h, "incr", &cls, NULL, 0, "{\"coll\":\"%s\",\"key\":\"ctr\","
           "\"by\":5}", coll);
    if (!has_cap(caps, "incr"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "incr without the capability");
        json_decref(r);
        return;
    }
    CHECK(r != NULL && json_integer_value(json_object_get(r, "value")) == 5,
          "incr by 5 from nothing");
    json_decref(r);
    r = must(h, "incr", "{\"coll\":\"%s\",\"key\":\"ctr\",\"by\":-2}", coll);
    CHECK(r != NULL && json_integer_value(json_object_get(r, "value")) == 3,
          "incr by -2: %lld", r != NULL ? (long long)json_integer_value(
              json_object_get(r, "value")) : -1LL);
    json_decref(r);
    r = must(h, "incr", "{\"coll\":\"%s\",\"key\":\"ctr\",\"by\":0}", coll);
    CHECK(r != NULL && json_integer_value(json_object_get(r, "value")) == 3,
          "incr by 0 does not read the counter");
    json_decref(r);
}

static void
t_scan_find(int h, const char *coll, json_t *caps)
{
    ta_nosql_errcls cls;
    json_t         *seen = json_object();
    json_t         *r;
    char           *cursor = NULL;
    int             pages = 0;
    int             dups = 0;
    int             maxpage = 0;
    int             n = 250;

    json_decref(op(h, "drop", NULL, NULL, 0, "{\"coll\":\"%s\"}", coll));
    r = must(h, "bulk", "{\"coll\":\"%s\",\"generate\":{\"count\":%d,"
             "\"size\":32,\"prefix\":\"k\",\"seed\":7}}", coll, n);
    CHECK(r != NULL && json_integer_value(json_object_get(r, "written")) == n,
          "bulk wrote a different number");
    json_decref(r);

    r = op(h, "scan", &cls, NULL, 0, "{\"coll\":\"%s\",\"limit\":100}", coll);
    if (!has_cap(caps, "scan"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "scan without the capability");
        json_decref(r);
    }
    else
    {
        json_decref(r);
        do {
            json_t *keys;
            size_t  i;
            json_t *k;

            r = cursor == NULL
                ? must(h, "scan", "{\"coll\":\"%s\",\"limit\":100}", coll)
                : must(h, "scan", "{\"coll\":\"%s\",\"limit\":100,"
                       "\"cursor\":\"%s\"}", coll, cursor);
            if (r == NULL)
                break;
            keys = json_object_get(r, "keys");
            if ((int)json_array_size(keys) > maxpage)
                maxpage = (int)json_array_size(keys);
            json_array_foreach(keys, i, k)
            {
                if (json_object_get(seen, json_string_value(k)) != NULL)
                    dups++;
                json_object_set_new(seen, json_string_value(k), json_true());
            }
            free(cursor);
            cursor = json_is_string(json_object_get(r, "cursor"))
                     ? strdup(json_string_value(json_object_get(r, "cursor")))
                     : NULL;
            json_decref(r);
            pages++;
        } while (cursor != NULL && pages < 100);
        CHECK((int)json_object_size(seen) == n,
              "scan saw %zu distinct keys of %d (%d pages, %d repeats)",
              json_object_size(seen), n, pages, dups);
        printf("  [%s] scan: %d keys in %d pages (largest %d), %d repeated\n",
               cur_target, (int)json_object_size(seen), pages, maxpage, dups);
    }
    json_decref(seen);

    r = op(h, "count", &cls, NULL, 0, "{\"coll\":\"%s\"}", coll);
    if (has_cap(caps, "count"))
        CHECK(r != NULL && json_integer_value(json_object_get(r, "count")) == n,
              "count is %lld, not %d", r != NULL ? (long long)
              json_integer_value(json_object_get(r, "count")) : -1LL, n);
    else
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "count without the capability");
    json_decref(r);

    r = op(h, "find", &cls, NULL, 0, "{\"coll\":\"%s\",\"filter\":"
           "{\"group\":3},\"limit\":10}", coll);
    if (!has_cap(caps, "find"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "find without the capability");
        json_decref(r);
        return;
    }
    json_decref(r);
    {
        int  total = 0;
        bool all_match = true;

        cursor = NULL;
        pages = 0;
        do {
            json_t *docs;
            size_t  i;
            json_t *d;

            r = cursor == NULL
                ? must(h, "find", "{\"coll\":\"%s\",\"filter\":{\"group\":3},"
                       "\"limit\":10}", coll)
                : must(h, "find", "{\"coll\":\"%s\",\"filter\":{\"group\":3},"
                       "\"limit\":10,\"cursor\":\"%s\"}", coll, cursor);
            if (r == NULL)
                break;
            docs = json_object_get(r, "docs");
            json_array_foreach(docs, i, d)
            {
                if (json_integer_value(json_object_get(json_object_get(d,
                                       "doc"), "group")) != 3)
                    all_match = false;
                total++;
            }
            free(cursor);
            cursor = json_is_string(json_object_get(r, "cursor"))
                     ? strdup(json_string_value(json_object_get(r, "cursor")))
                     : NULL;
            json_decref(r);
            pages++;
        } while (cursor != NULL && pages < 100);
        CHECK(total == n / 10 && all_match,
              "find group=3: %d documents (want %d), all match: %d", total,
              n / 10, all_match);
        r = must(h, "find", "{\"coll\":\"%s\",\"filter\":"
                 "{\"name\":\"item-42\"},\"limit\":10}", coll);
        CHECK(r != NULL && json_array_size(json_object_get(r, "docs")) == 1,
              "find by a string field");
        json_decref(r);
        /* An operator in a value is a value, not an operator. */
        r = op(h, "find", &cls, NULL, 0, "{\"coll\":\"%s\",\"filter\":"
               "{\"name\":{\"$ne\":null}},\"limit\":10}", coll);
        CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID,
              "an object in a filter must be refused");
        json_decref(r);
        r = must(h, "count", "{\"coll\":\"%s\",\"filter\":{\"group\":3}}",
                 coll);
        CHECK(r != NULL && json_integer_value(json_object_get(r, "count")) ==
              n / 10, "count with a filter");
        json_decref(r);
    }
}

static void
t_multi(int h, const char *coll, json_t *caps, json_t *info)
{
    ta_nosql_errcls cls;
    char            msg[512] = "";
    json_t         *r;
    int             i;

    r = op(h, "multi", &cls, msg, sizeof(msg), "{\"ops\":["
           "{\"op\":\"put\",\"coll\":\"%s\",\"key\":\"m1\",\"doc\":{\"a\":1}},"
           "{\"op\":\"put\",\"coll\":\"%s\",\"key\":\"m2\",\"doc\":{\"a\":2}},"
           "{\"op\":\"put\",\"coll\":\"%s\",\"key\":\"m3\","
           "\"doc\":{\"a\":3}}]}",
           coll, coll, coll);
    if (!has_cap(caps, "multi") ||
        (strcmp(json_string_value(json_object_get(info, "engine")),
                "mongodb") == 0 &&
         json_object_get(info, "replica_set") == NULL))
    {
        /* MongoDB has transactions on a replica set only. */
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "multi without the capability (or on a standalone MongoDB): "
              "%s", ta_nosql_errcls_name(cls));
        json_decref(r);
        return;
    }
    CHECK(r != NULL && json_is_true(json_object_get(r, "applied")),
          "multi not applied: %s", msg);
    json_decref(r);
    for (i = 1; i <= 3; i++)
    {
        json_t *g = must(h, "get", "{\"coll\":\"%s\",\"key\":\"m%d\"}",
                         coll, i);

        CHECK(g != NULL && json_integer_value(json_object_get(
                  json_object_get(g, "doc"), "a")) == i, "multi m%d", i);
        json_decref(g);
    }
    r = op(h, "multi", &cls, msg, sizeof(msg), "{\"ops\":["
           "{\"op\":\"put\",\"coll\":\"%s\",\"key\":\"n1\",\"doc\":{\"a\":1}},"
           "{\"op\":\"delete\",\"coll\":\"%s\",\"key\":\"m1\"}],"
           "\"if_absent\":{\"coll\":\"%s\",\"key\":\"m2\"}}", coll, coll, coll);
    if (!has_cap(caps, "multi_if"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "multi_if without the capability: %s", ta_nosql_errcls_name(cls));
        json_decref(r);
        return;
    }
    CHECK(r != NULL && json_is_false(json_object_get(r, "applied")),
          "conditional multi applied although its key exists: %s", msg);
    json_decref(r);
    {
        json_t *g1 = must(h, "get", "{\"coll\":\"%s\",\"key\":\"n1\"}", coll);
        json_t *g2 = must(h, "get", "{\"coll\":\"%s\",\"key\":\"m1\"}", coll);

        CHECK(g1 != NULL && json_is_false(json_object_get(g1, "found")),
              "a refused multi wrote n1");
        CHECK(g2 != NULL && json_is_true(json_object_get(g2, "found")),
              "a refused multi deleted m1");
        json_decref(g1);
        json_decref(g2);
    }
    r = must(h, "multi", "{\"ops\":["
             "{\"op\":\"put\",\"coll\":\"%s\",\"key\":\"n1\","
             "\"doc\":{\"a\":1}}],"
             "\"if_absent\":{\"coll\":\"%s\",\"key\":\"nothere\"}}",
             coll, coll);
    CHECK(r != NULL && json_is_true(json_object_get(r, "applied")),
          "conditional multi with an absent key not applied");
    json_decref(r);
}

static void
t_ttl(int h, const char *coll, json_t *caps)
{
    ta_nosql_errcls cls;
    json_t         *r;
    int             waited;

    r = op(h, "put", &cls, NULL, 0, "{\"coll\":\"%s\",\"key\":\"ttl\","
           "\"doc\":{\"a\":1},\"ttl_s\":2}", coll);
    if (!has_cap(caps, "ttl"))
    {
        CHECK(r == NULL && cls == TA_NOSQL_ERR_UNSUPPORTED,
              "ttl without the capability");
        json_decref(r);
        return;
    }
    CHECK(r != NULL, "put with a TTL failed");
    json_decref(r);
    r = must(h, "get", "{\"coll\":\"%s\",\"key\":\"ttl\"}", coll);
    CHECK(r != NULL && json_is_true(json_object_get(r, "found")),
          "an entry with a 2 s TTL is gone at once");
    json_decref(r);
    for (waited = 0; waited < 40; waited++)
    {
        usleep(250000);
        r = must(h, "get", "{\"coll\":\"%s\",\"key\":\"ttl\"}", coll);
        if (r != NULL && json_is_false(json_object_get(r, "found")))
        {
            json_decref(r);
            break;
        }
        json_decref(r);
    }
    CHECK(waited < 40, "an entry with a 2 s TTL is still there after 10 s");
    printf("  [%s] ttl: 2 s entry gone after %.2f s\n", cur_target,
           (waited + 1) * 0.25);
}

static void
t_load(int h, const char *coll, json_t *caps)
{
    json_t *r;

    if (has_cap(caps, "incr"))
    {
        json_decref(must(h, "delete", "{\"coll\":\"%s\",\"key\":\"hot\"}",
                         coll));
        r = must(h, "load", "{\"coll\":\"%s\",\"mode\":\"incr\","
                 "\"key\":\"hot\","
                 "\"threads\":4,\"ops\":100}", coll);
        if (r != NULL)
        {
            long long fin = json_integer_value(json_object_get(r, "final"));
            long long ok = json_integer_value(json_object_get(r, "succeeded"));
            long long distinct = json_integer_value(json_object_get(r,
                                                                  "distinct"));

            CHECK(fin == 400 && ok == 400,
                  "4x100 increments: final %lld, succeeded %lld", fin, ok);
            if (has_cap(caps, "incr_exact"))
                CHECK(distinct == 400, "an exact counter returned %lld "
                      "distinct values of 400", distinct);
            printf("  [%s] incr load: final %lld, %lld distinct, p99 %lld us\n",
                   cur_target, fin, distinct,
                   (long long)json_integer_value(json_object_get(
                       json_object_get(r, "lat_us"), "p99")));
        }
        json_decref(r);
    }
    json_decref(must(h, "delete", "{\"coll\":\"%s\",\"key\":\"rmw\"}", coll));
    r = must(h, "load", "{\"coll\":\"%s\",\"mode\":\"rmw\",\"key\":\"rmw\","
             "\"threads\":4,\"ops\":25,\"think_us\":3000}", coll);
    if (r != NULL)
    {
        long long lost = json_integer_value(json_object_get(r, "lost"));

        CHECK(lost > 0, "an unprotected read-modify-write lost no update "
              "(the control did not bite)");
        printf("  [%s] rmw control: %lld of 100 updates lost\n", cur_target,
               lost);
    }
    json_decref(r);
    r = must(h, "load", "{\"coll\":\"%s\",\"mode\":\"put\",\"threads\":2,"
             "\"ops\":200,\"size\":256,\"keys\":50}", coll);
    CHECK(r != NULL && json_integer_value(json_object_get(r, "errors")) == 0,
          "put load had errors");
    if (r != NULL)
        printf("  [%s] put load: %.0f ops/s, p50 %lld us p99 %lld us\n",
               cur_target,
               json_real_value(json_object_get(r, "throughput_ops_s")),
               (long long)json_integer_value(json_object_get(
                   json_object_get(r, "lat_us"), "p50")),
               (long long)json_integer_value(json_object_get(
                   json_object_get(r, "lat_us"), "p99")));
    json_decref(r);
}

/* The races: one write-if-absent applies; compare-and-set loses none. */
static void
t_races(int h, const char *coll, json_t *caps)
{
    json_t *r;

    if (has_cap(caps, "absent"))
    {
        r = must(h, "load", "{\"coll\":\"%s\",\"mode\":\"absent\","
                 "\"key\":\"race1\",\"threads\":8,\"ops\":10}", coll);
        CHECK(r != NULL && json_integer_value(json_object_get(r, "applied"))
              == 1, "write-if-absent race: %lld applied", r != NULL
              ? (long long)json_integer_value(json_object_get(r, "applied"))
              : -1LL);
        json_decref(r);
    }
    if (has_cap(caps, "cas"))
    {
        json_decref(must(h, "put", "{\"coll\":\"%s\",\"key\":\"race2\","
                         "\"doc\":{\"n\":0}}", coll));
        r = must(h, "load", "{\"coll\":\"%s\",\"mode\":\"cas\","
                 "\"key\":\"race2\",\"threads\":8,\"ops\":15}", coll);
        CHECK(r != NULL && json_integer_value(json_object_get(r, "lost")) == 0
              && json_integer_value(json_object_get(r, "applied")) > 0,
              "compare-and-set race lost %lld updates", r != NULL
              ? (long long)json_integer_value(json_object_get(r, "lost"))
              : -1LL);
        json_decref(r);
    }
}

static void
t_errors(int h, const char *coll)
{
    ta_nosql_errcls cls;
    json_t         *r;

    r = op(h, "put", &cls, NULL, 0, "{\"coll\":\"Bad-Name\",\"key\":\"k\","
           "\"doc\":{}}");
    CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID, "a bad collection name");
    json_decref(r);
    r = op(h, "put", &cls, NULL, 0, "{\"coll\":\"%s\",\"key\":\"k\","
           "\"doc\":{\"_id\":1}}", coll);
    CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID, "a reserved field");
    json_decref(r);
    r = op(h, "put", &cls, NULL, 0, "{\"coll\":\"%s\",\"key\":\"k\","
           "\"doc\":[1,2]}", coll);
    CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID, "a document that is an "
          "array");
    json_decref(r);
    r = op(h, "nosuchop", &cls, NULL, 0, "{}");
    CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID, "an unknown operation");
    json_decref(r);
    r = op(h, "get", &cls, NULL, 0, "{not json");
    CHECK(r == NULL && cls == TA_NOSQL_ERR_INVALID, "malformed arguments");
    json_decref(r);
    r = op(h, "scan", &cls, NULL, 0, "{\"coll\":\"%s\",\"limit\":100000}",
           coll);
    CHECK(r == NULL && (cls == TA_NOSQL_ERR_LIMIT ||
                        cls == TA_NOSQL_ERR_UNSUPPORTED),
          "an oversized page");
    json_decref(r);
}

static void
t_native(int h, const char *engine)
{
    const char *cmd = NULL;
    json_t     *r;

    if (strcmp(engine, "redis") == 0)
        cmd = "{\"argv\":[\"HSET\",\"tsf:h\",\"f\",\"v\"]}";
    else if (strcmp(engine, "mongodb") == 0)
        cmd = "{\"db\":\"admin\",\"command\":{\"ping\":1}}";
    else if (strcmp(engine, "cassandra") == 0)
        cmd = "{\"cql\":\"SELECT release_version FROM system.local\"}";
    else if (strcmp(engine, "elasticsearch") == 0)
        cmd = "{\"method\":\"GET\",\"path\":\"/_cluster/health\"}";
    else if (strcmp(engine, "neo4j") == 0)
        cmd = "{\"cypher\":\"RETURN $x + 1 AS y\",\"params\":{\"x\":41}}";
    else if (strcmp(engine, "memcached") == 0)
        cmd = "{\"stats\":\"\"}";
    else if (strcmp(engine, "etcd") == 0)
        cmd = "{\"path\":\"/v3/maintenance/status\",\"body\":{}}";
    if (cmd == NULL)
        return;
    r = must(h, "native", "{\"cmd\":%s}", cmd);
    if (r != NULL)
    {
        char *s = json_dumps(json_object_get(r, "reply"),
                             JSON_COMPACT | JSON_ENCODE_ANY);

        printf("  [%s] native: %.160s\n", cur_target, s);
        if (strcmp(engine, "neo4j") == 0)
            CHECK(json_integer_value(json_array_get(json_array_get(
                      json_object_get(json_object_get(r, "reply"), "rows"),
                      0), 0)) == 42, "neo4j: $x + 1 is not 42");
        free(s);
    }
    json_decref(r);
}

/* What the audit must find, by target: a comma list of check ids, or
 * "" for none at severity >= MEDIUM. */
static void
t_audit(const char *engine, const char *config, const char *expect)
{
    char        *out = NULL;
    char        *args = NULL;
    ta_nosql_err err;
    json_t      *r;
    json_t      *f;
    size_t       i;
    json_t      *finding;
    char         got[1024] = "";

    if (asprintf(&args, "{\"engine\":\"%s\",\"config\":%s,"
                 "\"default_creds\":true}", engine, config) < 0)
        return;
    if (ta_nosql_call(0, "audit", args, &out, &err) != 0)
    {
        CHECK(false, "audit: %s: %s", ta_nosql_errcls_name(err.cls), err.msg);
        free(args);
        return;
    }
    free(args);
    r = json_loads(out, 0, NULL);
    free(out);
    f = json_object_get(r, "findings");
    json_array_foreach(f, i, finding)
    {
        int sev = (int)json_integer_value(json_object_get(finding,
                                                          "severity"));

        if (sev >= 2)
        {
            if (got[0] != '\0')
                strncat(got, ",", sizeof(got) - strlen(got) - 1);
            strncat(got, json_string_value(json_object_get(finding, "check")),
                    sizeof(got) - strlen(got) - 1);
        }
        printf("  [%s] finding %d %s: %s\n", cur_target, sev,
               json_string_value(json_object_get(finding, "check")),
               json_string_value(json_object_get(finding, "detail")));
    }
    CHECK(strcmp(got, expect) == 0, "audit found {%s}, expected {%s}", got,
          expect);
    json_decref(r);
}

static void
t_secrets(const char *engine, const char *config)
{
    json_t      *cfg = json_loads(config, 0, NULL);
    char        *text;
    int          h = 0;
    char        *out = NULL;
    ta_nosql_err err;
    const char  *marker = "SeCrEt-marker-77xq";

    if (cfg == NULL || json_object_get(cfg, "password") == NULL)
    {
        json_decref(cfg);
        return;
    }
    json_object_set_new(cfg, "password", json_string(marker));
    text = json_dumps(cfg, JSON_COMPACT);
    if (ta_nosql_open(engine, text, &h, &out, &err) == 0)
    {
        CHECK(false, "a wrong password was accepted");
        ta_nosql_close(h, &err);
    }
    else
    {
        CHECK(err.cls == TA_NOSQL_ERR_AUTH,
              "a wrong password is %s, not auth: %s",
              ta_nosql_errcls_name(err.cls), err.msg);
        CHECK(strstr(err.msg, marker) == NULL,
              "the error message holds the password: %s", err.msg);
    }
    free(out);
    free(text);
    json_decref(cfg);
}

static void
run_target(const char *name, const char *spec)
{
    char         engine[32];
    const char  *config;
    const char  *expect;
    char        *cfg;
    char        *out = NULL;
    ta_nosql_err err;
    json_t      *info;
    json_t      *caps;
    int          h;
    char         coll[64];

    cur_target = name;
    if (sscanf(spec, "%31s", engine) != 1)
        return;
    config = spec + strlen(engine);
    while (*config == ' ')
        config++;
    expect = strchr(config, '|');
    cfg = expect != NULL ? strndup(config, (size_t)(expect - config))
                         : strdup(config);
    expect = expect != NULL ? expect + 1 : "";
    printf("== %s (%s)\n", name, engine);
    if (ta_nosql_open(engine, cfg, &h, &out, &err) != 0)
    {
        CHECK(false, "open: %s: %s", ta_nosql_errcls_name(err.cls), err.msg);
        free(cfg);
        return;
    }
    info = json_loads(out, 0, NULL);
    free(out);
    caps = json_object_get(info, "caps");
    /* Unique across runs too: Memcached cannot drop what a run left. */
    snprintf(coll, sizeof(coll), "ct_%ld_%ld", (long)getpid(),
             (long)time(NULL));

    t_crud(h, coll);
    t_conditional(h, coll, caps);
    t_incr(h, coll, caps);
    t_multi(h, coll, caps, info);
    t_ttl(h, coll, caps);
    t_errors(h, coll);
    t_native(h, engine);
    t_scan_find(h, coll, caps);
    t_load(h, coll, caps);
    t_races(h, coll, caps);
    t_secrets(engine, cfg);
    t_audit(engine, cfg, expect);
    {
        ta_nosql_errcls cls;
        json_t         *r = op(h, "drop", &cls, NULL, 0, "{\"coll\":\"%s\"}",
                               coll);

        json_decref(r);
    }
    ta_nosql_close(h, &err);
    {
        ta_nosql_errcls cls;
        json_t         *r = op(h, "ping", &cls, NULL, 0, "{}");

        CHECK(r == NULL && cls == TA_NOSQL_ERR_HANDLE,
              "a closed handle still works");
        json_decref(r);
    }
    json_decref(info);
    free(cfg);
}

int
main(int argc, char **argv)
{
    static const char *targets[] = {
        "REDIS", "REDIS_TLS", "REDIS_WEAK", "MONGODB", "MONGODB_TLS",
        "CASSANDRA", "ELASTICSEARCH", "NEO4J", "MEMCACHED", "ETCD",
        "ETCD_AUTH",
    };
    size_t i;

    setvbuf(stdout, NULL, _IOLBF, 0);
    for (i = 0; i < sizeof(targets) / sizeof(targets[0]); i++)
    {
        char        var[64];
        const char *spec;

        if (argc > 1 && strcasecmp(argv[1], targets[i]) != 0)
            continue;
        snprintf(var, sizeof(var), "TSF_NOSQL_%s", targets[i]);
        spec = getenv(var);
        if (spec == NULL)
        {
            printf("== %s: not set, skipped\n", targets[i]);
            continue;
        }
        run_target(targets[i], spec);
    }
    {
        ta_nosql_err err;
        ta_nosql_errcls cls = TA_NOSQL_OK;
        char        *out = NULL;
        int          bad_handle_rc = ta_nosql_call(12345, "ping", "{}", &out,
                                                   &err);

        cls = err.cls;
        cur_target = "core";
        CHECK(bad_handle_rc != 0 && cls == TA_NOSQL_ERR_HANDLE,
              "a made-up handle is accepted");
        free(out);
    }
    printf("%d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
