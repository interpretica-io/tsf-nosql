/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql load: threads that hammer one engine
 *
 * The "load" operation. Each thread opens a connection of its own with
 * the configuration of the handle it was started from, runs its share
 * of operations and records the latency of every one; the answer is
 * the counts, the errors by class and the exact latency percentiles.
 *
 * Modes:
 *  - put / get: documents of @c size bytes over @c keys keys;
 *  - incr: every thread increments one counter; the answer has the
 *    final value and how many distinct values the increments returned
 *    (an engine whose counter is atomic and exact returns each value
 *    once);
 *  - rmw: read-modify-write of one document with no condition - the
 *    known-bad control of a lost-update test: with @c think_us between
 *    the read and the write the final count falls short of the number
 *    of increments, and that shortfall is reported as "lost";
 *  - absent: every thread writes the one key if absent; "applied" counts
 *    the writes that applied (exactly one, on an engine that keeps its
 *    promise);
 *  - cas: every thread reads the one document and writes n + 1 under
 *    the version it read; "applied" counts the writes that applied,
 *    "conflicts" the ones refused, and the final n must equal
 *    "applied" ("lost" is the difference);
 *  - torn: thread 0 writes keys "a" and "b" to i, i = 1, 2, ..., in one
 *    multi-write; the other threads read b, then a: seeing a < b is a
 *    half-applied multi-write, counted in "torn" ("reads" read pairs);
 *  - scan_churn: thread 0 makes @c passes full scans of the collection
 *    while the other threads put and delete keys "x<n>" in it; every
 *    pass must see each of the @c stable keys "s<n>" written before
 *    the load, and "missing" counts the ones a pass did not see.
 */

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "ta_nosql_int.h"

/** Most threads one load may run. */
#define LOAD_THREADS_MAX    64
/** Most operations one load may run in total. */
#define LOAD_OPS_MAX        2000000

typedef enum load_mode {
    LOAD_PUT,
    LOAD_GET,
    LOAD_INCR,
    LOAD_RMW,
    LOAD_ABSENT,
    LOAD_CAS,
    LOAD_TORN,
    LOAD_SCAN_CHURN,
} load_mode;

typedef struct load_shared {
    const ta_nosql_driver *drv;
    const ta_nosql_conf   *cfg;
    load_mode              mode;
    const char            *coll;
    const char            *key;     /**< incr / rmw: the one key. */
    const char            *prefix;  /**< put / get: key prefix. */
    long long              keys;    /**< put / get: key space. */
    int                    size;    /**< put: payload bytes. */
    long long              think_us;
    long long              per_thread;
    long long              passes;  /**< scan_churn: full scans. */
    long long              stable;  /**< scan_churn: keys s0.. */
    int                    stop;    /**< scan_churn: the scans ended. */
    pthread_mutex_t        lock;    /**< The start gate: */
    pthread_cond_t         cond;
    int                    ready;   /**< threads at the gate, */
    bool                   go;      /**< open, */
    bool                   cancel;  /**< or closed for good. */
} load_shared;

typedef struct load_thread {
    load_shared *sh;
    int          index;
    pthread_t    tid;
    int64_t     *lat;       /**< Latency of each operation, us. */
    long long   *values;    /**< incr: values returned. */
    long long    done;      /**< Operations attempted. */
    long long    errors;    /**< ... that failed. */
    long long    by_cls[TA_NOSQL_ERR_INTERNAL + 1];
    long long    applied;   /**< absent / cas: writes applied. */
    long long    conflicts; /**< cas: writes refused. */
    long long    torn;      /**< torn: half-applied multi-writes seen. */
    long long    reads;     /**< torn: read pairs. */
    long long    missing;   /**< scan_churn: stable keys not seen. */
    long long    passes;    /**< scan_churn: passes made. */
    ta_nosql_err first;     /**< The first failure. */
    bool         connected;
    ta_nosql_err conn_err;
} load_thread;

static unsigned long long
lcg(unsigned long long *x)
{
    *x = *x * 6364136223846793005ULL + 1442695040888963407ULL;
    return *x >> 33;
}

static int
one_op(load_thread *t, void *dc, long long n, unsigned long long *rng,
       json_t *payload_doc, ta_nosql_err *err)
{
    load_shared *sh = t->sh;
    char         key[128];
    json_t      *doc = NULL;

    switch (sh->mode)
    {
        case LOAD_PUT:
            snprintf(key, sizeof(key), "%s%lld", sh->prefix,
                     (long long)(lcg(rng) % (unsigned long long)sh->keys));
            return sh->drv->put(dc, sh->coll, key, payload_doc, 0, NULL, err);

        case LOAD_GET:
        {
            int rc;

            snprintf(key, sizeof(key), "%s%lld", sh->prefix,
                     (long long)(lcg(rng) % (unsigned long long)sh->keys));
            rc = sh->drv->get(dc, sh->coll, key, &doc, NULL, err);
            json_decref(doc);
            return rc;
        }

        case LOAD_INCR:
        {
            long long v = 0;

            if (sh->drv->incr(dc, sh->coll, sh->key, 1, &v, err) != 0)
                return -1;
            t->values[n] = v;
            return 0;
        }

        case LOAD_RMW:
        {
            long long cur = 0;
            json_t   *next;
            int       rc;

            if (sh->drv->get(dc, sh->coll, sh->key, &doc, NULL, err) != 0)
                return -1;
            if (doc != NULL)
                cur = ta_nosql_json_int(doc, "n", 0);
            json_decref(doc);
            if (sh->think_us > 0)
            {
                struct timespec ts = {
                    .tv_sec = sh->think_us / 1000000,
                    .tv_nsec = (sh->think_us % 1000000) * 1000,
                };
                nanosleep(&ts, NULL);
            }
            next = json_pack("{s:I}", "n", (json_int_t)(cur + 1));
            rc = sh->drv->put(dc, sh->coll, sh->key, next, 0, NULL, err);
            json_decref(next);
            return rc;
        }

        case LOAD_ABSENT:
        {
            json_t *mine = json_pack("{s:i, s:I}", "t", t->index, "i",
                                     (json_int_t)n);
            bool    applied = false;
            int     rc;

            char   *nv = NULL;

            rc = sh->drv->put_if(dc, sh->coll, sh->key, mine, 0, NULL,
                                 &applied, &nv, err);
            free(nv);
            json_decref(mine);
            if (rc == 0 && applied)
                t->applied++;
            return rc;
        }

        case LOAD_CAS:
        {
            char     *ver = NULL;
            long long cur;
            json_t   *next;
            bool      applied = false;
            int       rc;

            if (sh->drv->get(dc, sh->coll, sh->key, &doc, &ver, err) != 0)
                return -1;
            if (doc == NULL || ver == NULL)
            {
                json_decref(doc);
                free(ver);
                ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                                 "cas load: the key has no document or no "
                                 "version");
                return -1;
            }
            cur = ta_nosql_json_int(doc, "n", 0);
            json_decref(doc);
            next = json_pack("{s:I}", "n", (json_int_t)(cur + 1));
            {
                char *nv = NULL;

                rc = sh->drv->put_if(dc, sh->coll, sh->key, next, 0, ver,
                                     &applied, &nv, err);
                free(nv);
            }
            json_decref(next);
            free(ver);
            if (rc == 0)
            {
                if (applied)
                    t->applied++;
                else
                    t->conflicts++;
            }
            return rc;
        }

        case LOAD_TORN:
        {
            int rc;

            if (t->index == 0)
            {
                json_t *ops = json_pack("[{s:s, s:s, s:s, s:{s:I}},"
                                        " {s:s, s:s, s:s, s:{s:I}}]",
                                        "op", "put", "coll", sh->coll,
                                        "key", "a", "doc", "n",
                                        (json_int_t)(n + 1),
                                        "op", "put", "coll", sh->coll,
                                        "key", "b", "doc", "n",
                                        (json_int_t)(n + 1));
                bool    applied = false;

                rc = sh->drv->multi(dc, ops, NULL, NULL, &applied, err);
                json_decref(ops);
                return rc;
            }
            else
            {
                json_t   *da = NULL;
                json_t   *db = NULL;
                long long a;
                long long b;

                /* b first: a multi-write that is atomic makes a at least
                 * the b it was written with. */
                if (sh->drv->get(dc, sh->coll, "b", &db, NULL, err) != 0)
                    return -1;
                rc = sh->drv->get(dc, sh->coll, "a", &da, NULL, err);
                b = db != NULL ? ta_nosql_json_int(db, "n", 0) : 0;
                a = da != NULL ? ta_nosql_json_int(da, "n", 0) : 0;
                json_decref(da);
                json_decref(db);
                if (rc != 0)
                    return -1;
                t->reads++;
                if (a < b)
                    t->torn++;
                return 0;
            }
        }

        case LOAD_SCAN_CHURN:
        {
            if (t->index == 0)
            {
                json_t *seen = json_object();
                char   *cursor = NULL;
                int     pages = 0;
                int     rc = 0;

                do {
                    json_t *keys = json_array();
                    char   *next = NULL;
                    size_t  i;
                    json_t *k;

                    rc = sh->drv->scan(dc, sh->coll, 50, cursor, keys, &next,
                                       err);
                    free(cursor);
                    cursor = next;
                    json_array_foreach(keys, i, k)
                    {
                        const char *ks = json_string_value(k);

                        if (ks[0] == 's')
                            json_object_set_new(seen, ks, json_true());
                    }
                    json_decref(keys);
                } while (rc == 0 && cursor != NULL && ++pages < 100000);
                free(cursor);
                if (rc == 0)
                {
                    long long got = (long long)json_object_size(seen);

                    t->passes++;
                    if (got < sh->stable)
                        t->missing += sh->stable - got;
                }
                json_decref(seen);
                return rc;
            }
            else
            {
                char k[32];
                int  rc;

                snprintf(k, sizeof(k), "x%llu",
                         lcg(rng) % (unsigned long long)sh->keys);
                if (lcg(rng) % 2 == 0)
                {
                    json_t *d = json_pack("{s:i}", "churn", t->index);

                    rc = sh->drv->put(dc, sh->coll, k, d, 0, NULL, err);
                    json_decref(d);
                }
                else
                {
                    bool deleted;

                    rc = sh->drv->del(dc, sh->coll, k, &deleted, err);
                }
                return rc;
            }
        }
    }
    return -1;
}

static void *
load_main(void *arg)
{
    load_thread       *t = arg;
    load_shared       *sh = t->sh;
    void              *dc = NULL;
    json_t            *payload_doc = NULL;
    unsigned long long rng = 0x9e3779b97f4a7c15ULL * (unsigned)(t->index + 1);
    long long          i;

    if (sh->drv->thread_init != NULL)
        sh->drv->thread_init();
    t->connected = ta_nosql_connect_checked(sh->drv, sh->cfg, &dc,
                                            &t->conn_err) == 0;
    if (sh->mode == LOAD_PUT)
    {
        char *payload = malloc((size_t)sh->size + 1);

        if (payload != NULL)
        {
            memset(payload, 'x', (size_t)sh->size);
            payload[sh->size] = '\0';
            payload_doc = json_pack("{s:i, s:s}", "t", t->index,
                                    "payload", payload);
            free(payload);
        }
    }
    /* Everyone starts together, connected or not. */
    pthread_mutex_lock(&sh->lock);
    sh->ready++;
    pthread_cond_broadcast(&sh->cond);
    while (!sh->go)
        pthread_cond_wait(&sh->cond, &sh->lock);
    pthread_mutex_unlock(&sh->lock);
    if (!t->connected || sh->cancel)
    {
        if (t->connected)
            sh->drv->disconnect(dc);
        json_decref(payload_doc);
        return NULL;
    }

    for (i = 0; i < sh->per_thread; i++)
    {
        if (sh->mode == LOAD_SCAN_CHURN)
        {
            if (t->index == 0 && i >= sh->passes)
                break;
            if (t->index != 0 && __atomic_load_n(&sh->stop, __ATOMIC_SEQ_CST))
                break;
        }
        ta_nosql_err err;
        int64_t      t0 = ta_nosql_now_us();
        int          rc;

        memset(&err, 0, sizeof(err));
        rc = one_op(t, dc, i, &rng, payload_doc, &err);
        t->lat[i] = ta_nosql_now_us() - t0;
        t->done++;
        if (rc != 0)
        {
            t->errors++;
            if ((int)err.cls >= 0 && err.cls <= TA_NOSQL_ERR_INTERNAL)
                t->by_cls[err.cls]++;
            if (t->errors == 1)
            {
                ta_nosql_err_scrub(&err, sh->cfg);
                t->first = err;
            }
            if (sh->mode == LOAD_INCR)
                t->values[i] = LLONG_MIN;
        }
    }
    if (sh->mode == LOAD_SCAN_CHURN && t->index == 0)
        __atomic_store_n(&sh->stop, 1, __ATOMIC_SEQ_CST);
    json_decref(payload_doc);
    sh->drv->disconnect(dc);
    return NULL;
}

static int
cmp_i64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;

    return x < y ? -1 : x > y;
}

static int
cmp_ll(const void *a, const void *b)
{
    long long x = *(const long long *)a;
    long long y = *(const long long *)b;

    return x < y ? -1 : x > y;
}

static json_int_t
pct(const int64_t *sorted, long long n, double p)
{
    long long idx;

    if (n == 0)
        return 0;
    idx = (long long)(p * (double)(n - 1) + 0.5);
    if (idx >= n)
        idx = n - 1;
    return sorted[idx];
}

static int
parse_mode(const char *s, load_mode *mode)
{
    if (strcmp(s, "put") == 0)
        *mode = LOAD_PUT;
    else if (strcmp(s, "get") == 0)
        *mode = LOAD_GET;
    else if (strcmp(s, "incr") == 0)
        *mode = LOAD_INCR;
    else if (strcmp(s, "rmw") == 0)
        *mode = LOAD_RMW;
    else if (strcmp(s, "absent") == 0)
        *mode = LOAD_ABSENT;
    else if (strcmp(s, "cas") == 0)
        *mode = LOAD_CAS;
    else if (strcmp(s, "torn") == 0)
        *mode = LOAD_TORN;
    else if (strcmp(s, "scan_churn") == 0)
        *mode = LOAD_SCAN_CHURN;
    else
        return -1;
    return 0;
}

/* After the load: the counter / the document the threads raced on. */
static int
final_value(load_shared *sh, long long *value, ta_nosql_err *err)
{
    void   *dc = NULL;
    json_t *doc = NULL;
    int     rc;

    if (ta_nosql_connect_checked(sh->drv, sh->cfg, &dc, err) != 0)
        return -1;
    if (sh->mode == LOAD_INCR)
    {
        rc = sh->drv->incr(dc, sh->coll, sh->key, 0, value, err);
    }
    else
    {
        rc = sh->drv->get(dc, sh->coll, sh->key, &doc, NULL, err);
        *value = doc != NULL ? ta_nosql_json_int(doc, "n", 0) : 0;
        json_decref(doc);
    }
    sh->drv->disconnect(dc);
    return rc;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_load_run(const ta_nosql_driver *drv, const ta_nosql_conf *cfg,
                  json_t *args, json_t *out, ta_nosql_err *err)
{
    load_shared  sh;
    load_thread *th = NULL;
    long long    threads = ta_nosql_json_int(args, "threads", 1);
    long long    ops = ta_nosql_json_int(args, "ops", 1000);
    const char  *mode = ta_nosql_json_str(args, "mode", "put");
    long long    total;
    long long    errors = 0;
    long long    by_cls[TA_NOSQL_ERR_INTERNAL + 1] = { 0 };
    int64_t     *all = NULL;
    int64_t      t0;
    int64_t      elapsed;
    long long    n = 0;
    long long    i;
    int          k;
    int          started = 0;
    int          rc = -1;
    json_t      *lat;
    json_t      *cls;
    bool         any_connected = false;
    ta_nosql_err conn_err;

    memset(&sh, 0, sizeof(sh));
    memset(&conn_err, 0, sizeof(conn_err));
    sh.drv = drv;
    sh.cfg = cfg;
    if (parse_mode(mode, &sh.mode) != 0)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'mode' must be put, get, incr, rmw, absent, cas, "
                         "torn or scan_churn");
        return -1;
    }
    sh.coll = ta_nosql_json_str(args, "coll", NULL);
    if (!ta_nosql_coll_ok(sh.coll))
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                         "'coll' must be a lower-case identifier");
        return -1;
    }
    sh.key = ta_nosql_json_str(args, "key", "counter");
    sh.prefix = ta_nosql_json_str(args, "prefix", "k");
    sh.keys = ta_nosql_json_int(args, "keys", 1000);
    sh.size = (int)ta_nosql_json_int(args, "size", 64);
    sh.think_us = ta_nosql_json_int(args, "think_us", 0);
    sh.passes = ta_nosql_json_int(args, "passes", 5);
    sh.stable = ta_nosql_json_int(args, "stable", 0);
    if (threads < 1 || threads > LOAD_THREADS_MAX || ops < 1 ||
        threads * ops > LOAD_OPS_MAX || sh.keys < 1 || sh.size < 0 ||
        sh.size > 1048576 || sh.think_us < 0 || sh.think_us > 1000000)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_LIMIT, 0,
                         "load limits: threads 1..%d, ops >= 1, threads*ops "
                         "<= %d, size <= 1 MiB, think_us <= 1 s",
                         LOAD_THREADS_MAX, LOAD_OPS_MAX);
        return -1;
    }
    {
        static const struct {
            load_mode    mode;
            unsigned int cap;
            const char  *what;
        } needs[] = {
            { LOAD_INCR, TA_NOSQL_CAP_INCR, "keep counters" },
            { LOAD_ABSENT, TA_NOSQL_CAP_ABSENT, "write only when a key is "
                                                "absent" },
            { LOAD_CAS, TA_NOSQL_CAP_CAS, "compare and set" },
            { LOAD_TORN, TA_NOSQL_CAP_MULTI, "write several keys "
                                             "atomically" },
            { LOAD_SCAN_CHURN, TA_NOSQL_CAP_SCAN, "list keys" },
        };
        size_t q;

        for (q = 0; q < sizeof(needs) / sizeof(needs[0]); q++)
        {
            if (needs[q].mode == sh.mode && (drv->caps & needs[q].cap) == 0)
            {
                ta_nosql_err_set(err, TA_NOSQL_ERR_UNSUPPORTED, 0,
                                 "%s does not %s", drv->name, needs[q].what);
                return -1;
            }
        }
        if ((sh.mode == LOAD_TORN || sh.mode == LOAD_SCAN_CHURN) &&
            threads < 2)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "%s needs at least two threads", mode);
            return -1;
        }
        if (sh.mode == LOAD_SCAN_CHURN && sh.passes > ops)
        {
            ta_nosql_err_set(err, TA_NOSQL_ERR_INVALID, 0,
                             "scan_churn: 'passes' may not exceed 'ops'");
            return -1;
        }
    }
    sh.per_thread = ops;

    th = calloc((size_t)threads, sizeof(*th));
    if (th == NULL)
        goto oom;
    for (k = 0; k < threads; k++)
    {
        th[k].sh = &sh;
        th[k].index = k;
        th[k].lat = calloc((size_t)ops, sizeof(int64_t));
        if (th[k].lat == NULL)
            goto oom;
        if (sh.mode == LOAD_INCR)
        {
            th[k].values = calloc((size_t)ops, sizeof(long long));
            if (th[k].values == NULL)
                goto oom;
        }
    }

    pthread_mutex_init(&sh.lock, NULL);
    pthread_cond_init(&sh.cond, NULL);
    for (started = 0; started < threads; started++)
    {
        if (pthread_create(&th[started].tid, NULL, load_main,
                           &th[started]) != 0)
            break;
    }
    pthread_mutex_lock(&sh.lock);
    while (sh.ready < started)
        pthread_cond_wait(&sh.cond, &sh.lock);
    sh.cancel = started < threads;
    sh.go = true;
    pthread_cond_broadcast(&sh.cond);
    pthread_mutex_unlock(&sh.lock);
    t0 = ta_nosql_now_us();
    for (k = 0; k < started; k++)
        pthread_join(th[k].tid, NULL);
    elapsed = ta_nosql_now_us() - t0;
    pthread_cond_destroy(&sh.cond);
    pthread_mutex_destroy(&sh.lock);
    if (sh.cancel)
    {
        ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, EAGAIN,
                         "could start only %d of %lld load threads",
                         started, threads);
        goto out;
    }

    total = 0;
    for (k = 0; k < threads; k++)
    {
        if (th[k].connected)
            any_connected = true;
        else if (conn_err.cls == TA_NOSQL_OK)
            conn_err = th[k].conn_err;
        total += th[k].done;
        errors += th[k].errors;
        for (i = 0; i <= TA_NOSQL_ERR_INTERNAL; i++)
            by_cls[i] += th[k].by_cls[i];
    }
    if (!any_connected)
    {
        *err = conn_err;
        goto out;
    }

    all = malloc((size_t)(total > 0 ? total : 1) * sizeof(int64_t));
    if (all == NULL)
        goto oom;
    for (k = 0; k < threads; k++)
    {
        memcpy(all + n, th[k].lat, (size_t)th[k].done * sizeof(int64_t));
        n += th[k].done;
    }
    qsort(all, (size_t)n, sizeof(int64_t), cmp_i64);

    lat = json_object();
    {
        long double sum = 0;

        for (i = 0; i < n; i++)
            sum += all[i];
        json_object_set_new(lat, "min", json_integer(n > 0 ? all[0] : 0));
        json_object_set_new(lat, "p50", json_integer(pct(all, n, 0.50)));
        json_object_set_new(lat, "p90", json_integer(pct(all, n, 0.90)));
        json_object_set_new(lat, "p99", json_integer(pct(all, n, 0.99)));
        json_object_set_new(lat, "p999", json_integer(pct(all, n, 0.999)));
        json_object_set_new(lat, "max", json_integer(n > 0 ? all[n - 1] : 0));
        json_object_set_new(lat, "mean",
                            json_real(n > 0 ? (double)(sum / n) : 0.0));
    }
    cls = json_object();
    for (i = 1; i <= TA_NOSQL_ERR_INTERNAL; i++)
    {
        if (by_cls[i] != 0)
            json_object_set_new(cls, ta_nosql_errcls_name((ta_nosql_errcls)i),
                                json_integer(by_cls[i]));
    }
    json_object_set_new(out, "mode", json_string(mode));
    json_object_set_new(out, "threads", json_integer(threads));
    json_object_set_new(out, "ops", json_integer(total));
    json_object_set_new(out, "errors", json_integer(errors));
    json_object_set_new(out, "errors_by_class", cls);
    json_object_set_new(out, "elapsed_us", json_integer(elapsed));
    json_object_set_new(out, "throughput_ops_s",
                        json_real(elapsed > 0 ? (double)total * 1e6 /
                                                (double)elapsed : 0.0));
    json_object_set_new(out, "lat_us", lat);
    for (k = 0; k < threads; k++)
    {
        if (th[k].errors > 0)
        {
            json_object_set_new(out, "first_error",
                json_pack("{s:s, s:s}", "class",
                          ta_nosql_errcls_name(th[k].first.cls),
                          "message", th[k].first.msg));
            break;
        }
    }

    {
        long long applied = 0, conflicts = 0, torn = 0, reads = 0;
        long long missing = 0, passes = 0;

        for (k = 0; k < threads; k++)
        {
            applied += th[k].applied;
            conflicts += th[k].conflicts;
            torn += th[k].torn;
            reads += th[k].reads;
            missing += th[k].missing;
            passes += th[k].passes;
        }
        if (sh.mode == LOAD_ABSENT || sh.mode == LOAD_CAS)
            json_object_set_new(out, "applied", json_integer(applied));
        if (sh.mode == LOAD_CAS)
            json_object_set_new(out, "conflicts", json_integer(conflicts));
        if (sh.mode == LOAD_TORN)
        {
            json_object_set_new(out, "torn", json_integer(torn));
            json_object_set_new(out, "reads", json_integer(reads));
        }
        if (sh.mode == LOAD_SCAN_CHURN)
        {
            json_object_set_new(out, "missing", json_integer(missing));
            json_object_set_new(out, "passes", json_integer(passes));
        }
        if (sh.mode == LOAD_CAS)
        {
            long long    final = 0;
            ta_nosql_err ferr;
            load_mode    saved = sh.mode;

            memset(&ferr, 0, sizeof(ferr));
            sh.mode = LOAD_RMW;
            if (final_value(&sh, &final, &ferr) != 0)
            {
                *err = ferr;
                goto out;
            }
            sh.mode = saved;
            json_object_set_new(out, "final", json_integer(final));
            json_object_set_new(out, "lost", json_integer(applied - final));
        }
    }

    if (sh.mode == LOAD_INCR || sh.mode == LOAD_RMW)
    {
        long long    final = 0;
        ta_nosql_err ferr;

        memset(&ferr, 0, sizeof(ferr));
        if (final_value(&sh, &final, &ferr) != 0)
        {
            *err = ferr;
            goto out;
        }
        json_object_set_new(out, "final", json_integer(final));
        json_object_set_new(out, "succeeded", json_integer(total - errors));
        if (sh.mode == LOAD_RMW)
        {
            json_object_set_new(out, "lost",
                                json_integer((total - errors) - final));
        }
        else
        {
            long long *vals = malloc((size_t)(total > 0 ? total : 1) *
                                     sizeof(long long));
            long long  m = 0;
            long long  distinct = 0;

            if (vals == NULL)
                goto oom;
            for (k = 0; k < threads; k++)
            {
                for (i = 0; i < th[k].done; i++)
                {
                    if (th[k].values[i] != LLONG_MIN)
                        vals[m++] = th[k].values[i];
                }
            }
            qsort(vals, (size_t)m, sizeof(long long), cmp_ll);
            for (i = 0; i < m; i++)
            {
                if (i == 0 || vals[i] != vals[i - 1])
                    distinct++;
            }
            free(vals);
            json_object_set_new(out, "distinct", json_integer(distinct));
        }
    }
    rc = 0;
    goto out;

oom:
    ta_nosql_err_set(err, TA_NOSQL_ERR_INTERNAL, ENOMEM, "out of memory");
out:
    if (th != NULL)
    {
        for (k = 0; k < threads; k++)
        {
            free(th[k].lat);
            free(th[k].values);
        }
        free(th);
    }
    free(all);
    return rc;
}
