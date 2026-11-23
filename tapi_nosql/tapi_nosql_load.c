/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: load on the agent, and its numbers as MI charts
 */

#define TE_LGR_USER     "TAPI NOSQL LOAD"

#include "te_config.h"

#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"
#include "te_mi_log.h"

#include "tapi_nosql_load.h"

/* See description in tapi_nosql_load.h */
te_errno
tapi_nosql_load(tapi_nosql_conn *conn, const tapi_nosql_load_args *args,
                tapi_nosql_load_result *result)
{
    json_t  *a = json_pack("{s:s, s:s, s:s, s:s, s:I, s:i, s:i, s:I, s:I}",
                           "mode", args->mode != NULL ? args->mode : "put",
                           "coll", args->coll,
                           "key", args->key != NULL ? args->key : "counter",
                           "prefix", args->prefix != NULL ? args->prefix : "k",
                           "keys", (json_int_t)(args->keys > 0 ? args->keys
                                                               : 1000),
                           "size", args->size > 0 ? args->size : 64,
                           "threads", args->threads > 0 ? args->threads : 1,
                           "ops", (json_int_t)(args->ops > 0 ? args->ops
                                                             : 1000),
                           "think_us", (json_int_t)args->think_us);
    json_t  *r = NULL;
    json_t  *lat;
    te_errno rc;

    memset(result, 0, sizeof(*result));
    rc = tapi_nosql_call(conn, "load", a, &r);
    json_decref(a);
    if (rc != 0)
        return rc;
    lat = json_object_get(r, "lat_us");
    result->ops = json_integer_value(json_object_get(r, "ops"));
    result->errors = json_integer_value(json_object_get(r, "errors"));
    result->throughput = json_real_value(json_object_get(r,
                                                         "throughput_ops_s"));
    result->elapsed_us = json_integer_value(json_object_get(r, "elapsed_us"));
    result->lat_min = json_integer_value(json_object_get(lat, "min"));
    result->lat_p50 = json_integer_value(json_object_get(lat, "p50"));
    result->lat_p90 = json_integer_value(json_object_get(lat, "p90"));
    result->lat_p99 = json_integer_value(json_object_get(lat, "p99"));
    result->lat_p999 = json_integer_value(json_object_get(lat, "p999"));
    result->lat_max = json_integer_value(json_object_get(lat, "max"));
    result->lat_mean = json_real_value(json_object_get(lat, "mean"));
    result->final = json_integer_value(json_object_get(r, "final"));
    result->succeeded = json_integer_value(json_object_get(r, "succeeded"));
    result->distinct = json_integer_value(json_object_get(r, "distinct"));
    result->lost = json_integer_value(json_object_get(r, "lost"));
    result->raw = r;
    RING("Load %s x%d: %lld ops, %lld errors, %.0f ops/s, latency p50 %lld "
         "p99 %lld max %lld us", args->mode != NULL ? args->mode : "put",
         args->threads, result->ops, result->errors, result->throughput,
         result->lat_p50, result->lat_p99, result->lat_max);
    return 0;
}

/* See description in tapi_nosql_load.h */
void
tapi_nosql_load_result_free(tapi_nosql_load_result *result)
{
    json_decref(result->raw);
    result->raw = NULL;
}

/* See description in tapi_nosql_load.h */
void
tapi_nosql_load_mi(const char *name, const tapi_nosql_load_result *result)
{
    static const double pct[] = { 0, 50, 90, 99, 99.9, 100 };
    te_mi_logger       *logger = NULL;
    long long           lat[6];
    unsigned int        i;

    lat[0] = result->lat_min;
    lat[1] = result->lat_p50;
    lat[2] = result->lat_p90;
    lat[3] = result->lat_p99;
    lat[4] = result->lat_p999;
    lat[5] = result->lat_max;
    if (te_mi_logger_meas_create("tsf-nosql", &logger) != 0)
        return;
    te_mi_logger_add_meas_key(logger, NULL, "series", "%s", name);
    te_mi_logger_add_meas(logger, NULL, TE_MI_MEAS_UNITLESS_VALUE,
                          "ops_per_s", TE_MI_MEAS_AGGR_SINGLE,
                          result->throughput, TE_MI_MEAS_MULTIPLIER_PLAIN);
    te_mi_logger_add_meas(logger, NULL, TE_MI_MEAS_EPE, "errors",
                          TE_MI_MEAS_AGGR_SINGLE, (double)result->errors,
                          TE_MI_MEAS_MULTIPLIER_PLAIN);
    for (i = 0; i < 6; i++)
    {
        te_mi_logger_add_meas(logger, NULL, TE_MI_MEAS_UNITLESS_VALUE,
                              "percentile", TE_MI_MEAS_AGGR_SINGLE, pct[i],
                              TE_MI_MEAS_MULTIPLIER_PLAIN);
        te_mi_logger_add_meas(logger, NULL, TE_MI_MEAS_LATENCY,
                              "latency", TE_MI_MEAS_AGGR_SINGLE,
                              (double)lat[i], TE_MI_MEAS_MULTIPLIER_MICRO);
    }
    te_mi_logger_add_meas_view(logger, NULL, TE_MI_MEAS_VIEW_LINE_GRAPH,
                               "latency", "Latency by percentile");
    te_mi_logger_meas_graph_axis_add_name(logger, NULL,
                                          TE_MI_MEAS_VIEW_LINE_GRAPH,
                                          "latency", TE_MI_GRAPH_AXIS_X,
                                          "percentile");
    te_mi_logger_meas_graph_axis_add(logger, NULL, TE_MI_MEAS_VIEW_LINE_GRAPH,
                                     "latency", TE_MI_GRAPH_AXIS_Y,
                                     TE_MI_MEAS_LATENCY, "latency");
    te_mi_logger_destroy(logger);
}
