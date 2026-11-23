/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: the audit report
 */

#define TE_LGR_USER     "TAPI NOSQL AUDIT"

#include "te_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"

#include "tapi_nosql_audit.h"
#include "tapi_nosql_rpc.h"

static const char *const sev_names[] = {
    "INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL",
};

/* See description in tapi_nosql_audit.h */
const char *
tapi_nosql_severity2str(tapi_nosql_severity severity)
{
    if ((unsigned)severity < sizeof(sev_names) / sizeof(sev_names[0]))
        return sev_names[severity];
    return "?";
}

/* See description in tapi_nosql_audit.h */
te_errno
tapi_nosql_audit(rcf_rpc_server *rpcs, const char *engine,
                 const json_t *config, bool default_creds,
                 tapi_nosql_report *report)
{
    rpc_nosql_err err = { 0, 0, NULL };
    json_t       *args = json_pack("{s:s, s:O, s:b}", "engine", engine,
                                   "config", config, "default_creds",
                                   default_creds);
    char         *text = json_dumps(args, JSON_COMPACT);
    char         *out = NULL;
    json_t       *r;
    json_t       *f;
    json_t       *item;
    size_t        i;
    te_errno      rc;

    json_decref(args);
    rc = rpc_nosql_call(rpcs, 0, "audit", text, &out, &err);
    /* The arguments hold the password. */
    memset(text, 0, strlen(text));
    free(text);
    if (rc != 0)
    {
        ERROR("Audit of %s failed: %s", engine,
              err.msg != NULL ? err.msg : "");
        rpc_nosql_err_free(&err);
        free(out);
        return rc;
    }
    rpc_nosql_err_free(&err);
    r = tapi_nosql_json(out);
    free(out);
    if (r == NULL)
        return TE_RC(TE_TAPI, TE_EBADMSG);
    free(report->engine);
    free(report->subject);
    report->engine = strdup(engine);
    report->subject = strdup(json_string_value(json_object_get(r, "subject"))
                             != NULL
                             ? json_string_value(json_object_get(r, "subject"))
                             : "");
    f = json_object_get(r, "findings");
    json_array_foreach(f, i, item)
    {
        tapi_nosql_finding *p = realloc(report->items,
                                        (report->n + 1) * sizeof(*p));

        if (p == NULL)
            break;
        report->items = p;
        p[report->n].severity = (tapi_nosql_severity)
            json_integer_value(json_object_get(item, "severity"));
        p[report->n].check = strdup(json_string_value(
                                 json_object_get(item, "check")));
        p[report->n].subject = strdup(json_string_value(
                                   json_object_get(item, "subject")));
        p[report->n].detail = strdup(json_string_value(
                                  json_object_get(item, "detail")));
        report->n++;
    }
    json_decref(r);
    return 0;
}

/* See description in tapi_nosql_audit.h */
unsigned int
tapi_nosql_report_count(const tapi_nosql_report *report,
                        tapi_nosql_severity min_severity)
{
    unsigned int i;
    unsigned int n = 0;

    for (i = 0; i < report->n; i++)
    {
        if (report->items[i].severity >= min_severity)
            n++;
    }
    return n;
}

/* See description in tapi_nosql_audit.h */
const tapi_nosql_finding *
tapi_nosql_report_get(const tapi_nosql_report *report, const char *check)
{
    unsigned int i;

    for (i = 0; i < report->n; i++)
    {
        if (strcmp(report->items[i].check, check) == 0)
            return &report->items[i];
    }
    return NULL;
}

/* See description in tapi_nosql_audit.h */
bool
tapi_nosql_report_has(const tapi_nosql_report *report, const char *check)
{
    return tapi_nosql_report_get(report, check) != NULL;
}

/* See description in tapi_nosql_audit.h */
void
tapi_nosql_report_log(const tapi_nosql_report *report)
{
    int          sev;
    unsigned int i;

    RING("Audit of %s %s: %u finding(s)",
         report->engine != NULL ? report->engine : "?",
         report->subject != NULL ? report->subject : "?", report->n);
    for (sev = TAPI_NOSQL_SEV_CRITICAL; sev >= TAPI_NOSQL_SEV_INFO; sev--)
    {
        for (i = 0; i < report->n; i++)
        {
            const tapi_nosql_finding *f = &report->items[i];

            if ((int)f->severity == sev)
                RING("%s %s [%s]: %s", tapi_nosql_severity2str(f->severity),
                     f->check, f->subject, f->detail);
        }
    }
}

static int
cmp_str(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* See description in tapi_nosql_audit.h */
char *
tapi_nosql_report_checks(const tapi_nosql_report *report,
                         tapi_nosql_severity min_severity)
{
    const char **names = calloc(report->n + 1, sizeof(*names));
    size_t       len = 1;
    unsigned int i;
    unsigned int k = 0;
    char        *out;

    if (names == NULL)
        return NULL;
    for (i = 0; i < report->n; i++)
    {
        if (report->items[i].severity >= min_severity)
        {
            names[k++] = report->items[i].check;
            len += strlen(report->items[i].check) + 1;
        }
    }
    qsort(names, k, sizeof(*names), cmp_str);
    out = calloc(1, len);
    if (out != NULL)
    {
        for (i = 0; i < k; i++)
        {
            if (i > 0 && strcmp(names[i], names[i - 1]) == 0)
                continue;
            if (out[0] != '\0')
                strcat(out, ",");
            strcat(out, names[i]);
        }
    }
    free(names);
    return out;
}

/* See description in tapi_nosql_audit.h */
void
tapi_nosql_report_free(tapi_nosql_report *report)
{
    unsigned int i;

    for (i = 0; i < report->n; i++)
    {
        free(report->items[i].check);
        free(report->items[i].subject);
        free(report->items[i].detail);
    }
    free(report->items);
    free(report->engine);
    free(report->subject);
    memset(report, 0, sizeof(*report));
}
