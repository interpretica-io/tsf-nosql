/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief ta_nosql audit: what a stranger can do to a server
 *
 * The "audit" operation. Three checks every engine gets, from the
 * outside, with connections of their own:
 *
 *  - nosql.auth.none: a client with no credentials reads a key. The
 *    read is of a key that is not there, so the check changes nothing;
 *    what counts is that the server answered it instead of refusing.
 *  - nosql.auth.default: with "default_creds": true, the engine's
 *    well-known credentials ("cassandra"/"cassandra" and the like) are
 *    tried, each the same way. Off by default: it is a login attempt.
 *  - nosql.tls.plaintext: the server takes an unencrypted connection.
 *
 * and then the engine's own checks (ta_nosql_driver.audit). A check that
 * could not decide says so with a *.not-assessed finding (INFO) rather
 * than passing silently. Findings carry a stable check identifier and a
 * subject (host:port), the tsf-cybersec model. No finding ever carries a
 * password, including a default one that worked.
 *
 * Authorized use only: run it against servers you own or are engaged
 * to test.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ta_nosql_int.h"

/** The key the read probes ask for. It is never written. */
#define PROBE_COLL  "tsf_audit_probe"
#define PROBE_KEY   "tsf-audit-probe-absent"

/* Connect with @p cfg and read the probe key: 0 if the read answered. */
static int
probe_read(const ta_nosql_driver *drv, const ta_nosql_conf *cfg,
           ta_nosql_err *err)
{
    void   *dc = NULL;
    json_t *doc = NULL;
    int     rc;

    if (ta_nosql_connect_checked(drv, cfg, &dc, err) != 0)
        return -1;
    rc = drv->get(dc, PROBE_COLL, PROBE_KEY, &doc, NULL, err);
    json_decref(doc);
    drv->disconnect(dc);
    ta_nosql_err_scrub(err, cfg);
    return rc;
}

static bool
refused(const ta_nosql_err *err)
{
    return err->cls == TA_NOSQL_ERR_AUTH || err->cls == TA_NOSQL_ERR_PERMISSION;
}

static bool
want(json_t *args, const char *check)
{
    json_t *only = json_object_get(args, "checks");
    size_t  i;
    json_t *v;

    if (!json_is_array(only))
        return true;
    json_array_foreach(only, i, v)
    {
        if (json_is_string(v) && strcmp(json_string_value(v), check) == 0)
            return true;
    }
    return false;
}

/* See description in ta_nosql_int.h */
int
ta_nosql_audit_run(const ta_nosql_driver *drv, const ta_nosql_conf *cfg,
                   json_t *args, json_t *out, ta_nosql_err *err)
{
    json_t       *findings = json_array();
    json_t       *checked = json_array();
    char          subject[300];
    ta_nosql_conf alt;
    ta_nosql_err  perr;
    bool          open_door = false;
    void         *dc = NULL;
    int           rc = 0;

    snprintf(subject, sizeof(subject), "%s:%d", cfg->host,
             cfg->port != 0 ? cfg->port : drv->default_port);

    /* nosql.auth.none */
    if (want(args, "nosql.auth.none"))
    {
        json_array_append_new(checked, json_string("nosql.auth.none"));
        ta_nosql_conf_copy(cfg, &alt);
        free(alt.user);
        free(alt.password);
        alt.user = NULL;
        alt.password = NULL;
        memset(&perr, 0, sizeof(perr));
        if (probe_read(drv, &alt, &perr) == 0)
        {
            open_door = true;
            ta_nosql_finding(findings, "nosql.auth.none", TA_NOSQL_SEV_HIGH,
                             subject,
                             "%s answers a client that sent no credentials "
                             "(a read was served)", drv->name);
        }
        else if (!refused(&perr))
        {
            ta_nosql_finding(findings, "nosql.auth.not-assessed",
                             TA_NOSQL_SEV_INFO, subject,
                             "an unauthenticated read neither succeeded nor "
                             "was refused: %s: %s",
                             ta_nosql_errcls_name(perr.cls), perr.msg);
        }
        ta_nosql_conf_free(&alt);
    }

    /* nosql.auth.default */
    if (want(args, "nosql.auth.default") &&
        ta_nosql_json_bool(args, "default_creds", false))
    {
        json_array_append_new(checked, json_string("nosql.auth.default"));
        if (open_door)
        {
            /* Every password works on a server that asks for none. */
        }
        else if (drv->default_creds == NULL)
        {
            ta_nosql_finding(findings, "nosql.auth.default-not-assessed",
                             TA_NOSQL_SEV_INFO, subject,
                             "%s has no well-known credentials to try",
                             drv->name);
        }
        else
        {
            const char *const *c;

            for (c = drv->default_creds; *c != NULL; c++)
            {
                const char *colon = strchr(*c, ':');

                if (colon == NULL)
                    continue;
                ta_nosql_conf_copy(cfg, &alt);
                free(alt.user);
                free(alt.password);
                alt.user = strndup(*c, (size_t)(colon - *c));
                alt.password = strdup(colon + 1);
                memset(&perr, 0, sizeof(perr));
                if (probe_read(drv, &alt, &perr) == 0)
                {
                    ta_nosql_finding(findings, "nosql.auth.default",
                                     TA_NOSQL_SEV_HIGH, subject,
                                     "user '%s' logs in with a well-known "
                                     "default password", alt.user);
                }
                ta_nosql_conf_free(&alt);
            }
        }
    }

    /* nosql.tls.plaintext */
    if (want(args, "nosql.tls.plaintext"))
    {
        json_array_append_new(checked, json_string("nosql.tls.plaintext"));
        ta_nosql_conf_copy(cfg, &alt);
        alt.tls = false;
        memset(&perr, 0, sizeof(perr));
        if (probe_read(drv, &alt, &perr) == 0 || refused(&perr))
        {
            /*
             * Refused credentials still mean the server read them, in
             * the clear: the transport was accepted.
             */
            ta_nosql_finding(findings, "nosql.tls.plaintext",
                             TA_NOSQL_SEV_MEDIUM, subject,
                             "%s accepts unencrypted connections%s",
                             drv->name,
                             alt.password != NULL
                             ? "; credentials cross the network in clear"
                             : "");
        }
        ta_nosql_conf_free(&alt);
    }

    /* The engine's own checks, on a connection made as configured. */
    if (drv->audit != NULL)
    {
        memset(&perr, 0, sizeof(perr));
        if (ta_nosql_connect_checked(drv, cfg, &dc, &perr) != 0)
            dc = NULL;
        rc = drv->audit(cfg, dc, args, findings, checked, err);
        if (dc != NULL)
            drv->disconnect(dc);
    }

    if (rc == 0)
    {
        json_object_set_new(out, "engine", json_string(drv->name));
        json_object_set_new(out, "subject", json_string(subject));
        json_object_set_new(out, "findings", findings);
        json_object_set_new(out, "checked", checked);
    }
    else
    {
        json_decref(findings);
        json_decref(checked);
    }
    return rc;
}
