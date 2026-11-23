/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL RPC server library
 *
 * The nosql_* RPCs (see ta_nosql/nosql_rpc.x.m4) on top of the
 * ta_nosql core. TARPC_FUNC_STATIC() binds an RPC to the function of the
 * same name: each has a plain C function first and the generated
 * wrapper after it. The strings the core returns are malloc()ed and go
 * straight into the out structure, which XDR frees after sending. The
 * connections live in the core, keyed by the handles these calls pass
 * through, and die with the RPC server process.
 *
 * Nothing here logs an argument: the configuration of nosql_open holds
 * the password.
 */

#define TE_LGR_USER     "RPC NOSQL"

#include "te_config.h"

#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "rpc_server.h"

#include "ta_nosql_core.h"

/* An RPC string result is never NULL. */
static char *
nonnull(char *s)
{
    return s != NULL ? s : strdup("");
}

/* ---- available ---------------------------------------------------- */

static int
nosql_available(unsigned int *engines, char **result)
{
    char *json = NULL;
    int   rc = ta_nosql_available(engines, &json);

    *result = nonnull(json);
    return rc;
}

TARPC_FUNC_STATIC(nosql_available, {},
{
    unsigned int engines = 0;

    MAKE_CALL(out->retval = func(&engines, &out->result));
    out->engines = engines;
    out->common.errno_changed = false;
})

/* ---- open --------------------------------------------------------- */

static int
nosql_open(const char *engine, const char *config, int *handle, int *errcls,
           int *native, char **result, char **errmsg)
{
    ta_nosql_err err;
    char        *json = NULL;
    int          rc;

    memset(&err, 0, sizeof(err));
    rc = ta_nosql_open(engine, config, handle, &json, &err);
    *errcls = (int)err.cls;
    *native = (int)err.native;
    *result = nonnull(json);
    *errmsg = strdup(err.msg);
    return rc;
}

TARPC_FUNC_STATIC(nosql_open, {},
{
    int handle = 0;
    int errcls = 0;
    int native = 0;

    MAKE_CALL(out->retval = func(in->engine, in->config, &handle, &errcls,
                                 &native, &out->result, &out->errmsg));
    out->handle = handle;
    out->errcls = errcls;
    out->native = native;
    out->common.errno_changed = false;
})

/* ---- close -------------------------------------------------------- */

static int
nosql_close(int handle, int *errcls, char **errmsg)
{
    ta_nosql_err err;
    int          rc;

    memset(&err, 0, sizeof(err));
    rc = ta_nosql_close(handle, &err);
    *errcls = (int)err.cls;
    *errmsg = strdup(err.msg);
    return rc;
}

TARPC_FUNC_STATIC(nosql_close, {},
{
    int errcls = 0;

    MAKE_CALL(out->retval = func(in->handle, &errcls, &out->errmsg));
    out->errcls = errcls;
    out->common.errno_changed = false;
})

/* ---- call --------------------------------------------------------- */

static int
nosql_call(int handle, const char *op, const char *args, int *errcls,
           int *native, char **result, char **errmsg)
{
    ta_nosql_err err;
    char        *json = NULL;
    int          rc;

    memset(&err, 0, sizeof(err));
    rc = ta_nosql_call(handle, op, args, &json, &err);
    *errcls = (int)err.cls;
    *native = (int)err.native;
    *result = nonnull(json);
    *errmsg = strdup(err.msg);
    return rc;
}

TARPC_FUNC_STATIC(nosql_call, {},
{
    int errcls = 0;
    int native = 0;

    MAKE_CALL(out->retval = func(in->handle, in->op, in->args, &errcls,
                                 &native, &out->result, &out->errmsg));
    out->errcls = errcls;
    out->native = native;
    out->common.errno_changed = false;
})
