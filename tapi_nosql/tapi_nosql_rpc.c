/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: RPC client wrappers
 *
 * The rcf_rpc_call() boilerplate behind tapi_nosql. An agent-side
 * failure is TE_EFAIL with the class and message in the error; an RPC
 * transport failure is TE_ECORRUPTED. The out strings are copied before
 * the RETVAL macro frees the out structure.
 *
 * nosql_open's configuration (it holds the password) is never logged;
 * the arguments of nosql_call are logged cut short.
 */

#define TE_LGR_USER     "TAPI NOSQL RPC"

#include "te_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "te_defs.h"
#include "te_errno.h"
#include "logger_api.h"
#include "tapi_rpc_internal.h"
#include "tarpc.h"

#include "tapi_nosql_rpc.h"

#define CHECK_RPC_ERRNO_UNCHANGED(_func, _var) \
    CHECK_RETVAL_VAR_ERR_COND(_func, _var, false,                    \
                              TE_RC(TE_TAPI, TE_ECORRUPTED), false)

/** How much of the arguments goes into the log. */
#define NOSQL_LOG_LEN   300

/* A text cut for the log (the TE logger takes no "%.*s"). */
static const char *
cut(const char *s, char *buf, size_t size)
{
    snprintf(buf, size, "%s", s != NULL ? s : "");
    if (s != NULL && strlen(s) >= size && size > 4)
        memcpy(buf + size - 4, "...", 4);
    return buf;
}

static char *
arg(const char *s)
{
    return (char *)(s != NULL ? s : "");
}

static char *
copy(const char *s)
{
    return strdup(s != NULL ? s : "");
}

/* See description in tapi_nosql_rpc.h */
void
rpc_nosql_err_free(rpc_nosql_err *err)
{
    if (err == NULL)
        return;
    free(err->msg);
    err->msg = NULL;
}

static void
set_err(rpc_nosql_err *err, int cls, int native, const char *msg)
{
    if (err == NULL)
        return;
    free(err->msg);
    err->cls = cls;
    err->native = native;
    err->msg = copy(msg);
}

/* See description in tapi_nosql_rpc.h */
te_errno
rpc_nosql_available(rcf_rpc_server *rpcs, unsigned int *engines, char **json)
{
    tarpc_nosql_available_in  in;
    tarpc_nosql_available_out out;
    char                      buf[NOSQL_LOG_LEN];
    te_errno                  rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));

    rcf_rpc_call(rpcs, "nosql_available", &in, &out);
    CHECK_RPC_ERRNO_UNCHANGED(nosql_available, out.retval);
    TAPI_RPC_LOG(rpcs, nosql_available, "", "%d engines=0x%x %s", out.retval,
                 out.engines, cut(out.result, buf, sizeof(buf)));
    if (engines != NULL)
        *engines = out.engines;
    if (json != NULL)
        *json = copy(out.result);
    rc = RPC_IS_CALL_OK(rpcs)
         ? (out.retval == 0 ? 0 : TE_RC(TE_TAPI, TE_EFAIL))
         : TE_RC(TE_TAPI, TE_ECORRUPTED);
    RETVAL_TE_ERRNO(nosql_available, rc);
}

/* See description in tapi_nosql_rpc.h */
te_errno
rpc_nosql_open(rcf_rpc_server *rpcs, const char *engine, const char *config,
               int *handle, char **result, rpc_nosql_err *err)
{
    tarpc_nosql_open_in  in;
    tarpc_nosql_open_out out;
    te_errno             rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.engine = arg(engine);
    in.config = arg(config);

    rcf_rpc_call(rpcs, "nosql_open", &in, &out);
    CHECK_RPC_ERRNO_UNCHANGED(nosql_open, out.retval);
    /* The configuration is deliberately absent from this line. */
    TAPI_RPC_LOG(rpcs, nosql_open, "%s", "%d handle=%d cls=%d %s",
                 arg(engine), out.retval, out.handle, out.errcls,
                 out.errmsg != NULL ? out.errmsg : "");
    if (handle != NULL)
        *handle = out.handle;
    if (result != NULL)
        *result = out.retval == 0 ? copy(out.result) : NULL;
    set_err(err, out.errcls, out.native, out.errmsg);
    rc = !RPC_IS_CALL_OK(rpcs) ? TE_RC(TE_TAPI, TE_ECORRUPTED)
         : out.retval == 0 ? 0 : TE_RC(TE_TAPI, TE_EFAIL);
    RETVAL_TE_ERRNO(nosql_open, rc);
}

/* See description in tapi_nosql_rpc.h */
te_errno
rpc_nosql_close(rcf_rpc_server *rpcs, int handle, rpc_nosql_err *err)
{
    tarpc_nosql_close_in  in;
    tarpc_nosql_close_out out;
    te_errno              rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.handle = handle;

    rcf_rpc_call(rpcs, "nosql_close", &in, &out);
    CHECK_RPC_ERRNO_UNCHANGED(nosql_close, out.retval);
    TAPI_RPC_LOG(rpcs, nosql_close, "%d", "%d %s", handle, out.retval,
                 out.errmsg != NULL ? out.errmsg : "");
    set_err(err, out.errcls, 0, out.errmsg);
    rc = !RPC_IS_CALL_OK(rpcs) ? TE_RC(TE_TAPI, TE_ECORRUPTED)
         : out.retval == 0 ? 0 : TE_RC(TE_TAPI, TE_EFAIL);
    RETVAL_TE_ERRNO(nosql_close, rc);
}

/* See description in tapi_nosql_rpc.h */
te_errno
rpc_nosql_call(rcf_rpc_server *rpcs, int handle, const char *op,
               const char *args, char **result, rpc_nosql_err *err)
{
    tarpc_nosql_call_in  in;
    tarpc_nosql_call_out out;
    char                 abuf[NOSQL_LOG_LEN];
    char                 rbuf[NOSQL_LOG_LEN];
    te_errno             rc;

    memset(&in, 0, sizeof(in));
    memset(&out, 0, sizeof(out));
    in.handle = handle;
    in.op = arg(op);
    in.args = arg(args);

    rcf_rpc_call(rpcs, "nosql_call", &in, &out);
    CHECK_RPC_ERRNO_UNCHANGED(nosql_call, out.retval);
    TAPI_RPC_LOG(rpcs, nosql_call, "%d %s %s", "%d %s", handle, arg(op),
                 cut(args, abuf, sizeof(abuf)), out.retval,
                 out.retval == 0 ? cut(out.result, rbuf, sizeof(rbuf))
                                 : (out.errmsg != NULL ? out.errmsg : ""));
    if (result != NULL)
        *result = out.retval == 0 ? copy(out.result) : NULL;
    set_err(err, out.errcls, out.native, out.errmsg);
    rc = !RPC_IS_CALL_OK(rpcs) ? TE_RC(TE_TAPI, TE_ECORRUPTED)
         : out.retval == 0 ? 0 : TE_RC(TE_TAPI, TE_EFAIL);
    RETVAL_TE_ERRNO(nosql_call, rc);
}
