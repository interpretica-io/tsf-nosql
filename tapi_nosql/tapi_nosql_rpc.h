/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief NoSQL TAPI: one function per RPC
 *
 * The layer beneath tapi_nosql.h, one wrapper per nosql_* RPC (see
 * ta_nosql/nosql_rpc.x.m4). Strings come back malloc()ed; the caller
 * frees them. A test uses tapi_nosql.h; this is for a test that wants
 * the wire as it is.
 */

#ifndef __TAPI_NOSQL_RPC_H__
#define __TAPI_NOSQL_RPC_H__

#include "te_errno.h"
#include "rcf_rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/** A failure as the agent reports it. */
typedef struct rpc_nosql_err {
    int   cls;          /**< TAPI_NOSQL_ERR_* class. */
    int   native;       /**< The engine's own code. */
    char *msg;          /**< Message (malloc()ed), scrubbed of secrets. */
} rpc_nosql_err;

/** Free the message of an error. */
extern void rpc_nosql_err_free(rpc_nosql_err *err);

/**
 * nosql_available().
 *
 * @param[in]  rpcs     RPC server.
 * @param[out] engines  Bitmask of TAPI_NOSQL_ENGINE_*.
 * @param[out] json     The description (malloc()ed), or @c NULL.
 *
 * @return Status code (TE_ECORRUPTED: the RPC itself failed).
 */
extern te_errno rpc_nosql_available(rcf_rpc_server *rpcs,
                                    unsigned int *engines, char **json);

/**
 * nosql_open(). @p config holds the password: it is not logged.
 *
 * @param[in]  rpcs    RPC server.
 * @param[in]  engine  Engine name.
 * @param[in]  config  Configuration, JSON text.
 * @param[out] handle  The connection's handle.
 * @param[out] result  What the server said (malloc()ed), or @c NULL.
 * @param[out] err     The failure, or @c NULL.
 *
 * @return 0, TE_EFAIL when the agent refused (see @p err),
 *         TE_ECORRUPTED when the RPC failed.
 */
extern te_errno rpc_nosql_open(rcf_rpc_server *rpcs, const char *engine,
                               const char *config, int *handle,
                               char **result, rpc_nosql_err *err);

/** nosql_close(); returns as rpc_nosql_open(). */
extern te_errno rpc_nosql_close(rcf_rpc_server *rpcs, int handle,
                                rpc_nosql_err *err);

/**
 * nosql_call(). The arguments are logged cut short, the result is not.
 *
 * @return as rpc_nosql_open().
 */
extern te_errno rpc_nosql_call(rcf_rpc_server *rpcs, int handle,
                               const char *op, const char *args,
                               char **result, rpc_nosql_err *err);

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !__TAPI_NOSQL_RPC_H__ */
