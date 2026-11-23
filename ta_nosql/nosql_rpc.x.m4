/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief RPC for NoSQL database sessions
 *
 * The RPCs of rpcs_nosql, a thin layer over ta_nosql, which keeps the
 * connections in the agent's RPC server process and talks to each
 * engine over its own client library. Add this file to the rpcxdr
 * definitions of the engine platform and of the agent platform:
 *
 *   TE_LIB_PARMS([rpcxdr], [<platform>], [],
 *                [--with-rpcdefs=tarpc_job.x.m4,../ta_nosql/nosql_rpc.x.m4])
 *
 * Four RPCs carry everything. nosql_open makes a connection from a JSON
 * configuration and returns its handle; nosql_call runs one named
 * operation on a handle with a JSON object of arguments and returns a
 * JSON object; nosql_close ends a connection; nosql_available says
 * which engines the agent has. Documents, results and reports are JSON
 * text, so no value crosses XDR as a float and nothing about one
 * engine shapes the wire.
 *
 * A failure is a class (errcls: see TA_NOSQL_ERR_* in ta_nosql_core.h
 * and TAPI_NOSQL_ERR_* in tapi_nosql.h, kept in step), the engine's own
 * code (native) and a message. The password of a configuration is
 * carried in nosql_open's config and nowhere else, and the agent
 * scrubs it out of every message and result it returns.
 *
 * Field names avoid the RPC language's keywords ("version",
 * "program", "string").
 */

/* nosql_available(): engines compiled in, their clients and caps. */
struct tarpc_nosql_available_in {
    struct tarpc_in_arg common;
};

struct tarpc_nosql_available_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_uint      engines;
    string          result<>;
};

/* nosql_open(): connect; the handle and what the server said. */
struct tarpc_nosql_open_in {
    struct tarpc_in_arg common;

    string          engine<>;
    string          config<>;
};

struct tarpc_nosql_open_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       handle;
    tarpc_int       errcls;
    tarpc_int       native;
    string          result<>;
    string          errmsg<>;
};

/* nosql_close(): end a connection. */
struct tarpc_nosql_close_in {
    struct tarpc_in_arg common;

    tarpc_int       handle;
};

struct tarpc_nosql_close_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       errcls;
    string          errmsg<>;
};

/* nosql_call(): one operation on a connection (handle 0: none). */
struct tarpc_nosql_call_in {
    struct tarpc_in_arg common;

    tarpc_int       handle;
    string          op<>;
    string          args<>;
};

struct tarpc_nosql_call_out {
    struct tarpc_out_arg common;

    tarpc_int       retval;
    tarpc_int       errcls;
    tarpc_int       native;
    string          result<>;
    string          errmsg<>;
};

program nosql
{
    version ver0
    {
        RPC_DEF(nosql_available)
        RPC_DEF(nosql_open)
        RPC_DEF(nosql_close)
        RPC_DEF(nosql_call)
    } = 1;
} = 51;
