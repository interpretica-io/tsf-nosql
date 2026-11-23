# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# External TE library (see TE_EXT_REPO): engine-side TAPI for NoSQL
# database sessions on agents. Built as a shared library on the engine
# platform. It needs the nosql_* RPCs in rpcxdr, so add
# ../ta_nosql/nosql_rpc.x.m4 to the rpcxdr definitions of the engine
# platform. Values are jansson json_t.

if get_option('engine')
    build_lib_shared = true
endif
build_lib_static = not get_option('engine')
install_lib = install_dev

headers += files(
    'tapi_nosql.h',
    'tapi_nosql_assert.h',
    'tapi_nosql_audit.h',
    'tapi_nosql_cybersec.h',
    'tapi_nosql_load.h',
    'tapi_nosql_rpc.h',
)

sources += files(
    'tapi_nosql.c',
    'tapi_nosql_assert.c',
    'tapi_nosql_audit.c',
    'tapi_nosql_load.c',
    'tapi_nosql_rpc.c',
)

deps += [ dependency('jansson') ]

te_libs += [
    'tapi',
    'tapi_rpc',
    'rcfrpc',
    'confapi',
    'rcfapi',
    'rpcxdr',
    'logger_core',
    'tools',
]
