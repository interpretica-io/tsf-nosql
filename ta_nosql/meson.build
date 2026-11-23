# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# External TE library (see TE_EXT_REPO): agent-side NoSQL database
# sessions. Plain C with no TE header (see ta_nosql_core.h), linked into
# and called in-process by the agent's RPC server: each engine is a
# driver behind a seam (ta_nosql_int.h) over the engine's own client
# library. Nothing is spawned: no redis-cli, no mongosh, no cqlsh.
#
# Every driver is optional. A driver is compiled in only when its client
# library is found; the module builds with any of them, or with none
# (tapi_nosql_available() then reports no engine and a suite skips).
# jansson is required: documents are JSON.

headers += files(
    'ta_nosql_core.h',
)

sources += files(
    'ta_nosql_util.c',
    'ta_nosql_core.c',
    'ta_nosql_load.c',
    'ta_nosql_audit.c',
)

te_libs += [
    'tools',
]

nosql_defs = []

deps += [ dependency('jansson') ]
deps += [ dependency('threads') ]

# Redis: hiredis (Debian/Ubuntu: libhiredis-dev), with TLS when
# hiredis_ssl is there too.
hiredis_dep = dependency('hiredis', required: false)
if hiredis_dep.found()
    deps += [ hiredis_dep ]
    sources += files('ta_nosql_redis.c')
    nosql_defs += '-DTA_NOSQL_HAVE_REDIS'
    hiredis_ssl_dep = dependency('hiredis_ssl', required: false)
    if hiredis_ssl_dep.found()
        deps += [ hiredis_ssl_dep ]
        nosql_defs += '-DTA_NOSQL_HAVE_REDIS_SSL'
    endif
endif

# MongoDB: libmongoc (Debian/Ubuntu: libmongoc-dev).
mongoc_dep = dependency('libmongoc-1.0', required: false)
if mongoc_dep.found()
    deps += [ mongoc_dep ]
    sources += files('ta_nosql_mongo.c')
    nosql_defs += '-DTA_NOSQL_HAVE_MONGODB'
endif

# Cassandra: the DataStax C/C++ driver (not packaged by Debian/Ubuntu;
# built from source, see ci/Dockerfile).
cassandra_dep = dependency('cassandra', required: false)
if cassandra_dep.found()
    deps += [ cassandra_dep ]
    sources += files('ta_nosql_cassandra.c')
    nosql_defs += '-DTA_NOSQL_HAVE_CASSANDRA'
endif

# Memcached: libmemcached (Debian/Ubuntu: libmemcached-dev).
memcached_dep = dependency('libmemcached', required: false)
if memcached_dep.found()
    deps += [ memcached_dep ]
    sources += files('ta_nosql_memcached.c')
    nosql_defs += '-DTA_NOSQL_HAVE_MEMCACHED'
endif

# Elasticsearch, Neo4j and etcd speak JSON over HTTP: libcurl.
curl_dep = dependency('libcurl', required: false)
if curl_dep.found()
    deps += [ curl_dep ]
    sources += files('ta_nosql_http.c', 'ta_nosql_elastic.c',
                     'ta_nosql_neo4j.c', 'ta_nosql_etcd.c')
    nosql_defs += '-DTA_NOSQL_HAVE_CURL'
endif

if nosql_defs.length() == 0
    warning('tsf-nosql: none of hiredis, libmongoc, cassandra, ' +
            'libmemcached, libcurl found; ta_nosql will report no engine ' +
            'and the suite will skip.')
else
    deps += declare_dependency(compile_args: nosql_defs)
endif
