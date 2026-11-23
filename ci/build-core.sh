#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# Build the agent-side core of ta_nosql on its own - no TE - with every
# driver whose client library pkg-config finds, under ASan and UBSan,
# together with ci/core_test (the contract tests against live servers)
# and ci/nosql_cli (one operation from the command line).
#
# Usage: ci/build-core.sh [build directory]   (default: build-ci)

set -euo pipefail

top="$(cd "$(dirname "$0")/.." && pwd)"
out="${1:-${top}/build-ci}"
mkdir -p "${out}"

# shellcheck disable=SC2054 # the commas are part of -fsanitize=
cflags=(-std=gnu11 -g -O1 -Wall -Wextra -Werror -Wno-unused-parameter
        -fsanitize=address,undefined -fno-omit-frame-pointer
        -I"${top}/ta_nosql")
# shellcheck disable=SC2054
libs=(-fsanitize=address,undefined -lpthread -lm)
srcs=(ta_nosql_util.c ta_nosql_core.c ta_nosql_load.c ta_nosql_audit.c)

have() { pkg-config --exists "$1"; }

# Add a package's compiler and linker flags (one word each).
use() {
    local w

    read -ra w <<< "$(pkg-config --cflags "$1")"
    cflags+=(${w[@]+"${w[@]}"})
    read -ra w <<< "$(pkg-config --libs "$1")"
    libs+=(${w[@]+"${w[@]}"})
}

use jansson

if have hiredis ; then
    srcs+=(ta_nosql_redis.c)
    cflags+=(-DTA_NOSQL_HAVE_REDIS)
    use hiredis
    if have hiredis_ssl ; then
        cflags+=(-DTA_NOSQL_HAVE_REDIS_SSL)
        use hiredis_ssl
    fi
fi
if have libmongoc-1.0 ; then
    srcs+=(ta_nosql_mongo.c)
    cflags+=(-DTA_NOSQL_HAVE_MONGODB)
    use libmongoc-1.0
fi
if have cassandra ; then
    srcs+=(ta_nosql_cassandra.c)
    cflags+=(-DTA_NOSQL_HAVE_CASSANDRA)
    use cassandra
fi
if have libmemcached ; then
    srcs+=(ta_nosql_memcached.c)
    cflags+=(-DTA_NOSQL_HAVE_MEMCACHED)
    use libmemcached
fi
if have libcurl ; then
    srcs+=(ta_nosql_http.c ta_nosql_elastic.c ta_nosql_neo4j.c ta_nosql_etcd.c)
    cflags+=(-DTA_NOSQL_HAVE_CURL)
    use libcurl
fi

objs=()
for s in "${srcs[@]}" ; do
    o="${out}/${s%.c}.o"
    cc "${cflags[@]}" -c "${top}/ta_nosql/${s}" -o "${o}"
    objs+=("${o}")
done
for prog in core_test nosql_cli ; do
    cc "${cflags[@]}" -c "${top}/ci/${prog}.c" -o "${out}/${prog}.o"
    cc -o "${out}/${prog}" "${out}/${prog}.o" "${objs[@]}" "${libs[@]}"
done
echo "built ${out}/core_test and ${out}/nosql_cli (${#srcs[@]} units)"
