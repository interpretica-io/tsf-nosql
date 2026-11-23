#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
# Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved.
#
# Build the core (ci/build-core.sh) and run ci/core_test against the
# servers of tsf-nosql-ts/scripts/nosql-servers.sh, which must be up
# on this host's loopback. Run it where the client libraries are: the
# image of ci/Dockerfile has them all.
#
#   docker build -t tsf-nosql-core ci
#   docker run --rm --network host -v "$PWD":/src -v <work>:/w -w /src \
#       tsf-nosql-core ci/core-test.sh [TARGET]
#
# <work> is the directory nosql-servers.sh was given (its ca.crt is
# the CA of the TLS servers). Each target's spec is
# "<engine> <config>|<findings the audit must report at MEDIUM or above>".

set -euo pipefail

top="$(cd "$(dirname "$0")/.." && pwd)"
ca="${TSF_NOSQL_CA:-/w/ca.crt}"
app='tsf-fixture-app-3Wq'

export TSF_NOSQL_REDIS='redis {"port":56379}|nosql.auth.none,nosql.tls.plaintext,redis.config-access,redis.protected-mode'
export TSF_NOSQL_REDIS_TLS="redis {\"port\":56380,\"user\":\"tsf\",\"password\":\"${app}\",\"tls\":{\"ca\":\"${ca}\"}}|"
export TSF_NOSQL_REDIS_WEAK='redis {"port":56381,"password":"foobared"}|nosql.auth.default,nosql.tls.plaintext,redis.config-access'
export TSF_NOSQL_MONGODB='mongodb {"port":57017,"replica_set":"rs0"}|nosql.auth.none,nosql.tls.plaintext'
export TSF_NOSQL_MONGODB_TLS="mongodb {\"port\":57018,\"user\":\"tsf\",\"password\":\"${app}\",\"auth_db\":\"tsf\",\"tls\":{\"ca\":\"${ca}\"}}|"
export TSF_NOSQL_CASSANDRA='cassandra {"port":59042,"timeout_ms":20000}|nosql.auth.none,nosql.tls.plaintext'
export TSF_NOSQL_ELASTICSEARCH="elasticsearch {\"port\":59200,\"user\":\"tsf\",\"password\":\"${app}\",\"timeout_ms\":20000}|nosql.tls.plaintext"
export TSF_NOSQL_NEO4J="neo4j {\"port\":57474,\"user\":\"neo4j\",\"password\":\"${app}\",\"timeout_ms\":20000}|nosql.tls.plaintext"
export TSF_NOSQL_MEMCACHED='memcached {"port":51211}|nosql.auth.none,nosql.tls.plaintext,memcached.udp-reflection'
export TSF_NOSQL_ETCD='etcd {"port":52379}|nosql.auth.none,nosql.tls.plaintext'
export TSF_NOSQL_ETCD_AUTH="etcd {\"port\":52380,\"user\":\"tsf\",\"password\":\"${app}\"}|nosql.tls.plaintext"

"${top}/ci/build-core.sh" /tmp/tsf-nosql-core >/dev/null
ASAN_OPTIONS=detect_leaks=1 /tmp/tsf-nosql-core/core_test "$@"
