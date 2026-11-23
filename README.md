# tsf-nosql

tsf-nosql lets a Test Environment (TE) test work with NoSQL databases:
store and read documents, query, count, write under a condition, keep
counters, expire entries, write several keys at once, run the engine's
own commands, load a server from many threads and measure it, and audit
what a stranger can do to it. It is an external TE repository, consumed
with the `TE_EXT_REPO` builder directive.

The database is reached from a Test Agent. The connection lives in the
agent's RPC server, which links the engine's own client library. No
`redis-cli`, `mongosh` or `cqlsh` is run and no program output is
parsed.

| Engine | Client library | Not offered |
|---|---|---|
| Redis | hiredis (TLS with hiredis_ssl) | find, count |
| MongoDB | libmongoc | |
| Cassandra | DataStax C/C++ driver | find, exact counter, conditional multi |
| Elasticsearch | libcurl (REST) | ttl, multi |
| Neo4j | libcurl (HTTP Cypher API) | ttl |
| Memcached | libmemcached | scan, find, count, multi, drop, TLS, auth |
| etcd | libcurl (v3 JSON gateway) | find |

Every driver is optional at build time: a driver is compiled in when its
client library is found, and `tapi_nosql_supports()` tells a test which
engines the agent has. An operation an engine does not do natively
fails as `unsupported` rather than being emulated; `tapi_nosql_has_cap()`
says beforehand.

## The data model

One model for every engine:

- a collection is a lower-case identifier (`[a-z][a-z0-9_]*`, up to 48
  characters). It is a key prefix in Redis, Memcached and etcd, a
  collection in MongoDB, a table in Cassandra, an index in Elasticsearch
  and a node label in Neo4j;
- a document is a JSON object under a string key of up to 200 bytes. It
  reads back equal to what was written, types included: an integer stays
  an integer, `1.0` stays a real, `{"$gt": 1}` stays an object. Top-level
  field names starting with `_` or `$` are reserved for the module;
- a version is an opaque string a read returns and a compare-and-set
  compares. It changes on every write of the key;
- a counter is an integer under a key, changed by `incr` only.

How each engine stores a document is in its driver's file comment
(`ta_nosql/ta_nosql_<engine>.c`). Two details matter to a test:
Elasticsearch keeps the document in an object it does not index plus a
copy of its top-level scalar fields for queries, because its dynamic
mapping refuses many valid documents; Neo4j does the same with node
properties.

## Libraries

- `ta_nosql` (agent side): a plain-C core with no TE header,
  `ta_nosql_core.h`. It keeps the connections in a handle registry,
  checks every operation's arguments once and puts each driver's answer
  into the same shape (`ta_nosql_core.c`); runs loads (`ta_nosql_load.c`)
  and audits (`ta_nosql_audit.c`). An engine is one translation unit
  behind `ta_nosql_int.h`.
- `rpcs_nosql`: the four RPCs, `nosql_available`, `nosql_open`,
  `nosql_close` and `nosql_call` (program 51, `ta_nosql/nosql_rpc.x.m4`).
- `tapi_nosql` (engine side, values are jansson `json_t`):
  `tapi_nosql.h` is the API tests use; `tapi_nosql_assert.h` the
  assertions, `tapi_nosql_load.h` loads and their MI charts,
  `tapi_nosql_audit.h` the security checks, `tapi_nosql_cybersec.h` an
  optional header-only bridge to tsf-cybersec reports, `tapi_nosql_rpc.h`
  one function per RPC.

tsf-nosql depends on TE, jansson and the client libraries. tsf-cybersec
is optional.

## Example

```c
json_t          *cfg = json_pack("{s:s, s:i, s:s, s:s, s:{s:s}}",
                                 "host", "db.lab", "port", 6380,
                                 "user", "tester",
                                 "password", getenv("REDIS_PASSWORD"),
                                 "tls", "ca", "/etc/lab/ca.pem");
tapi_nosql_conn *db = NULL;
json_t          *doc = json_pack("{s:s, s:i}", "name", "alice", "age", 30);
json_t          *got = NULL;
char            *ver = NULL;
bool             applied = false;

CHECK_RC(tapi_nosql_open(pco, "redis", cfg, &db, NULL));
CHECK_RC(tapi_nosql_put(db, "users", "u1", doc, 0, NULL));
CHECK_RC(tapi_nosql_get(db, "users", "u1", &got, &ver));
CHECK_RC(tapi_nosql_put_if_version(db, "users", "u1", doc, ver, &applied,
                                   NULL));
CHECK_RC(tapi_nosql_assert_doc(db, "users", "u1", doc));
tapi_nosql_close(db);
```

## Operations

`tapi_nosql_call(conn, op, args, &result)` runs any of them; the typed
functions in `tapi_nosql.h` wrap each.

| Operation | Arguments | Result |
|---|---|---|
| `ping` | | `rtt_us` |
| `info` | | `engine`, `server_version`, `client`, `tls`, `caps`, engine extras |
| `put` | `coll`, `key`, `doc`, `ttl_s` | `version` (or null) |
| `put_if` | `coll`, `key`, `doc`, `if`: `absent` or `version` (+ `version`), `ttl_s` | `applied`, `version` |
| `get` | `coll`, `key` | `found`, `doc`, `version` |
| `delete` | `coll`, `key` | `deleted` |
| `incr` | `coll`, `key`, `by` (0 reads) | `value` |
| `scan` | `coll`, `limit` (up to 1000), `cursor` | `keys`, `cursor` (null at the end) |
| `find` | `coll`, `filter` (field: string, integer or boolean), `limit`, `cursor` | `docs` ([{key, doc}]), `cursor` |
| `count` | `coll`, `filter` | `count` |
| `multi` | `ops` ([{op: put or delete, coll, key, doc}]), `if_absent` {coll, key} | `applied` |
| `drop` | `coll` | |
| `native` | `cmd` (see below) | `reply` |
| `bulk` | `coll`, `items` or `generate` {count, size, prefix, seed}, `batch` | `written`, `elapsed_us` |
| `load` | `mode` (put, get, incr, rmw, absent, cas, torn, scan_churn), `coll`, `key`, `prefix`, `keys`, `size`, `threads`, `ops`, `think_us`, `passes`, `stable` | counts, errors by class, latency percentiles, and per mode `final`, `distinct`, `lost`, `applied`, `conflicts`, `torn`, `missing` |
| `audit` | `default_creds`, `checks` | `findings`, `checked` |
| `set_timeout` | `ms` | |
| `registry` | (no connection needed) | `open`, `connections` |

Not applying is a result: a `put_if` whose condition fails returns
`applied: false`, not an error.

`find` compares with the value as a value: MongoDB gets `{field: {$eq:
value}}`, Neo4j a parameter, Elasticsearch a `term` query. An object in
a filter is refused, so input an application passes through cannot
become an operator.

### Native commands

| Engine | `cmd` |
|---|---|
| Redis | `{"argv": ["HSET", "k", "f", "v"]}` |
| MongoDB | `{"db": "admin", "command": {"buildInfo": 1}}` (the first field names the command) |
| Cassandra | `{"cql": "SELECT ... WHERE k = ?", "params": [1]}` |
| Elasticsearch, etcd | `{"method": "POST", "path": "/...", "body": {...}}`, the reply carries the HTTP `status` and `body` |
| Neo4j | `{"cypher": "RETURN $x", "params": {"x": 1}}` |
| Memcached | `{"stats": ""}` (or `"settings"`, `"items"`, `"slabs"`) |

## Errors

A failure has a class, the engine's own code and a message. The classes:
`connect`, `auth`, `permission`, `tls`, `timeout`, `invalid`,
`unsupported`, `unavailable`, `limit`, `protocol`, `handle`, `internal`.
`tapi_nosql_last_cls()` gives the class of the last failure on a
connection, and `tapi_nosql_errno()` the `te_errno` each maps to
(`TE_ETIMEDOUT` for a timeout, `TE_EACCES` for refused credentials and
so on). A failure that took the whole deadline is a timeout whatever the
client library called it.

A connection that failed is reconnected before its next operation, so a
server that comes back makes the same handle work again. A handle
carries a random per-process generation: after the RPC server restarts,
an old handle is refused, never mistaken for a new connection.

The password of a configuration is never written into a result, an
error message or an audit finding: messages are scrubbed of it before
they leave the agent.

## Security checks

`tapi_nosql_audit()` runs on the agent with connections of its own.
Every engine gets:

- `nosql.auth.none` (HIGH): a client with no credentials is served. The
  probe reads a key that does not exist, so it changes nothing;
- `nosql.auth.default` (HIGH): a well-known credential logs in
  (`cassandra`/`cassandra`, `elastic`/`changeme`, Redis `foobared` and
  the like). Only when asked: it is a login attempt;
- `nosql.tls.plaintext` (MEDIUM): the server takes unencrypted
  connections.

and the engine's own: `redis.config-access` and `redis.protected-mode`,
`mongodb.superuser`, `cassandra.superuser`, `elasticsearch.superuser`
(the account given administers the server), `memcached.udp-reflection`
(UDP answers; the detail has the amplification factor),
`neo4j.version-disclosed`, `etcd.open/version` and `etcd.open/metrics`
(served to anyone). A check that could not decide reports
`*.not-assessed` at INFO instead of passing.

Authorized use only: audit servers you own or are engaged to test.

## Load and measurements

`tapi_nosql_load()` starts threads on the agent, each with a connection
of its own, and records every operation's latency. The answer has the
exact percentiles, the throughput and the errors by class.
`tapi_nosql_load_mi()` logs it as a TE MI artifact with a line graph of
latency over percentile.

The modes for concurrency tests:

- `incr`: one counter; the final value and how many distinct values the
  increments returned (on an exact counter, each appears once);
- `rmw`: read-modify-write with no condition, the known-bad control of a
  lost-update test: with `think_us` between the read and the write it
  loses updates, and reports how many;
- `absent`: every thread writes one key if absent; `applied` must be 1;
- `cas`: every thread reads a document `{"n"}` and writes n + 1 under the
  version it read; the final n must equal `applied` (`lost` is the
  difference). This mode found that the Neo4j driver compared before
  taking the node's lock, so two compare-and-sets on one version both
  applied; the driver now locks first;
- `torn`: thread 0 writes keys `a` and `b` together, the others read `b`
  then `a`; `torn` counts multi-writes seen half applied;
- `scan_churn`: thread 0 scans the collection `passes` times while the
  others write and delete around `stable` keys written before; `missing`
  counts the stable keys a pass did not see.

## Engine notes

- MongoDB removes expired documents in a background pass, every 60 s by
  default (`ttlMonitorSleepSecs`); a document is readable until then.
  `multi` is a transaction and needs a replica set; on a standalone
  server it fails as `unsupported`.
- Redis `SCAN` takes the page size as a hint and may return a key twice
  during a walk; `tapi_nosql_scan_all()` drops the repeats and counts
  them.
- Elasticsearch searches what the last refresh made visible, so `scan`,
  `find` and `count` refresh the index first (configuration
  `refresh_before_query`, on by default). String fields longer than
  8191 characters are stored but not matched by `find`.
- Cassandra's counter update returns nothing, so `incr` reads the
  counter after updating it; under concurrency the value returned can
  include other clients' increments. A logged batch takes no condition
  across partitions, so there is no conditional `multi`.
- Memcached keeps items of at most 1 MiB (its `-I` default), the
  item's overhead included, and keeps nothing over a restart.

## Limits

Each of these fails with the class `limit`, so a test can tell "too big"
from "wrong": a document over 8 MiB and more than 255 connections in one
RPC server (the module's own), Elasticsearch's 1000 fields per index,
MongoDB's 180 levels of nesting, etcd's 1.5 MiB request, Redis at
`maxmemory` with `noeviction`, Memcached's 1 MiB item.

## Configuration

`host`, `port` (0: the engine's default), `user`, `password`,
`database` (MongoDB database, Cassandra keyspace, Neo4j database, Redis
database number), `tls` (`true`, or `{"enabled", "ca", "verify"}`),
`timeout_ms` (default 5000), `connect_timeout_ms` (default 3000). Engine
extras:

- MongoDB: `replica_set`, `auth_db`, `seeds` (`"h1:p1,h2:p2"`, the
  members of a replica set), `write_concern` (`"majority"` or a number),
  `journal`, `read_preference` (`secondary` and the others);
- Cassandra: `host` may list several nodes (`"10.0.0.1,10.0.0.2"`),
  `consistency` (`ONE`, `QUORUM`, `ALL`, `LOCAL_QUORUM` and the others),
  `create_keyspace`, `replication_factor`;
- etcd: `endpoints` (`["h1:2379", "h2:2379"]`): a member that went away or
  has no leader is replaced by the next;
- Elasticsearch: `index_prefix`, `refresh_before_query`;
- Redis: `tls_server_name`.

A deadline bounds everything an operation does: MongoDB's server
selection and connection handshake are capped by `timeout_ms`, so a
server that stopped answering fails at the deadline.

## Building

In the suite's `conf/builder.conf`:

```
TE_EXT_REPO_USE([tsf_nosql], [], [ta_nosql rpcs_nosql tapi_nosql])
TE_LIB_PARMS([rpcxdr], [], [],
             [--with-rpcdefs=tarpc_job.x.m4,../ta_nosql/nosql_rpc.x.m4])
TE_TA_APP([ta_rpcprovider], [], [linux], [ta_rpcprovider], [], [],
          [ta_nosql rpcs_nosql ... rpcserver agentlib rpcxdrta ...],
          [\${EXT_SOURCES}/build.sh --extra-deps='libpcre2-8 jansson hiredis hiredis_ssl libmongoc-1.0 cassandra libmemcached libcurl'],
          [ta_rpcs], [])
```

On Ubuntu 24.04 the client libraries are `libhiredis-dev`,
`libmongoc-dev`, `libmemcached-dev`, `libcurl4-openssl-dev` and
`libjansson-dev`. The Cassandra driver is not packaged; `ci/Dockerfile`
builds it from source (release 2.17.1, CMake, libuv).

## Testing

`ci/core-test.sh` builds the core without TE, under AddressSanitizer
and UBSan, and runs its contract tests (`ci/core_test.c`, about 700
checks) against live servers: every operation on every engine, with the
expectation taken from the engine's capabilities, and each audit
checked against the weaknesses its server was set up with. The servers
are those of
[tsf-nosql-ts](https://github.com/interpretica-io/tsf-nosql-ts):

```sh
../tsf-nosql-ts/scripts/nosql-servers.sh up /tmp/nosql-work
docker build -t tsf-nosql-core ci
docker run --rm --network host -v "$PWD":/src -v /tmp/nosql-work:/w \
    -w /src tsf-nosql-core ci/core-test.sh
```

`ci/nosql_cli` runs one operation from the command line. The RPC layer
and the engine-side TAPI are tested end to end by tsf-nosql-ts.

## Licence

MIT, see `LICENSE`.
