# RIP-2 Proxy Admin 1M Client Benchmark Report

## Scope

This report records the local 1,000,000 synthetic client JMH run for the
RIP-2 Proxy Admin M1 read model, the generated public gRPC endpoint, and the
internal coordinator experiment.

The acceptance signal for this checkpoint is paginated online-client query
P99 latency below 1 second on a single local proxy read model and through the
generated public `ProxyAdminService` endpoint. The coordinator numbers are
included as forward-looking evidence for the internal multi-proxy experiment;
the public M1 contract remains `LOCAL_PROXY` until the public protobuf API is
accepted.

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-07-10 |
| Read-model commit | `bc83087f5f40` |
| Public endpoint benchmark source | `GrpcProxyAdminApplicationBenchmark` in this checkpoint |
| Machine | Apple M4 |
| Logical CPUs | 10 |
| Memory | 16 GB |
| OS | macOS 26.5.1 |
| JDK | Temurin OpenJDK 17.0.18+8 |
| JMH | 1.36 |
| JVM args | `-Xms2g -Xmx6g` |

## Constrained Heap Worst-Case Proof

The final memory gate was rerun on 2026-07-11 with a 4 GiB fixed heap
(`-Xms4g -Xmx4g -XX:+UseG1GC`), one fork, four caller threads, one 2-second
warmup iteration, and three 3-second measurement iterations. Each process
recorded JMH sample-time percentiles, the JMH GC profiler, a JFR recording, a
unified GC log, and `/usr/bin/time -l` process statistics.

The public endpoint case used a real generated `ProxyAdminServiceGrpc`
Server/Channel with separate production-shaped server and query executors:
four threads and a bounded 10,000-task queue for each executor. The request
combined the broad `client-` prefix, `JAVA` language, connect time `100..100`,
and `pageSize=100`.

| Scenario | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC count/time | Max JFR heapUsed | Max RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Broad prefix over 1M clients | 4.067 | 5.210 | 137.526 | 3745.513 | 6511.3 | 0 / 0 ms | 775.9 MiB | 1119.2 MiB |
| Broad prefix + language + connect time | 4.112 | 5.087 | 243.610 | 9495.904 | 5509.6 | 0 / 0 ms | 772.9 MiB | 1114.0 MiB |
| Deep page `pageNum=10000`, `pageSize=100` | 0.009 | 0.010 | 0.016 | 593.494 | 1140.6 | 5 / 8 ms | 1126.4 MiB | 1208.7 MiB |
| Generated public gRPC combined filters | 16.564 | 26.241 | 29.042 | 35.193 | 188604.8 B/op | 3 / 26 ms | 1000.3 MiB | 1283.0 MiB |

Acceptance P99 summary: broad prefix `137.526 ms`, combined filters
`243.610 ms`, deep page `0.016 ms`, and generated public gRPC `29.042 ms`.

All four P99 values are below 1 second. All processes exited normally with no
OOM, zero swaps, and maximum RSS below 1.3 GiB. Max values are retained because
JFR and workstation scheduling can create rare outliers; the contest
acceptance metric is P99.

The two read-model scenarios with zero measurement GC had no
`jdk.GCHeapSummary` event in the measurement-only JMH recording. Their listed
max heap values come from a second fork-start JFR probe using
`-XX:StartFlightRecording`, which captured the 1M setup GCs. The latency and
allocation values remain those from the full formal runs above.

Performance TDD exposed and fixed three concrete problems:

- Broad prefix initially measured P99 `8388.608 ms` and about 40.1 MB/op
  because every query copied the complete prefix range. It now uses the live
  ordered index range view.
- The three-filter intersection initially measured P99 `13505.659 ms` and
  about 40.1 MB/op because it materialized a million-client `TreeSet`. It now
  drives the smallest ordered index and stops after finding `pageSize + 1`
  matching clients.
- Deep page initially measured P99 `1157.313 ms` because every query skipped
  999,900 tree nodes. A lazily rebuilt client-id page-anchor cache now starts
  from the nearest 100-client boundary and is invalidated by insert/remove.

Representative command template, with all repository paths relative to the
RocketMQ root:

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat proxy/target/rip2-benchmark/classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listDeepPage \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 \
  -wi 1 -i 3 -w 2s -r 3s -f 1 -t 4 \
  -jvmArgsAppend "-Xms4g -Xmx4g -XX:+UseG1GC" \
  -prof gc
```

## Build And Launcher

Use the repository-owned launcher for new evidence instead of reconstructing a
classpath or writing the only copy of a result under `/tmp`:

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
dev/run_rip2_benchmark.sh \
  million-read-model-first-page \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listFirstPage
```

The launcher accepts exactly one JMH method and defaults to 1,000,000 clients,
a fixed 4 GiB G1 heap, one fork, and four caller threads. Override its `RIP2_*`
environment variables for a short smoke run. It writes evidence under
`target/rip2-benchmark-results/<label>/`: `jmh.json`, `jmh.log`,
`recording.jfr`, `gc.log`, `time.txt`, `classpath.txt`, `command.txt`, and a
copy of `runner.sh`, `source-files.txt`, and a `SHA256SUMS` manifest. Verify the
manifest from inside that output directory so
its relative paths resolve correctly.

Before the read-model benchmark, the modified proxy sources were recompiled and
the focused read-model tests passed:

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
mvn -pl proxy -am \
  "-Dtest=ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientQueryTest,ProxyClientInfoTest,ProxyClientReadServiceCleanerTest" \
  -DfailIfNoTests=false test -DskipITs
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile=/tmp/rocketmq-proxy-test-classpath.txt
```

The focused test run finished with `Tests run: 65, Failures: 0, Errors: 0` and
`BUILD SUCCESS`.

Before the generated public endpoint benchmark, the generated gRPC endpoint and
benchmark setup tests passed:

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
mvn -pl proxy -am \
  "-Dtest=GrpcProxyAdminApplicationBenchmarkTest,GrpcProxyAdminApplicationTest,ProxyClientReadServiceBenchmarkTest" \
  -DfailIfNoTests=false test -DskipITs
```

The run finished with `Tests run: 29, Failures: 0, Errors: 0, Skipped: 0` and
`BUILD SUCCESS`.

## Read Model 1M Result

Command:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 \
  -jvmArgsAppend "-Xms2g -Xmx6g" \
  -rf json -rff /tmp/rip2-readmodel-jmh-1m-optimized-full.json
```

JMH completed in 00:05:04 and wrote
`/tmp/rip2-readmodel-jmh-1m-optimized-full.json`.

| Operation | Score ms/op | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `describeClient` | 0.001 | 0.000 | 0.001 | 0.017 | 3.027 |
| `listByClientIdPrefixPage` | 3.080 | 0.551 | 0.584 | 0.603 | 10519.314 |
| `listByConnectTimeRangePage` | 0.007 | 0.001 | 0.029 | 0.069 | 200.540 |
| `listByGroupPage` | 0.020 | 0.005 | 0.007 | 0.561 | 17.433 |
| `listByLanguagePage` | 0.008 | 0.002 | 0.011 | 0.087 | 206.832 |
| `listByProxyIdPage` | 0.011 | 0.003 | 0.003 | 0.004 | 355.467 |
| `listByTopicPage` | 0.023 | 0.005 | 0.008 | 0.681 | 20.972 |
| `listFirstPage` | 0.012 | 0.002 | 0.007 | 0.012 | 376.439 |
| `listNextPage` | 0.012 | 0.002 | 0.007 | 0.008 | 125.960 |

Worst read-model P99: `listByTopicPage` at 0.681 ms.
All read-model operations stayed below the 1 second P99 target.

The max column includes workstation scheduling and GC outliers observed during
sample-time collection. It is retained for transparency, but the contest target
tracked here is P99.

## Generated Public gRPC Endpoint 1M Result

This benchmark starts a real generated `ProxyAdminServiceGrpc` server and
blocking client channel on local loopback, then drives the public
`GrpcProxyAdminApplication` through the same endpoint executor, authorization
facade, request converter, response converter, and read model used by the
Proxy admin server wiring.

Command:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplicationBenchmark \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 \
  -wi 1 -i 3 -r 2 -w 1 -f 1 -t 4 \
  -jvmArgsAppend "-Xms2g -Xmx6g" \
  -rf json -rff /tmp/rip2-public-grpc-jmh-1m.json
```

JMH completed in 00:01:14 and wrote `/tmp/rip2-public-grpc-jmh-1m.json`.

| Operation | Score ms/op | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `describeClient` | 0.059 | 0.055 | 0.092 | 0.137 | 1.585 |
| `listClients` | 0.189 | 0.171 | 0.279 | 0.387 | 1.890 |
| `listClientsByClientIdPrefix` | 2.465 | 2.413 | 2.982 | 3.576 | 4.571 |
| `listClientsByConnectTimeRange` | 0.179 | 0.166 | 0.271 | 0.407 | 1.309 |
| `listClientsByGroup` | 0.217 | 0.194 | 0.372 | 0.511 | 2.511 |
| `listClientsByLanguage` | 0.177 | 0.162 | 0.291 | 0.410 | 1.737 |
| `listClientsByTopic` | 0.192 | 0.173 | 0.316 | 0.449 | 2.421 |

Worst generated public endpoint P99: `listClientsByClientIdPrefix` at
3.576 ms. All generated public endpoint operations stayed below the 1 second
P99 target.

## Coordinator 1M Result

The coordinator results below are retained from the earlier 2026-07-08 internal
experiment at commit `251c18567f37`. They were not rerun in the
`bc83087f5f40` read-model checkpoint because the public M1 contract remains
`LOCAL_PROXY` and the coordinator is not part of the public performance claim.

Command:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminCoordinatorServiceBenchmark \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 -p pageSize=100 \
  -jvmArgsAppend "-Xms2g -Xmx6g" \
  -rf json -rff /tmp/rip2-coordinator-jmh-1m.json \
  > /tmp/rip2-coordinator-jmh-1m.txt 2>&1
```

JMH wrote `/tmp/rip2-coordinator-jmh-1m.json`.

| Operation | Score ms/op | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `describeClientAllProxies` | 0.107 | 0.104 | 0.199 | 0.239 | 5.603 |
| `listAllProxiesByClientIdPrefixPage` | 5.249 | 5.071 | 6.808 | 8.069 | 16.105 |
| `listAllProxiesByConnectTimeRangePage` | 5.306 | 5.210 | 6.453 | 7.820 | 14.811 |
| `listAllProxiesByGroupPage` | 0.175 | 0.156 | 0.295 | 0.420 | 6.275 |
| `listAllProxiesByLanguagePage` | 5.169 | 4.997 | 6.578 | 8.302 | 38.076 |
| `listAllProxiesByTopicPage` | 0.155 | 0.140 | 0.259 | 0.377 | 37.618 |
| `listAllProxiesFirstPage` | 5.142 | 4.997 | 6.480 | 7.902 | 66.322 |
| `listAllProxiesNextPage` | 5.518 | 5.325 | 7.004 | 9.011 | 69.337 |
| `listProxyIdPage` | 0.033 | 0.032 | 0.063 | 0.086 | 27.787 |

Worst coordinator P99: `listAllProxiesNextPage` at 9.011 ms. The coordinator
path is still internal exploratory work, but this run shows the current
fan-out/merge implementation has enough headroom for later `MULTI_PROXY`
discussion.

## Interpretation

- The local read model satisfies the 1M synthetic client P99 target for all
  implemented official query shapes: unfiltered page, group, topic, prefix,
  language, connect-time range, proxy-id, and describe.
- The generated public gRPC endpoint also satisfies the 1M synthetic client P99
  target for the public M1 methods and filters. The benchmark traverses a real
  generated server/channel and the public endpoint adapter; the slowest public
  path was `listClientsByClientIdPrefix` at 3.576 ms P99.
- Single-bucket time queries reuse the existing time index. Multi-bucket time
  ranges do not union client-id sets: production queries drive the smallest
  available ordered index and validate connect time per candidate until the
  bounded page is full.
- The coordinator experiment shows low P99 latency with 100 synthetic proxy
  shards and `pageSize=100`, but it is not part of the public M1 promise.

## Wide Connect-Time Range 1M Proof

The 2026-07-12 follow-up distributes 1,000,000 synthetic clients evenly over
100 connect-time buckets (`100..199`) and queries the complete range with
`pageSize=100`. This covers the multi-bucket path that the earlier exact
`100..100` benchmark did not exercise. Both runs used Temurin 17.0.18, four
caller threads, one 2-second warmup, three 3-second measurements, a fixed 4 GiB
G1 heap, and the JMH GC profiler.

| Path | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC count/time | Max RSS | Swap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read model `listByWideConnectTimeRangePage` | 0.004 | 0.008 | 0.010 | 60.228 | 1113.352 | 4 / 2 ms | 2448.4 MiB | 0 |
| Generated gRPC `listClientsByWideConnectTimeRange` | 0.196 | 0.275 | 0.394 | 10.781 | 174970.776 | 15 / 22 ms | 2916.2 MiB | 0 |

Both P99 values are below 1 second and both forks exited normally without OOM.
The raw JMH JSON files are
`/tmp/rip2-wide-connect-time-jmh-1m.json` and
`/tmp/rip2-public-wide-connect-time-jmh-1m.json`.

Representative include names:

```bash
org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listByWideConnectTimeRangePage
org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplicationBenchmark.listClientsByWideConnectTimeRange
```

## Reproducible Deep Full-Range Pagination Proof

The final 2026-07-12 follow-up tests `pageNum=10000`, `pageSize=100` over the
complete `100..199` connect-time range. The optimized implementation is at
`1dd5c6fd1f7e8f5d684213d72c4b965d214f977d`; reproducible runner and evidence
checkpoint `8c3098d51615189677118200955aeb6bdcbf90c0` produced the final data below.
The first k-way bucket merge run
made the cost visible: read-model P99 was `2906.653 ms`. The final path detects
that the requested time bounds cover the ordered time index, then reuses the
mutation-invalidated client-id page anchors. Partial ranges still use the
bounded k-way bucket merge.

Both final runs used the repository launcher with its default one fork, four
caller threads, one 2-second warmup, three 3-second measurements, 1,000,000
clients, and fixed 4 GiB G1 heap:

```bash
dev/run_rip2_benchmark.sh \
  evidence-v4-million-deep-readmodel-20260712 \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listDeepPageByWideConnectTimeRange
dev/run_rip2_benchmark.sh \
  evidence-v4-million-deep-public-grpc-20260712 \
  org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplicationBenchmark.listClientsDeepPageByWideConnectTimeRange
```

| Path | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC count/time | Max RSS | Swap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read model deep full-range page | 0.009 | 0.010 | 0.011 | 722.469 | 1147.567 | 5 / 10 ms | 1223.7 MiB | 0 |
| Generated public gRPC deep full-range page | 0.271 | 0.582 | 0.843 | 8.389 | 178710.734 | 11 / 26 ms | 2792.3 MiB | 0 |

Both P99 values are below one second; both forks exited normally without OOM.
The final read-model P99 is `0.011 ms`; the generated public gRPC P99 is
`0.843 ms`.
Each output directory contains `build.log`, `environment.txt`, `command.txt`,
the resolved classpath, JMH JSON/log, GC log, JFR, process-time output, the
exact `runner.sh`, tracked `source-files.txt`, and a verified `SHA256SUMS`
manifest. Key durable digests are:

| Evidence | `jmh.json` SHA-256 | `recording.jfr` SHA-256 | `environment.txt` SHA-256 |
| --- | --- | --- | --- |
| Read model | `81f309ab9559d60772b26f59ec3a1d4de618840f3a4b949a934d8367b1672308` | `199ecbaed817c8b8ed33bed6fb8af376f98223ec55ba04502b00cfd203cf7e13` | `072f1e1557028ee10acb59f4a47f2309ec6eb401bcce5c32ee96da0667e2a8c5` |
| Generated public gRPC | `d519f533b3d20e57a9fec0d15dca339ef769da3eae18817ad34b04a1ca91ee91` | `4b43c03774bb86eb7ccd9bdb3a02171b0c83f780377e8bf36adfd0b7b8eef2a9` | `e3c78578f6f1a96d4796e89de0da9823951f3d291db0ce36eca461c04d6bbe83` |

Both bundles pin `runner.sh` SHA-256
`f58b88d5234c97ea6942968e970cca247b8821e018337418bd68bf2d26ae6975`
and `source-files.txt` SHA-256
`841a5ec9c6a4059a88b8f4e42714182f9c8ac1e8a2a7ef5461f05c6c5dc09251`.
