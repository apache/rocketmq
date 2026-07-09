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

## Build And Launcher

Before the read-model benchmark, the modified proxy sources were recompiled and
the focused read-model tests passed:

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
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
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
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
- The single-language and single-connect-time synthetic buckets no longer
  materialize 1,000,000-client candidate copies for page-bounded reads; the
  local read model reuses the existing single filter index and only materializes
  intersections when multiple filters are combined.
- The coordinator experiment shows low P99 latency with 100 synthetic proxy
  shards and `pageSize=100`, but it is not part of the public M1 promise.
