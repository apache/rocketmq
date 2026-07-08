# RIP-2 Proxy Admin 1M Client Benchmark Report

## Scope

This report records the local 1,000,000 synthetic client JMH run for the
RIP-2 Proxy Admin M1 read model and the internal coordinator experiment.

The acceptance signal for this checkpoint is paginated online-client query
P99 latency below 1 second on a single local proxy read model. The coordinator
numbers are included as forward-looking evidence for the internal multi-proxy
experiment; the public M1 contract remains `LOCAL_PROXY` until the public
protobuf API is accepted.

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-07-08 |
| Commit | `251c18567f37` |
| Machine | Apple M4 |
| Logical CPUs | 10 |
| Memory | 16 GB |
| OS | macOS 26.5.1 |
| JDK | Temurin OpenJDK 17.0.18+8 |
| JMH | 1.36 |
| JVM args | `-Xms2g -Xmx6g` |

## Build And Launcher

The benchmark used a clean proxy test compile so JMH generated classes were
rebuilt from the current sources:

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -pl proxy -am -DskipTests -DskipITs clean test-compile
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile=/tmp/rocketmq-proxy-test-classpath.txt
```

The compile finished with `BUILD SUCCESS` in 01:40.

## Read Model 1M Result

Command:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 \
  -jvmArgsAppend "-Xms2g -Xmx6g" \
  -rf json -rff /tmp/rip2-readmodel-jmh-1m.json \
  > /tmp/rip2-readmodel-jmh-1m.txt 2>&1
```

JMH completed in 00:05:01 and wrote
`/tmp/rip2-readmodel-jmh-1m.json`.

| Operation | Score ms/op | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `describeClient` | 0.001 | 0.000 | 0.001 | 0.016 | 2.617 |
| `listByClientIdPrefixPage` | 3.207 | 0.656 | 0.752 | 0.839 | 4815.061 |
| `listByConnectTimeRangePage` | 74.320 | 35.914 | 233.597 | 344.793 | 541.065 |
| `listByGroupPage` | 0.088 | 0.020 | 0.028 | 0.040 | 482.869 |
| `listByLanguagePage` | 77.623 | 40.370 | 225.496 | 318.673 | 606.077 |
| `listByProxyIdPage` | 0.710 | 0.182 | 0.209 | 0.242 | 2382.365 |
| `listByTopicPage` | 0.048 | 0.008 | 0.012 | 0.016 | 233.570 |
| `listFirstPage` | 0.012 | 0.003 | 0.009 | 0.010 | 49.807 |
| `listNextPage` | 0.014 | 0.003 | 0.009 | 0.011 | 41.091 |

Worst read-model P99: `listByConnectTimeRangePage` at 344.793 ms.
All read-model operations stayed below the 1 second P99 target.

The max column includes workstation scheduling and GC outliers observed during
sample-time collection. It is retained for transparency, but the contest target
tracked here is P99.

## Coordinator 1M Result

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
- Language and connect-time filters are intentionally the heaviest local paths
  because the synthetic data puts all clients in the same language and
  connection-time bucket.
- The coordinator experiment shows low P99 latency with 100 synthetic proxy
  shards and `pageSize=100`, but it is not part of the public M1 promise.
- Public endpoint numbers remain blocked on the `rocketmq-apis` ownership gate;
  the endpoint-ready adapter and executor are already covered by unit and
  in-process integration tests.
