# RIP-2 Proxy Admin 1M Client Benchmark 报告

## 范围

本文记录 RIP-2 Proxy Admin M1 read model、生成版 public gRPC endpoint 和内部
coordinator 实验路径在本机的 1,000,000 synthetic client JMH 运行结果。

本 checkpoint 的验收信号是：单 proxy 本地 read model，以及生成版 public
`ProxyAdminService` endpoint 的分页在线客户端查询 P99 小于 1 秒。Coordinator
结果作为内部 multi-proxy 探索证据记录；在公开 protobuf API 被社区接受前，
M1 公共语义仍然只承诺 `LOCAL_PROXY`。

## 运行环境

| 项目 | 值 |
| --- | --- |
| 日期 | 2026-07-10 |
| Read-model commit | `bc83087f5f40` |
| Public endpoint benchmark source | 本 checkpoint 中的 `GrpcProxyAdminApplicationBenchmark` |
| 机器 | Apple M4 |
| 逻辑 CPU | 10 |
| 内存 | 16 GB |
| OS | macOS 26.5.1 |
| JDK | Temurin OpenJDK 17.0.18+8 |
| JMH | 1.36 |
| JVM 参数 | `-Xms2g -Xmx6g` |

## 构建和启动方式

Read-model benchmark 前，已重新编译修改后的 proxy 源码，并跑过聚焦
read-model 单测：

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -pl proxy -am \
  "-Dtest=ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientQueryTest,ProxyClientInfoTest,ProxyClientReadServiceCleanerTest" \
  -DfailIfNoTests=false test -DskipITs
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile=/tmp/rocketmq-proxy-test-classpath.txt
```

聚焦测试结果为 `Tests run: 65, Failures: 0, Errors: 0`，并以
`BUILD SUCCESS` 结束。

生成版 public endpoint benchmark 前，已验证 generated gRPC endpoint 和
benchmark setup：

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -pl proxy -am \
  "-Dtest=GrpcProxyAdminApplicationBenchmarkTest,GrpcProxyAdminApplicationTest,ProxyClientReadServiceBenchmarkTest" \
  -DfailIfNoTests=false test -DskipITs
```

测试结果为 `Tests run: 29, Failures: 0, Errors: 0, Skipped: 0`，并以
`BUILD SUCCESS` 结束。

## Read Model 1M 结果

命令：

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark \
  -p clientCount=1000000 -p groupCount=1000 -p topicCount=10000 -p proxyCount=100 \
  -jvmArgsAppend "-Xms2g -Xmx6g" \
  -rf json -rff /tmp/rip2-readmodel-jmh-1m-optimized-full.json
```

JMH 运行完成，耗时 00:05:04，并写入
`/tmp/rip2-readmodel-jmh-1m-optimized-full.json`。

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

最慢 read-model P99 是 `listByTopicPage`，为 0.681 ms。所有
read-model 查询的 P99 都低于 1 秒目标。

Max 列保留了本机 sample-time 采集中的调度和 GC 尾部尖刺，作为透明记录；
本 checkpoint 对齐的比赛目标是 P99。

## 生成版 Public gRPC Endpoint 1M 结果

该 benchmark 会在本地 loopback 上启动真实生成版 `ProxyAdminServiceGrpc`
server 和 blocking client channel，然后通过 public `GrpcProxyAdminApplication`
调用与 Proxy admin server 接线相同的 endpoint executor、authorization facade、
request converter、response converter 和 read model。

命令：

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

JMH 运行完成，耗时 00:01:14，并写入
`/tmp/rip2-public-grpc-jmh-1m.json`。

| Operation | Score ms/op | P50 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| `describeClient` | 0.059 | 0.055 | 0.092 | 0.137 | 1.585 |
| `listClients` | 0.189 | 0.171 | 0.279 | 0.387 | 1.890 |
| `listClientsByClientIdPrefix` | 2.465 | 2.413 | 2.982 | 3.576 | 4.571 |
| `listClientsByConnectTimeRange` | 0.179 | 0.166 | 0.271 | 0.407 | 1.309 |
| `listClientsByGroup` | 0.217 | 0.194 | 0.372 | 0.511 | 2.511 |
| `listClientsByLanguage` | 0.177 | 0.162 | 0.291 | 0.410 | 1.737 |
| `listClientsByTopic` | 0.192 | 0.173 | 0.316 | 0.449 | 2.421 |

最慢 generated public endpoint P99 是 `listClientsByClientIdPrefix`，为
3.576 ms。所有 generated public endpoint 查询 P99 都低于 1 秒目标。

## Coordinator 1M 结果

下面的 coordinator 结果保留 2026-07-08 内部探索运行数据，commit 为
`251c18567f37`。本次 `bc83087f5f40` read-model checkpoint 未重跑
coordinator，因为 public M1 合约仍然只承诺 `LOCAL_PROXY`，coordinator 不属于
public 性能承诺。

命令：

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

JMH 写入 `/tmp/rip2-coordinator-jmh-1m.json`。

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

最慢 coordinator P99 是 `listAllProxiesNextPage`，为 9.011 ms。Coordinator
仍然是内部探索能力，但本次结果说明当前 fan-out/merge 实现对后续
`MULTI_PROXY` 讨论有足够余量。

## 结论

- 本地 read model 在 1M synthetic client 下满足所有已实现官方查询形态的
  P99 小于 1 秒目标：无过滤分页、group、topic、prefix、language、
  connect-time range、proxy-id 和 describe。
- 生成版 public gRPC endpoint 在 1M synthetic client 下同样满足 public M1
  方法和过滤字段的 P99 小于 1 秒目标。该 benchmark 经过真实生成版
  server/channel 和 public endpoint adapter；最慢 public 路径是
  `listClientsByClientIdPrefix`，P99 为 3.576 ms。
- 单 language 和单 connect-time synthetic 桶现在不会再为 page-bounded read
  物化 1,000,000 个 client 的候选集副本；本地 read model 会复用现有单过滤索引，
  只在组合多个过滤条件时才物化交集。
- Coordinator 实验在 100 个 synthetic proxy shard 和 `pageSize=100` 下仍有
  较低 P99，但它不属于 M1 公共承诺。
