# RIP-2 Proxy Admin 1M Client Benchmark 报告

## 范围

本文记录 RIP-2 Proxy Admin M1 read model 和内部 coordinator 实验路径在本机的
1,000,000 synthetic client JMH 运行结果。

本 checkpoint 的验收信号是：单 proxy 本地 read model 的分页在线客户端查询
P99 小于 1 秒。Coordinator 结果作为内部 multi-proxy 探索证据记录；在公开
protobuf API 被社区接受前，M1 公共语义仍然只承诺 `LOCAL_PROXY`。

## 运行环境

| 项目 | 值 |
| --- | --- |
| 日期 | 2026-07-08 |
| Commit | `251c18567f37` |
| 机器 | Apple M4 |
| 逻辑 CPU | 10 |
| 内存 | 16 GB |
| OS | macOS 26.5.1 |
| JDK | Temurin OpenJDK 17.0.18+8 |
| JMH | 1.36 |
| JVM 参数 | `-Xms2g -Xmx6g` |

## 构建和启动方式

Benchmark 先执行 clean proxy test compile，确保 JMH 生成类来自当前源码：

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -pl proxy -am -DskipTests -DskipITs clean test-compile
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile=/tmp/rocketmq-proxy-test-classpath.txt
```

本次 compile 以 `BUILD SUCCESS` 结束，耗时 01:40。

## Read Model 1M 结果

命令：

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

JMH 运行完成，耗时 00:05:01，并写入
`/tmp/rip2-readmodel-jmh-1m.json`。

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

最慢 read-model P99 是 `listByConnectTimeRangePage`，为 344.793 ms。所有
read-model 查询的 P99 都低于 1 秒目标。

Max 列保留了本机 sample-time 采集中的调度和 GC 尾部尖刺，作为透明记录；
本 checkpoint 对齐的比赛目标是 P99。

## Coordinator 1M 结果

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
- language 与 connect-time 查询是本地最重路径，因为 synthetic 数据把所有
  client 放在同一种语言和同一个连接时间桶里。
- Coordinator 实验在 100 个 synthetic proxy shard 和 `pageSize=100` 下仍有
  较低 P99，但它不属于 M1 公共承诺。
- 真实 public endpoint benchmark 仍受 `rocketmq-apis` 归属 gate 阻塞；当前
  endpoint-ready adapter 和 executor 已由单测与 in-process integration tests 覆盖。
