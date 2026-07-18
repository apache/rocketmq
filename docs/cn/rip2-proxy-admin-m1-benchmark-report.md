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

## 限制堆最坏场景证明

2026-07-11 的最终内存门禁使用 4 GiB fixed heap
(`-Xms4g -Xmx4g -XX:+UseG1GC`)，配置为单 fork、4 个调用线程、1 次
2 秒 warmup 和 3 次 3 秒 measurement。每个进程同时采集 JMH
sample-time percentile、JMH GC profiler、JFR、unified GC log 和
`/usr/bin/time -l` 资源数据。

Public endpoint 场景通过真实生成版 `ProxyAdminServiceGrpc`
Server/Channel，server executor 和 query executor 都使用与生产默认一致的
4 线程、10,000 有界队列。请求同时使用宽 `client-` prefix、
`JAVA` language、`100..100` connect time 和 `pageSize=100`。

| 场景 | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC 次数/时间 | JFR max heapUsed | Max RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1M client 宽 prefix | 4.067 | 5.210 | 137.526 | 3745.513 | 6511.3 | 0 / 0 ms | 775.9 MiB | 1119.2 MiB |
| 宽 prefix + language + connect time | 4.112 | 5.087 | 243.610 | 9495.904 | 5509.6 | 0 / 0 ms | 772.9 MiB | 1114.0 MiB |
| 深分页 `pageNum=10000`、`pageSize=100` | 0.009 | 0.010 | 0.016 | 593.494 | 1140.6 | 5 / 8 ms | 1126.4 MiB | 1208.7 MiB |
| 生成版 public gRPC 组合过滤 | 16.564 | 26.241 | 29.042 | 35.193 | 188604.8 B/op | 3 / 26 ms | 1000.3 MiB | 1283.0 MiB |

验收 P99 摘要：宽 prefix `137.526 ms`、组合过滤 `243.610 ms`、深分页
`0.016 ms`、生成版 public gRPC `29.042 ms`。

4 个场景的 P99 都低于 1 秒；所有进程正常退出，无 OOM、zero swaps，
max RSS 低于 1.3 GiB。Max 列保留 JFR 和工作站调度带来的罕见尾部值；
比赛验收指标为 P99。

两个 measurement GC 为 0 的 read-model 场景不会在 JMH 仅 measurement
录制中产生 `jdk.GCHeapSummary`。表中这两项 max heap 来自从 fork 启动就开始的
`-XX:StartFlightRecording` 补充探针，它覆盖了 1M setup GC；延迟和 allocation
仍使用上表正式运行数据。

Performance TDD 暴露并修复了三个问题：

- 宽 prefix 修复前 P99 为 `8388.608 ms`、约 40.1 MB/op，原因是每次查询
  都复制完整 prefix range；现在复用有序索引范围视图。
- 三过滤器交集修复前 P99 为 `13505.659 ms`、约 40.1 MB/op，原因是
  物化百万 client `TreeSet`；现在由最小有序索引驱动，找到
  `pageSize + 1` 个匹配项即停止。
- 深分页修复前 P99 为 `1157.313 ms`，原因是每次都跳过 999,900 个树节点。
  现在使用惰性重建的 client-id page anchor cache，从最近的 100-client 边界开始，
  insert/remove 会使 cache 失效。

命令模板（所有仓库路径都相对于 RocketMQ 根目录）：

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

## 构建和启动方式

新增证据统一使用仓库内 launcher，不再手工拼 classpath，也不再把唯一原始结果
写到 `/tmp`：

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
dev/run_rip2_benchmark.sh \
  million-read-model-first-page \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listFirstPage
```

launcher 要求正好匹配一个 JMH 方法，默认使用 1,000,000 clients、固定 4 GiB
G1 heap、一个 fork 和四个调用线程；短冒烟可通过 `RIP2_*` 环境变量覆盖。
证据写入 `target/rip2-benchmark-results/<label>/`，包含 `jmh.json`、
`jmh.log`、`recording.jfr`、`gc.log`、`time.txt`、`classpath.txt`、
`command.txt`、`runner.sh`、`source-files.txt` 和 `SHA256SUMS`。校验
manifest 时应先进入该输出目录，使其中的
相对路径正确解析。

Read-model benchmark 前，已重新编译修改后的 proxy 源码，并跑过聚焦
read-model 单测：

```bash
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
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
export JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}"
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
- 单时间桶查询复用现有时间索引。多桶时间范围不会 union client-id set：
  生产查询由最小的可用有序索引驱动，逐个候选验证 connect time，
  找满有界 page 后即停止。
- Coordinator 实验在 100 个 synthetic proxy shard 和 `pageSize=100` 下仍有
  较低 P99，但它不属于 M1 公共承诺。

## 宽 Connect-Time Range 1M 证明

2026-07-12 补充运行将 1,000,000 个 synthetic client 均匀分布在
100 个 connect-time 桶（`100..199`），并以 `pageSize=100` 查询完整范围。
这覆盖了之前精确 `100..100` benchmark 未走到的多桶路径。两次运行均使用
Temurin 17.0.18、4 个 caller thread、1 次 2 秒 warmup、3 次 3 秒
measurement、固定 4 GiB G1 heap 和 JMH GC profiler。

| 路径 | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC 次数/时间 | Max RSS | Swap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read model `listByWideConnectTimeRangePage` | 0.004 | 0.008 | 0.010 | 60.228 | 1113.352 | 4 / 2 ms | 2448.4 MiB | 0 |
| Generated gRPC `listClientsByWideConnectTimeRange` | 0.196 | 0.275 | 0.394 | 10.781 | 174970.776 | 15 / 22 ms | 2916.2 MiB | 0 |

两个 P99 都低于 1 秒，两个 fork 都正常退出，无 OOM。原始 JMH JSON 为
`/tmp/rip2-wide-connect-time-jmh-1m.json` 和
`/tmp/rip2-public-wide-connect-time-jmh-1m.json`。

代表性 include name：

```bash
org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listByWideConnectTimeRangePage
org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplicationBenchmark.listClientsByWideConnectTimeRange
```

## 可复现的全集深分页证明

2026-07-12 最终补充运行以 `pageNum=10000`、`pageSize=100` 查询完整
`100..199` connect-time 范围。优化实现位于
`1dd5c6fd1f7e8f5d684213d72c4b965d214f977d`；可复现 runner 和证据 checkpoint
`8c3098d51615189677118200955aeb6bdcbf90c0` 生成了下面的最终数据。首次 k-way bucket merge
运行暴露出 read-model P99 `2906.653 ms`；最终实现先用时间索引边界确认查询
覆盖全集，再复用会在 mutation 后失效重建的 client-id page anchor。部分范围仍走
有界 k-way bucket merge。

两次正式运行都使用仓库 launcher 默认参数：一个 fork、四个 caller thread、
一次 2 秒 warmup、三次 3 秒 measurement、1,000,000 clients 和固定 4 GiB
G1 heap：

```bash
dev/run_rip2_benchmark.sh \
  evidence-v4-million-deep-readmodel-20260712 \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark.listDeepPageByWideConnectTimeRange
dev/run_rip2_benchmark.sh \
  evidence-v4-million-deep-public-grpc-20260712 \
  org.apache.rocketmq.proxy.grpc.v2.admin.GrpcProxyAdminApplicationBenchmark.listClientsDeepPageByWideConnectTimeRange
```

| 路径 | P50 ms | P95 ms | P99 ms | Max ms | Allocation B/op | Measurement GC 次数/时间 | Max RSS | Swap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read model 全集深分页 | 0.009 | 0.010 | 0.011 | 722.469 | 1147.567 | 5 / 10 ms | 1223.7 MiB | 0 |
| Generated public gRPC 全集深分页 | 0.271 | 0.582 | 0.843 | 8.389 | 178710.734 | 11 / 26 ms | 2792.3 MiB | 0 |

两个 P99 均低于 1 秒，两个 fork 都正常退出且无 OOM。每个输出目录都包含
最终 read-model P99 为 `0.011 ms`，generated public gRPC P99 为 `0.843 ms`。
`build.log`、`environment.txt`、`command.txt`、解析后的 classpath、JMH
JSON/log、GC log、JFR、进程时间输出、精确的 `runner.sh`、已跟踪的
`source-files.txt` 以及已校验的 `SHA256SUMS` manifest。
关键 digest 如下：

| 证据 | `jmh.json` SHA-256 | `recording.jfr` SHA-256 | `environment.txt` SHA-256 |
| --- | --- | --- | --- |
| Read model | `81f309ab9559d60772b26f59ec3a1d4de618840f3a4b949a934d8367b1672308` | `199ecbaed817c8b8ed33bed6fb8af376f98223ec55ba04502b00cfd203cf7e13` | `072f1e1557028ee10acb59f4a47f2309ec6eb401bcce5c32ee96da0667e2a8c5` |
| Generated public gRPC | `d519f533b3d20e57a9fec0d15dca339ef769da3eae18817ad34b04a1ca91ee91` | `4b43c03774bb86eb7ccd9bdb3a02171b0c83f780377e8bf36adfd0b7b8eef2a9` | `e3c78578f6f1a96d4796e89de0da9823951f3d291db0ce36eca461c04d6bbe83` |

两个 evidence bundle 都固定 `runner.sh` SHA-256
`f58b88d5234c97ea6942968e970cca247b8821e018337418bd68bf2d26ae6975`
和 `source-files.txt` SHA-256
`841a5ec9c6a4059a88b8f4e42714182f9c8ac1e8a2a7ef5461f05c6c5dc09251`。
