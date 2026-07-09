# RIP-2 Proxy Admin 参赛提交包

## 当前状态

本文汇总 `rip2-proxy-admin-m1` 分支，可用于最终 PR、issue comment 或比赛提交。

当前远端分支 HEAD 由 draft PR 和 RIP-2 issue comment 维护，因为本文档本身
可能会随着证据刷新继续提交。已同步到远端的最新 RocketMQ 代码实现
checkpoint：

```text
7b89ba60fca2a18859519f7b2b822f73c2f4ed2c Add proxy admin public grpc benchmark
```

该 checkpoint 后，live draft PR 和 RIP-2 issue summary 已同步 generated public
gRPC endpoint 1M benchmark、`728` tests broad verification 结果和最新实现分支
代码 checkpoint。

本分支已经完成 `ProxyAdminService` 在线客户端查询所需的 proxy 侧基础能力和
生成版公开 endpoint：read model、客户端生命周期写入、service 层、参数校验、
授权、generated gRPC adapter、独立 admin server 接线、独立 admin 查询线程池、
可观测性、内部跨 proxy 探索、文档和 1M synthetic client benchmark。

本分支已包含生成版公开 `ProxyAdminService` endpoint 接线。权威 protobuf
来源已发布到
[pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api](https://github.com/pilichoumao/rocketmq-apis/tree/rip2-proxy-admin-public-api)，
commit 为 `c372905ce927cf8957333e7ac07877f295fd7ec9`。为了完成参赛验证，
生成版 Java artifact 以
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` 安装到本机 Maven。

Public API proposal 也已创建为 draft PR
[apache/rocketmq-apis#112](https://github.com/apache/rocketmq-apis/pull/112)。
RocketMQ 实现也已创建为 draft PR
[apache/rocketmq#10603](https://github.com/apache/rocketmq/pull/10603)，用于
proxy 侧实现审阅。当前实现分支仍依赖从 API proposal 本地生成并安装的
`rocketmq-proto:2.2.0-rip2-SNAPSHOT`，因此在 API 归属和 artifact 发布路径
明确前仍不应作为可直接合并的上游 CI PR 处理。

## 要求对照

| 比赛要求 | 当前状态 | 证据 |
| --- | --- | --- |
| public admin service，包含 `ListClients`、`DescribeClient`、`ListClientsByGroup`、`ListClientsByTopic` | 已通过生成版 `ProxyAdminServiceGrpc` 暴露；当 `enableProxyAdminGrpcServer=true` 时注册到独立 admin gRPC server。 | `GrpcProxyAdminApplication`、`ProxyStartup`、`GrpcProxyAdminApplicationTest`、[rocketmq-apis/admin.proto](https://github.com/pilichoumao/rocketmq-apis/blob/rip2-proxy-admin-public-api/apache/rocketmq/v2/admin.proto)。 |
| 在线 client read model | 已实现 `clientId`、group、topic、client type、language、connect time 和 proxy id 索引。 | `ProxyClientReadService`、`ProxyClientInfo`、`ProxyClientQuery`、`ProxyClientPage`。 |
| 官方过滤字段 | 已支持 exact client id、client id prefix、group、topic、client language、connect time range、`pageNum >= 1`、省略 `pageSize` 默认值、负数 `pageSize` 拒绝和 `pageSize <= 100`。 | `GrpcProxyAdminApplicationTest`、`ProxyClientQueryTest`、`ProxyClientReadServiceTest`、`ProxyClientAdminRequestConverterTest`。 |
| 生命周期写入 | 已覆盖 telemetry settings、heartbeat、unregister、termination、stream completion 和 error cleanup。 | `ClientActivityTest`、`DefaultGrpcMessagingActivityTest`。 |
| ACL | 逻辑资源为 `proxy.admin.client`；list 类操作使用 `LIST`，describe 使用 `GET`。 | `ClientAdminAuthPolicyTest`、`DefaultClientAdminAuthorizationServiceTest`、`AuthorizingClientAdminServiceTest`。 |
| 独立 admin 执行路径 | 已实现 admin query executor，并完成独立 admin gRPC server 注册路径。 | `ProxyClientAdminEndpointExecutor`、`ProxyStartup`、`GrpcProxyAdminWiringTest`、`ProxyStartupTest`。 |
| 可观测性 | 已实现 metrics、trace attributes 和低基数结构化失败日志。 | `ProxyMetricsManagerTest`、`MeteredClientAdminServiceTest`、`MeteredAuthorizingClientAdminServiceTest`、`ProxyClientAdminObservabilityTest`。 |
| E2E / integration 覆盖 | 生成版 public gRPC Server/Channel 测试已覆盖四个 RPC、官方过滤字段、public pagination/hasMore、省略 public pagination 字段的默认值、所有 public RPC 的非 local scope 拒绝、`PROXY_SCOPE_PROXY_ID` 在 proxy-id 校验前被拒绝、bad-request contract mapping、not found 语义和 Dashboard-facing client view 字段；proto-free endpoint 和 peer tests 继续覆盖内部路径。 | `GrpcProxyAdminApplicationTest`、`ProxyClientAdminEndpointIntegrationTest`、`ProxyClientAdminInProcessPeerMessageTransportTest`、`ProxyClientAdminPeerGrpcServiceTest`、`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。 |
| 1M benchmark | 已在本机 Apple M4、16 GB、JDK 17 下完成，所有 local read-model 和 generated public gRPC endpoint P99 均低于 1 秒。 | `docs/cn/rip2-proxy-admin-m1-benchmark-report.md`。 |
| 中英文文档 | 已完成 user guide、Dashboard integration contract、public API discussion、benchmark report、smoke guide、review runbook、acceptance audit 和提交包。 | `docs/en/rip2-proxy-admin-m1-user-guide.md`、`docs/cn/rip2-proxy-admin-m1-user-guide.md`、`docs/en/rip2-proxy-admin-m1-dashboard-contract.md`、`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`、`docs/en/rip2-proxy-admin-public-api-discussion.md`、`docs/cn/rip2-proxy-admin-public-api-discussion.md`。 |

## API 摘要

推荐 public service：

```proto
service ProxyAdminService {
  rpc ListClients(ListClientsRequest) returns (ListClientsResponse);
  rpc DescribeClient(DescribeClientRequest) returns (DescribeClientResponse);
  rpc ListClientsByGroup(ListClientsByGroupRequest)
      returns (ListClientsByGroupResponse);
  rpc ListClientsByTopic(ListClientsByTopicRequest)
      returns (ListClientsByTopicResponse);
}
```

M1 public scope 建议只开放 `LOCAL_PROXY`。`ALL_PROXIES` 和 `PROXY_ID` 已作为
内部探索实现，但在社区确认 peer discovery、timeout、authorization 和
page-token ownership 语义前应继续保持 gated。

Public request 字段建议包括：

- `clientId`
- `clientIdPrefix`
- `group`
- `topic`
- `clientLanguage`
- `connectTimeStartMillis`
- `connectTimeEndMillis`
- `pageNum`
- `pageSize`
- `scope`

`pageNum` 从 1 开始。`pageSize` 最大为 100。如果未来公开 page token，则必须
把 token 视为 opaque；当前 token 实现仍属于内部实现细节。

## 配置摘要

Public admin server gate 和 executor 默认值：

| 配置 | 默认值 | 含义 |
| --- | ---: | --- |
| `enableProxyAdminGrpcServer` | `false` | 为兼容性保留 public admin gRPC server opt-in。 |
| `proxyAdminGrpcServerPort` | `8082` | 独立 admin gRPC 端口。 |
| `proxyAdminGrpcThreadPoolNums` | `4` | 独立 admin server request executor 线程数。 |
| `proxyAdminGrpcThreadPoolQueueCapacity` | `10000` | 独立 admin server request queue 容量。 |
| `proxyClientAdminQueryThreadPoolNums` | `4` | 内部 admin query executor 线程数。 |
| `proxyClientAdminQueryThreadPoolQueueCapacity` | `10000` | 内部 admin query queue 容量。 |
| `enableProxyClientAdminCrossProxyQuery` | `false` | 内部 `ALL_PROXIES` 和 `PROXY_ID` 实验开关。 |
| `proxyClientAdminPeerRequestTimeoutMillis` | `2000` | peer discovery/request 有界等待时间。 |
| `proxyClientAdminCoordinatorPageTokenTtlMillis` | `300000` | 内部 fan-out coordinator token 保留时间。 |
| `proxyClientAdminPeerGrpcTargets` | 空 | 内部 cross-proxy 测试用静态 peer targets。 |

## ACL 摘要

逻辑资源：

```text
proxy.admin.client
```

RocketMQ ACL resource 编码：

```text
Admin:proxy.admin.client
```

建议策略形态：

```yaml
subject: User:admin-dashboard
resources:
  - resource: Admin:proxy.admin.client
    actions:
      - LIST
      - GET
```

`ListClients`、`ListClientsByGroup`、`ListClientsByTopic` 需要 `LIST`。
`DescribeClient` 需要 `GET`。

## 可观测性摘要

Admin metrics：

- `rocketmq_proxy_client_admin_requests_total`
- `rocketmq_proxy_client_admin_request_latency`
- `rocketmq_proxy_client_read_model_operations_total`
- `rocketmq_proxy_client_total`
- `rocketmq_proxy_client_type_total`
- `rocketmq_proxy_client_index_total`

Admin labels、trace 和日志属性：

- `operation`
- `result`
- `scope`
- `status`
- `filters`
- `page_size`
- `result_size`

失败日志不会记录 auth subject、client id、group、topic 或 proxy id 明文。

## 验证快照

以下最终验证命令均使用 JDK 17 运行，运行时工作区已包含生成版 public endpoint
和 package smoke 构建修复。Focused public endpoint 验证已在新增
Dashboard-facing client view 字段、contest filters、scope gates、bad requests
和 sparse client metadata 默认值、`ListClientsByGroup` /
`ListClientsByTopic` grouped filter pagination、exact client id 过滤和 public
`pageSize` capped pagination、四个 RPC authorization mapping、缺失
`DescribeClient.client_id` 校验 generated gRPC 覆盖后刷新；随后又在新增
explicit `LOCAL_PROXY` 对四个 RPC 成功、`PROXY_ID` 对四个 RPC 保持 gated 的
generated gRPC 证据后刷新；随后又在新增 `BAD_REQUEST` 和 `UNAUTHORIZED`
错误响应均不携带结果体的 generated gRPC 证据后刷新；随后又在新增省略 public
`pageNum` / `pageSize` 时所有 list-style RPC 均返回第一页 100 个 client 的
generated gRPC 证据后刷新；随后又在新增 `ListClientsByGroup` 和
`ListClientsByTopic` 同时支持精确 `client_id` 过滤并将 public `pageSize`
大于 100 的请求 capped 到 100 的 generated gRPC 证据后刷新；随后又在新增
explicit `LOCAL_PROXY` 下四个 RPC 均忽略 reserved `proxy_id` 字段的
generated gRPC 证据后刷新；随后又在新增负数 public `pageSize` 对所有
list-style RPC 均返回 `BAD_REQUEST` 的 generated gRPC 和 converter 证据后刷新
broad proxy admin 验证；随后又在将核心 `ProxyClientQuery` page-size guard
与 public adapter 对齐、让直接内部 query 构造也拒绝负数后刷新；随后又在新增
`PROXY_SCOPE_PROXY_ID` 即使省略 `proxy_id` 也先按 M1 scope gate 拒绝的
generated gRPC 证据后刷新；随后又在新增 generated public gRPC endpoint
benchmark setup 测试后刷新。Package smoke 已在同一 HEAD 上刷新。

Focused generated public API verification：

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am \
-Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 52, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T04:04:54+08:00
```

Broad proxy admin verification：

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am \
"-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 728, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T05:14:32+08:00
```

Package smoke：

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am -DskipTests package -DskipITs
```

结果：

```text
BUILD SUCCESS
Finished at: 2026-07-10T04:07:14+08:00
```

Package smoke 最初暴露出 `target/generated-test-sources/test-annotations`
下的 JMH annotation-generated test sources 被第二次 `source:jar` checkstyle
扫描的问题。最终 `pom.xml` 将 Checkstyle 限定在手写测试源，同时保留测试编译和
benchmark 生成能力。

验收审计：

- `docs/en/rip2-proxy-admin-m1-acceptance-audit.md`
- `docs/cn/rip2-proxy-admin-m1-acceptance-audit.md`

Dashboard CLIENT-01 交接契约：

- `docs/en/rip2-proxy-admin-m1-dashboard-contract.md`
- `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`

评审复现手册：

- `docs/en/rip2-proxy-admin-m1-review-runbook.md`
- `docs/cn/rip2-proxy-admin-m1-review-runbook.md`

Broad verification 的最新包级 JaCoCo 覆盖率：

| Package | Instruction | Branch | Line |
| --- | ---: | ---: | ---: |
| `org/apache/rocketmq/proxy/service/admin/client` | 93.82% | 86.83% | 95.48% |
| `org/apache/rocketmq/proxy/grpc/v2/admin` | 92.66% | 85.05% | 94.52% |

JDK 17 下 JaCoCo 0.8.5 会对部分 JDK 和 Mockito 生成类打印 instrumentation
stack traces。只有在 Surefire 零 failure/error 且 Maven 成功退出时，才把这些
日志视为环境噪声。

Benchmark 证据：

- local read-model 最慢 P99：`listByTopicPage`，0.681 ms。
- generated public gRPC endpoint 最慢 P99：
  `listClientsByClientIdPrefix`，3.576 ms。
- coordinator 实验最慢 P99：`listAllProxiesNextPage`，9.011 ms。
- 完整报告：`docs/cn/rip2-proxy-admin-m1-benchmark-report.md`。

## PR 描述草稿

标题：

```text
[RIP-2] Add proxy admin online client query foundation
```

正文：

````markdown
## Summary

This PR implements the proxy-side RIP-2 online client query foundation and
generated public gRPC endpoint for a standalone `ProxyAdminService`.

Implemented:

- process-local online client read model populated from gRPC client lifecycle
  events.
- query semantics for client id, client id prefix, group, topic, language,
  connect time range, `pageNum`, and `pageSize <= 100`.
- internal `ClientAdminService`, proto-free `ProxyClientAdminActivity`, and
  generated `GrpcProxyAdminApplication`.
- generated public endpoint executor/handler with `LOCAL_PROXY` public-scope
  validation, including `PROXY_ID` gating before proxy-id validation.
- ACL facade using `Admin:proxy.admin.client` with LIST/GET actions.
- dedicated admin query executor and independent admin gRPC server registration.
- low-cardinality metrics, trace attributes, and sanitized failure logs.
- internal cross-proxy coordinator and peer transport experiments behind config.
- English/Chinese docs and 1M synthetic client benchmark report covering the
  local read model and generated public gRPC endpoint.

The authoritative public proto is published in
`pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api`. For contest
verification this branch depends on local
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` generated from that
proto. Draft PR https://github.com/apache/rocketmq-apis/pull/112 tracks the
upstream API review. Draft PR https://github.com/apache/rocketmq/pull/10603
tracks the downstream RocketMQ implementation review.

## Tests

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
mvn -pl proxy -am \
"-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result: `Tests run: 728, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`.

## Benchmark

1M synthetic client JMH on Apple M4, 16 GB, Temurin JDK 17.0.18:

- local read-model worst P99: 0.681 ms.
- generated public gRPC endpoint worst P99: 3.576 ms.
- internal coordinator experiment worst P99: 9.011 ms.

See `docs/en/rip2-proxy-admin-m1-benchmark-report.md`.
````

## Issue comment

已发布到 [apache/rocketmq#10599](https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687)：

```markdown
I have prepared the RIP-2 Proxy Admin online client query implementation branch
at `pilichoumao/rocketmq:rip2-proxy-admin-m1`.

The branch is ready for community review of the proxy-side foundation:

- local online-client read model and lifecycle writes.
- `ListClients`, `DescribeClient`, `ListClientsByGroup`, and
  `ListClientsByTopic` through generated public gRPC stubs.
- official filters, negative `pageSize` rejection, and `pageSize <= 100`.
- independent ACL resource `Admin:proxy.admin.client`.
- dedicated admin query executor and admin server registration.
- metrics/tracing/logging coverage.
- generated public gRPC Server/Channel tests, in-process endpoint/peer tests,
  and 1M synthetic benchmark report covering the read model and public endpoint.
- English and Chinese user docs.

The public API source is published at
https://github.com/pilichoumao/rocketmq-apis/tree/rip2-proxy-admin-public-api.
The API proposal draft PR is https://github.com/apache/rocketmq-apis/pull/112.
The RocketMQ implementation draft PR is https://github.com/apache/rocketmq/pull/10603.
The remaining community question is whether Apache accepts the proposed service
name, field names, field numbers, and upstream publication path.
```

## 最终检查清单

- 保持 `origin` 为 `https://github.com/pilichoumao/rocketmq.git`。
- 不向 `apache/rocketmq` push。
- 任意代码改动后重新跑 broad proxy verification。
- 手工验证运行中 Proxy 时，按 `docs/cn/rip2-proxy-admin-m1-final-smoke.md`
  执行最终冒烟。
- 最终 commit 前执行 `git diff --check`。
- 普通 push 失败时使用代理：

```bash
HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890 \
git push origin rip2-proxy-admin-m1
```
