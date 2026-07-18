# RIP-2 Proxy Admin 参赛提交包

## 当前状态

本文汇总 `rip2-proxy-admin-m1` 分支，可用于最终 PR、issue comment 或比赛提交。

已同步到远端的最新 RocketMQ 实现代码 checkpoint：

```text
1dd5c6fd1f7e8f5d684213d72c4b965d214f977d Guard grpc client lifecycle ownership
```

最新 generated public gRPC endpoint 1M benchmark 代码 checkpoint：

```text
4a086b5431328e3ea0d1d6e29751e3d30226b316 Optimize full range client deep queries
```

可复现 benchmark runner 和最终 evidence 源码 checkpoint：

```text
8c3098d51615189677118200955aeb6bdcbf90c0 Preserve RIP-2 benchmark evidence across runs
```

实现分支已同步到上述实现 checkpoint。live draft PR 和 RIP-2 issue summary
会在每个最终文档 checkpoint 后刷新，包含 generated public gRPC endpoint 1M
benchmark、当前 broad verification、
Dashboard 表格/详情字段证据、generated service descriptor verification 证据，
以及 public `BAD_REQUEST` / `UNAUTHORIZED` / `NOT_FOUND` /
`INTERNAL_SERVER_ERROR` status mapping 和 response-body contract 证据。后续纯文档证据刷新
commit 可能继续推进分支 HEAD，但不会改变上述实现 checkpoint。

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
本次记录验证使用的已安装 jar 为
`rocketmq-proto jar SHA-256: 7ae515ec32832f31634c47c36291ec4e2451f9cde589e59d956802596b6bad4d`。

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
| 生命周期写入 | 已覆盖 telemetry settings、heartbeat、unregister、termination、stream completion 和 error cleanup。每流 generation、transport identity 和分片 lifecycle lock 防止旧连接回调或 unary 事件删除、覆盖新 session。 | `ClientActivityTest`、`DefaultGrpcMessagingActivityTest`。 |
| ACL | 逻辑资源为 `proxy.admin.client`；list 类操作使用 `LIST`，describe 使用 `GET`。 | `ClientAdminAuthPolicyTest`、`DefaultClientAdminAuthorizationServiceTest`、`AuthorizingClientAdminServiceTest`。 |
| 独立 admin 执行路径 | 已实现 admin query executor，并完成独立 admin gRPC server 注册路径。排队请求在 converter、ACL 或 service 工作前会再次检查 gRPC cancellation 和 deadline。 | `ProxyClientAdminEndpointExecutor`、`ProxyStartup`、`GrpcProxyAdminWiringTest`、`ProxyStartupTest`。 |
| 可观测性 | 已实现 metrics、trace attributes 和低基数结构化失败日志。进入 service 前被拒绝的 public 请求由 endpoint executor 计量一次；已经委托的请求仍只由 service 计量，避免重复计数。 | `ProxyMetricsManagerTest`、`MeteredClientAdminServiceTest`、`MeteredAuthorizingClientAdminServiceTest`、`ProxyClientAdminObservabilityTest`、`ProxyClientAdminEndpointExecutorTest`、`GrpcProxyAdminWiringTest`。 |
| E2E / integration 覆盖 | 生成版 public gRPC Server/Channel 测试已覆盖 public service descriptor、四个 RPC、官方过滤字段、public pagination/hasMore、省略 public pagination 字段的默认值、所有 public RPC 的非 local scope 拒绝、`PROXY_SCOPE_PROXY_ID` 在 proxy-id 校验前被拒绝、`BAD_REQUEST` 和 `UNAUTHORIZED` status-only response body contract、`NOT_FOUND` status/message 且不携带 client result body、public `INTERNAL_SERVER_ERROR` response mapping，以及 Dashboard-facing `ListClients` 表格字段和 `DescribeClient` 详情字段；proto-free endpoint 和 peer tests 继续覆盖内部路径。 | `GrpcProxyAdminApplicationTest`、`ProxyClientAdminEndpointIntegrationTest`、`ProxyClientAdminInProcessPeerMessageTransportTest`、`ProxyClientAdminPeerGrpcServiceTest`、`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。 |
| 1M benchmark | 已在本机 Apple M4、16 GB、JDK 17 下完成。4 GiB fixed-heap 套件覆盖宽 prefix、组合过滤、第 10000 页、宽/全 connect-time range 和生产形态执行器的生成版 public gRPC。可复现的深页全范围证据为 read-model P99 0.011 ms、generated gRPC P99 0.843 ms；所有 fixed-heap 运行中 max RSS 为 2916.2 MiB，全部 zero swaps 且无 OOM。 | `docs/cn/rip2-proxy-admin-m1-benchmark-report.md`。 |
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

Endpoint executor 会记录 admin service 尚无法接管的 request adapter/context
校验失败和 query executor 拒绝。一旦请求已经委托给 endpoint handler，原有
service wrapper 仍是唯一 metrics owner，从而避免重复统计请求。

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
benchmark setup 测试后刷新；随后又在新增 generated public service descriptor
测试后刷新 focused public endpoint/startup verification；随后又在 descriptor 和
Dashboard-facing 测试纳入 broad suite 后，在最新分支 HEAD 上刷新 broad proxy
admin verification；随后又在新增 submission guard 后，在最新分支 HEAD 上刷新
package smoke。
Generated public endpoint verification 现在也覆盖
`GrpcProxyAdminApplicationTest#publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService`，
用于证明未预期 endpoint 层失败会作为 public `INTERNAL_SERVER_ERROR` response
status 返回，而不会泄露为 gRPC transport error。Focused/broad Maven
verification 与 package smoke 已在这份 reviewer-facing 证据同步后再次刷新。
Generated public `DescribeClient` not-found contract 也由
`GrpcProxyAdminApplicationTest#describeMissingClientReturnsNotFoundStatusWithoutClientBodyThroughGeneratedGrpcService`
固定，验证 public `NOT_FOUND` status/message 不会携带 `ProxyClient` result body。
Generated public error body contract 也由
`GrpcProxyAdminApplicationTest#publicServiceMapsBadRequestResponsesWithoutResultBodiesThroughGeneratedGrpcService`
和
`GrpcProxyAdminApplicationTest#publicServiceMapsUnauthorizedResponsesWithoutResultBodiesThroughGeneratedGrpcService`
固定，验证 `BAD_REQUEST` 和 `UNAUTHORIZED` response 在四个 public RPC 上均保持
status-only，不携带业务结果体。
Endpoint failure metrics 由
`ProxyClientAdminEndpointExecutorTest#recordsRejectedQueryExecutorMetricsBeforeServiceInvocation`、
`ProxyClientAdminEndpointExecutorTest#recordsRequestAdapterFailureMetricsBeforeServiceInvocation`
和
`ProxyClientAdminEndpointExecutorTest#successfulEndpointDelegationDoesNotRecordDuplicateFailureMetrics`
固定；
endpoint executor 还会在独立查询线程传播 gRPC 和 OpenTelemetry context，并且不会对
inline task admission 之后的失败重复计量。认证与 transport 信任边界测试确认：
已认证 principal、socket address 和完整 `proxy_protocol_*` 命名空间会
覆盖或清除客户端提供的 metadata。更严格的 subject 策略仅用于 public
admin pipeline，messaging pipeline 保留现有的 whitelist 行为。
`GrpcProxyAdminProductionInterceptorE2ETest` 在真实 loopback 端口上闭合了
production interceptor 双 server 门禁：生成版 admin service 不存在于
messaging server，messaging service 不存在于 admin server；认证 subject 和
transport 源 IP 能到达 ACL，认证失败不会调用 ACL，认证成功后的 ACL
拒绝返回不带 result body 的 `UNAUTHORIZED`。

Production interceptor 双 server 验证：

```text
Tests run: 63, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-11T19:24:15+08:00
```
`GrpcProxyAdminWiringTest#createDefaultActivityWiresEndpointFailureMetricsRecorder`
验证 production activity 注入共享 OTel recorder。

Focused generated public API verification：

```bash
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am \
-Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 57, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-12T02:37:36+08:00
```

Dashboard 表格/详情字段 focused verification：

```bash
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am \
"-Dtest=GrpcProxyAdminApplicationTest#listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService+describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService" \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T05:38:56+08:00
```

Public service descriptor focused verification：

```bash
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am \
-Dtest=GrpcProxyAdminApplicationTest#bindServiceExposesGeneratedProxyAdminUnaryMethods \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T05:47:33+08:00
```

Broad proxy admin verification：

```bash
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am \
"-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

结果：

```text
Tests run: 779, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-12T02:35:18+08:00
```

Package smoke：

```bash
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am -DskipTests package -DskipITs
```

结果：

```text
BUILD SUCCESS
Finished at: 2026-07-12T02:36:24+08:00
```

轻量提交门禁：

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github
```

`--check-apis-remote` 会自动识别
`../rocketmq-apis/rip2-proxy-admin-public-api` 配置的 upstream remote，因此无论
配套 API remote 名为 `origin` 还是 `fork`，reviewer 都可以使用同一个命令。

结果：

```text
RIP-2 submission guard passed.
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
| `org/apache/rocketmq/proxy/service/admin/client` | 92.93% | 86.62% | 94.41% |
| `org/apache/rocketmq/proxy/grpc/v2/admin` | 92.81% | 85.79% | 94.73% |

JDK 17 下 JaCoCo 0.8.5 会对部分 JDK 和 Mockito 生成类打印 instrumentation
stack traces。只有在 Surefire 零 failure/error 且 Maven 成功退出时，才把这些
日志视为环境噪声。

Benchmark 证据：

- local read-model 最慢 P99：`listByTopicPage`，0.681 ms。
- generated public gRPC endpoint 最慢 P99：
  `listClientsByClientIdPrefix`，3.576 ms。
- coordinator 实验最慢 P99：`listAllProxiesNextPage`，9.011 ms。
- 4 GiB 限制堆最坏场景：宽 prefix P99 137.526 ms、组合过滤 243.610 ms、
  第 10000 页 0.016 ms、生成版 public gRPC 组合过滤 29.042 ms。
- 可复现的全范围第 10000 页：read model P99 0.011 ms、generated public
  gRPC P99 0.843 ms，证据包含 JMH/JFR/GC/进程统计和 SHA-256 manifest。
- 早期限制堆 filter 套件的 JFR heapUsed 最大 1126.4 MiB、RSS 1283.0 MiB；
  新的深页全范围 generated gRPC 证据 max RSS 为 2792.3 MiB，宽范围
  generated gRPC 运行的 2916.2 MiB 是所有 fixed-heap 运行的最大值。全部运行
  zero swaps 且无 OOM。
- 完整报告：`docs/cn/rip2-proxy-admin-m1-benchmark-report.md`。

## 已记录的运行中冒烟

从提交代码 checkpoint `ad55872f1c38df6086a0e7b208f6635ce259dd3e`
构建完整 release distribution，并启动真实本地 NameServer、8081 data-plane
gRPC 和 8082 独立 public admin listener。

```text
mvn -Prelease-all -DskipTests -DskipITs package
BUILD SUCCESS
Finished at: 2026-07-12T02:49:18+08:00
```

通过正式 sibling proto 直接执行 `grpcurl`，验证四条 public RPC 路由。
`ListClients`、`ListClientsByGroup` 和 `ListClientsByTopic` 返回 `OK`；离线
describe 探针返回 `NOT_FOUND` 和
`Client not found: offline-smoke-client`。Public contract 对两类代表性非法请求
返回：

```text
public proxy admin endpoint only supports LOCAL_PROXY scope: PROXY_SCOPE_ALL_PROXIES
pageSize must be greater than or equal to 0
```

运行中的 distribution 还双向证明服务隔离，并确认 admin listener 未开启
reflection：

```text
Method not found: apache.rocketmq.v2.ProxyAdminService/ListClients
Method not found: apache.rocketmq.v2.MessagingService/QueryRoute
server does not support the reflection API
```

cleanup trap 终止完整 smoke 环境，最终 socket 审计记录：

```text
port-9876-closed
port-8081-closed
port-8082-closed
```

完整启动、请求、隔离与清理命令见
`docs/cn/rip2-proxy-admin-m1-final-smoke.md`。

### 鉴权运行时证明

可复现的鉴权 distribution smoke 以 exit code `0` 完成：

```bash
RIP2_AUTH_SMOKE_RUN_ID=final-20260712-v2 dev/run_rip2_authenticated_smoke.sh
```

```text
authenticated-super-list=OK
unsigned-list=UNAUTHORIZED: username cannot be null.
bad-signature-list=UNAUTHORIZED: check signature failed.
rip2-list-list=OK
rip2-list-describe=UNAUTHORIZED
rip2-get-describe=NOT_FOUND
rip2-get-list=UNAUTHORIZED
resource=Admin:proxy.admin.client
```

LIST-only 和 GET-only 用户产生预期拒绝：

```text
User:rip2-list has no permission to access Admin:proxy.admin.client from 127.0.0.1, no matched policies.
User:rip2-get has no permission to access Admin:proxy.admin.client from 127.0.0.1, no matched policies.
```

隔离 NameServer、data-plane gRPC、admin gRPC 和内嵌 Broker 都已由 runner
终止：`port-9876-closed`、`port-8081-closed`、`port-8082-closed` 和
`port-10911-closed`。

## GitHub Actions 批准门禁

当前 strict release gate 在 job 执行前被阻塞，并不是测试失败。RocketMQ
checkpoint `8b123422033f8dc0a7641c21363d1aad625b74d8` 已生成七个 workflow
run，它们的 conclusion 都是 `action_required`，且没有 check run：

- `Build and Run Tests by Maven`
- `Build and Run Tests by Bazel`
- `CodeQL Analysis`
- `Coverage`
- `License checker`
- `Misspell Check`
- `Run Integration Tests`

rocketmq-apis checkpoint
`c372905ce927cf8957333e7ac07877f295fd7ec9` 的 `CI` workflow 也返回相同的
`action_required`。这些 fork PR workflow 必须获得 Apache maintainer approval
后，hosted job 才能启动。Strict guard 现在会按两个准确 HEAD 查询 Actions runs，
并把这一状态与 checks 缺失或失败分开报告。

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --require-github-checks
```

等待外部批准期间，非 strict review guard 保持绿色；只有 workflow 实际运行并
通过后，strict command 才应转绿。

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
JAVA_HOME="${JAVA_HOME:?Set JAVA_HOME to a JDK 17 installation}" \
mvn -pl proxy -am \
"-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result: `Tests run: 779, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`.

轻量提交门禁：

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github
```

Result: `RIP-2 submission guard passed.`

## Benchmark

1M synthetic client JMH on Apple M4, 16 GB, Temurin JDK 17.0.18:

- local read-model worst P99: 0.681 ms.
- generated public gRPC endpoint worst P99: 3.576 ms.
- internal coordinator experiment worst P99: 9.011 ms.
- 4 GiB fixed heap worst-case P99: broad prefix 137.526 ms, combined filters
  243.610 ms, deep page 0.016 ms, and production-shaped public gRPC 29.042 ms.
- Reproducible full-range deep page 10000 P99: read model 0.011 ms and generated
  public gRPC 0.843 ms. Maximum RSS across all fixed-heap runs was 2916.2 MiB;
  every run had zero swaps and no OOM.

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
