# RIP-2 Proxy Admin 在线客户端查询指南

## 当前状态

本文说明本分支已经实现的 RIP-2 M1 admin 查询行为。当前代码已经包含内部
read model、授权封装、metrics、无 proto 依赖的 admin adapter、生成版公开
`ProxyAdminService` endpoint、独立 admin 查询线程池以及 benchmark 覆盖。
公开 endpoint 当前依赖本地 contest artifact
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`，该 artifact 由配套
`rocketmq-apis` proposal 分支生成。上游合并仍受 protobuf 归属、字段编号、
生成 artifact 版本和发布路径的社区决策约束。

最终参赛提交入口是 `docs/cn/rip2-proxy-admin-m1-submission-package.md`，其中
包含要求对照、验证快照、PR 链接、issue comment 链接，以及明确的 public
protobuf 归属 gate。

Dashboard CLIENT-01 字段级联调契约见
`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。

M1 对外语义建议只暴露 `LOCAL_PROXY`，即查询当前 proxy 进程上的在线客户端。
跨 proxy fan-out 和 `PROXY_ID` 查询目前属于内部探索能力，不建议作为 M1 公共
接口承诺。

## 公共服务

当前 public API proposal 新增独立的 `ProxyAdminService`，不扩展现有
`MessagingService`。

已实现 RPC：

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

`GrpcProxyAdminApplication` 只负责 protobuf request/response 转换。实际校验、
授权、metrics、错误映射、分页和查询逻辑都复用当前已经实现的 admin endpoint
executor、activity 和 service。

## 查询字段

生成版 endpoint、内部 service 和无 proto adapter 已支持以下字段：

| 字段 | 含义 |
| --- | --- |
| `scope` | M1 公共 API 只接受 `LOCAL_PROXY`。 |
| `clientId` | 精确客户端 id 查询或过滤。 |
| `clientIdPrefix` | 按 client id 稳定排序的前缀查询。 |
| `group` | 按消费者组索引过滤。 |
| `topic` | 按 topic 索引过滤。 |
| `clientLanguage` | 按客户端语言过滤，例如 `JAVA`。 |
| `connectTimeStartMillis` | 连接时间闭区间下界。 |
| `connectTimeEndMillis` | 连接时间闭区间上界。 |
| `pageNum` | 公共分页页码，从 1 开始，必须 `>= 1`。 |
| `pageSize` | 公共分页大小，最大为 `100`。 |

内部 read model 仍保留 opaque page token 路径，用于 local 兼容和 coordinator
探索。公开 protobuf 请求应优先使用 `pageNum/pageSize`；如果未来公开 token，
也必须把 token 视为 opaque 值。

## 响应字段

客户端响应包含：

- `clientId`
- `clientType`
- `language`
- `version`
- `localAddress`
- `remoteAddress`
- `connectionTime`
- `lastActiveTime`
- `groups`
- `topics`
- `proxyId`

当前 public API proposal 已保留赛题要求的 client id、language、version、
local address、remote address 和 connection time 信息。最终字段编号和发布位置
仍需社区 review。

## 错误语义

生成版公开 endpoint 和无 proto endpoint 都会将异常映射为 RocketMQ v2
`Status`：

| 场景 | Code |
| --- | --- |
| 非法参数、非法 page token、非法 scope、M1 不支持的 public scope、缺少必填 id | `BAD_REQUEST` |
| 已提供的 `client_id` 不在线，或 `DescribeClient` 查询为空 | `NOT_FOUND` |
| 授权失败或缺少认证 subject | `UNAUTHORIZED` |
| peer discovery 或 peer request 超时 | `PROXY_TIMEOUT` |
| admin 查询线程池队列满 | `TOO_MANY_REQUESTS` |
| 不支持的未来操作 | `NOT_IMPLEMENTED` |
| 未预期运行时异常 | `INTERNAL_SERVER_ERROR` |

## ACL

逻辑 ACL 资源为 `proxy.admin.client`。

- `ListClients`、`ListClientsByGroup`、`ListClientsByTopic` 需要 `LIST`。
- `DescribeClient` 需要 `GET`。

gRPC request pipeline 会把认证后的 subject 传入 `ProxyContext`。认证未成功时，
public admin pipeline 不信任客户端自行提供的 subject header；用于源 IP 授权的
地址也必须由 transport 层覆盖客户端 metadata；所有客户端提供的
`proxy_protocol_*` header 都会先清除，再回填可信 transport 属性。Admin endpoint 不能在日志中
明文记录 subject。

## 执行线程池和可观测性

公开 admin gRPC service 在 `enableProxyAdminGrpcServer=true` 时运行在独立
admin server 上。生成版 service 通过
`ProxyStartup.createProxyAdminGrpcBindableServices(...)` 注册，和数据面
`MessagingService` server 分离。默认值：

- `enableProxyAdminGrpcServer = false`
- `proxyAdminGrpcServerPort = 8082`
- `proxyAdminGrpcThreadPoolNums = 4`
- `proxyAdminGrpcThreadPoolQueueCapacity = 10000`
- server 线程名前缀 `ProxyAdminGrpcRequestExecutorThread`

Admin 查询使用独立 executor，避免阻塞 messaging 写路径。默认值：

- `proxyClientAdminQueryThreadPoolNums = 4`
- `proxyClientAdminQueryThreadPoolQueueCapacity = 10000`
- 线程名前缀 `ProxyClientAdminQueryThread_`

Metrics、trace 和日志字段保持低基数：

- operation
- result code
- scope
- status
- page size
- filter presence
- result size
- duration

失败日志会记录 operation、status、result、scope、filters、page size 和 result
size，但不会记录 client id、group、topic、proxy id 或认证 subject 明文。

## 验证命令

验证 benchmark setup 和官方字段场景：

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  -Dtest=ProxyClientReadServiceBenchmarkTest,ProxyClientAdminCoordinatorServiceBenchmarkTest \
  -DfailIfNoTests=false test -DskipITs
```

当前较完整的 proxy 提交前验证：

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  "-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
  -DfailIfNoTests=false test -DskipITs
```

1M client JMH benchmark 的启动方式见
`docs/en/rip2-proxy-admin-m1-design.md`。本机已完成的 1M 运行结果见
`docs/cn/rip2-proxy-admin-m1-benchmark-report.md`。

## M1 限制

- 本 contest 分支已经包含生成版公开 gRPC endpoint，但在 `rocketmq-apis`
  proposal 被接受并发布前，仍依赖本地
  `rocketmq-proto:2.2.0-rip2-SNAPSHOT` artifact。
- 公共 M1 scope 为 `LOCAL_PROXY`；跨 proxy 查询仍是内部探索。
- read model 是进程内内存模型，proxy 重启后依赖客户端生命周期事件重新构建。
- `pageSize` 最大为 `100`；调用方应使用 `pageNum` 获取后续页。
