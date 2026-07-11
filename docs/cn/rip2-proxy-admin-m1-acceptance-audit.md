# RIP-2 Proxy Admin M1 验收审计

本文把公开 RIP-2 tracking issue 的验收项逐条映射到当前 contest 分支证据。
它区分“本地已经实现并验证”的内容，以及仍需要社区 review 或外部集成环境的内容。

## 审阅入口

| Artifact | Link |
| --- | --- |
| RocketMQ 实现 draft PR | https://github.com/apache/rocketmq/pull/10603 |
| Public API draft PR | https://github.com/apache/rocketmq-apis/pull/112 |
| RIP-2 tracking issue comment | https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687 |
| 实现分支 | `pilichoumao/rocketmq:rip2-proxy-admin-m1` |
| API 分支 | `pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api` |
| 评审复现手册 | `docs/cn/rip2-proxy-admin-m1-review-runbook.md` |

## M1 初始版本验收

| `apache/rocketmq#10599` 验收项 | 当前状态 | 证据 | 剩余门禁 |
| --- | --- | --- | --- |
| Proxy Admin 架构和 proto 定义通过社区 review。 | review 中。 | Draft PR #10603 和 #112 已作为 review artifact 打开。设计采用独立 `ProxyAdminService` 和独立 admin gRPC server。 | 社区需要确认 proto 归属、字段编号、生成 artifact 版本和发布路径，然后才能把 PR 视为 merge-ready。 |
| 4 个核心 client query RPC 完整实现，支持过滤和分页。 | 本地已实现并测试。 | `GrpcProxyAdminApplication` 暴露 `ListClients`、`DescribeClient`、`ListClientsByGroup`、`ListClientsByTopic`；`GrpcProxyAdminApplicationTest` 使用生成版 Server/Channel 调用。 | 上游合并等待正式 `rocketmq-proto` artifact。 |
| 1M client 下分页查询 P99 < 1s，且无 OOM 风险。 | 本地已实现并验证。 | 4 GiB 限制堆证明覆盖宽 prefix（137.526 ms P99）、组合过滤（243.610 ms）、第 10000 页（0.016 ms）和使用生产形态执行器的生成版 public gRPC（29.042 ms）。JFR max heapUsed 为 1126.4 MiB，max RSS 为 1283.0 MiB，全部运行 zero swaps 且无 OOM。 | 硬件或实现明显变化时重跑。 |
| ACL 2.0 使用独立 `proxy.admin.client` 资源。 | 已实现并测试。 | `ClientAdminAuthPolicyTest`、`DefaultClientAdminAuthorizationServiceTest`、`AuthorizingClientAdminServiceTest`；list 类 RPC 需要 `LIST`，describe 需要 `GET`。 | 资源命名仍待社区 review。 |
| Admin 接口启用 OpenTelemetry metrics。 | 已覆盖 service 和进入 service 前的 endpoint 失败并完成测试。 | `MeteredClientAdminServiceTest`、`MeteredAuthorizingClientAdminServiceTest`、`ClientAdminMetricsContextTest`、`ProxyClientAdminObservabilityTest`、`ProxyClientAdminEndpointExecutorTest`、`GrpcProxyAdminWiringTest`；维度包括 operation、scope、status/result、duration、filters、page size 和 result size。Request adapter/context 失败和 query executor 拒绝会在 service 调用前计量，同时不会重复统计已委托请求。 | 生产 dashboard 接入不在当前分支内。 |
| 与 RIP-1 Dashboard CLIENT-01 完成 E2E 联调。 | Public runtime path 已在本地证明；Dashboard 集成尚未证明。 | `GrpcProxyAdminApplicationTest` 通过生成版 gRPC Server/Channel 验证 Dashboard-facing client view 字段。完整 release distribution 也已通过真实本地 NameServer 和 Proxy 启动，并用直接 `grpcurl` 在隔离 admin listener 上调用全部四个 public RPC。`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md` 已记录 Dashboard 交接契约。 | 需要包含 RIP-1 Dashboard client 的外部集成环境。 |
| 提供完整中英文接口文档、鉴权示例和最佳实践。 | 已实现。 | 中英文 user guide、Dashboard contract、final smoke、benchmark report、submission package 和本文。 | review 过程中保持链接更新。 |

## 整体架构验收

| 验收项 | 当前状态 | 证据 | 剩余门禁 |
| --- | --- | --- | --- |
| 独立 admin service framework 支持后续模块扩展。 | M1 已实现。 | `ProxyClientAdminEndpointExecutor`、`ProxyClientAdminEndpointHandler`、`GrpcProxyAdminApplication`、`ProxyStartup` admin service 注册。 | 后续模块应复用同一 service/activity 边界。 |
| Admin 接口按端口、线程池、认证隔离。 | 已实现并通过黑盒测试。 | `GrpcProxyAdminProductionInterceptorE2ETest` 通过生产 `GrpcServerBuilder` interceptor 启动独立 loopback messaging/admin server。完整 distribution smoke 进一步证明 8081 不含 `ProxyAdminService`、8082 不含 `MessagingService`，且 8082 未开启 reflection。现有配置/executor tests 覆盖认证流程、`enableProxyAdminGrpcServer`、`proxyAdminGrpcServerPort` 和独立线程池。 | 生产部署需显式开启 admin server。 |
| 接口契约遵守兼容性规则。 | draft contract 采用 additive protobuf 风格。 | `../rocketmq-apis/apache/rocketmq/v2/admin.proto` 和 `docs/en/rip2-proxy-admin-m1-public-api-draft.proto` 使用独立 service 和稳定字段编号 proposal。 | Apache 需要在 merge 前确认最终 contract。 |

## 质量和文档

| 验收项 | 当前状态 | 证据 | 剩余门禁 |
| --- | --- | --- | --- |
| 核心模块单测覆盖率 >=85%。 | RIP-2 core packages 本地已验证。 | submission package 记录 `service.admin.client` line coverage 94.41%、`grpc.v2.admin` line coverage 94.73%；branch coverage 分别为 86.62% 和 85.79%。 | 代码变更后需要重跑 coverage。 |
| 集成测试覆盖接口、鉴权和异常处理。 | 本地已覆盖。 | 生成版 gRPC Server/Channel tests、production interceptor 双 server E2E、proto-free endpoint integration tests、subject/address 信任边界测试、peer tests、ACL tests，以及 public bad-request/not-found/unauthorized/internal-error response mapping tests。已记录的 release-distribution smoke 也调用全部四个 RPC，并验证 live BAD_REQUEST/NOT_FOUND 语义。 | Dashboard CLIENT-01 仍是外部 E2E 项。 |
| 提供性能 benchmark report。 | 已实现。 | 中英文 benchmark report 包含命令、环境、1M-client read-model、生成版 public gRPC endpoint、coordinator 场景、P50/P95/P99 和 heap 设置。 | 硬件或实现明显变化后需要重跑。 |
| 中英文文档与代码同步。 | contest 分支已同步。 | 中英文 user guide、final smoke、benchmark report、submission package 均已存在。 | review 过程中保持 PR 和 issue 链接更新。 |

## 最终门禁汇总

当前 contest 分支已经本地实现并验证 M1 public endpoint 行为、local read-model
和 generated public endpoint 性能、ACL、可观测性、文档和生成版
gRPC Server/Channel 覆盖和限制堆性能证明。剩余门禁都是外部社区/集成项：

- 社区接受 `rocketmq-apis` public proto proposal。
- 发布包含 `ProxyAdminServiceGrpc` 的正式 `rocketmq-proto` artifact。
- 在包含 RIP-1 dashboard client 的环境中完成 Dashboard CLIENT-01 联调。字段级
  契约已记录在 `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。
