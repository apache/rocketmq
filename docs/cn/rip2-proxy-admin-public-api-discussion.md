# RIP-2 Proxy Admin Public API 讨论稿

## 目的

本文最初用于讨论 RIP-2 Proxy Admin 在线客户端查询 API。当前参赛分支已经包含
RocketMQ 侧下游实现和配套的 `rocketmq-apis` proposal 分支，因此本文保留为
public contract 的设计依据和评审 checklist。

当前实现分支已经包含生成版 public `ProxyAdminServiceGrpc` adapter、本地 read
model、鉴权 facade、metrics hooks、客户端生命周期写入、独立 admin gRPC server
注册，以及跨 proxy coordinator seam。剩余决策是 upstream protobuf 归属、字段号、
artifact 版本和发布路径。

## 当前 upstream 状态

截至 2026-07-10 最终参赛刷新时，upstream RocketMQ 仍没有已接受的 public
`ProxyAdminService` protobuf API。配套 API proposal 已打开为
<https://github.com/apache/rocketmq-apis/pull/112>，RocketMQ 实现 draft PR 已打开为
<https://github.com/apache/rocketmq/pull/10603>。

为了完成本地参赛验证，当前实现分支依赖
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`，该 artifact 由配套
`rocketmq-apis` proposal 分支生成。直到社区接受 protobuf 位置并发布官方 artifact
前，public API 仍是 proposal；但下游 endpoint 已实现并通过测试验证。

## 推荐方案

新增独立 public `ProxyAdminService`，不要扩展现有 `MessagingService`。

理由：

- 在线客户端查询属于管理 API，不属于客户端消息收发流量。
- Admin RPC 需要与 producer / consumer RPC 不同的鉴权、错误处理、限流和运维控制。
- 独立 service 让部署方可以独立暴露、保护或关闭 admin RPC。
- Proxy 实现可以在 `GrpcMessagingApplication` 旁注册该 service，同时复用同一个
  `DefaultGrpcMessagingActivity` 和 read model。

M1 默认只开放 local proxy 语义。`ALL_PROXIES` 和 `PROXY_ID` 可以作为 future-compatible
enum values 保留，但 public endpoint 在社区接受 coordinator 和 peer transport 语义前
应返回 `BAD_REQUEST`。

## RPC 草案

proposal mirror 位于 `docs/en/rip2-proxy-admin-m1-public-api-draft.proto`。本地验证的
权威 proposal source 是 sibling checkout 中
`../rocketmq-apis/apache/rocketmq/v2/admin.proto`，分支为
`rip2-proxy-admin-public-api`。

参赛面向的 unary RPC：

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

响应模型返回现有 v2 `Status`，再附带一个 `ProxyClient` 或一页 `ProxyClient` entries。
错误响应只返回 status。

public request shape 应遵循 RIP-2 issue 中的过滤字段：`clientId`、`clientIdPrefix`、
`group`、`topic`、`clientLanguage`、`connectTimeStart`、`connectTimeEnd`、`pageNum`
和 `pageSize`。`pageNum` 从 1 开始，`pageSize` 上限为 100。当前分支仍保留内部
page-token 路径用于 coordinator 实验，但该 token 不是面向参赛的 public contract。

当前 draft 在 `ProxyClient` response 中包含 `proxy_id`。Local M1 response 可以填充
服务端 proxy name，未来 cross-proxy response 也可以使用同一字段标识每个 client 的
来源 proxy。因为它属于 public response surface，字段是否最终保留仍需要社区 review。

## Scope 语义

`PROXY_SCOPE_UNSPECIFIED` 应按 `PROXY_SCOPE_LOCAL_PROXY` 处理。

`PROXY_SCOPE_LOCAL_PROXY` 只查询连接到当前 proxy 进程的客户端。这是 M1 public 行为。

`PROXY_SCOPE_ALL_PROXIES` 为内部 coordinator fan-out 预留。它应保持 gated，直到 peer
discovery、peer authorization、page-token ownership 和 failure semantics 被接受。

`PROXY_SCOPE_PROXY_ID` 为查询指定 proxy 预留。当前 M1 public endpoint 会先按
scope gate 拒绝该 scope，即使请求省略 `proxy_id` 也是如此。未来 public
coordinator rollout 中，如果使用该 scope，则必须提供 `proxy_id`；届时 adapter
应在创建 request context 或调用 service 之前拒绝缺失或空白的 `proxy_id`。

## 内部跨 Proxy 实验

本 fork 包含 proto-free 内部跨 proxy 实验，用于在不修改 public API 的前提下验证未来
scope 语义：

- `ProxyClientAdminScopeRouter` 将 `LOCAL_PROXY` 保持在本地 activity 上；只有当
  `enableProxyClientAdminCrossProxyQuery` 开启时，才把内部 `ALL_PROXIES` 和
  `PROXY_ID` 请求路由到 coordinator。
- coordinator fan out peer-local list 请求，按稳定的 `(client_id, proxy_id)` 顺序合并
  pages，并维护 `cp1:` coordinator page tokens，token 内包含 per-peer cursors 和有界
  retention。
- raw internal peer protocol 和 `ProxyClientAdminPeerGrpcTransport` 可以在没有生成版
  public admin stubs 的情况下演练 static peer targets。
- peer discovery 和 peer calls 由 `TimedProxyClientAdminPeerClient` 包装，因此 bounded
  waits 会表现为 `PROXY_TIMEOUT`，而不是泛化的 internal errors。

这些组件是 proposal 的实现证据，不是 public API 承诺。public endpoint 在社区接受
peer discovery、timeout、token ownership 和 partial failure semantics 前仍只开放
`LOCAL_PROXY`。

## 分页

参赛面向的 public API 使用 `pageNum` 和 `pageSize`。`pageNum` 从 1 开始，`pageSize`
上限为 100；local query 按 client id 稳定排序。

内部 M1 local read model 已有 page-token 路径，因为 local results 按 client id 排序，
且 coordinator 实验需要 per-peer cursors。除非社区后续选择 opaque token contract 来
支持 cross-proxy pagination，否则该 token 路径应保持内部使用。

未来 cross-proxy pagination 可以继续在内部使用 coordinator-owned tokens，携带 scope、
filters、last emitted `(client_id, proxy_id)`、per-peer cursors 和 token creation time。
过期 coordinator tokens 应在 peer fan-out 前被拒绝。

## Endpoint 实现形态

当前分支实现了独立
`GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase`。
它没有把 admin methods 加到 `GrpcMessagingApplication`。

每个 generated unary method 都保持 thin adapter：

- 通过 `ProxyClientAdminContextFactory` 读取 gRPC metadata。
- 通过 `ProxyClientAdminRequestConverter` 将 protobuf requests 转为内部 DTO。
- 调用 `ProxyClientAdminEndpointExecutor`。
- 将 `ProxyClientAdminPageView` 和 `ProxyClientAdminClientView` 转换为 protobuf responses。
- 将鉴权、校验、分页、metrics 和错误映射保留在现有 activity、scope router、endpoint
  handler 和 service layer 之后。

`ProxyStartup.createProxyAdminGrpcBindableServices` 会在独立 admin gRPC server 上注册
public `GrpcProxyAdminApplication`，并复用同一个 `DefaultGrpcMessagingActivity`。

## 兼容性

public API 应保持 additive：

- 不修改现有 messaging RPCs。
- 不改变 producer 或 consumer client lifecycle 行为。
- M1 不要求持久化或分布式 registry。
- restart 行为不变；clients 通过 telemetry 和 heartbeat 重新填充 read model。

draft 中的 public field numbers 应在生成类正式落地前确认。社区 review 开始后，应避免
重排字段号。

## 开放问题

1. protobuf 应放在现有 v2 messaging APIs 旁的 `rocketmq-apis` 中，还是单独 admin API 文件？
2. `ProxyAdminService` 是否是可接受的 public service 名称？
3. cross-proxy enum values 是否应从第一天就存在但被拒绝，还是完全延后？
4. public admin service registration 应由什么 deployment-level 控制项 gate？
5. `proxy.admin.client` 是否是 `LIST` / `GET` client-query ACL resource 的合适名称？
6. 第一版 public release 是否应在 local result 中暴露 `ProxyClient.proxy_id`，还是只留给未来
   cross-proxy scopes？

## 当前分支实现状态

在 public API ownership 继续推进期间，当前分支已可用于 proxy 侧 review：

- local read model，支持稳定分页和 secondary indexes。
- gRPC client lifecycle 写入 read model。
- 内部 `ClientAdminService` 和 `ProxyClientAdminActivity`。
- 与 proposed public model 对齐的 request DTOs 和 response views。
- scope mapper、内部 page-token codec，以及 public page-number adapter。
- endpoint executor 和 endpoint handler，并在创建 request context 前执行 M1 public
  `LOCAL_PROXY` scope gate。
- generated `GrpcProxyAdminApplication`，覆盖 `ListClients`、`DescribeClient`、
  `ListClientsByGroup` 和 `ListClientsByTopic`。
- 通过 `ProxyStartup` 注册独立 admin gRPC server。
- authorization facade 和 metrics hooks。
- internal coordinator、peer gRPC service、static peer transport、timeout handling，以及
  coordinator-scope metrics，用于不修改 public protobuf APIs 的 cross-proxy 实验。
