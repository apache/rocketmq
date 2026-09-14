# Proxy Admin 管理接口

> 英文版见 `docs/proxy-admin.md`。

## 1. 动机

RocketMQ 5.0 把客户端访问放到了无状态的 Proxy 后面，但运维侧仍然通过 broker 侧的结构（broker 上的 `ConsumerManager`、Remoting 时代的 admin 命令）来观测客户端。连到 Proxy 的 gRPC 客户端对这些工具不可见：控制面无法回答"哪些 SDK 客户端在线、它们订阅了什么、是否健康"，只能依赖间接的指标推断。

控制面 dashboard 需要一个标准的服务端接口来读取完整的 gRPC 客户端数据。本次改动在 Proxy 上实现这个接口。

## 2. 契约选择：上游 `Admin` service

gRPC 契约是**上游 `Admin` service**，定义在 `apache/rocketmq-apis` 仓库（main 分支，作为 git submodule 引入，见 §6）的 `apache/rocketmq/v2/admin.proto` 中。本实现刻意**不**在协议层引入自定义的 `ProxyAdminService`：控制面 RPC 接口位于 dashboard 和多语言 SDK 已经在消费的、经过社区评审的版本化 proto 中，并与上游演进保持增量兼容。

proxy 实现 `AdminGrpc.AdminImplBase`（全部 16 个 RPC），并把它绑定在一个独立的 admin gRPC server 上。

## 3. 目标

1. 在 Proxy 上提供一个独立、专用的 gRPC Admin server，与数据面 `MessagingService` 隔离。
2. 来自 rocketmq-apis main 的稳定、向后兼容契约（`Admin` service，`apache/rocketmq/v2/admin.proto`）。
3. 通过专用的 `proxy.admin.*` ACL 2.0 资源实现一等公民的鉴权，区分只读与高权限动作。
4. 集群级语义但不把拓扑泄漏进协议：每个 proxy 都暴露连接/订阅数据的集群可见视图，并把客户端定向 RPC 转发到持有该客户端的 proxy。

## 4. 设计决策

### D1 — 服务放置

上游 `Admin` service 绑定在自己的 gRPC server/端口（`grpcAdminServerPort`，默认 8088）上，有自己的拦截器链，与数据面（8081）分离。整个接口由全局 kill switch `grpcAdminServerEnable` 门控，**默认为 false**：admin server 是 opt-in 的，除非显式启用否则不会启动。

理由：控制面流量绝不能与数据面竞争，且 admin 端口可以只对运维网络开放防火墙。admin server 刻意不暴露 channelz 或 proto reflection。它复用数据面的 `GrpcChannelManager` / `GrpcClientSettingsManager`，使在线客户端对 admin 查询可见。本特性引入的运行时线程池均归实例所有并接入 Proxy 生命周期：admin timeout scheduler 随 `ProxyAdminGrpcService` 关闭，blocking gateway executor 随 `AdminService` 关闭。

### D2 — 鉴权：专用 `proxy.admin.*` 资源

凭据通过标准 gRPC `Authorization` metadata 传入（与数据面相同的方案，ACL 2.0 签名）。认证以拦截器方式执行，顺序排在标准链路的 `HeaderInterceptor` **之后**（因 gRPC 按注册的逆序调用拦截器，故注册在其之前）。这个顺序是必需的：`HeaderInterceptor` 会把入站的 `x-mq-channel-id` 替换为从传输层派生的 channel id，而认证结果按 channel id 缓存，若在客户端自带的值上做认证，一条连接的请求就能复用另一条连接的缓存结果、跳过签名校验。

每个 RPC 映射到一个资源 + 一个动作：

| 资源 | RPC | 动作 |
|---|---|---|
| `proxy.admin.client` | ListSubscription, ListConsumerConnection (List); DescribeSubscription, DescribeGroupAccumulation, GetConsumerRunningInfo, QueryTimeSpan (Get) | List / Get |
| `proxy.admin.config` | ChangeLogLevel | Update |
| `proxy.admin.connection` | PrintThreadStackTrace, VerifyMessage | Update（高权限）|
| `proxy.admin.route` | GetTopicRoute | Get |
| `proxy.admin.ops` | GetProxyRuntimeStats, DescribeTopicStatus, QueryMessage (Get); ResetGroupOffset (Update); DeleteSubscription (Delete); AdminSendMessage (Pub) | Get / Update / Delete / Pub |

建模说明：ACL 2.0 的资源类型是 cluster/namespace/topic/group。admin 资源建模为 CLUSTER 类型的字面量，使用保留名称（资源 key `cluster:proxy.admin.<module>`），从而在不与真实集群名冲突、不改动鉴权核心的前提下实现精确的最小权限匹配。

模式：
- 集群鉴权关闭、`grpcAdminServerAuthEnable=false` → 开放接口（与数据面语义相同）;
- 集群鉴权开启 → 针对每方法 `proxy.admin.*` 资源的标准 authenticate + authorize 流程;
- `grpcAdminServerAuthEnable=true` → fail-closed。admin 接口必须既能认证调用方**又能**强制每方法 ACL，因此该模式要求集群级的认证**和**授权开关都开启。任一关闭时所有请求都被拒绝（认证关 → `UNAUTHENTICATED`; 授权关 → `FAILED_PRECONDITION`），而不是在没有真正权限校验的情况下放行。这是刻意为之：ACL 2.0 评估器本身受集群授权开关门控，若在授权关闭时服务请求，会静默放行每个调用，包括破坏性的那些。

审计：每个被服务的 RPC 都会向鉴权审计 logger 写入 `[PROXY-ADMIN-AUDIT] subject/method/resource/action/sourceIp`（与 ACL 2.0 引擎自身的审计日志一起，满足 Console 用户 + AK + 资源 + 操作的审计四元组要求）。

### D3 — 多 proxy 语义：集群可见，协议不含拓扑

一个 gRPC 客户端同一时刻只连到一个 proxy，但 dashboard 仍需从任意单个 admin 端点看到整个集群。两个机制在不给协议加 scope/peering 字段的前提下做到这点：

- **连接 / 订阅数据是集群可见的。** 列表类 RPC（`ListConsumerConnection`、`ListSubscription`、`DescribeSubscription`）把 broker 侧的 `ConsumerConnection` 视图（覆盖 remoting 客户端，以及被 `HeartbeatSyncer` 在 proxy 间同步的 gRPC 客户端）与本 proxy 自己的 `ConsumerManager`（覆盖本地接入的 gRPC v2 客户端）合并。结果按 group + topic 去重，因此应答不随在线消费者数量膨胀，也不取决于查询的是哪个 proxy。
- **客户端定向 RPC 转发到持有该客户端的 proxy。** `PrintThreadStackTrace`、`VerifyMessage`、`GetConsumerRunningInfo` 需要只有持有方 proxy 才有的实时 telemetry 流。`ProxyAdminForwarder` 识别出属于 peer 的客户端（其 `ConsumerManager` 条目是 `RemoteChannel`，`getRemoteProxyIp()` 为 peer 的 `localServeAddr`），把整个 RPC——连同原始 metadata——转发到 peer 的 admin 端口，并用 `x-mq-admin-forwarded` 头防止转发成环。peer 的 admin 端口按集群统一假设（心跳同步载荷不携带它）; 异构 admin 端口的集群需要先把它加入心跳记录。

`GetProxyRuntimeStats` 刻意报告**本进程**的状态（自身的连接/生产者/消费者计数），因此想要按节点统计的 dashboard 直接查询每个 proxy。

### D4 — 每个 RPC 的数据源

| 数据源 | RPC | 机制 |
|---|---|---|
| 集群可见连接 | ListConsumerConnection, ListSubscription, DescribeSubscription | broker 侧 `ConsumerConnection`（remoting + proxy 间同步的客户端）与本 proxy 的 `ConsumerManager`（本地 gRPC v2 客户端）合并 |
| 客户端定向 telemetry relay | PrintThreadStackTrace, VerifyMessage, GetConsumerRunningInfo | relay 到持有方 `GrpcClientChannel` 的 telemetry command; 客户端在 peer 上时转发到 peer proxy（D3）|
| Broker 网关（异步、多 broker fan-out）| DescribeTopicStatus, DescribeGroupAccumulation, ResetGroupOffset, QueryMessage, QueryTimeSpan, GetTopicRoute, DeleteSubscription | proxy 自己的异步 `AdminService` remoting 网关; 每个 broker hop 有独立 deadline，慢/无响应的 broker 不会挂起整个 RPC |
| Proxy producer 路径 | AdminSendMessage | `MessagingProcessor.sendMessage`（定时 / FIFO 消息通过 system properties 生效）|
| Proxy 运行时 | ChangeLogLevel | root logger 上重定位的 logback API |

所有 broker 侧调用都走异步 `AdminService` 网关并并发扇出到所有相关 broker，因此 gRPC 执行线程不会被阻塞。

### D5 — 协议覆盖

连接/订阅列表是集群可见的，同时包含 remoting 客户端（通过 broker 侧 `ConsumerConnection`）和 gRPC v2 客户端（通过 proxy 的 `ConsumerManager`）。客户端定向 telemetry RPC（`PrintThreadStackTrace`、`VerifyMessage`、`GetConsumerRunningInfo`）作用于持有 proxy telemetry 流的 gRPC v2 客户端，并支持跨 proxy 转发; 它们的完整 running-info 载荷受 telemetry 协议能承载的内容限制（见 §8 诚实边界）。

## 5. 能力映射

| 需求 | 交付为 | 说明 |
|---|---|---|
| ListClients（按 group/topic/clientId 前缀过滤，分页）| `ListConsumerConnection(group[, topic])` | clientId 前缀过滤和分页在上游契约里无法表达; 这是本次部分交付的已记录边界 |
| DescribeClient（SDK 版本、订阅、心跳、鉴权、Pop 进度）| `DescribeSubscription`（每客户端订阅）+ `GetConsumerRunningInfo`（订阅 + 运行信息）| 心跳/鉴权状态/Pop 进度字段在上游契约里不存在 |
| ListClientsByGroup / ListClientsByTopic | `ListConsumerConnection(group[, topic])` | 完全覆盖 |
| 多 proxy 集群聚合 | 集群可见连接数据 + 客户端定向 RPC 转发 | 见 D3 |

M2+ 项（配置热更新、配额、连接控制、Pop/batch 诊断、路由观测流）不属于上游 `Admin` 契约，不在本次交付范围内。

## 6. 构建 proto 源码

`apache/rocketmq-apis` 仓库（main 分支，包含 `apache/rocketmq/v2/admin.proto`）作为 **git submodule** 引入（`rocketmq-apis/`，见 `.gitmodules`）; CI workflow 用 `submodules: true` 检出。

- **Maven**：`rocketmq-proto` 模块在构建时通过 `protobuf-maven-plugin` 为 `apache/rocketmq/v2/{definition,service,admin}.proto` 生成 Java + gRPC stub; 它的版本跟随 reactor（`${revision}`），因此构建本仓库不需要已发布的 proto 制品。
- **Bazel**：`//rocketmq-proto:rocketmq-proto` 通过对 submodule 的 `genrule`（作为 `@rocketmq_apis` 外部仓库呈现）构建相同的类，使用 pin 的 `protoc` / `protoc-gen-grpc-java` 二进制和 well-known-type proto。
- 其他消费者（dashboard、SDK）继续使用 rocketmq-apis 发布的制品; 只有本仓库从源码构建 proto。

## 7. 配置参考

| Key | 默认值 | 含义 |
|---|---|---|
| `grpcAdminServerEnable` | false | kill switch; admin 接口是 opt-in 的。设为 `true` 启动 admin gRPC server; `false`（默认）= 不启动 |
| `grpcAdminServerPort` | 8088 | 专用 admin gRPC 端口（<=0 禁用）|
| `grpcAdminServerAuthEnable` | false | fail-closed 模式; 要求集群认证**和**授权都已开启（见 D2）|
| `grpcAdminServerRequestTimeoutMillis` | 3000 | admin RPC 扇出的每个 broker hop 的 deadline |

## 8. 诚实边界

admin 接口绝不填写自己无法如实提供的字段; 下列情况以明确 status 回应或留空，而不是伪造：

- **DeleteSubscription 在开源里事实上是 no-op。** 开源代码没有任何地方写入 `SubscriptionGroupConfig.subscriptionDataSet`（broker 只读它），因此没有持久化的 per-topic 订阅可删。该 RPC 返回 `NOT_FOUND` 并说明原因，**消费组及其位点保持不动**。需要该 RPC 的发行版用外部订阅存储填充同一字段。
- **QueryMessage** 支持按 `message_id`（unique-key 索引）和 `message_key` 查询。`subscription` / `lite_topic` / 纯时间范围扫描三种变体需要开源 broker 不提供的分页游标，返回 `BAD_REQUEST`。
- **GetProxyRuntimeStats** 的 `in_tps` / `out_tps` 留空：开源 proxy 没有进程级吞吐计数器，填 0 会与空闲 proxy 无法区分。
- **GetConsumerRunningInfo** 对 gRPC v2 客户端只返回 `subscriptions`：v2 telemetry 协议没有承载 `properties`、`message_queue_table`、`consume_status_table` 的回包。该 RPC 返回 `OK` 并附说明性 status message，而不是空壳。
- **DescribeTopicStatus** 的 `create_timestamp` / `tags` 留空，因为 broker 不记录 topic 创建时间。

## 9. 里程碑

- M1（本次交付）：基于上游 `Admin` service 的在线客户端查询 — ListConsumerConnection / DescribeSubscription / GetConsumerRunningInfo / DescribeGroupAccumulation，以集群可见方式并支持跨 proxy 转发提供，外加完整的 16-RPC `Admin` 接口用于 dashboard 集成。
- 未来（需要上游 proto 演进）：客户端列表的 clientId 前缀过滤 / 分页、心跳和鉴权状态字段，以及 M2+ 接口（配置/配额/连接/路由观测）。

## 10. 验收标准映射

| 标准 | 状态 |
|---|---|
| 设计文档 + 稳定向后兼容的 proto 契约 | 本文档 + 上游 `admin.proto`（rocketmq-apis main）|
| 客户端查询 RPC 合入服务端仓库 | `ProxyAdminGrpcService` 在 proxy 上实现全部 16 个 `Admin` RPC |
| 独立 ACL 控制、只读/高危分离、最小权限文档 | D2 资源/动作 + 下方 §11（最小权限配置指南）|
| 与 dashboard 的 E2E | 契约 = 上游 `Admin` service，为 dashboard 集成冻结（跨仓库）|

## 11. 最小权限配置指南

admin 接口对每个 RPC 按专用的 `proxy.admin.*` ACL 2.0 资源鉴权（见上方 §4 决策 D2）。本节给出每个运维角色的最小权限策略。

### 11.1 资源与动作模型

资源（ACL 2.0 key; 建模为带保留名称的 cluster 类型字面量）：

| 资源 key | 保护 |
|---|---|
| `cluster:proxy.admin.client` | 在线客户端查询与客户端诊断 |
| `cluster:proxy.admin.config` | proxy 运行时日志级别变更 |
| `cluster:proxy.admin.connection` | 客户端定向 telemetry 命令：线程栈、verify message（高危）|
| `cluster:proxy.admin.route` | topic 路由视图 |
| `cluster:proxy.admin.ops` | broker 侧运维：stats/topic status/message query（读）和 reset offset / delete subscription / admin send（高危）|

动作分类：

- 只读：`Get`, `List`
- 高权限（变更 / 破坏性）：`Update`, `Delete`, `Pub`

服务端把每个 RPC 映射到恰好一个（资源, 动作）对; 授予只读动作绝不会授权高权限 RPC。

### 11.2 角色模板

所有命令针对集群的任意 broker/namesrv 运行（ACL 2.0 存储）。先创建用户：

```bash
sh mqadmin createUser -n <namesrv-addr> -u <username> -p <password>
```

#### 角色 A — 只读观察者（dashboard 服务账号）

在线客户端、订阅、堆积、诊断、配置/路由视图。

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List \
  -d Allow
```

说明：`proxy.admin.ops` 上的 `Get,List` 覆盖只读的 broker 侧 RPC; 变更类 ops RPC 需要 `Update`/`Delete`/`Pub`，仍被拒绝。

#### 角色 B — on-call 运维（观察者 + 客户端诊断命令）

角色 A 加上向已连接客户端下发 telemetry 命令（线程栈 / verify message）的能力。

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-oncall \
  -r cluster:proxy.admin.connection \
  -a Update \
  -d Allow
# 外加上面的角色 A 授权
```

#### 角色 C — Admin（完全控制，break-glass）

offset 重置、订阅删除、admin send、客户端诊断命令。

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-admin \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.connection,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List,Update,Delete,Pub \
  -d Allow
```

（把角色 C 账号控制在最小范围; 每次使用都会以 `[PROXY-ADMIN-AUDIT]` 前缀记录在鉴权审计日志里。）

#### 环境限制（推荐）

通过 `-i` sourceIp 选项把 admin 访问限制到运维网络：

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client \
  -a Get,List \
  -d Allow \
  -i 10.0.0.0/8
```

### 11.3 Fail-Closed 模式

默认情况下 admin server 遵循集群级认证/授权开关（与数据面行为相同）。fail-closed 模式让 admin 接口拒绝服务任何它无法完全授权的请求：

```properties
# proxy.json / -D grpcAdminServerAuthEnable=true
grpcAdminServerAuthEnable: true
```

`grpcAdminServerAuthEnable=true` 时，admin 接口要求集群认证**和**授权开关都开启。认证关闭时请求以 `UNAUTHENTICATED` 拒绝; 授权关闭时以 `FAILED_PRECONDITION` 拒绝。这保证 `proxy.admin.*` ACL 被真正强制执行——当 admin 端口无法做网络隔离时使用。

### 11.4 启用 / 禁用接口

接口**默认关闭**（`grpcAdminServerEnable=false`）。要启用：

```properties
grpcAdminServerEnable: true    # 在 grpcAdminServerPort 上启动 admin gRPC server
```

要保持禁用（默认），保留 `grpcAdminServerEnable: false`，或把 `grpcAdminServerPort` 设为 0 / 负数，此时 admin gRPC server 完全不启动。

### 11.5 审计

每个被服务的 admin RPC 都会把 subject（Console 登录用户 / AK）、method、resource、action 和 source IP 记录到鉴权审计 logger; 被拒绝的请求由 ACL 2.0 引擎自身记录。这满足审计四元组要求（Console 用户 + AK + 资源 + 操作）。
