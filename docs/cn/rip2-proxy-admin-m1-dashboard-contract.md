# RIP-2 Proxy Admin M1 Dashboard 对接契约

本文是 RIP-1 Dashboard CLIENT-01 联调交接契约，说明 Dashboard 可以从 RIP-2
M1 分支的生成版公开 `apache.rocketmq.v2.ProxyAdminService` endpoint 消费哪些
字段。

当前分支已经通过
`GrpcProxyAdminApplicationTest#describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService`
用真实生成版 gRPC server/channel 验证这些字段。剩余门禁是外部项：需要在包含
RIP-1 Dashboard client 的环境中完成联合 E2E。

## Endpoint

启动 Proxy 时开启公开 admin server：

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

Dashboard client 应调用 admin gRPC server，不要调用数据面的
`MessagingService` server。

M1 public scope 是本进程 local view：

- 省略 `scope` 或使用 `PROXY_SCOPE_LOCAL_PROXY` 会被接受。
- `PROXY_SCOPE_ALL_PROXIES` 和 `PROXY_SCOPE_PROXY_ID` 会被拒绝，直到社区确认
  multi-proxy discovery、authorization、timeout 和 pagination ownership 语义。

## 推荐 Dashboard 调用流

表格页使用 `ListClients`：

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":100}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Dashboard 搜索可以使用以下过滤字段：

- `client_id`：精确 client 查询。
- `client_id_prefix`：client id 前缀搜索。
- `group`：consumer group 维度过滤。
- `topic`：topic 维度过滤。
- `client_language`：client language 过滤。
- `connect_time_start_millis` 和 `connect_time_end_millis`：连接时间窗口。

点击表格行进入详情时使用 `DescribeClient`：

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-dashboard"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

从 group 或 topic 页面跳转时，可以使用 `ListClientsByGroup` 和
`ListClientsByTopic` 快捷查询。

## 字段映射

| Dashboard 字段 | Proto 字段 | 含义 |
| --- | --- | --- |
| Client ID | `ProxyClient.client_id` | 稳定 client identity，用于表格 row key 和 `DescribeClient`。 |
| Client type | `ProxyClient.client_type` | RocketMQ client 角色，例如 `PRODUCER` 或 `PUSH_CONSUMER`。 |
| Groups | `ProxyClient.groups` | client 关联的 group；列表会排序，便于稳定展示。 |
| Topics | `ProxyClient.topics` | client 关联的 topic；列表会排序，便于稳定展示。 |
| Language | `ProxyClient.language` | telemetry metadata 中的 client language，例如 `JAVA`。 |
| Remote address | `ProxyClient.remote_address` | Proxy 观察到的 client 侧远端 socket 地址。 |
| Local address | `ProxyClient.local_address` | 该连接在 Proxy 侧的本地 socket 地址。 |
| Version | `ProxyClient.version` | telemetry metadata 中的 client version。 |
| Connect time | `ProxyClient.connect_time_millis` | Proxy 首次观察到该 client connection 的 epoch millis。 |
| Last active time | `ProxyClient.last_active_time_millis` | 最近 heartbeat 或 telemetry activity 的 epoch millis。 |
| Proxy ID | `ProxyClient.proxy_id` | 持有该 local view 的 Proxy id。 |
| Pagination | `has_more` 加 `page_num` / `page_size` | public list response 使用 page-number pagination，`page_size` 上限为 100。 |
| Status | `status.code`、`status.message` | `OK`、`BAD_REQUEST`、`NOT_FOUND`、`UNAUTHORIZED` 或 `INTERNAL_SERVER_ERROR`。 |

稀疏 telemetry metadata 会以 protobuf 默认值返回：字符串为空、列表为空、enum
可能为 `CLIENT_TYPE_UNSPECIFIED`，read model 没有值时 timestamp 字段为 `0`。

## 详情响应示例

生成版 public gRPC 测试会注入以下 Dashboard-facing client，并通过真实生成版
gRPC server/channel 断言每个字段：

```json
{
  "status": {
    "code": "OK"
  },
  "client": {
    "clientId": "client-dashboard",
    "clientType": "PUSH_CONSUMER",
    "groups": [
      "dashboard-group"
    ],
    "topics": [
      "dashboard-topic"
    ],
    "language": "JAVA",
    "remoteAddress": "127.0.0.1:8080",
    "localAddress": "192.168.0.1:8080",
    "version": "V5_0_0",
    "connectTimeMillis": "100",
    "lastActiveTimeMillis": "200",
    "proxyId": "proxy-a"
  }
}
```

## Dashboard 验收清单

外部 RIP-1 Dashboard CLIENT-01 E2E 需要验证：

- Dashboard 可以连接独立 admin gRPC server 的配置端口。
- `ListClients` 至少能展示 client id、client type、groups、topics、language、
  remote/local address、version、connect time、last active time 和 proxy id。
- `DescribeClient` 可以打开选中 `client_id` 的详情视图。
- group 和 topic 过滤可以调用 `ListClientsByGroup` / `ListClientsByTopic`，
  或调用带对应过滤字段的 `ListClients`。
- 分页使用 `page_num >= 1` 且 `1 <= page_size <= 100`。
- 当前没有在线 gRPC client 时，空 list response 是合法状态，Dashboard 需要正常展示。
- `BAD_REQUEST`、`NOT_FOUND`、`UNAUTHORIZED` status 要正常展示，不能假设错误响应一定携带结果体。
- M1 public UI 不应暴露 `PROXY_SCOPE_ALL_PROXIES` 和 `PROXY_SCOPE_PROXY_ID`
  控件。
