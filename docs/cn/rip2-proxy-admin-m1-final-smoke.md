# RIP-2 Proxy Admin M1 最终冒烟

本冒烟用于验证生成版公开 `apache.rocketmq.v2.ProxyAdminService` 端点。
执行前需要先完成 `rocketmq-apis` 分支构建，并在本机安装
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`。

## 构建

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

预期：

```text
BUILD SUCCESS
```

提交分支上最新记录的 package smoke：

```text
BUILD SUCCESS
Finished at: 2026-07-10T03:02:58+08:00
```

## 启动公开 admin gRPC server

使用常规 local 或 cluster Proxy 配置，并设置：

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

然后按现有 RocketMQ 部署方式启动 Proxy。公开 admin service 只注册在
admin gRPC server 上，不注册到数据面 `MessagingService` server。

## grpcurl 手工调用

查询客户端列表：

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

查询单个客户端：

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

按 group 查询：

```bash
grpcurl -plaintext \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

按 topic 查询：

```bash
grpcurl -plaintext \
  -d '{"topic":"topic-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByTopic
```

成功响应形态：

```json
{
  "status": {
    "code": "OK"
  },
  "clients": [],
  "hasMore": false
}
```

`client-a`、`group-a`、`topic-a` 是样例值。手工冒烟时使用目标集群中真实在线的
ID。如果当前没有在线 gRPC 客户端，`clients` 可以为空。单元测试和集成测试会注入
synthetic clients 来验证非空响应。

## M1 scope 规则

公开 M1 端点只支持省略 scope 或 `PROXY_SCOPE_LOCAL_PROXY`。
`PROXY_SCOPE_ALL_PROXIES` 和 `PROXY_SCOPE_PROXY_ID` 会被拒绝，直到社区确认
多 Proxy discovery、鉴权、超时和分页归属语义。
