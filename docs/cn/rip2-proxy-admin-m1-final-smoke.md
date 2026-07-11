# RIP-2 Proxy Admin M1 最终冒烟

本冒烟用于验证生成版公开 `apache.rocketmq.v2.ProxyAdminService` 端点。
执行前需要先完成 `rocketmq-apis` 分支构建，并在本机安装
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`。

本次记录验证的 artifact 身份：

```text
rocketmq-apis commit: c372905ce927cf8957333e7ac07877f295fd7ec9
rocketmq-proto jar SHA-256: 7ae515ec32832f31634c47c36291ec4e2451f9cde589e59d956802596b6bad4d
```

## 前置依赖

在 RocketMQ 仓库根目录执行，并确保当前 Java 为 JDK 17：

```bash
test -f pom.xml
test -f ../rocketmq-apis/apache/rocketmq/v2/admin.proto
command -v grpcurl
command -v openssl
command -v xxd
java -version
```

## 构建

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
mvn -Prelease-all -DskipTests -DskipITs package
```

预期：

```text
BUILD SUCCESS
```

提交分支上最新记录的 package smoke：

```text
BUILD SUCCESS
Finished at: 2026-07-12T00:30:07+08:00
```

## 启动公开 admin gRPC server

先创建隔离的 local smoke 配置：

```bash
mkdir -p target
cat > target/rip2-smoke-rmq-proxy.json <<'EOF'
{
  "rocketMQClusterName": "DefaultCluster",
  "namesrvAddr": "127.0.0.1:9876",
  "proxyMode": "local",
  "enableProxyAdminGrpcServer": true,
  "proxyAdminGrpcServerPort": 8082,
  "authenticationEnabled": false,
  "authorizationEnabled": false
}
EOF
```

使用组装后的 distribution 启动 NameServer 和 local-mode Proxy：

```bash
DIST=distribution/target/rocketmq-5.5.0/rocketmq-5.5.0
test -x "$DIST/bin/mqnamesrv"
test -x "$DIST/bin/mqproxy"
nohup sh "$DIST/bin/mqnamesrv" > target/rip2-smoke-namesrv.log 2>&1 &
nohup sh "$DIST/bin/mqproxy" \
  -pc "$(pwd)/target/rip2-smoke-rmq-proxy.json" \
  -pm local -n 127.0.0.1:9876 \
  > target/rip2-smoke-proxy.log 2>&1 &
```

等待 `target/rip2-smoke-proxy.log` 出现启动成功信息，并确认 8082 端口已监听。
公开 admin service 只注册在
admin gRPC server 上，不注册到数据面 `MessagingService` server。admin listener
也不开放 server reflection、Channelz 或实验性 internal peer RPC；`grpcurl`
必须显式加载已 checkout 的 proto。

验证结束后关闭隔离 smoke 进程：

```bash
sh "$DIST/bin/mqshutdown" proxy
sh "$DIST/bin/mqshutdown" namesrv
```

## grpcurl 手工调用

上面的 local smoke 配置关闭 authentication，因此使用空 header 数组，
下面每个命令都可直接执行。在开启 authentication 的部署中，使用已配置的
access key 和 secret key 设置数组。不要把两个值提交到仓库：

```bash
AUTH_ARGS=()
export ACCESS_KEY='<configured-access-key>'
export SECRET_KEY='<configured-secret-key>'
export MQ_DATE_TIME="$(date -u +%Y%m%dT%H%M%SZ)"
export MQ_SIGNATURE="$(printf '%s' "$MQ_DATE_TIME" \
  | openssl dgst -sha1 -hmac "$SECRET_KEY" -binary \
  | xxd -p -c 256)"
AUTH_ARGS=(
  -H "x-mq-date-time: $MQ_DATE_TIME"
  -H "authorization: MQv2-HMAC-SHA1 Credential=$ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=$MQ_SIGNATURE"
)
```

本地 smoke 配置保持 `AUTH_ARGS=()`。只有开启 authentication 时才执行凭据块。

查询客户端列表：

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

查询单个客户端：

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

按 group 查询：

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

按 topic 查询：

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
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

字段级 Dashboard CLIENT-01 交接契约和非空 `DescribeClient` 示例见
`docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`。

## M1 scope 规则

公开 M1 端点只支持省略 scope 或 `PROXY_SCOPE_LOCAL_PROXY`。
`PROXY_SCOPE_ALL_PROXIES` 和 `PROXY_SCOPE_PROXY_ID` 会被拒绝，直到社区确认
多 Proxy discovery、鉴权、超时和分页归属语义。
即使省略 `proxy_id`，`PROXY_SCOPE_PROXY_ID` 也会先按 M1 scope gate 被拒绝，
保证 public caller 在 proxy-id 校验前看到一致的 scope-gate contract。
