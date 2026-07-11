# RIP-2 Proxy Admin M1 Final Smoke

This smoke verifies the generated public `apache.rocketmq.v2.ProxyAdminService`
endpoint after the `rocketmq-apis` branch has been built and
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` has been installed
locally.

Artifact identity for the recorded verification:

```text
rocketmq-apis commit: c372905ce927cf8957333e7ac07877f295fd7ec9
rocketmq-proto jar SHA-256: 7ae515ec32832f31634c47c36291ec4e2451f9cde589e59d956802596b6bad4d
```

## Prerequisites

Run from the RocketMQ repository root with JDK 17 active:

```bash
test -f pom.xml
test -f ../rocketmq-apis/apache/rocketmq/v2/admin.proto
command -v grpcurl
command -v openssl
command -v xxd
java -version
```

## Build

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
mvn -Prelease-all -DskipTests -DskipITs package
```

Expected:

```text
BUILD SUCCESS
```

Latest recorded package smoke on the submission branch:

```text
BUILD SUCCESS
Finished at: 2026-07-12T02:36:24+08:00
```

## Start Proxy With Public Admin Server

Create an isolated local smoke config:

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

Start NameServer and the local-mode Proxy from the assembled distribution:

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

Wait until `target/rip2-smoke-proxy.log` reports startup success and port 8082
is accepting connections. The public admin service is registered only on the admin gRPC
server, not on the data-plane `MessagingService` server. The admin listener
intentionally exposes neither server reflection, Channelz, nor the experimental
internal peer RPC. Load the checked-out proto explicitly with `grpcurl`.

Stop the isolated smoke processes after verification:

```bash
sh "$DIST/bin/mqshutdown" proxy
sh "$DIST/bin/mqshutdown" namesrv
```

## Manual grpcurl Calls

The local smoke config above disables authentication and uses an empty header
array, so every command below is directly executable. For an authenticated
deployment, set the array from a configured access key and secret key. Do not
commit either value:

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

Keep `AUTH_ARGS=()` for the local smoke configuration. Run the credential block
only when authentication is enabled.

List clients:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Describe one client:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

List clients by group:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

List clients by topic:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  "${AUTH_ARGS[@]}" \
  -d '{"topic":"topic-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByTopic
```

Expected successful response shape:

```json
{
  "status": {
    "code": "OK"
  },
  "clients": [],
  "hasMore": false
}
```

`client-a`, `group-a`, and `topic-a` are sample values. Use IDs that are online
in the target cluster during manual smoke. `clients` may be empty when no gRPC
clients are online. The unit and integration tests seed synthetic clients to
validate non-empty responses.

For the field-level Dashboard CLIENT-01 handoff contract and a non-empty
`DescribeClient` example, see
`docs/en/rip2-proxy-admin-m1-dashboard-contract.md`.

## M1 Scope Rule

The public M1 endpoint supports omitted scope and
`PROXY_SCOPE_LOCAL_PROXY`. It rejects `PROXY_SCOPE_ALL_PROXIES` and
`PROXY_SCOPE_PROXY_ID` until the community finalizes multi-proxy discovery,
authorization, timeout, and page ownership semantics.
`PROXY_SCOPE_PROXY_ID` is rejected as an M1 scope error even when `proxy_id` is
omitted, so public callers see the same scope-gate contract before proxy-id
validation.
