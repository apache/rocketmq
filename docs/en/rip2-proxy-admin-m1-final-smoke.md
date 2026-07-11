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

## Build

```bash
mvn -pl proxy -am -DskipTests package -DskipITs
```

Expected:

```text
BUILD SUCCESS
```

Latest recorded package smoke on the submission branch:

```text
BUILD SUCCESS
Finished at: 2026-07-12T00:30:07+08:00
```

## Start Proxy With Public Admin Server

Use a normal local or cluster Proxy config and set:

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

Then start Proxy with the same command used by the existing RocketMQ
deployment. The public admin service is registered only on the admin gRPC
server, not on the data-plane `MessagingService` server. The admin listener
intentionally exposes neither server reflection, Channelz, nor the experimental
internal peer RPC. Load the checked-out proto explicitly with `grpcurl`.

## Manual grpcurl Calls

When authentication is enabled, prepare request metadata from a configured
access key and secret key. Do not commit either value:

```bash
export ACCESS_KEY='<configured-access-key>'
export SECRET_KEY='<configured-secret-key>'
export MQ_DATE_TIME="$(date -u +%Y%m%dT%H%M%SZ)"
export MQ_SIGNATURE="$(printf '%s' "$MQ_DATE_TIME" \
  | openssl dgst -sha1 -hmac "$SECRET_KEY" -binary \
  | xxd -p -c 256)"
```

The commands below include authenticated headers. When both authentication and
authorization are disabled for an isolated local smoke, omit the two `-H`
arguments.

List clients:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -H "x-mq-date-time: $MQ_DATE_TIME" \
  -H "authorization: MQv2-HMAC-SHA1 Credential=$ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=$MQ_SIGNATURE" \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Describe one client:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -H "x-mq-date-time: $MQ_DATE_TIME" \
  -H "authorization: MQv2-HMAC-SHA1 Credential=$ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=$MQ_SIGNATURE" \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

List clients by group:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -H "x-mq-date-time: $MQ_DATE_TIME" \
  -H "authorization: MQv2-HMAC-SHA1 Credential=$ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=$MQ_SIGNATURE" \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

List clients by topic:

```bash
grpcurl -plaintext \
  -import-path ../rocketmq-apis \
  -proto apache/rocketmq/v2/admin.proto \
  -H "x-mq-date-time: $MQ_DATE_TIME" \
  -H "authorization: MQv2-HMAC-SHA1 Credential=$ACCESS_KEY, SignedHeaders=x-mq-date-time, Signature=$MQ_SIGNATURE" \
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
