# RIP-2 Proxy Admin M1 Final Smoke

This smoke verifies the generated public `apache.rocketmq.v2.ProxyAdminService`
endpoint after the `rocketmq-apis` branch has been built and
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` has been installed
locally.

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
Finished at: 2026-07-10T04:07:14+08:00
```

## Start Proxy With Public Admin Server

Use a normal local or cluster Proxy config and set:

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

Then start Proxy with the same command used by the existing RocketMQ
deployment. The public admin service is registered only on the admin gRPC
server, not on the data-plane `MessagingService` server.

## Manual grpcurl Calls

List clients:

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Describe one client:

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-a"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

List clients by group:

```bash
grpcurl -plaintext \
  -d '{"group":"group-a","page_num":1,"page_size":10}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClientsByGroup
```

List clients by topic:

```bash
grpcurl -plaintext \
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
