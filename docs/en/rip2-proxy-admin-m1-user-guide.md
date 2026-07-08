# RIP-2 Proxy Admin Online Client Query Guide

## Status

This document describes the RIP-2 M1 admin query behavior implemented in this
branch. The proxy already has the internal read model, authorization facade,
metrics, proto-free admin adapter, dedicated admin executor, and benchmark
coverage. The public `ProxyAdminService` protobuf endpoint is intentionally not
registered in this fork until the `rocketmq-apis` ownership and compatibility
decision is made.

M1 supports `LOCAL_PROXY` semantics. Cross-proxy fan-out and `PROXY_ID` routing
exist as internal experiments, but the public M1 contract should expose only the
current proxy's online clients.

## Proposed Public Service

The recommended public API is a standalone `ProxyAdminService`, separate from
the existing `MessagingService`.

Planned RPCs:

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

The endpoint adapter should only translate protobuf requests and responses. It
should reuse the existing admin activity/service for validation, authorization,
metrics, error mapping, pagination, and read-model queries.

## Query Fields

Supported query fields in the internal service and proto-free adapter:

| Field | Meaning |
| --- | --- |
| `scope` | M1 public API accepts `LOCAL_PROXY` only. |
| `clientId` | Exact client id lookup for `DescribeClient` or filtering. |
| `clientIdPrefix` | Stable ordered prefix scan over client ids. |
| `group` | Consumer group index filter. |
| `topic` | Topic index filter. |
| `clientLanguage` | Client language filter, for example `JAVA`. |
| `connectTimeStartMillis` | Inclusive lower bound for connection time. |
| `connectTimeEndMillis` | Inclusive upper bound for connection time. |
| `pageNum` | One-based public page number, must be `>= 1`. |
| `pageSize` | Public page size, capped at `100`. |

The internal read model still supports an opaque page token path for local and
coordinator experiments. Public protobuf requests should prefer
`pageNum/pageSize`; any future public token must be treated as opaque.

## Response Fields

Client responses expose:

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

The public proto can choose final field names, but it should preserve the contest
fields: client id, language, version, local address, remote address, and
connection time.

## Error Semantics

The proto-free endpoint maps failures to RocketMQ v2 `Status` codes:

| Case | Code |
| --- | --- |
| Invalid argument, invalid page token, invalid scope, missing required id | `BAD_REQUEST` |
| Missing client id or empty result for `DescribeClient` | `NOT_FOUND` |
| Authorization failure or missing subject | `UNAUTHORIZED` |
| Peer discovery or peer request timeout | `PROXY_TIMEOUT` |
| Queue saturation in the admin executor | `TOO_MANY_REQUESTS` |
| Unsupported public scope or unsupported future operation | `NOT_IMPLEMENTED` |
| Unexpected runtime failure | `INTERNAL_SERVER_ERROR` |

## ACL

The logical ACL resource is `proxy.admin.client`.

- `ListClients`, `ListClientsByGroup`, and `ListClientsByTopic` require `LIST`.
- `DescribeClient` requires `GET`.

The gRPC request pipeline propagates the authenticated subject into
`ProxyContext`. The admin endpoint must use that subject for authorization and
must not log the subject value in plain text.

## Execution And Observability

Admin queries run on a dedicated executor so they do not block the messaging
write path. Defaults:

- `proxyClientAdminQueryThreadPoolNums = 4`
- `proxyClientAdminQueryThreadPoolQueueCapacity = 10000`
- thread name prefix `ProxyClientAdminQueryThread_`

Metrics and trace/log fields are low cardinality:

- operation
- result code
- scope
- status
- page size
- filter presence
- result size
- duration

Failure logs include operation, status, result, scope, filters, page size, and
result size. They deliberately omit client ids, group names, topic names, proxy
ids, and auth subjects.

## Verification

Run the focused benchmark and documentation checkpoint tests:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  -Dtest=ProxyClientReadServiceBenchmarkTest,ProxyClientAdminCoordinatorServiceBenchmarkTest \
  -DfailIfNoTests=false test -DskipITs
```

Run the current broader pre-submit proxy suite:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  -Dtest=ProxyClientAdmin*Test,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest \
  -DfailIfNoTests=false test -DskipITs
```

Use the JMH commands in `docs/en/rip2-proxy-admin-m1-design.md` for local
1M-client benchmark runs.

## Known M1 Limits

- No public generated gRPC endpoint is registered until `rocketmq-apis`
  ownership is confirmed.
- Public M1 scope is `LOCAL_PROXY`; cross-proxy query remains internal
  exploratory work.
- The read model is in-memory and process-local. It is rebuilt from client
  lifecycle events after proxy restart.
- `pageSize` is capped at `100`; callers should request subsequent pages with
  `pageNum`.
