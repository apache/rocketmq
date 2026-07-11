# RIP-2 Proxy Admin Online Client Query Guide

## Status

This document describes the RIP-2 M1 admin query behavior implemented in this
branch. The proxy already has the internal read model, authorization facade,
metrics, proto-free admin adapter, generated public `ProxyAdminService`
endpoint, dedicated admin executor, and benchmark coverage. The public endpoint
is built against the local contest artifact
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`, generated from the
companion `rocketmq-apis` proposal branch. Upstream merge remains gated by the
community decision for protobuf ownership, field numbers, generated artifact
version, and publication path.

The final contest submission entry point is
`docs/en/rip2-proxy-admin-m1-submission-package.md`. It contains the requirement
matrix, verification snapshot, PR links, issue comment link, and the explicit
public protobuf ownership gate.

Dashboard CLIENT-01 field-level integration is documented in
`docs/en/rip2-proxy-admin-m1-dashboard-contract.md`.

M1 supports `LOCAL_PROXY` semantics. Cross-proxy fan-out and `PROXY_ID` routing
exist as internal experiments, but the public M1 contract should expose only the
current proxy's online clients.

## Public Service

The public API proposal is a standalone `ProxyAdminService`, separate from the
existing `MessagingService`.

Implemented RPCs:

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

`GrpcProxyAdminApplication` only translates protobuf requests and responses. It
reuses the existing admin endpoint executor/activity/service for validation,
authorization, metrics, error mapping, pagination, and read-model queries.

## Query Fields

Supported query fields in the generated endpoint, internal service, and
proto-free adapter:

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

The current public API proposal preserves the contest fields: client id,
language, version, local address, remote address, and connection time. Final
field numbers and publication location still require community review.

## Error Semantics

The generated public endpoint and proto-free endpoint map failures to RocketMQ
v2 `Status` codes:

| Case | Code |
| --- | --- |
| Invalid argument, invalid page token, invalid scope, unsupported M1 public scope, missing required id | `BAD_REQUEST` |
| Supplied client id is not online or `DescribeClient` has no result | `NOT_FOUND` |
| Authorization failure or missing subject | `UNAUTHORIZED` |
| Peer discovery or peer request timeout | `PROXY_TIMEOUT` |
| Queue saturation in the admin executor | `TOO_MANY_REQUESTS` |
| Unsupported future operation | `NOT_IMPLEMENTED` |
| Unexpected runtime failure | `INTERNAL_SERVER_ERROR` |

## ACL

The logical ACL resource is `proxy.admin.client`.

- `ListClients`, `ListClientsByGroup`, and `ListClientsByTopic` require `LIST`.
- `DescribeClient` requires `GET`.

The gRPC request pipeline propagates the authenticated subject into
`ProxyContext`. A client-supplied subject header is ignored by the public admin
pipeline unless authentication succeeds. Transport-derived addresses replace
client metadata, and all client-supplied `proxy_protocol_*` headers are cleared
before trusted transport attributes are copied, ahead of source-IP
authorization. The admin endpoint
must not log the subject value in plain text.

## Execution And Observability

The public admin gRPC service runs on the admin-only server when
`enableProxyAdminGrpcServer=true`. The generated service is registered through
`ProxyStartup.createProxyAdminGrpcBindableServices(...)`, separate from the
data-plane `MessagingService` server. Defaults:

- `enableProxyAdminGrpcServer = false`
- `proxyAdminGrpcServerPort = 8082`
- `proxyAdminGrpcThreadPoolNums = 4`
- `proxyAdminGrpcThreadPoolQueueCapacity = 10000`
- server thread name prefix `ProxyAdminGrpcRequestExecutorThread`

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
  "-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
  -DfailIfNoTests=false test -DskipITs
```

Use the JMH commands in `docs/en/rip2-proxy-admin-m1-design.md` for local
1M-client benchmark runs. A completed 1M local run is recorded in
`docs/en/rip2-proxy-admin-m1-benchmark-report.md`.

## Known M1 Limits

- The generated public gRPC endpoint is present in this contest branch, but it
  depends on the local `rocketmq-proto:2.2.0-rip2-SNAPSHOT` artifact until the
  `rocketmq-apis` proposal is accepted and published.
- Public M1 scope is `LOCAL_PROXY`; cross-proxy query remains internal
  exploratory work.
- The read model is in-memory and process-local. It is rebuilt from client
  lifecycle events after proxy restart.
- `pageSize` is capped at `100`; callers should request subsequent pages with
  `pageNum`.
