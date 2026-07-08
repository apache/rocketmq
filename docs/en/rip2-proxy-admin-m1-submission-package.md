# RIP-2 Proxy Admin Submission Package

## Status

This package summarizes the `rip2-proxy-admin-m1` branch for the RIP-2 Proxy
Admin online client query contest task. It is meant to be copied into the final
PR, issue comment, or contest submission.

Latest verified code checkpoint before this submission-docs update:

```text
54263a5c0da96a95068c0aa47a54eda7b749cbcd Improve RIP-2 proxy admin test coverage
```

The branch implements the proxy-side foundation for `ProxyAdminService`
online-client queries: read model, lifecycle writes, service layer, validation,
authorization, endpoint-ready adapter, admin executor, observability, internal
cross-proxy exploration, docs, and 1M synthetic client benchmark evidence.

The only hard external gate is the public protobuf ownership decision. This
RocketMQ repository currently consumes generated `rocketmq-proto` classes and
does not contain the public `.proto` source for a standalone
`ProxyAdminService`. Therefore this fork intentionally keeps the public API as
a documentation draft and does not pretend that the generated public endpoint is
already registered.

## Requirement Matrix

| Contest requirement | Branch status | Evidence |
| --- | --- | --- |
| Public admin service with `ListClients`, `DescribeClient`, `ListClientsByGroup`, and `ListClientsByTopic` | Endpoint-ready internal adapter is implemented; generated public `ProxyAdminServiceGrpc` is blocked by `rocketmq-apis` ownership. | `docs/en/rip2-proxy-admin-m1-public-api-draft.proto`, `docs/en/rip2-proxy-admin-public-api-discussion.md`, `proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/admin`. |
| Online client read model | Implemented with `clientId`, group, topic, client type, language, connect time, and proxy id indexes. | `ProxyClientReadService`, `ProxyClientInfo`, `ProxyClientQuery`, `ProxyClientPage`. |
| Official filters | Implemented: exact client id, client id prefix, group, topic, client language, connect time range, `pageNum >= 1`, `pageSize <= 100`. | `ProxyClientQueryTest`, `ProxyClientReadServiceTest`, `ProxyClientAdminRequestConverterTest`. |
| Lifecycle population | Implemented for telemetry settings, heartbeat, unregister, termination, stream completion, and error cleanup. | `ClientActivityTest`, `DefaultGrpcMessagingActivityTest`. |
| ACL | Implemented with logical resource `proxy.admin.client`; list RPCs use `LIST`, describe uses `GET`. | `ClientAdminAuthPolicyTest`, `DefaultClientAdminAuthorizationServiceTest`, `AuthorizingClientAdminServiceTest`. |
| Independent admin execution | Implemented admin query executor and startup seam for a dedicated admin gRPC server. | `ProxyClientAdminEndpointExecutor`, `ProxyStartup`, `GrpcProxyAdminWiringTest`, `ProxyStartupTest`. |
| Observability | Implemented metrics, trace attributes, and structured failure logs with low-cardinality labels. | `ProxyMetricsManagerTest`, `MeteredClientAdminServiceTest`, `MeteredAuthorizingClientAdminServiceTest`, `ProxyClientAdminObservabilityTest`. |
| E2E / integration coverage | Public generated gRPC E2E is blocked by protobuf ownership; proto-free in-process endpoint integration and peer tests cover the same service path. | `ProxyClientAdminEndpointIntegrationTest`, `ProxyClientAdminInProcessPeerMessageTransportTest`, `ProxyClientAdminPeerGrpcServiceTest`. |
| 1M benchmark | Completed on local Apple M4, 16 GB, JDK 17. All local read-model P99 values are below 1 second. | `docs/en/rip2-proxy-admin-m1-benchmark-report.md`. |
| English and Chinese docs | Completed for user guide, benchmark report, and submission package. | `docs/en/rip2-proxy-admin-m1-user-guide.md`, `docs/cn/rip2-proxy-admin-m1-user-guide.md`. |

## API Summary

Recommended public service:

```proto
service ProxyAdminService {
  rpc ListClients(ListClientsRequest) returns (ListClientsResponse);
  rpc DescribeClient(DescribeClientRequest) returns (DescribeClientResponse);
  rpc ListClientsByGroup(ListClientsByGroupRequest)
      returns (ListClientsByGroupResponse);
  rpc ListClientsByTopic(ListClientsByTopicRequest)
      returns (ListClientsByTopicResponse);
}
```

M1 public scope should be `LOCAL_PROXY` only. `ALL_PROXIES` and `PROXY_ID` are
implemented as internal experiments and should stay gated until the community
accepts peer discovery, timeout, authorization, and page-token ownership
semantics.

Public request fields should include:

- `clientId`
- `clientIdPrefix`
- `group`
- `topic`
- `clientLanguage`
- `connectTimeStartMillis`
- `connectTimeEndMillis`
- `pageNum`
- `pageSize`
- `scope`

`pageNum` is 1-based. `pageSize` is capped at 100. Public page tokens, if added
later, must be treated as opaque; the current token implementation is internal.

## Configuration Summary

Public admin server gate and executor defaults:

| Config | Default | Meaning |
| --- | ---: | --- |
| `enableProxyAdminGrpcServer` | `false` | Keep the future public admin gRPC server opt-in for compatibility. |
| `proxyAdminGrpcServerPort` | `8082` | Dedicated admin gRPC port. |
| `proxyAdminGrpcThreadPoolNums` | `4` | Dedicated admin server request executor size. |
| `proxyAdminGrpcThreadPoolQueueCapacity` | `10000` | Dedicated admin server request queue capacity. |
| `proxyClientAdminQueryThreadPoolNums` | `4` | Internal admin query executor size. |
| `proxyClientAdminQueryThreadPoolQueueCapacity` | `10000` | Internal admin query queue capacity. |
| `enableProxyClientAdminCrossProxyQuery` | `false` | Gate internal `ALL_PROXIES` and `PROXY_ID` experiments. |
| `proxyClientAdminPeerRequestTimeoutMillis` | `2000` | Bounded peer discovery/request wait. |
| `proxyClientAdminCoordinatorPageTokenTtlMillis` | `300000` | Coordinator token retention for internal fan-out. |
| `proxyClientAdminPeerGrpcTargets` | empty | Static peer targets for internal cross-proxy tests. |

## ACL Summary

Logical resource:

```text
proxy.admin.client
```

RocketMQ ACL resource encoding:

```text
Admin:proxy.admin.client
```

Suggested policy shape:

```yaml
subject: User:admin-dashboard
resources:
  - resource: Admin:proxy.admin.client
    actions:
      - LIST
      - GET
```

`LIST` is required for `ListClients`, `ListClientsByGroup`, and
`ListClientsByTopic`. `GET` is required for `DescribeClient`.

## Observability Summary

Admin metrics:

- `rocketmq_proxy_client_admin_requests_total`
- `rocketmq_proxy_client_admin_request_latency`
- `rocketmq_proxy_client_read_model_operations_total`
- `rocketmq_proxy_client_total`
- `rocketmq_proxy_client_type_total`
- `rocketmq_proxy_client_index_total`

Admin labels and trace/log attributes:

- `operation`
- `result`
- `scope`
- `status`
- `filters`
- `page_size`
- `result_size`

Failure logs intentionally omit raw auth subjects, client ids, group names,
topic names, and proxy ids.

## Verification Snapshot

Latest broad proxy verification:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
mvn -pl proxy -am \
"-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result on 2026-07-08 Asia/Shanghai:

```text
Tests run: 700, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Package-level JaCoCo coverage from that run:

| Package | Instruction | Branch | Line |
| --- | ---: | ---: | ---: |
| `org/apache/rocketmq/proxy/service/admin/client` | 93.82% | 86.83% | 95.48% |
| `org/apache/rocketmq/proxy/grpc/v2/admin` | 92.66% | 85.05% | 94.52% |

JDK 17 prints JaCoCo 0.8.5 instrumentation stack traces for some JDK and
Mockito-generated classes in this repository. They are treated as environment
noise only when Surefire reports zero failures/errors and Maven exits
successfully.

Benchmark evidence:

- Local read-model worst P99: `listByConnectTimeRangePage` at 344.793 ms.
- Coordinator experiment worst P99: `listAllProxiesNextPage` at 9.011 ms.
- Full report: `docs/en/rip2-proxy-admin-m1-benchmark-report.md`.

## PR Description Draft

Title:

```text
[RIP-2] Add proxy admin online client query foundation
```

Body:

````markdown
## Summary

This PR implements the proxy-side RIP-2 online client query foundation for a
future standalone `ProxyAdminService`.

Implemented:

- process-local online client read model populated from gRPC client lifecycle
  events.
- query semantics for client id, client id prefix, group, topic, language,
  connect time range, `pageNum`, and `pageSize <= 100`.
- internal `ClientAdminService` and proto-free `ProxyClientAdminActivity`.
- endpoint-ready executor/handler with `LOCAL_PROXY` public-scope validation.
- ACL facade using `Admin:proxy.admin.client` with LIST/GET actions.
- dedicated admin query executor and startup seam for a future independent
  admin gRPC server.
- low-cardinality metrics, trace attributes, and sanitized failure logs.
- internal cross-proxy coordinator and peer transport experiments behind config.
- English/Chinese docs and 1M synthetic client benchmark report.

Public generated `ProxyAdminServiceGrpc` registration is intentionally not part
of this branch because this repository currently consumes generated
`rocketmq-proto` classes and the public API ownership/field-number decision
needs community confirmation first. The proposed API is documented in
`docs/en/rip2-proxy-admin-m1-public-api-draft.proto` and
`docs/en/rip2-proxy-admin-public-api-discussion.md`.

## Tests

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
mvn -pl proxy -am \
"-Dtest=ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result: `Tests run: 700, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`.

## Benchmark

1M synthetic client JMH on Apple M4, 16 GB, Temurin JDK 17.0.18:

- local read-model worst P99: 344.793 ms.
- internal coordinator experiment worst P99: 9.011 ms.

See `docs/en/rip2-proxy-admin-m1-benchmark-report.md`.
````

## Issue Comment Draft

```markdown
I have prepared the RIP-2 Proxy Admin online client query implementation branch
at `pilichoumao/rocketmq:rip2-proxy-admin-m1`.

The branch is ready for community review of the proxy-side foundation:

- local online-client read model and lifecycle writes.
- `ListClients`, `DescribeClient`, `ListClientsByGroup`, and
  `ListClientsByTopic` semantics through an endpoint-ready internal adapter.
- official filters and `pageSize <= 100`.
- independent ACL resource `Admin:proxy.admin.client`.
- dedicated admin query executor and admin server startup seam.
- metrics/tracing/logging coverage.
- in-process endpoint/peer integration tests and 1M synthetic benchmark report.
- English and Chinese user docs.

The remaining question is public API ownership: should the standalone
`ProxyAdminService` proto land in `rocketmq-apis`, and are the service name,
field names, and field numbers in
`docs/en/rip2-proxy-admin-m1-public-api-draft.proto` acceptable? Until that is
confirmed, the branch does not register generated public `ProxyAdminServiceGrpc`
stubs.
```

## Final Checklist

- Keep `origin` as `https://github.com/pilichoumao/rocketmq.git`.
- Do not push to `apache/rocketmq`.
- Re-run the broad proxy verification after any code change.
- Re-run `git diff --check` before final commit.
- Use the proxy push command if a normal push fails:

```bash
HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890 \
git push origin rip2-proxy-admin-m1
```
