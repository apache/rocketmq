# RIP-2 Proxy Admin Submission Package

## Status

This package summarizes the `rip2-proxy-admin-m1` branch for the RIP-2 Proxy
Admin online client query contest task. It is meant to be copied into the final
PR, issue comment, or contest submission.

The latest synchronized RocketMQ implementation-code checkpoint is:

```text
8ae372157ba149efa3803854aee7fa641a32b623 Optimize wide connect time queries
```

The latest generated public gRPC endpoint 1M benchmark code checkpoint is:

```text
8ae372157ba149efa3803854aee7fa641a32b623 Optimize wide connect time queries
```

The live draft PRs and RIP-2 issue summary have been refreshed through that
implementation-code checkpoint to include the generated public gRPC endpoint
1M benchmark, the `767`-test broad verification result, Dashboard table/detail
field evidence, generated service descriptor verification evidence, and public
`BAD_REQUEST` / `UNAUTHORIZED` / `NOT_FOUND` / `INTERNAL_SERVER_ERROR`
status-mapping and response-body contract evidence.
Later documentation-only evidence refresh commits may move the branch head
without changing the implementation checkpoint above.

The branch implements the proxy-side foundation and generated public endpoint
for `ProxyAdminService` online-client queries: read model, lifecycle writes,
service layer, validation, authorization, generated gRPC adapter, dedicated
admin server wiring, admin executor, observability, internal cross-proxy
exploration, docs, and 1M synthetic client benchmark evidence.

This branch now contains the generated public `ProxyAdminService` endpoint
wiring. The authoritative protobuf source is published in
[pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api](https://github.com/pilichoumao/rocketmq-apis/tree/rip2-proxy-admin-public-api)
at commit `c372905ce927cf8957333e7ac07877f295fd7ec9`. For contest
verification, the generated Java artifact is installed locally as
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`.
The installed jar used by the recorded verification has
`rocketmq-proto jar SHA-256: 7ae515ec32832f31634c47c36291ec4e2451f9cde589e59d956802596b6bad4d`.

The public API proposal is also opened as draft PR
[apache/rocketmq-apis#112](https://github.com/apache/rocketmq-apis/pull/112).
The RocketMQ implementation is opened as draft PR
[apache/rocketmq#10603](https://github.com/apache/rocketmq/pull/10603) for
proxy-side review. It remains downstream of the API decision because this
branch currently compiles against the local
`rocketmq-proto:2.2.0-rip2-SNAPSHOT` artifact generated from the proposal.

## Requirement Matrix

| Contest requirement | Branch status | Evidence |
| --- | --- | --- |
| Public admin service with `ListClients`, `DescribeClient`, `ListClientsByGroup`, and `ListClientsByTopic` | Exposed through generated `ProxyAdminServiceGrpc` and registered on the independent admin gRPC server when `enableProxyAdminGrpcServer=true`. | `GrpcProxyAdminApplication`, `ProxyStartup`, `GrpcProxyAdminApplicationTest`, [rocketmq-apis/admin.proto](https://github.com/pilichoumao/rocketmq-apis/blob/rip2-proxy-admin-public-api/apache/rocketmq/v2/admin.proto). |
| Online client read model | Implemented with `clientId`, group, topic, client type, language, connect time, and proxy id indexes. | `ProxyClientReadService`, `ProxyClientInfo`, `ProxyClientQuery`, `ProxyClientPage`. |
| Official filters | Implemented: exact client id, client id prefix, group, topic, client language, connect time range, `pageNum >= 1`, omitted `pageSize` defaults, negative `pageSize` rejection, and `pageSize <= 100`. | `GrpcProxyAdminApplicationTest`, `ProxyClientQueryTest`, `ProxyClientReadServiceTest`, `ProxyClientAdminRequestConverterTest`. |
| Lifecycle population | Implemented for telemetry settings, heartbeat, unregister, termination, stream completion, and error cleanup. | `ClientActivityTest`, `DefaultGrpcMessagingActivityTest`. |
| ACL | Implemented with logical resource `proxy.admin.client`; list RPCs use `LIST`, describe uses `GET`. | `ClientAdminAuthPolicyTest`, `DefaultClientAdminAuthorizationServiceTest`, `AuthorizingClientAdminServiceTest`. |
| Independent admin execution | Implemented admin query executor and dedicated admin gRPC server registration. | `ProxyClientAdminEndpointExecutor`, `ProxyStartup`, `GrpcProxyAdminWiringTest`, `ProxyStartupTest`. |
| Observability | Implemented metrics, trace attributes, and structured failure logs with low-cardinality labels. Public failures rejected before service invocation are metered once at the endpoint executor; delegated requests remain service-metered to avoid double counting. | `ProxyMetricsManagerTest`, `MeteredClientAdminServiceTest`, `MeteredAuthorizingClientAdminServiceTest`, `ProxyClientAdminObservabilityTest`, `ProxyClientAdminEndpointExecutorTest`, `GrpcProxyAdminWiringTest`. |
| E2E / integration coverage | Generated public gRPC Server/Channel tests cover the public service descriptor, all four RPCs, official filters, public pagination/hasMore, omitted public pagination defaults, non-local scope rejection across every public RPC, `PROXY_SCOPE_PROXY_ID` rejection before proxy-id validation, `BAD_REQUEST` and `UNAUTHORIZED` status-only response body contracts, `NOT_FOUND` status/message without a client result body, public `INTERNAL_SERVER_ERROR` response mapping, and Dashboard-facing `ListClients` table plus `DescribeClient` detail fields. Proto-free endpoint and peer tests continue to cover internal paths. | `GrpcProxyAdminApplicationTest`, `ProxyClientAdminEndpointIntegrationTest`, `ProxyClientAdminInProcessPeerMessageTransportTest`, `ProxyClientAdminPeerGrpcServiceTest`, `docs/en/rip2-proxy-admin-m1-dashboard-contract.md`. |
| 1M benchmark | Completed on local Apple M4, 16 GB, JDK 17. The 4 GiB fixed heap worst-case suite covers broad prefix, combined filters, deep page 10000, and generated public gRPC with production-shaped executors; all P99 values are below 1 second, max RSS is 1283.0 MiB, and every process completed with zero swaps and no OOM. | `docs/en/rip2-proxy-admin-m1-benchmark-report.md`. |
| English and Chinese docs | Completed for user guide, Dashboard integration contract, public API discussion, benchmark report, smoke guide, review runbook, acceptance audit, and submission package. | `docs/en/rip2-proxy-admin-m1-user-guide.md`, `docs/cn/rip2-proxy-admin-m1-user-guide.md`, `docs/en/rip2-proxy-admin-m1-dashboard-contract.md`, `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`, `docs/en/rip2-proxy-admin-public-api-discussion.md`, `docs/cn/rip2-proxy-admin-public-api-discussion.md`. |

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
| `enableProxyAdminGrpcServer` | `false` | Keep the public admin gRPC server opt-in for compatibility. |
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

The endpoint executor records request-adapter/context validation failures and
query-executor rejections that occur before the admin service can meter the
operation. Once a request is delegated to the endpoint handler, the existing
service wrapper remains the sole metrics owner, preventing duplicate request
counts.

## Verification Snapshot

Final verification commands below were run with JDK 17 after the generated
public endpoint and package-smoke build fix were present in the working tree.
The focused public endpoint verification was refreshed after adding generated
gRPC coverage for Dashboard-facing client view fields, contest filters, scope
gates, bad requests, sparse client metadata defaults, grouped filter
pagination for `ListClientsByGroup` / `ListClientsByTopic`, exact client id
filtering, public `pageSize` capping, all four RPC authorization mapping, and
missing `DescribeClient.client_id` validation. It was refreshed again after
adding generated gRPC evidence that explicit `LOCAL_PROXY` succeeds for every
RPC and `PROXY_ID` remains gated for every RPC. Broad proxy admin verification
was refreshed again after adding generated gRPC evidence that `BAD_REQUEST`
and `UNAUTHORIZED` error responses do not carry result bodies. It was refreshed
again after adding generated gRPC evidence that omitted public `pageNum` /
`pageSize` defaults return the first 100 clients for all list-style RPCs. It
was refreshed again after adding generated gRPC evidence that
`ListClientsByGroup` and `ListClientsByTopic` honor exact `client_id`
filtering while capping public `pageSize` values above 100. It was refreshed
again after adding generated gRPC evidence that explicit `LOCAL_PROXY` ignores
reserved `proxy_id` for all four RPCs. It was refreshed again after adding
generated gRPC and converter evidence that negative public `pageSize` values are
rejected as `BAD_REQUEST` for all list-style RPCs. It was refreshed again after
aligning the core `ProxyClientQuery` page-size guard with the public adapter so
direct internal query construction rejects negative values too. It was
refreshed again after adding generated gRPC evidence that
`PROXY_SCOPE_PROXY_ID` is gated as an M1 scope error before proxy-id validation
even when `proxy_id` is omitted. Broad proxy admin verification was refreshed
again after adding the generated public gRPC endpoint benchmark setup test.
Focused public endpoint/startup verification was refreshed again after adding
the generated public service descriptor test. Broad proxy admin verification
was refreshed again on the latest branch head after the descriptor and
Dashboard-facing tests were included in the broad suite. Package smoke was
refreshed again on the latest branch head after the submission guard was added.
Generated public endpoint verification now also covers
`GrpcProxyAdminApplicationTest#publicServiceMapsUnexpectedEndpointFailureToInternalServerErrorThroughGeneratedGrpcService`,
which proves unexpected endpoint-layer failures are returned as public
`INTERNAL_SERVER_ERROR` response statuses instead of escaping as transport
errors. Focused and broad Maven verification plus package smoke were refreshed
again after synchronizing that evidence into the reviewer-facing documents.
The generated public `DescribeClient` not-found contract is also pinned by
`GrpcProxyAdminApplicationTest#describeMissingClientReturnsNotFoundStatusWithoutClientBodyThroughGeneratedGrpcService`,
which verifies the public `NOT_FOUND` status/message does not include a
`ProxyClient` result body.
Generated public error body contracts are pinned by
`GrpcProxyAdminApplicationTest#publicServiceMapsBadRequestResponsesWithoutResultBodiesThroughGeneratedGrpcService`
and
`GrpcProxyAdminApplicationTest#publicServiceMapsUnauthorizedResponsesWithoutResultBodiesThroughGeneratedGrpcService`,
which verify `BAD_REQUEST` and `UNAUTHORIZED` responses stay status-only across
all four public RPCs.
Endpoint failure metrics are pinned by
`ProxyClientAdminEndpointExecutorTest#recordsRejectedQueryExecutorMetricsBeforeServiceInvocation`,
`ProxyClientAdminEndpointExecutorTest#recordsRequestAdapterFailureMetricsBeforeServiceInvocation`,
and
`ProxyClientAdminEndpointExecutorTest#successfulEndpointDelegationDoesNotRecordDuplicateFailureMetrics`.
The endpoint executor also preserves gRPC and OpenTelemetry contexts across the
query executor and does not double-count failures after inline task admission.
Authentication and transport trust-boundary tests verify that authenticated
principals, socket addresses, and the complete `proxy_protocol_*` namespace
replace or clear client-supplied metadata. The normal messaging pipeline keeps
its pre-existing whitelisted-subject behavior; the stricter subject policy is
scoped to the public admin pipeline.
`GrpcProxyAdminProductionInterceptorE2ETest` closes the production-interceptor
dual-server gate on real loopback ports. It verifies that the generated admin
service is absent from the messaging server, the messaging service is absent
from the admin server, authenticated subjects and transport source IP reach
ACL, authentication failure bypasses ACL, and authenticated ACL denial returns
`UNAUTHORIZED` without a result body.

Production-interceptor dual-server verification:

```text
Tests run: 63, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-11T19:24:15+08:00
```
`GrpcProxyAdminWiringTest#createDefaultActivityWiresEndpointFailureMetricsRecorder`
verifies the production activity supplies the shared OTel recorder.

Focused generated public API verification:

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am \
-Dtest=GrpcProxyAdminApplicationTest,ProxyStartupTest,GrpcProxyAdminWiringTest \
-DfailIfNoTests=false test -DskipITs
```

Result:

```text
Tests run: 56, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-11T18:41:53+08:00
```

Focused Dashboard table/detail field verification:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
mvn -pl proxy -am \
"-Dtest=GrpcProxyAdminApplicationTest#listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService+describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService" \
-DfailIfNoTests=false test -DskipITs
```

Result:

```text
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T05:38:56+08:00
```

Focused public service descriptor verification:

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am \
-Dtest=GrpcProxyAdminApplicationTest#bindServiceExposesGeneratedProxyAdminUnaryMethods \
-DfailIfNoTests=false test -DskipITs
```

Result:

```text
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-10T05:47:33+08:00
```

Broad proxy admin verification:

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am \
"-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result:

```text
Tests run: 767, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Finished at: 2026-07-12T00:28:33+08:00
```

Package smoke:

```bash
JAVA_HOME="$(/usr/libexec/java_home -v 17)" \
mvn -pl proxy -am -DskipTests package -DskipITs
```

Result:

```text
BUILD SUCCESS
Finished at: 2026-07-12T00:30:07+08:00
```

Lightweight submission guard:

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github
```

`--check-apis-remote` auto-detects the upstream remote configured for
`../rocketmq-apis/rip2-proxy-admin-public-api`, so reviewers can use the same
command whether the companion API remote is named `origin` or `fork`.

Result:

```text
RIP-2 submission guard passed.
```

The package smoke originally exposed stale JMH annotation-generated test sources
under `target/generated-test-sources/test-annotations` being included in the
second `source:jar` checkstyle pass. The final `pom.xml` fixes this by keeping
Checkstyle on hand-written test sources while preserving test compilation and
benchmark generation.

Acceptance audit:

- `docs/en/rip2-proxy-admin-m1-acceptance-audit.md`
- `docs/cn/rip2-proxy-admin-m1-acceptance-audit.md`

Dashboard CLIENT-01 handoff contract:

- `docs/en/rip2-proxy-admin-m1-dashboard-contract.md`
- `docs/cn/rip2-proxy-admin-m1-dashboard-contract.md`

Reviewer reproduction runbook:

- `docs/en/rip2-proxy-admin-m1-review-runbook.md`
- `docs/cn/rip2-proxy-admin-m1-review-runbook.md`

Latest package-level JaCoCo coverage from the broad verification:

| Package | Instruction | Branch | Line |
| --- | ---: | ---: | ---: |
| `org/apache/rocketmq/proxy/service/admin/client` | 93.14% | 86.29% | 94.59% |
| `org/apache/rocketmq/proxy/grpc/v2/admin` | 92.76% | 85.64% | 94.67% |

JDK 17 prints JaCoCo 0.8.5 instrumentation stack traces for some JDK and
Mockito-generated classes in this repository. They are treated as environment
noise only when Surefire reports zero failures/errors and Maven exits
successfully.

Benchmark evidence:

- Local read-model worst P99: `listByTopicPage` at 0.681 ms.
- Generated public gRPC endpoint worst P99:
  `listClientsByClientIdPrefix` at 3.576 ms.
- Wide connect-time range over 100 synthetic time buckets: read model P99
  `0.010 ms` at `1113.352 B/op`; generated public gRPC P99 `0.394 ms` at
  `174970.776 B/op`, with zero swaps and no OOM under a 4 GiB fixed heap.
- Coordinator experiment worst P99: `listAllProxiesNextPage` at 9.011 ms.
- 4 GiB constrained-heap worst cases: broad prefix 137.526 ms P99,
  combined filters 243.610 ms, deep page 10000 at 0.016 ms, and generated
  public gRPC combined filters at 29.042 ms.
- Constrained-heap maximums: 1126.4 MiB JFR heapUsed, 1283.0 MiB RSS, and
  188604.8 B/op on the generated public gRPC path; all runs had zero swaps and
  no OOM.
- Full report: `docs/en/rip2-proxy-admin-m1-benchmark-report.md`.

## PR Description Draft

Title:

```text
[RIP-2] Add proxy admin online client query foundation
```

Body:

````markdown
## Summary

This PR implements the proxy-side RIP-2 online client query foundation and
generated public gRPC endpoint for a standalone `ProxyAdminService`.

Implemented:

- process-local online client read model populated from gRPC client lifecycle
  events.
- query semantics for client id, client id prefix, group, topic, language,
  connect time range, `pageNum`, and `pageSize <= 100`.
- internal `ClientAdminService`, proto-free `ProxyClientAdminActivity`, and
  generated `GrpcProxyAdminApplication`.
- generated public endpoint executor/handler with `LOCAL_PROXY` public-scope
  validation, including `PROXY_ID` gating before proxy-id validation.
- ACL facade using `Admin:proxy.admin.client` with LIST/GET actions.
- dedicated admin query executor and independent admin gRPC server registration.
- low-cardinality metrics, trace attributes, and sanitized failure logs.
- internal cross-proxy coordinator and peer transport experiments behind config.
- English/Chinese docs and 1M synthetic client benchmark report covering the
  local read model and generated public gRPC endpoint.

The authoritative public proto is published in
`pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api`. For contest
verification this branch depends on local
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT` generated from that
proto. Draft PR https://github.com/apache/rocketmq-apis/pull/112 tracks the
upstream API review. Draft PR https://github.com/apache/rocketmq/pull/10603
tracks the downstream RocketMQ implementation review.

## Tests

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
mvn -pl proxy -am \
"-Dtest=GrpcServerTest,ProxyClientAdmin*Test,GrpcProxyAdmin*Test,TimedProxyClientAdminPeerClientTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,AuthenticationPipelineTest,HeaderInterceptorTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ClientAdminMetricsContextTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ProxyClientReadServiceBenchmarkTest,ProxyClientReadServiceCleanerTest,ClientActivityTest" \
-DfailIfNoTests=false test -DskipITs
```

Result: `Tests run: 767, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`.

Lightweight submission guard:

```bash
python3 dev/rip2_submission_guard.py --check-remote --check-apis-remote --check-github
```

Result: `RIP-2 submission guard passed.`

## Benchmark

1M synthetic client JMH on Apple M4, 16 GB, Temurin JDK 17.0.18:

- local read-model worst P99: 0.681 ms.
- generated public gRPC endpoint worst P99: 3.576 ms.
- internal coordinator experiment worst P99: 9.011 ms.
- 4 GiB fixed heap worst-case P99: broad prefix 137.526 ms, combined filters
  243.610 ms, deep page 0.016 ms, and production-shaped public gRPC 29.042 ms.
- Maximum JFR heapUsed 1126.4 MiB, maximum RSS 1283.0 MiB, zero swaps, no OOM.

See `docs/en/rip2-proxy-admin-m1-benchmark-report.md`.
````

## Issue Comment

Posted to [apache/rocketmq#10599](https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687):

```markdown
I have prepared the RIP-2 Proxy Admin online client query implementation branch
at `pilichoumao/rocketmq:rip2-proxy-admin-m1`.

The branch is ready for community review of the proxy-side foundation:

- local online-client read model and lifecycle writes.
- `ListClients`, `DescribeClient`, `ListClientsByGroup`, and
  `ListClientsByTopic` through generated public gRPC stubs.
- official filters, negative `pageSize` rejection, and `pageSize <= 100`.
- independent ACL resource `Admin:proxy.admin.client`.
- dedicated admin query executor and admin server registration.
- metrics/tracing/logging coverage.
- generated public gRPC Server/Channel tests, in-process endpoint/peer tests,
  and 1M synthetic benchmark report covering the read model and public endpoint.
- English and Chinese user docs.

The public API source is published at
https://github.com/pilichoumao/rocketmq-apis/tree/rip2-proxy-admin-public-api.
The API proposal draft PR is https://github.com/apache/rocketmq-apis/pull/112.
The RocketMQ implementation draft PR is https://github.com/apache/rocketmq/pull/10603.
The remaining community question is whether Apache accepts the proposed service
name, field names, field numbers, and upstream publication path.
```

## Final Checklist

- Keep `origin` as `https://github.com/pilichoumao/rocketmq.git`.
- Do not push to `apache/rocketmq`.
- Re-run the broad proxy verification after any code change.
- Re-run the final smoke in `docs/en/rip2-proxy-admin-m1-final-smoke.md` when
  manually validating a running Proxy.
- Re-run `git diff --check` before final commit.
- Use the proxy push command if a normal push fails:

```bash
HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890 \
git push origin rip2-proxy-admin-m1
```
