# RIP-2 Proxy Admin M1 Acceptance Audit

This audit maps the public RIP-2 tracking issue acceptance criteria to the
current contest branch evidence. It separates what is locally implemented and
verified from what still requires community review or an external integration
environment.

## Review Artifacts

| Artifact | Link |
| --- | --- |
| RocketMQ implementation draft PR | https://github.com/apache/rocketmq/pull/10603 |
| Public API draft PR | https://github.com/apache/rocketmq-apis/pull/112 |
| RIP-2 tracking issue comment | https://github.com/apache/rocketmq/issues/10599#issuecomment-4926996687 |
| Implementation branch | `pilichoumao/rocketmq:rip2-proxy-admin-m1` |
| API branch | `pilichoumao/rocketmq-apis:rip2-proxy-admin-public-api` |
| Reviewer reproduction runbook | `docs/en/rip2-proxy-admin-m1-review-runbook.md` |

## M1 Initial Release Acceptance

| Criterion from `apache/rocketmq#10599` | Current status | Evidence | Remaining gate |
| --- | --- | --- | --- |
| Proxy Admin architecture and proto definitions pass community review. | In review. | Draft PRs #10603 and #112 are open as review artifacts. The design recommends a standalone `ProxyAdminService` on the independent admin gRPC server. | Community must accept proto ownership, field numbers, generated artifact version, and publication path before either PR is treated as merge-ready. |
| All 4 core client query RPCs are implemented with filtering and pagination. | Locally implemented and tested. | `GrpcProxyAdminApplication` exposes `ListClients`, `DescribeClient`, `ListClientsByGroup`, and `ListClientsByTopic`; `GrpcProxyAdminApplicationTest` exercises generated Server/Channel calls. | Upstream merge waits for the official `rocketmq-proto` artifact. |
| Paginated query P99 latency < 1s at 1M clients without OOM risk. | Locally implemented and verified. | The 4 GiB constrained-heap proof covers broad prefix (137.526 ms P99), combined filters (243.610 ms), deep page 10000 (0.016 ms), and generated public gRPC with production-shaped executors (29.042 ms). Max JFR heapUsed is 1126.4 MiB, max RSS is 1283.0 MiB, and all runs completed with zero swaps and no OOM. | Re-run on material hardware or implementation changes. |
| ACL 2.0 integration uses independent `proxy.admin.client` resource. | Implemented and tested. | Unit tests cover policy construction and evaluation. `dev/run_rip2_authenticated_smoke.sh` additionally starts the full distribution with authentication/authorization enabled and proves a LIST-only user can list but cannot describe, while a GET-only user can describe but cannot list, all against `Admin:proxy.admin.client`. | Community review of exact resource naming. |
| OpenTelemetry metrics for admin interfaces are enabled. | Implemented and tested across service and pre-service endpoint failures. | `MeteredClientAdminServiceTest`, `MeteredAuthorizingClientAdminServiceTest`, `ClientAdminMetricsContextTest`, `ProxyClientAdminObservabilityTest`, `ProxyClientAdminEndpointExecutorTest`, and `GrpcProxyAdminWiringTest`; metrics include operation, scope, status/result, duration, filters, page size, and result size. Request-adapter/context failures and query-executor rejection are recorded before service invocation without double-counting delegated requests. | Production dashboard wiring is outside this branch. |
| End-to-end joint debugging with RIP-1 Dashboard CLIENT-01 passes. | Public runtime path is locally proven; Dashboard integration is not. | `GrpcProxyAdminApplicationTest` verifies the Dashboard-facing client view fields through a generated gRPC Server/Channel. A full release distribution was also started with a real local NameServer and Proxy, and direct `grpcurl` calls reached all four public RPCs on the isolated admin listener. `docs/en/rip2-proxy-admin-m1-dashboard-contract.md` records the Dashboard handoff contract. | Requires a RIP-1 Dashboard integration environment. This remains an external validation item. |
| Complete bilingual interface documentation, authentication examples, and best-practice manuals are provided. | Implemented. | English and Chinese user guides, Dashboard contract docs, final smoke guides, benchmark reports, submission packages, and this audit. | Keep links current as review progresses. |

## Overall Architecture Acceptance

| Criterion | Current status | Evidence | Remaining gate |
| --- | --- | --- | --- |
| Independent admin service framework supports later modules. | Implemented for M1. | `ProxyClientAdminEndpointExecutor`, `ProxyClientAdminEndpointHandler`, `GrpcProxyAdminApplication`, and `ProxyStartup` admin service registration. | Future modules should reuse the same service/activity boundary. |
| Admin interfaces are isolated by port, thread pool, and authentication. | Implemented and black-box tested. | `GrpcProxyAdminProductionInterceptorE2ETest` starts separate loopback messaging/admin servers through production `GrpcServerBuilder` interceptors. The full distribution smoke additionally proved that `ProxyAdminService` is absent from 8081, `MessagingService` is absent from 8082, and reflection is disabled on 8082. Existing config/executor tests cover authentication flow, `enableProxyAdminGrpcServer`, `proxyAdminGrpcServerPort`, and independent thread pools. | Operational deployment must enable the admin server explicitly. |
| Interface contracts follow compatibility rules. | Draft contract follows additive protobuf style. | `../rocketmq-apis/apache/rocketmq/v2/admin.proto` and `docs/en/rip2-proxy-admin-m1-public-api-draft.proto` use a standalone service and stable field numbers in the proposal. | Apache must approve the final contract before merge. |

## Quality And Documentation

| Criterion | Current status | Evidence | Remaining gate |
| --- | --- | --- | --- |
| Unit test coverage of core modules is >=85%. | Locally verified for RIP-2 core packages. | Submission package records `service.admin.client` line coverage 94.41% and `grpc.v2.admin` line coverage 94.73%; branch coverage is 86.62% and 85.79% respectively. | Re-run coverage after any code changes. |
| Integration tests cover interfaces, authentication, and exception handling. | Covered locally. | Generated gRPC Server/Channel tests, production-interceptor dual-server E2E, proto-free endpoint integration tests, trusted subject/address boundary tests, peer tests, ACL tests, and public bad-request/not-found/unauthorized/internal-error response mapping tests. The recorded release-distribution smoke also reached all four RPCs and verified live BAD_REQUEST/NOT_FOUND semantics. | Dashboard CLIENT-01 remains an external E2E item. |
| Performance benchmark report is available. | Implemented. | English and Chinese benchmark reports include commands, environment, 1M-client read-model, generated public gRPC endpoint, coordinator scenarios, P50/P95/P99, and heap settings. | Re-run if hardware or implementation changes materially. |
| Bilingual docs are synchronized with the code release. | Implemented for contest branch. | English and Chinese user guides, final smoke guides, benchmark reports, and submission packages are present. | Keep PR and issue links current. |

## GitHub Actions Approval Evidence

The current RocketMQ pull-request head created seven workflow runs, all with
conclusion `action_required`: `Build and Run Tests by Maven`,
`Build and Run Tests by Bazel`, `CodeQL Analysis`, `Coverage`,
`License checker`, `Misspell Check`, and `Run Integration Tests`. The current
rocketmq-apis head created one `CI` run with the same conclusion. No job or
check run has started; Apache maintainer approval is required for these fork PR
workflows.

## Final Gate Summary

The contest branch is locally implemented and verified for M1 public endpoint
behavior, local read-model and generated public endpoint performance, ACL,
observability, docs, generated gRPC Server/Channel coverage, and constrained
heap performance. The remaining gates are external community/integration items:

- community acceptance of the `rocketmq-apis` public proto proposal.
- official `rocketmq-proto` artifact publication with `ProxyAdminServiceGrpc`.
- maintainer approval and successful completion of the currently
  `action_required` GitHub Actions workflows on both draft PRs.
- Dashboard CLIENT-01 joint E2E in an environment that contains the RIP-1
  dashboard client. The field-level contract is documented in
  `docs/en/rip2-proxy-admin-m1-dashboard-contract.md`.
