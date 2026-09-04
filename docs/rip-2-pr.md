# [RIP-2] Implement Proxy Admin Standardized Management Interface on the Proxy

## Summary

This PR implements **RIP-2: Proxy Admin Standardized Management Interface** — the
**upstream `Admin` gRPC service** (`apache/rocketmq/v2/admin.proto`,
rocketmq-apis main, `org.apache.rocketmq:rocketmq-proto:2.2.0`) bound by the
RocketMQ Proxy on a **dedicated admin gRPC server**, isolated from the data-plane
`MessagingService`, with fine-grained ACL 2.0 authorization under dedicated
`proxy.admin.*` resources.

It closes the observability gap introduced by RocketMQ 5.0's stateless Proxy
architecture: gRPC clients attached to a Proxy were previously invisible to
operations tools that rely on broker-side `ConsumerManager` and Remoting-era admin
commands. The proxy now serves all 16 `Admin` RPCs — including the RIP-2 M1
online-client-query set — from the Proxy itself, so RIP-1 (dashboard `CLIENT-01`)
can read complete gRPC client data through a stable, community-reviewed contract.

### Contract choice

The gRPC contract is the **upstream `Admin` service from rocketmq-apis main** —
no bespoke `ProxyAdminService` is added to the protocol layer. Earlier drafts of
this work defined a custom `ProxyAdminService` on a local rocketmq-apis feature
branch; that surface has been **removed** in favor of the upstream contract, so
the dashboard/SDKs consume one versioned proto without forked API evolution.

### RIP-2 M1 capability mapping

| RIP-2 M1 requirement | Delivered as |
|---|---|
| ListClients (filter by group/topic) | `ListConsumerConnection(group[, topic])` |
| DescribeClient (SDK version, subscriptions) | `DescribeSubscription` + `GetConsumerRunningInfo` |
| ListClientsByGroup / ListClientsByTopic | `ListConsumerConnection(group[, topic])` |
| Multi-proxy aggregation (local view OR cluster) | local view per proxy node |

Documented boundaries (upstream contract does not yet carry them): client-listing
pagination / clientId-prefix filters, heartbeat & auth-status fields, Remoting
client coverage. M2+ surfaces (config hot update, quotas, connection kick,
Pop/batch diagnostics, route-event streaming) are out of scope.

## What This PR Adds

### 1. Dedicated Admin gRPC Server

A second gRPC server started by `ProxyStartup`, on its own port
(`adminGrpcPort`, default **8083**), with its own interceptor chain
(metrics → auth → standard pipeline). It reuses the data plane's
`GrpcChannelManager` / `GrpcClientSettingsManager` so online clients are visible.
The admin server intentionally does NOT expose channelz or proto reflection
(control-plane attack surface kept minimal). A global kill switch
`proxyAdminEnabled` disables the whole surface.

### 2. The `Admin` Service Implemented on the Proxy

`ProxyAdminGrpcService` (extends `AdminGrpc.AdminImplBase`) implements all 16
RPCs. Data sources per RPC:

| Source | RPCs |
|--------|------|
| Proxy-local state (`GrpcChannelManager` + `GrpcClientSettingsManager`) | `ListConsumerConnection`, `ListSubscription`, `DescribeSubscription`, `GetConsumerRunningInfo`, `GetProxyRuntimeStats` |
| Client-directed telemetry commands | `PrintThreadStackTrace`, `VerifyMessage` |
| Broker-internal data (proxy's own `AdminService` remoting gateway; multi-broker fan-out where required) | `DescribeTopicStatus`, `DescribeGroupAccumulation`, `ResetGroupOffset`, `QueryMessage`, `DeleteSubscription`, `QueryTimeSpan`, `GetTopicRoute` |
| Proxy producer path | `AdminSendMessage` |
| Proxy runtime | `ChangeLogLevel` |

### 3. Dedicated `proxy.admin.*` ACL 2.0 Authorization

`ProxyAdminAuthInterceptor` maps every RPC to exactly one `(resource, action)`
pair across five resources:

- `proxy.admin.client` — online client query & diagnostics (`Get`/`List`)
- `proxy.admin.config` — proxy log-level change (`Update`)
- `proxy.admin.connection` — client-directed telemetry commands (`Update`, high privilege)
- `proxy.admin.route` — route view (`Get`)
- `proxy.admin.ops` — broker-facing ops (`Get`/`List` for queries;
  `Update`/`Delete`/`Pub` for mutations)

High-privilege RPCs (`ResetGroupOffset`, `DeleteSubscription`, `AdminSendMessage`,
`PrintThreadStackTrace`, `VerifyMessage`) can **never** be authorized by a
read-only grant. A fail-closed `proxyAdminRequireAuth` mode rejects requests
without verifiable credentials even when cluster-wide auth is off. Every served
RPC writes a `[PROXY-ADMIN-AUDIT]` log (subject / method / resource / action /
sourceIp).

### 4. Multi-Proxy Semantics: Local View

Each proxy answers from its own state (local view — the RIP-2 M1 "二选一" local
option). Cluster-wide pictures are merged upstream (RIP-1 dashboard) by querying
every proxy and deduplicating by `client_id`; no peering/scope fields are added
to the protocol and the proxy needs no membership configuration.

### 5. Observability (Acceptance Criteria #4)

`ProxyAdminMetricsManager` / `ProxyAdminMetricsInterceptor` export two
OpenTelemetry instruments honoring the proxy's metrics exporter configuration:

- `rocketmq_proxy_admin_rpc_total{rpc_method, status, error_type?}` — error rate
- `rocketmq_proxy_admin_rpc_latency{rpc_method, status}` (ms histogram) — RT P50/P99

### 6. Protocol-Pure Architecture

`AdminModelConverter` is the **only** class that imports both the broker's
internal wire types (`org.apache.rocketmq.remoting.*`) and the gRPC protocol
(`apache.rocketmq.v2.*`). The gRPC admin service stays protocol-pure (v2 only);
the broker gateway (`DefaultAdminService`) stays remoting-pure.

## Files Changed

See the commits on this branch (diffstat over `develop`). Highlights:

### Documentation
- `docs/rip-2-proxy-admin.md` — full RIP-2 proposal (motivation, goals, design decisions, contract, observability, configuration, milestones, acceptance criteria)
- `docs/rip-2-least-privilege.md` — least-privilege configuration guide with role templates (read-only observer, on-call operator, admin)
- `docs/rip-2-issue.md` / `docs/rip-2-pr.md` — issue and PR copy

### New Source — `proxy/grpc/admin/`
| File | Responsibility |
|------|----------------|
| `ProxyAdminGrpcService.java` | the `Admin` service surface: online client query (RIP-2 M1) + broker-facing ops via the proxy's managed client |
| `AdminModelConverter.java` | bridge between broker wire types and v2 proto (only class importing both worlds) |
| `ProxyAdminAuthInterceptor.java` | per-RPC ACL 2.0 authorization over `proxy.admin.*` resources |
| `ProxyAdminMetricsManager.java` | OpenTelemetry RT & error-rate metrics |
| `ProxyAdminMetricsInterceptor.java` | per-RPC metrics recording |

### New Tests
- `AdminModelConverterTest.java`, `ProxyAdminAuthInterceptorTest.java`,
  `ProxyAdminGrpcServiceTest.java`, `DefaultAdminServiceTest.java` (enhanced)

### Modified Source
- `ProxyStartup.java` — starts the dedicated admin gRPC server, wires shared channel/settings managers, metrics
- `ProxyConfig.java` — new config keys (`adminGrpcPort`, `proxyAdminEnabled`, `proxyAdminRequireAuth`)
- `AdminService.java` / `DefaultAdminService.java` — broker-facing gateway methods (offsets, consume stats, reset, delete subscription, query message, topic config/route)
- `GrpcConverter.java`, `GrpcChannelManager.java`, `GrpcMessagingApplication.java`, `DefaultGrpcMessagingActivity.java`, `DefaultMessagingProcessor.java` — shared-component exposure for the admin server

## Configuration

| Key | Default | Meaning |
|-----|---------|---------|
| `proxyAdminEnabled` | `true` | Kill switch; `false` = admin server not started |
| `adminGrpcPort` | `8083` | Dedicated admin gRPC port (`≤0` disables) |
| `proxyAdminRequireAuth` | `false` | Fail-closed credential enforcement |

## Build Prerequisite

> The proto artifact is the upstream rocketmq-apis **main branch**
> (`java/VERSION` = 2.2.0, `Admin` service in `admin.proto`), installed as a
> local Maven dependency:
>
> ```bash
> cd rocketmq-apis                      # apache/rocketmq-apis main
> bazel build //java:assemble-maven     # then install the jar/pom:
> mvn install:install-file -Dfile=<jar> -DpomFile=<pom> \
>   -DgroupId=org.apache.rocketmq -DartifactId=rocketmq-proto -Dversion=2.2.0
> ```
>
> Offline fallback: generate with `protoc` 3.20.1 + `protoc-gen-grpc-java`
> 1.53.0 (both from Maven Central) and install the jar with the 2.1.2 pom as
> template. The `rocketmq-apis` repository is intentionally **not** vendored or
> submoduled into this repository.

## How to Test

1. Install `rocketmq-proto:2.2.0` from rocketmq-apis main (see Build Prerequisite).
2. Start a Proxy with `proxyAdminEnabled=true` (default) and `adminGrpcPort=8083`.
3. Connect gRPC clients to the data-plane port (8081).
4. Call `ListConsumerConnection` / `DescribeSubscription` / `GetConsumerRunningInfo`
   on the admin port (8083) — verify connected clients appear with correct
   SDK version/language and subscriptions.
5. Verify `proxy.admin.*` ACL enforcement: a read-only user can query but not
   reset offset / delete subscription; a high-privilege user can.
6. Verify metrics: `rocketmq_proxy_admin_rpc_total` and
   `rocketmq_proxy_admin_rpc_latency` are exported.
7. Run the unit test suite:
   ```bash
   mvn test -pl proxy -Dtest='org.apache.rocketmq.proxy.grpc.admin.*Test,org.apache.rocketmq.proxy.service.admin.*Test'
   ```

## Acceptance Criteria Mapping

| Criterion | Status |
|-----------|--------|
| RIP document + stable backward-compatible proto contract | `docs/rip-2-proxy-admin.md` + upstream rocketmq-apis `admin.proto` |
| Client query RPCs merged into the server repo | `ProxyAdminGrpcService` implements all 16 `Admin` RPCs on the proxy |
| Independent ACL control, read-only/high-risk separation, least-privilege doc | D2 resources/actions + `docs/rip-2-least-privilege.md` |
| RPC RT & error-rate metrics | `ProxyAdminMetricsManager` instruments |
| E2E with RIP-1 dashboard | Contract = upstream `Admin` service, frozen for dashboard CLIENT-01 integration (cross-repo) |

## Compatibility

- **No impact on data plane**: the admin server is a separate gRPC server on a
  separate port; if `proxyAdminEnabled=false` (or `adminGrpcPort≤0`), the proxy
  behaves exactly as before.
- **ACL 2.0**: no changes to the auth core engine — admin resources are modeled as
  cluster-typed literals with reserved names.
- **Proto**: upstream contract only; the implementation is additive-compatible
  with future rocketmq-apis evolution.

## Related

- Issue: [RIP-2] Proxy Admin Standardized Management Interface (`docs/rip-2-issue.md`)
- RIP-1 Control Plane 5.0 dashboard (requirement `CLIENT-01`)
- Proto contract: `apache/rocketmq-apis` main branch (`apache/rocketmq/v2/admin.proto`)
