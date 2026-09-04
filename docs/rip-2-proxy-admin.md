# RIP-2: Proxy Admin Standardized Management Interface

## 1. Motivation

RocketMQ 5.0 moved client access behind the stateless Proxy, but operations still
observe clients through broker-side structures (`ConsumerManager` on the broker,
Remoting-era admin commands). gRPC clients attached to a Proxy are invisible to
those tools: the control plane cannot answer "which SDK clients are online, what
do they subscribe to, are they healthy" without indirect metrics heuristics.

RIP-1 (Control Plane 5.0 dashboard) requirement CLIENT-01 explicitly depends on a
standard server-side interface to read complete gRPC client data. RIP-2 implements
that interface on the Proxy itself.

## 2. Contract choice: upstream `Admin` service

The gRPC contract is the **upstream `Admin` service** defined in
`apache/rocketmq/v2/admin.proto` of the `apache/rocketmq-apis` repository (main
branch, proto artifact `org.apache.rocketmq:rocketmq-proto:2.2.0`). This
implementation deliberately does NOT introduce a bespoke
`ProxyAdminService` in the protocol layer: the control-plane RPC surface lives in
the same versioned, community-reviewed proto that the dashboard and multi-language
SDKs already consume, and stays additive-compatible with upstream evolution.

The proxy implements `AdminGrpc.AdminImplBase` (all 16 RPCs) and binds it on a
dedicated admin gRPC server.

## 3. Goals

1. A dedicated, independent gRPC Admin server on the Proxy, isolated from the
   data-plane `MessagingService`.
2. Stable, backward-compatible contract from rocketmq-apis main (`Admin`
   service, proto 2.2.0).
3. First-class authorization under dedicated `proxy.admin.*` ACL 2.0 resources
   with read-only / high-privilege action separation.
4. The admin surface exposes its own call RT and error-rate metrics.
5. Multi-Proxy semantics: local view per proxy node (the RIP-2 spec allows
   choosing between "local view" and "cluster aggregation").

## 4. Design Decisions

### D1 — Service placement

The upstream `Admin` service is bound on its own gRPC server/port
(`adminGrpcPort`, default 8083) with its own interceptor chain, separate from the
data plane (8081). A global kill switch `proxyAdminEnabled` disables the whole
surface.

Rationale: control-plane traffic must never contend with the data plane, and the
admin port can be firewalled to the operations network only. The admin server
intentionally does NOT expose channelz or proto reflection. It reuses the data
plane's `GrpcChannelManager` / `GrpcClientSettingsManager` so online clients are
visible to admin queries.

### D2 — Authorization: dedicated `proxy.admin.*` resources

Credentials arrive in the standard gRPC `Authorization` metadata (same scheme as
the data plane, ACL 2.0 signature). Every RPC maps to one resource + one action:

| Resource | RPCs | Actions |
|---|---|---|
| `proxy.admin.client` | ListSubscription, ListConsumerConnection (List); DescribeSubscription, DescribeGroupAccumulation, GetConsumerRunningInfo, QueryTimeSpan (Get) | List / Get |
| `proxy.admin.config` | ChangeLogLevel | Update |
| `proxy.admin.connection` | PrintThreadStackTrace, VerifyMessage | Update (high privilege) |
| `proxy.admin.route` | GetTopicRoute | Get |
| `proxy.admin.ops` | GetProxyRuntimeStats, DescribeTopicStatus, QueryMessage (Get); ResetGroupOffset (Update); DeleteSubscription (Delete); AdminSendMessage (Pub) | Get / Update / Delete / Pub |

Modeling note: ACL 2.0 resource types are cluster/namespace/topic/group. The
admin resources are modeled as CLUSTER-typed literals with reserved names
(resource keys `cluster:proxy.admin.<module>`), which yields exact
least-privilege matching without colliding with real cluster names and without
changing the auth core.

Modes:
- cluster auth disabled, `proxyAdminRequireAuth=false` → open surface (same
  semantics as the data plane);
- cluster auth enabled → standard authenticate + authorize pipeline;
- `proxyAdminRequireAuth=true` → fail-closed: requests without verifiable
  credentials are rejected even if the cluster-wide switch is off.

Audit: every served RPC writes `[PROXY-ADMIN-AUDIT] subject/method/resource/
action/sourceIp` to the auth audit logger (satisfies the Console-user + AK +
resource + operation audit tuple requirement together with the ACL 2.0 engine's
own audit log).

### D3 — Multi-proxy semantics: local view

Each proxy answers admin queries from its own state and returns the **local
view** only. This is the "local view" option of the RIP-2 M1 requirement
(local view OR cluster aggregation). A dashboard/CLI that wants a cluster-wide
picture queries each proxy's admin endpoint and merges/deduplicates by
`client_id` upstream (a gRPC client is attached to exactly one proxy at a time),
which keeps the protocol free of scope/peering fields and the proxy free of
membership configuration. The earlier draft's peer fan-out (`proxyAdminPeerEndpoints`)
was dropped together with the bespoke contract it served.

### D4 — Data sources per RPC

| Source | RPCs | Mechanism |
|---|---|---|
| Proxy-local state | ListSubscription, DescribeSubscription, ListConsumerConnection, GetConsumerRunningInfo, GetProxyRuntimeStats | `GrpcChannelManager` + `GrpcClientSettingsManager` (client settings / subscriptions reported over telemetry) |
| Client-directed commands | PrintThreadStackTrace, VerifyMessage | telemetry command written to the target `GrpcClientChannel` |
| Broker-internal data | DescribeTopicStatus, DescribeGroupAccumulation, ResetGroupOffset, QueryMessage, DeleteSubscription, QueryTimeSpan, GetTopicRoute | proxy's own `AdminService` remoting gateway (multi-broker fan-out where required) |
| Proxy producer path | AdminSendMessage | `MessagingProcessor.sendMessage` |
| Proxy runtime | ChangeLogLevel | relocated logback API on the root logger |

### D5 — Protocol coverage

This iteration tracks gRPC clients (the proxy's `GrpcChannelManager` is the
authority). Remoting clients remain observable through the existing broker-side
channels and are not part of the gRPC `ClientInfo` listings.

## 5. RIP-2 M1 capability mapping

| RIP-2 M1 requirement | Delivered as | Notes |
|---|---|---|
| ListClients (filter by group/topic/clientId prefix, paged) | `ListConsumerConnection(group[, topic])` | clientId-prefix filter and pagination are not expressible in the upstream contract; documented boundary of this partial delivery |
| DescribeClient (SDK version, subscriptions, heartbeat, auth, Pop progress) | `DescribeSubscription` (per-client subscriptions) + `GetConsumerRunningInfo` (subscriptions + running info) | heartbeat/auth-status/Pop-progress fields do not exist in the upstream contract |
| ListClientsByGroup / ListClientsByTopic | `ListConsumerConnection(group[, topic])` | fully covered |
| Multi-proxy cluster aggregation | local view per proxy | see D3 |

M2+ items (config hot update, quotas, connection control, Pop/batch diagnostics,
route observation streaming) are not part of the upstream `Admin` contract and
are out of scope for this delivery.

## 6. Building the proto artifact

`org.apache.rocketmq:rocketmq-proto:2.2.0` is generated from the
`apache/rocketmq-apis` repository **main branch** (`java/VERSION` = 2.2.0).
Install it into the local Maven repository once, then build this repo normally;
the apis repository is intentionally NOT vendored or submoduled here.

- Preferred: `bazel build //java:assemble-maven` in the rocketmq-apis checkout,
  then `mvn install:install-file` the produced jar/pom.
- Offline fallback (no bazel network access): generate with
  `protoc` 3.20.1 + `protoc-gen-grpc-java` 1.53.0 from Maven Central,
  compile the generated sources against `protobuf-java`/`grpc-*` jars, and
  install the jar with the 2.1.2 pom as template (version bumped).

## 7. Observability

The admin server exports (OpenTelemetry, honoring the proxy's metrics exporter
configuration):

- `rocketmq_proxy_admin_rpc_total{rpc_method, status=success|error, error_type?}`
  — error rate = rate(status="error")
- `rocketmq_proxy_admin_rpc_latency{rpc_method, status}` (ms histogram) — RT P50/P99

Transport-level failures (auth rejections, permission denials) and business
failures are both counted as errors; successful RPCs are counted once.

## 8. Configuration Reference

| Key | Default | Meaning |
|---|---|---|
| `proxyAdminEnabled` | true | kill switch; false = admin server not started |
| `adminGrpcPort` | 8083 | dedicated admin gRPC port (<=0 disables) |
| `proxyAdminRequireAuth` | false | fail-closed credential enforcement |

## 9. Milestones

- M1 (this delivery): online client query over the upstream `Admin` service —
  ListConsumerConnection / DescribeSubscription / GetConsumerRunningInfo /
  DescribeGroupAccumulation, served from the proxy's local view, plus the full
  16-RPC `Admin` surface for RIP-1 dashboard integration.
- Future (needs upstream proto evolution): clientId-prefix filter / pagination
  for client listings, heartbeat & auth-status fields, Remoting client coverage,
  and the M2+ surfaces (config/quota/connection/route observation).

## 10. Acceptance Criteria Mapping

| Criterion | Status |
|---|---|
| RIP document + stable backward-compatible proto contract | this document + upstream `admin.proto` (rocketmq-apis main) |
| Client query RPCs merged into the server repo | `ProxyAdminGrpcService` implements all 16 `Admin` RPCs on the proxy |
| Independent ACL control, read-only/high-risk separation, least-privilege doc | D2 resources/actions + `docs/rip-2-least-privilege.md` |
| RPC RT & error-rate metrics | §7 instruments |
| E2E with RIP-1 dashboard | contract = upstream `Admin` service, frozen for dashboard CLIENT-01 integration (cross-repo) |
