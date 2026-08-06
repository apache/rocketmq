# RIP-2: Proxy Admin Standardized Management Interface

## 1. Motivation

RocketMQ 5.0 moved client access behind the stateless Proxy, but operations still
observe clients through broker-side structures (`ConsumerManager` on the broker,
Remoting-era admin commands). gRPC clients attached to a Proxy are invisible to
those tools: the control plane cannot answer "which SDK clients are online, what
do they subscribe to, are they healthy" without indirect metrics heuristics.

RIP-1 (Control Plane 5.0 dashboard) requirement CLIENT-01 explicitly depends on a
standard server-side interface to read complete gRPC client data. RIP-2 defines
and implements that interface on the Proxy itself.

## 2. Goals

1. A dedicated, independent gRPC Admin service on the Proxy, isolated from the
   data-plane `MessagingService`.
2. A stable, backward-compatible proto contract (`ProxyAdminService` in
   `apache/rocketmq/v2/admin.proto`, rocketmq-apis).
3. First-class authorization under dedicated `proxy.admin.*` ACL 2.0 resources
   with read-only / high-privilege action separation.
4. The service exposes its own call RT and error-rate metrics.
5. Multi-Proxy semantics: a documented, predictable story for cluster-wide views.

Non-goals (this iteration): broker-side quota storage, Remoting client kick
(Remoting clients remain observable via existing broker channels; the service
reports gRPC clients, proto carries a `protocol` field for future D5 coverage).

## 3. Design Decisions

### D1 — Service placement

Dedicated `ProxyAdminService` (Option B), separate from both the data-plane
`MessagingService` and the broker-facing `Admin` service. Served on its own gRPC
server/port (`adminGrpcPort`, default 8083) with its own interceptor chain. A
global kill switch `proxyAdminEnabled` disables the whole surface.

Rationale: control-plane traffic must never contend with the data plane, and the
admin port can be firewalled to the operations network only. The admin server
intentionally does NOT expose channelz or proto reflection.

### D2 — Authorization: dedicated `proxy.admin.*` resources

Credentials arrive in the standard gRPC `Authorization` metadata (same scheme as
the data plane, ACL 2.0 signature). Every RPC maps to one resource + one action:

| Resource | RPCs | Actions |
|---|---|---|
| `proxy.admin.client` | ListClients / ListClientsByGroup / ListClientsByTopic | List |
| `proxy.admin.client` | DescribeClient / DescribePopReceiptHandles / DescribeBatchConsumeDiagnostics / ListSubscription / DescribeSubscription / ListConsumerConnection / DescribeGroupAccumulation / GetConsumerRunningInfo / QueryTimeSpan | Get |
| `proxy.admin.config` | DescribeProxyConfig | Get |
| `proxy.admin.config` | UpdateProxyConfig / ChangeLogLevel | Update |
| `proxy.admin.connection` | KickClient / DisconnectChannel / PrintThreadStackTrace / VerifyMessage | Update (high privilege) |
| `proxy.admin.quota` | DescribeQuota | Get |
| `proxy.admin.quota` | UpdateQuota | Update (high privilege) |
| `proxy.admin.route` | DescribeRouteTopology / GetTopicRoute | Get |
| `proxy.admin.route` | SubscribeRouteEvents | List |
| `proxy.admin.ops` | GetProxyRuntimeStats / DescribeTopicStatus / QueryMessage | Get |
| `proxy.admin.ops` | ResetGroupOffset | Update (high privilege) |
| `proxy.admin.ops` | DeleteSubscription | Delete (high privilege) |
| `proxy.admin.ops` | AdminSendMessage | Pub (high privilege) |

Modeling note: ACL 2.0 resource types are cluster/namespace/topic/group. The
admin resources are modeled as CLUSTER-typed literals with reserved names
(resource keys `cluster:proxy.admin.<module>`), which yields exact least-
privilege matching without colliding with real cluster names and without
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

### D3 — Multi-proxy semantics

Each Proxy returns its LOCAL view, tagged with `proxy_endpoint` + a monotonic
`epoch`, so a consumer can always attribute and deduplicate results.

Cluster-wide view: `ListClientsRequest.scope = PROXY_SCOPE_ALL_PROXIES`. When
`proxyAdminPeerEndpoints` (host:port of peer admin servers) is configured, the
serving proxy fans the query out to all peers in parallel, merges the local
views and dedups by `client_id` (a client is attached to exactly one proxy at a
time; local view wins on duplicates). Peer failures degrade gracefully: an
unreachable peer is skipped with a warning, the merged view of the remaining
nodes is returned. When no peers are configured, the request is served from the
local view — the scheme therefore satisfies the spec's "local view OR cluster
aggregation" requirement with both options available, chosen per request.

Rationale for not building a central registry: proxies are stateless and have no
membership service; peer-list configuration is explicit, auditable, and matches
how operators already firewall the admin port.

### D4 — Pagination

Cursor-based `next_token` for client listings. The cursor is the clientId-sorted
position (base64-opaque) of the last returned element: page boundaries stay
stable while clients connect/disconnect between calls — offset pagination would
shift or duplicate entries under churn and does not scale to very large
connection counts. Diagnostic snapshots (pop handles, batch diagnostics) use
offset pagination (`page_num`/`page_size`, max 100) because they are bounded,
point-in-time views.

### D5 — Protocol coverage

`ClientInstance.protocol` distinguishes GRPC vs REMOTING. This iteration tracks
gRPC clients (the proxy's `GrpcChannelManager` is the authority); the field
keeps the contract forward-compatible for Remoting coverage.

## 4. Proto Contract (rocketmq-apis, `apache/rocketmq/v2/admin.proto`)

`service ProxyAdminService` — 14 RPCs:

M1 (required by RIP-1 CLIENT-01):
- `ListClients(ClientFilter, page_size, next_token, ProxyScope)`
- `DescribeClient(client_id)` → ClientDetail (instance, settings, subscriptions,
  recent_heartbeats, auth_status, consume_progress, network_info)
- `ListClientsByGroup(group, ...)`
- `ListClientsByTopic(topic, ...)`

M2:
- `DescribeProxyConfig()` / `UpdateProxyConfig(ProxyRuntimeConfig)` → changed_fields
- `KickClient(client_id, reason)` / `DisconnectChannel(channel_id, reason)`
- `DescribeQuota(...)` / `UpdateQuota(QuotaPolicy)`
- `DescribePopReceiptHandles(group[, topic], page)` → summary + handles
  (renew/renewRetry counts, nextVisibleTime, invisibleTime, expired flag, lock owner)
- `DescribeBatchConsumeDiagnostics(group[, topic][, client_id], page)` →
  per-client unacked/renew/expired aggregates + group summary
- `SubscribeRouteEvents(topics, event_types)` → server-streaming
  (ROUTE_SNAPSHOT / TOPIC_CREATE / TOPIC_DELETE / QUEUE_SCALE / BROKER_ONLINE /
  BROKER_OFFLINE), replaying current snapshots on subscribe
- `DescribeRouteTopology([topic])` → proxy→broker links + per-broker queue load

Compatibility rules: additive-only field evolution, all new fields optional,
`ProxyScope` defaults to local, no field number reuse.

Building the proto artifact: `org.apache.rocketmq:rocketmq-proto:2.3.0` is
generated from the `rocketmq-apis` repository (branch
`feature/rip-2-proxy-admin-grpc`), which carries the `ProxyAdminService`
contract and a self-contained Maven build (`mvn clean install` in the
rocketmq-apis checkout; protoc and grpc-java plugins come from Maven Central).
The apis repository is consumed as a local development dependency and is
intentionally NOT vendored or submoduled into this repository; CI/developers
install the artifact into their local repository once, then build this repo
normally.

## 5. Observability

The admin server exports (OpenTelemetry, honoring the proxy's metrics exporter
configuration):

- `rocketmq_proxy_admin_rpc_total{rpc_method, status=success|error, error_type?}`
  — error rate = rate(status="error")
- `rocketmq_proxy_admin_rpc_latency{rpc_method, status}` (ms histogram) — RT P50/P99

Transport-level failures (auth rejections, permission denials) and business
failures are both counted as errors; successful RPCs are counted once.

## 6. Configuration Reference

| Key | Default | Meaning |
|---|---|---|
| `proxyAdminEnabled` | true | D2 kill switch; false = admin server not started |
| `adminGrpcPort` | 8083 | dedicated admin gRPC port (<=0 disables) |
| `proxyAdminRequireAuth` | false | fail-closed credential enforcement |
| `proxyAdminPeerEndpoints` | [] | peer admin endpoints for D3 ALL_PROXIES fan-out |
| `proxyAdminPeerTimeoutMillis` | 3000 | per-peer fan-out timeout |
| `proxyAdminHeartbeatHistorySize` | 16 | heartbeat records kept per client |

## 7. Milestones

- M1 (this delivery): online client query — ListClients / DescribeClient /
  ByGroup / ByTopic with stable cursor pagination and cluster fan-out.
- M2 (this delivery): runtime config hot update, connection control (kick /
  disconnect), quota visualization & adjustment, route observation
  (SubscribeRouteEvents streaming + DescribeRouteTopology).
- M3/M4 (this delivery): POP receipt-handle diagnostics and batch consumption
  diagnostics from the proxy's own receipt-handle tracking.
- Future: Remoting client coverage under the same contract (D5), broker-side
  quota storage integration.

## 8. Acceptance Criteria Mapping

| Criterion | Status |
|---|---|
| RIP document + stable backward-compatible proto contract | this document + rocketmq-apis `admin.proto` |
| Client query RPCs merged; pagination scales with connection churn | D4 stable cursor; page cost O(pageSize) after sort |
| Independent ACL control, read-only/high-risk separation, least-privilege doc | D2 resources/actions + `docs/rip-2-least-privilege.md` |
| RPC RT & error-rate metrics | §5 instruments |
| E2E with RIP-1 dashboard | contract frozen for dashboard CLIENT-01 integration (cross-repo) |
