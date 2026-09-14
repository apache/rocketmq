# Proxy Admin Management Interface

> 中文版见 `docs/proxy-admin-zh.md`。

## 1. Motivation

RocketMQ 5.0 moved client access behind the stateless Proxy, but operations still
observe clients through broker-side structures (`ConsumerManager` on the broker,
Remoting-era admin commands). gRPC clients attached to a Proxy are invisible to
those tools: the control plane cannot answer "which SDK clients are online, what
do they subscribe to, are they healthy" without indirect metrics heuristics.

A control-plane dashboard needs a standard server-side interface to read complete
gRPC client data. This change implements that interface on the Proxy itself.

## 2. Contract choice: upstream `Admin` service

The gRPC contract is the **upstream `Admin` service** defined in
`apache/rocketmq/v2/admin.proto` of the `apache/rocketmq-apis` repository (main
branch, consumed as a git submodule — see §6). This implementation deliberately
does NOT introduce a bespoke `ProxyAdminService` in the protocol layer: the
control-plane RPC surface lives in the same versioned, community-reviewed proto
that the dashboard and multi-language SDKs already consume, and stays
additive-compatible with upstream evolution.

The proxy implements `AdminGrpc.AdminImplBase` (all 16 RPCs) and binds it on a
dedicated admin gRPC server.

## 3. Goals

1. A dedicated, independent gRPC Admin server on the Proxy, isolated from the
   data-plane `MessagingService`.
2. Stable, backward-compatible contract from rocketmq-apis main (`Admin`
   service, `apache/rocketmq/v2/admin.proto`).
3. First-class authorization under dedicated `proxy.admin.*` ACL 2.0 resources
   with read-only / high-privilege action separation.
4. Cluster-wide semantics without leaking topology into the protocol: every
   proxy exposes a cluster-visible view of connection/subscription data and
   forwards client-directed RPCs to the proxy that owns the client.

## 4. Design Decisions

### D1 — Service placement

The upstream `Admin` service is bound on its own gRPC server/port
(`grpcAdminServerPort`, default 8088) with its own interceptor chain, separate
from the data plane (8081). The whole surface is gated by a global kill switch
`grpcAdminServerEnable`, which **defaults to false**: the admin server is opt-in
and is not started unless explicitly enabled.

Rationale: control-plane traffic must never contend with the data plane, and the
admin port can be firewalled to the operations network only. The admin server
intentionally does NOT expose channelz or proto reflection. It reuses the data
plane's `GrpcChannelManager` / `GrpcClientSettingsManager` so online clients are
visible to admin queries. Runtime executors introduced by this feature are
instance-owned and registered in the Proxy lifecycle: the admin timeout scheduler
stops with `ProxyAdminGrpcService`, and the blocking gateway executor stops with
`AdminService`.

### D2 — Authorization: dedicated `proxy.admin.*` resources

Credentials arrive in the standard gRPC `Authorization` metadata (same scheme as
the data plane, ACL 2.0 signature). Authentication runs as an interceptor, ordered
**after** the standard pipeline's `HeaderInterceptor` (registered before it, since
gRPC invokes interceptors in reverse registration order). That ordering is
load-bearing: `HeaderInterceptor` replaces the inbound `x-mq-channel-id` with the
channel id derived from the transport, and authentication results are cached per
channel id, so authenticating on the client-supplied value would let a request on
one connection reuse another connection's cached success and skip signature
verification.

Every RPC maps to one resource + one action:

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
- cluster auth disabled, `grpcAdminServerAuthEnable=false` → open surface (same
  semantics as the data plane);
- cluster auth enabled → standard authenticate + authorize pipeline against the
  per-method `proxy.admin.*` resource;
- `grpcAdminServerAuthEnable=true` → fail-closed. The admin surface must be able
  to both authenticate the caller **and** enforce the per-method ACL, so this
  mode requires the cluster-wide authentication **and** authorization switches to
  be on. If either is off every request is refused (authentication off →
  `UNAUTHENTICATED`; authorization off → `FAILED_PRECONDITION`) rather than served
  without a real permission check. This is deliberate: the ACL 2.0 evaluator is
  itself gated by the cluster authorization switch, so serving requests with
  authorization off would silently pass every call, including the destructive
  ones.

Audit: every served RPC writes `[PROXY-ADMIN-AUDIT] subject/method/resource/
action/sourceIp` to the auth audit logger (satisfies the Console-user + AK +
resource + operation audit tuple requirement together with the ACL 2.0 engine's
own audit log).

### D3 — Multi-proxy semantics: cluster-visible, no topology in the protocol

A gRPC client is attached to exactly one proxy at a time, but a dashboard must
still see the whole cluster from any single admin endpoint. Two mechanisms make
that work without adding scope/peering fields to the protocol:

- **Connection / subscription data is cluster-visible.** The listing RPCs
  (`ListConsumerConnection`, `ListSubscription`, `DescribeSubscription`) merge
  the broker-side `ConsumerConnection` view (which covers remoting clients, and
  gRPC clients replicated between proxies by `HeartbeatSyncer`) with this proxy's
  own `ConsumerManager` (which covers the gRPC v2 clients attached locally). The
  result is deduplicated by group + topic, so the answer does not scale with the
  number of online consumers and does not depend on which proxy is queried.
- **Client-directed RPCs are forwarded to the owning proxy.**
  `PrintThreadStackTrace`, `VerifyMessage` and `GetConsumerRunningInfo` need the
  live telemetry stream that only the owning proxy holds. `ProxyAdminForwarder`
  detects a client owned by a peer (its `ConsumerManager` entry is a
  `RemoteChannel` whose `getRemoteProxyIp()` is the peer's `localServeAddr`) and
  forwards the whole RPC — original metadata included — to the peer's admin port,
  guarding against loops with the `x-mq-admin-forwarded` header. The peer admin
  port is assumed uniform across the cluster (the heartbeat sync payload does not
  carry it); a cluster running heterogeneous admin ports would need that added to
  the heartbeat record first.

`GetProxyRuntimeStats` intentionally reports the **local** process (its own
connection/producer/consumer counts), so a dashboard that wants per-node stats
queries each proxy directly.

### D4 — Data sources per RPC

| Source | RPCs | Mechanism |
|---|---|---|
| Cluster-visible connections | ListConsumerConnection, ListSubscription, DescribeSubscription | broker-side `ConsumerConnection` (remoting + peer-synced clients) merged with this proxy's `ConsumerManager` (local gRPC v2 clients) |
| Client-directed telemetry relay | PrintThreadStackTrace, VerifyMessage, GetConsumerRunningInfo | telemetry command relayed to the owning `GrpcClientChannel`; forwarded to the peer proxy when the client is remote (D3) |
| Broker gateway (async, multi-broker fan-out) | DescribeTopicStatus, DescribeGroupAccumulation, ResetGroupOffset, QueryMessage, QueryTimeSpan, GetTopicRoute, DeleteSubscription | the proxy's own async `AdminService` remoting gateway; each broker hop is bounded by its own deadline so a slow/unresponsive broker cannot hang the RPC |
| Proxy producer path | AdminSendMessage | `MessagingProcessor.sendMessage` (system properties honoured for timer / FIFO messages) |
| Proxy runtime | ChangeLogLevel | relocated logback API on the root logger |

Every broker-facing call goes through the asynchronous `AdminService` gateway and
is fanned out to all relevant brokers concurrently, so the gRPC executor thread is
never blocked.

### D5 — Protocol coverage

The connection/subscription listings are cluster-visible and include both
remoting clients (via the broker-side `ConsumerConnection`) and gRPC v2 clients
(via the proxy's `ConsumerManager`). The client-directed telemetry RPCs
(`PrintThreadStackTrace`, `VerifyMessage`, `GetConsumerRunningInfo`) operate on the
gRPC v2 client that holds a proxy telemetry stream, with cross-proxy forwarding;
their full running-info payload is bounded by what the telemetry protocol can
carry (see §8, honest boundaries).

## 5. Capability mapping

| Requirement | Delivered as | Notes |
|---|---|---|
| ListClients (filter by group/topic/clientId prefix, paged) | `ListConsumerConnection(group[, topic])` | clientId-prefix filter and pagination are not expressible in the upstream contract; documented boundary of this partial delivery |
| DescribeClient (SDK version, subscriptions, heartbeat, auth, Pop progress) | `DescribeSubscription` (per-client subscriptions) + `GetConsumerRunningInfo` (subscriptions + running info) | heartbeat/auth-status/Pop-progress fields do not exist in the upstream contract |
| ListClientsByGroup / ListClientsByTopic | `ListConsumerConnection(group[, topic])` | fully covered |
| Multi-proxy cluster aggregation | cluster-visible connection data + client-directed RPC forwarding | see D3 |

M2+ items (config hot update, quotas, connection control, Pop/batch diagnostics,
route observation streaming) are not part of the upstream `Admin` contract and
are out of scope for this delivery.

## 6. Building the proto sources

The `apache/rocketmq-apis` repository (main branch, containing
`apache/rocketmq/v2/admin.proto`) is consumed as a **git submodule**
(`rocketmq-apis/`, see `.gitmodules`); CI workflows check it out with
`submodules: true`.

- **Maven**: the `rocketmq-proto` module generates the Java + gRPC stubs for
  `apache/rocketmq/v2/{definition,service,admin}.proto` at build time via
  `protobuf-maven-plugin`; its version follows the reactor (`${revision}`), so
  no published proto artifact is required to build this repository.
- **Bazel**: `//rocketmq-proto:rocketmq-proto` builds the same classes through a
  `genrule` over the submodule (surfaced as the `@rocketmq_apis` external
  repository), using pinned `protoc` / `protoc-gen-grpc-java` binaries and the
  well-known-type protos.
- Other consumers (dashboard, SDKs) continue to use the artifact published by
  rocketmq-apis; only this repository builds the proto from source.

## 7. Configuration Reference

| Key | Default | Meaning |
|---|---|---|
| `grpcAdminServerEnable` | false | kill switch; the admin surface is opt-in. Set `true` to start the admin gRPC server; `false` (default) = not started |
| `grpcAdminServerPort` | 8088 | dedicated admin gRPC port (<=0 disables) |
| `grpcAdminServerAuthEnable` | false | fail-closed mode; requires cluster authentication **and** authorization to be enabled (see D2) |
| `grpcAdminServerRequestTimeoutMillis` | 3000 | per-broker-hop deadline for the broker calls an admin RPC fans out to |

## 8. Honest boundaries

The admin surface never fills a field it cannot truthfully supply; the following
are answered with an explicit status or left unset rather than faked:

- **DeleteSubscription is effectively a no-op in open source.** Nothing in the
  open-source codebase writes `SubscriptionGroupConfig.subscriptionDataSet` (the
  broker only reads it), so there is no persisted per-topic subscription to
  remove. The RPC returns `NOT_FOUND` with an explanation and **leaves the
  consumer group and its offsets untouched**. Distributions that need this RPC
  back it with an external subscription store that populates the same field.
- **QueryMessage** supports lookup by `message_id` (unique-key index) and
  `message_key`. The `subscription` / `lite_topic` / pure time-range scan variants
  need a paging cursor the open-source broker does not provide and return
  `BAD_REQUEST`.
- **GetProxyRuntimeStats** leaves `in_tps` / `out_tps` unset: the open-source
  proxy keeps no per-process throughput counter, and reporting 0 would be
  indistinguishable from an idle proxy.
- **GetConsumerRunningInfo** over a gRPC v2 client returns only `subscriptions`:
  the v2 telemetry protocol has no reply message carrying `properties`,
  `message_queue_table` or `consume_status_table`. The RPC returns `OK` with an
  explanatory status message rather than an empty shell.
- **DescribeTopicStatus** leaves `create_timestamp` / `tags` unset because the
  broker does not record a topic creation time.

## 9. Milestones

- M1 (this delivery): online client query over the upstream `Admin` service —
  ListConsumerConnection / DescribeSubscription / GetConsumerRunningInfo /
  DescribeGroupAccumulation, served cluster-visibly with cross-proxy forwarding,
  plus the full 16-RPC `Admin` surface for dashboard integration.
- Future (needs upstream proto evolution): clientId-prefix filter / pagination
  for client listings, heartbeat & auth-status fields, and the M2+ surfaces
  (config/quota/connection/route observation).

## 10. Acceptance Criteria Mapping

| Criterion | Status |
|---|---|
| Design document + stable backward-compatible proto contract | this document + upstream `admin.proto` (rocketmq-apis main) |
| Client query RPCs merged into the server repo | `ProxyAdminGrpcService` implements all 16 `Admin` RPCs on the proxy |
| Independent ACL control, read-only/high-risk separation, least-privilege doc | D2 resources/actions + §11 (Least-Privilege Configuration Guide) below |
| E2E with the dashboard | contract = upstream `Admin` service, frozen for dashboard integration (cross-repo) |

## 11. Least-Privilege Configuration Guide

The admin surface authorizes every RPC against dedicated `proxy.admin.*`
ACL 2.0 resources (see §4 decision D2 above). This section shows the
minimum-permission policy for each operational role.

### 11.1 Resource & Action Model

Resources (ACL 2.0 keys; modeled as cluster-typed literals with reserved names):

| Resource key | Protects |
|---|---|
| `cluster:proxy.admin.client` | online client query & client diagnostics |
| `cluster:proxy.admin.config` | proxy runtime log-level change |
| `cluster:proxy.admin.connection` | client-directed telemetry commands: thread stack, verify message (HIGH) |
| `cluster:proxy.admin.route` | topic route view |
| `cluster:proxy.admin.ops` | broker-facing ops: stats/topic status/message query (read) and reset offset / delete subscription / admin send (HIGH) |

Action classes:

- Read-only: `Get`, `List`
- High privilege (mutating / disruptive): `Update`, `Delete`, `Pub`

The server maps every RPC to exactly one (resource, action) pair; granting a
read-only action can never authorize a high-privilege RPC.

### 11.2 Role Templates

All commands run against any broker/namesrv of the cluster (ACL 2.0 storage).
Create users first:

```bash
sh mqadmin createUser -n <namesrv-addr> -u <username> -p <password>
```

#### Role A — Read-only observer (dashboard service account)

Online clients, subscriptions, accumulation, diagnostics, config/route views.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List \
  -d Allow
```

Note: `Get,List` on `proxy.admin.ops` covers the read-only broker-facing RPCs;
the mutating ops RPCs require `Update`/`Delete`/`Pub` and stay denied.

#### Role B — On-call operator (observer + client diagnostics commands)

Role A plus the ability to direct telemetry commands (thread stack / verify
message) at a connected client.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-oncall \
  -r cluster:proxy.admin.connection \
  -a Update \
  -d Allow
# plus the Role A grant above
```

#### Role C — Admin (full control, break-glass)

Offset reset, subscription deletion, admin send, client diagnostics commands.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-admin \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.connection,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List,Update,Delete,Pub \
  -d Allow
```

(Keep Role C accounts to a minimum; every use is recorded in the auth audit
log with the `[PROXY-ADMIN-AUDIT]` prefix.)

#### Environment restriction (recommended)

Restrict admin access to the operations network via the `-i` sourceIp option:

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client \
  -a Get,List \
  -d Allow \
  -i 10.0.0.0/8
```

### 11.3 Fail-Closed Mode

By default the admin server follows the cluster-wide authentication/authorization
switches (same behavior as the data plane). Fail-closed mode makes the admin
surface refuse to serve anything it cannot fully authorize:

```properties
# proxy.json / -D grpcAdminServerAuthEnable=true
grpcAdminServerAuthEnable: true
```

With `grpcAdminServerAuthEnable=true`, the admin surface requires the cluster
authentication **and** authorization switches to be enabled. If authentication is
off, requests are rejected with `UNAUTHENTICATED`; if authorization is off, they
are rejected with `FAILED_PRECONDITION`. This guarantees the `proxy.admin.*` ACL
is genuinely enforced — use it when the admin port cannot be network-isolated.

### 11.4 Enabling / Disabling the Surface

The surface is **off by default** (`grpcAdminServerEnable=false`). To enable it:

```properties
grpcAdminServerEnable: true    # start the admin gRPC server on grpcAdminServerPort
```

To keep it disabled (the default), leave `grpcAdminServerEnable: false`, or set
`grpcAdminServerPort` to 0 / negative — the admin gRPC server is then not started
at all.

### 11.5 Audit

Every served admin RPC logs subject (Console login user / AK), method, resource,
action and source IP to the auth audit logger; denied requests are logged by the
ACL 2.0 engine itself. This satisfies the four-tuple audit requirement
(Console user + AK + resource + operation).
