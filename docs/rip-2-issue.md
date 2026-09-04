# [RIP-2] Proxy Admin Standardized Management Interface

## Background

RocketMQ 5.0 moved client access behind the stateless Proxy, but operations still
observe clients through broker-side structures (`ConsumerManager` on the broker,
Remoting-era admin commands). **gRPC clients attached to a Proxy are invisible to
those tools**: the control plane cannot answer *"which SDK clients are online, what
do they subscribe to, are they healthy?"* without indirect metrics heuristics.

RIP-1 (Control Plane 5.0 dashboard, requirement `CLIENT-01`) explicitly depends on
a standard server-side interface to read complete gRPC client data. **There is
currently no such interface on the Proxy.**

## Problem Statement

1. **No admin gRPC surface on the Proxy.** The data-plane `MessagingService`
   gRPC service is not designed for control-plane queries (client enumeration,
   subscription inspection, diagnostics). Operators must SSH into brokers and
   run Remoting-era `mqadmin` commands, which cannot see gRPC clients.
2. **No least-privilege authorization for admin operations.** Existing admin
   operations share the broker ACL scope; there is no way to grant a read-only
   observer vs. a break-glass admin on the Proxy itself.
3. **No documented multi-proxy story.** Each Proxy only knows its own clients;
   there is no documented mechanism to aggregate client state across proxies.
4. **No self-service observability.** Admin RPCs are not instrumented with their
   own RT / error-rate metrics, so admin-interface health is invisible to
   monitoring.
5. **No protocol-pure admin path.** Broker-internal wire types leak into admin
   tooling, coupling every consumer to internal remoting classes.

## Proposal

Implement **RIP-2: Proxy Admin Standardized Management Interface** — the
**upstream `Admin` gRPC service** (`apache/rocketmq/v2/admin.proto`,
`apache/rocketmq-apis` main branch, proto `2.2.0`) bound by the Proxy on a
dedicated admin gRPC server, isolated from the data plane, with fine-grained
ACL 2.0 authorization.

The proxy serves all 16 `Admin` RPCs; the RIP-2 M1 online-client-query
capabilities map onto the contract as follows:

| RIP-2 M1 requirement | Delivered as |
|---|---|
| ListClients (filter by group/topic) | `ListConsumerConnection(group[, topic])` |
| DescribeClient (subscriptions, SDK info) | `DescribeSubscription` + `GetConsumerRunningInfo` |
| ListClientsByGroup / ListClientsByTopic | `ListConsumerConnection(group[, topic])` |
| Multi-proxy aggregation (local view OR cluster) | local view per proxy node |

### Goals

1. A dedicated gRPC Admin server on the Proxy, served on its own port
   (`adminGrpcPort`, default **8083**), isolated from the data-plane
   `MessagingService`. A global kill switch `proxyAdminEnabled` disables the
   whole surface.
2. A stable, backward-compatible contract: the upstream `Admin` service from
   rocketmq-apis main (`org.apache.rocketmq:rocketmq-proto:2.2.0`) — no bespoke
   protocol additions required.
3. First-class authorization under dedicated `proxy.admin.*` ACL 2.0 resources
   with read-only (`Get`/`List`) / high-privilege (`Update`/`Delete`/`Pub`)
   action separation.
4. The admin surface exposes its own call RT and error-rate metrics
   (OpenTelemetry).
5. Multi-Proxy semantics: each proxy serves its **local view**; cluster-wide
   pictures are merged upstream (RIP-1 dashboard) by querying every proxy and
   deduplicating by `client_id`.

### Non-Goals (this iteration)

- Remoting client coverage (Remoting clients remain observable via existing
  broker channels).
- Client-listing pagination / clientId-prefix filters (not expressible in the
  upstream contract; future proto evolution).
- M2+ surfaces (config hot update, quotas, connection kick, Pop/batch
  diagnostics, route-event streaming) — they have no upstream contract yet.

## Design Decisions

| ID | Decision | Summary |
|----|----------|---------|
| D1 | Service placement | Upstream `Admin` service bound on its own gRPC server/port (8083), separate from the data plane. Intentionally does NOT expose channelz or proto reflection; reuses the data plane's `GrpcChannelManager`/`GrpcClientSettingsManager`. |
| D2 | Authorization | Every RPC maps to one `proxy.admin.*` resource + one action. Resources modeled as cluster-typed literals (`cluster:proxy.admin.<module>`). Fail-closed `proxyAdminRequireAuth` mode. Audit logging per served RPC. |
| D3 | Multi-proxy semantics | Local view per proxy (the "二选一" local option). Cluster-wide views are merged upstream by the dashboard; no peering/scope fields are added to the protocol. |
| D4 | Data sources | Client lists/subscriptions from proxy-local channel & settings managers; client-directed diagnostics via telemetry commands; broker-internal data exclusively through the proxy's own `AdminService` remoting gateway (multi-broker fan-out). |
| D5 | Protocol coverage | This iteration tracks gRPC clients; Remoting clients remain observable through broker-side channels. |

### D2 — Authorization Matrix

| Resource | RPCs | Action |
|----------|------|--------|
| `proxy.admin.client` | ListSubscription / ListConsumerConnection | List |
| `proxy.admin.client` | DescribeSubscription / DescribeGroupAccumulation / GetConsumerRunningInfo / QueryTimeSpan | Get |
| `proxy.admin.config` | ChangeLogLevel | Update |
| `proxy.admin.connection` | PrintThreadStackTrace / VerifyMessage | Update (high privilege) |
| `proxy.admin.route` | GetTopicRoute | Get |
| `proxy.admin.ops` | GetProxyRuntimeStats / DescribeTopicStatus / QueryMessage | Get |
| `proxy.admin.ops` | ResetGroupOffset | Update (high privilege) |
| `proxy.admin.ops` | DeleteSubscription | Delete (high privilege) |
| `proxy.admin.ops` | AdminSendMessage | Pub (high privilege) |

### RPC Surface — `Admin` service (16 RPCs, all served by the Proxy)

- Online client query (RIP-2 M1): `ListConsumerConnection`,
  `DescribeSubscription`, `GetConsumerRunningInfo`, `DescribeGroupAccumulation`
- Proxy-local state: `GetProxyRuntimeStats`, `ListSubscription`, `ChangeLogLevel`
- Client-directed diagnostics: `PrintThreadStackTrace`, `VerifyMessage`
- Broker-facing (via the proxy's own remoting gateway): `DescribeTopicStatus`,
  `QueryMessage`, `QueryTimeSpan`, `ResetGroupOffset`, `DeleteSubscription`,
  `AdminSendMessage`, `GetTopicRoute`

### Observability

- `rocketmq_proxy_admin_rpc_total{rpc_method, status, error_type?}` — error rate
- `rocketmq_proxy_admin_rpc_latency{rpc_method, status}` (ms histogram) — RT P50/P99

### Configuration Reference

| Key | Default | Meaning |
|-----|---------|---------|
| `proxyAdminEnabled` | true | Kill switch; false = admin server not started |
| `adminGrpcPort` | 8083 | Dedicated admin gRPC port (≤0 disables) |
| `proxyAdminRequireAuth` | false | Fail-closed credential enforcement |

## Acceptance Criteria

| Criterion | Status |
|-----------|--------|
| RIP document + stable backward-compatible proto contract | `docs/rip-2-proxy-admin.md` + upstream rocketmq-apis `admin.proto` |
| Client query RPCs merged into the server repo | `ProxyAdminGrpcService` implements all 16 `Admin` RPCs on the proxy |
| Independent ACL control, read-only/high-risk separation, least-privilege doc | D2 resources/actions + `docs/rip-2-least-privilege.md` |
| RPC RT & error-rate metrics | `ProxyAdminMetricsManager` instruments |
| E2E with RIP-1 dashboard | Contract = upstream `Admin` service, frozen for dashboard CLIENT-01 integration (cross-repo) |

## References

- RIP-1 Control Plane 5.0 dashboard (requirement `CLIENT-01`)
- `docs/rip-2-proxy-admin.md` — full RIP proposal
- `docs/rip-2-least-privilege.md` — least-privilege configuration guide
- `apache/rocketmq-apis` main branch (`apache/rocketmq/v2/admin.proto`) — proto contract
