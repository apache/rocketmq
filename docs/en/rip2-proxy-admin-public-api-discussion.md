# RIP-2 Proxy Admin Public API Discussion Draft

## Purpose

This note started as a community-discussion draft for the RIP-2 Proxy Admin
online client query API. The current contest branch now carries the downstream
implementation and a companion `rocketmq-apis` proposal branch, so this file is
kept as the rationale and review checklist for the public contract.

The implementation branch now includes a generated public
`ProxyAdminServiceGrpc` adapter, local read model, authorization facade, metrics
hooks, lifecycle integration, independent admin gRPC server registration, and
cross-proxy coordinator seams. The remaining decision is the upstream protobuf
ownership, field numbering, artifact version, and publication path.

## Current Upstream Status

As of the final contest refresh on 2026-07-10, upstream RocketMQ still does not
contain an accepted public `ProxyAdminService` protobuf API. The companion API
proposal is open as <https://github.com/apache/rocketmq-apis/pull/112>, and the
RocketMQ implementation draft is open as
<https://github.com/apache/rocketmq/pull/10603>.

For local contest verification, the implementation branch consumes
`org.apache.rocketmq:rocketmq-proto:2.2.0-rip2-SNAPSHOT`, generated from the
companion `rocketmq-apis` proposal branch. Until the community accepts the
protobuf location and publishes an official artifact, the public API remains a
proposal even though the downstream endpoint is implemented and tested.

## Recommendation

Add a standalone public `ProxyAdminService` instead of extending the existing
`MessagingService`.

Rationale:

- Online client query is an administrative API, not client messaging traffic.
- Admin RPCs need different authorization, error handling, rate limiting, and
  operational controls from producer and consumer RPCs.
- A standalone service lets deployments expose, protect, or disable admin RPCs
  independently from the messaging service.
- The proxy implementation can register the service beside
  `GrpcMessagingApplication` while sharing one `DefaultGrpcMessagingActivity`
  and one read model.

M1 should expose only local proxy semantics by default. `ALL_PROXIES` and
`PROXY_ID` can remain in the enum as future-compatible values, but public
endpoints should reject them with `BAD_REQUEST` until the community accepts the
coordinator and peer transport semantics.

## Proposed RPCs

The proposal mirror is maintained in
`docs/en/rip2-proxy-admin-m1-public-api-draft.proto`. The authoritative proposal
source for local verification is
`../rocketmq-apis/apache/rocketmq/v2/admin.proto` on the
`rip2-proxy-admin-public-api` branch.

The proposed contest-facing unary RPCs are:

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

The response model returns an existing v2 `Status` plus either a `ProxyClient`
or a page of `ProxyClient` entries. Error responses should be status-only.

The public request shape should follow the RIP-2 issue filters:
`clientId`, `clientIdPrefix`, `group`, `topic`, `clientLanguage`,
`connectTimeStart`, `connectTimeEnd`, `pageNum`, and `pageSize`. `pageNum` is
1-based and `pageSize` is capped at 100. The branch still has an internal
page-token path for coordinator experiments, but that token representation is
not the contest-facing public contract.

The draft currently includes `proxy_id` in `ProxyClient` responses. Local M1
responses can populate it with the serving proxy name, and future cross-proxy
responses can use the same field to identify the source proxy for each client.
The field should still be confirmed during community API review because it is
part of the public response surface.

## Scope Semantics

`PROXY_SCOPE_UNSPECIFIED` should be treated as `PROXY_SCOPE_LOCAL_PROXY`.

`PROXY_SCOPE_LOCAL_PROXY` should query only clients connected to the current
proxy process. This is the M1 public behavior.

`PROXY_SCOPE_ALL_PROXIES` is reserved for fan-out through the internal
coordinator. It should stay gated until peer discovery, peer authorization,
page-token ownership, and failure semantics are accepted.

`PROXY_SCOPE_PROXY_ID` is reserved for querying one named proxy. Requests using
this scope must provide `proxy_id`; the adapter should reject missing or blank
`proxy_id` before request context creation or service invocation.

## Internal Cross-Proxy Experiment

This fork now contains a proto-free internal cross-proxy experiment to validate
the future scope semantics without changing the public API:

- `ProxyClientAdminScopeRouter` keeps `LOCAL_PROXY` on the local activity and
  routes internal `ALL_PROXIES` and `PROXY_ID` requests to a coordinator only
  when `enableProxyClientAdminCrossProxyQuery` is enabled.
- The coordinator fans out peer-local list requests, merges pages in stable
  `(client_id, proxy_id)` order, and owns `cp1:` coordinator page tokens with
  per-peer cursors and bounded token retention.
- A raw internal peer protocol and `ProxyClientAdminPeerGrpcTransport` allow
  static peer targets to be exercised without generated public admin stubs.
- Peer discovery and peer calls are wrapped by
  `TimedProxyClientAdminPeerClient`, so bounded waits surface as
  `PROXY_TIMEOUT` instead of generic internal errors.

These pieces are implementation evidence for the proposal, not a public API
commitment. The public endpoint should still expose only `LOCAL_PROXY` until the
community accepts the peer discovery, timeout, token ownership, and partial
failure semantics.

## Pagination

The contest-facing public API should use `pageNum` and `pageSize`. `pageNum` is
1-based, `pageSize` is capped at 100, and stable ordering is by client id for
local queries.

The internal M1 local read model already has a page-token path because local
results are sorted by client id and the coordinator experiment needs per-peer
cursors. That token path should remain internal unless the community later
chooses an opaque token contract for cross-proxy pagination.

Future cross-proxy pagination can still use coordinator-owned tokens internally
to carry scope, filters, last emitted `(client_id, proxy_id)`, per-peer cursors,
and token creation time. Expired coordinator tokens should be rejected before
peer fan-out.

## Endpoint Implementation Shape

The branch implements a dedicated
`GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase`.
It does not add admin methods to `GrpcMessagingApplication`.

Each generated unary method should stay thin:

- read gRPC metadata through `ProxyClientAdminContextFactory`.
- convert protobuf requests to internal DTOs through
  `ProxyClientAdminRequestConverter`.
- call `ProxyClientAdminEndpointExecutor`.
- translate `ProxyClientAdminPageView` and `ProxyClientAdminClientView` into
  protobuf responses.
- keep authorization, validation, pagination, metrics, and error mapping behind
  the existing activity, scope router, endpoint handler, and service layer.

`ProxyStartup.createProxyAdminGrpcBindableServices` registers the public
`GrpcProxyAdminApplication` on the independent admin gRPC server with the same
shared `DefaultGrpcMessagingActivity`.

## Compatibility

The public API should be additive:

- no changes to existing messaging RPCs.
- no change to producer or consumer client lifecycle behavior.
- no persistence or distributed registry requirement for M1.
- restart behavior stays unchanged; clients repopulate the read model through
  telemetry and heartbeat.

Public field numbers in the draft should be confirmed before generated classes
land. Once community review starts, avoid reshuffling field numbers.

## Open Questions

1. Should the protobuf live beside existing v2 messaging APIs in
   `rocketmq-apis`, or in a dedicated admin API file?
2. Is `ProxyAdminService` the accepted public service name?
3. Should cross-proxy enum values be present from day one but rejected until
   enabled, or should they be deferred entirely?
4. What deployment-level control should gate the public admin service
   registration?
5. Is `proxy.admin.client` the accepted client-query ACL resource name for
   `LIST` and `GET`, or should the resource name follow another admin-resource
   convention?
6. Should the first public release expose `proxy_id` in `ProxyClient` responses
   for local results, or return it only for future cross-proxy scopes?

## Implementation State In This Branch

This branch is ready for proxy-side review while the public API ownership
decision proceeds:

- local read model with stable pagination and secondary indexes.
- gRPC client lifecycle writes into the read model.
- internal `ClientAdminService` and `ProxyClientAdminActivity`.
- request DTOs and response views matching the proposed public model.
- scope mapper, internal page-token codec, and future public page-number
  adapter.
- endpoint executor and endpoint handler, including the M1 public
  `LOCAL_PROXY` scope gate before request context creation.
- generated `GrpcProxyAdminApplication` for `ListClients`, `DescribeClient`,
  `ListClientsByGroup`, and `ListClientsByTopic`.
- independent admin gRPC server registration through `ProxyStartup`.
- authorization facade and metrics hooks.
- internal coordinator, peer gRPC service, static peer transport, timeout
  handling, and coordinator-scope metrics for cross-proxy experiments without
  modifying public protobuf APIs.
