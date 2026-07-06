# RIP-2 Proxy Admin Public API Discussion Draft

## Purpose

This note is a community-discussion draft for the RIP-2 Proxy Admin online
client query API. It is intentionally documentation-only. It does not modify
`rocketmq-apis`, generate protobuf classes, or register a new public endpoint in
this fork.

The implementation branch already carries a proto-free internal adapter, local
read model, authorization facade, metrics hooks, lifecycle integration,
cross-proxy coordinator seams, and startup registration seams. The remaining
decision is where and how the public protobuf API should land.

## Current Upstream Status

After fetching `upstream/develop` at commit `2af604f3a` on 2026-07-06, this
RocketMQ repository does not contain a public `ProxyAdminService` protobuf API
or `.proto` source files to update directly. The proxy module consumes generated
`apache.rocketmq.v2.MessagingServiceGrpc` classes from the external
`rocketmq-apis` project.

Because of that ownership boundary, this fork should keep the API as a proposal
until the community confirms the protobuf location and compatibility process.

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

The documentation-only draft is maintained in
`docs/en/rip2-proxy-admin-m1-public-api-draft.proto`.

The proposed unary RPCs are:

- `ListClients`
- `DescribeClient`
- `ListClientsByGroup`
- `ListClientsByTopic`

The response model returns an existing v2 `Status` plus either a `ProxyClient`
or a page of `ProxyClient` entries. Error responses should be status-only.

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

## Pagination

Public page tokens should be opaque. Clients must not rely on token contents or
assume the token is a client id.

The internal M1 local read model uses the last emitted client id as its cursor
because local results are sorted by client id. The public adapter should encode
that internal token as a versioned opaque token and should reject malformed,
unknown-version, overlong, or noncanonical versioned tokens.

Future cross-proxy pagination should use coordinator-owned tokens that carry
scope, filters, last emitted `(client_id, proxy_id)`, and per-peer cursors. The
local page-token format should not become the cross-proxy public contract.

## Endpoint Implementation Shape

Once generated public protobuf classes are available, add a dedicated
`GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase`.
Do not add admin methods to `GrpcMessagingApplication`.

Each generated unary method should stay thin:

- read gRPC metadata through `ProxyClientAdminContextFactory`.
- convert protobuf requests to internal DTOs through
  `ProxyClientAdminRequestConverter`.
- call `ProxyClientAdminEndpointExecutor`.
- translate `ProxyClientAdminPageView` and `ProxyClientAdminClientView` into
  protobuf responses.
- keep authorization, validation, pagination, metrics, and error mapping behind
  the existing activity, scope router, endpoint handler, and service layer.

`ProxyStartup.createGrpcBindableServices` already has a default-empty
`ProxyAdminServiceFactory` seam so the future application can be registered
beside `GrpcMessagingApplication` with the same shared activity.

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
5. Are the proposed `LIST` and `GET` cluster-level ACL mappings sufficient for
   M1, or should client-query-specific resources be introduced later?
6. Should the first public release expose `proxy_id` in `ProxyClient` responses
   for local results, or return it only for future cross-proxy scopes?

## Implementation State In This Branch

This branch is ready for a thin generated endpoint adapter after the API
ownership decision:

- local read model with stable pagination and secondary indexes.
- gRPC client lifecycle writes into the read model.
- internal `ClientAdminService` and `ProxyClientAdminActivity`.
- request DTOs and response views matching the proposed public model.
- scope mapper and page-token codec.
- endpoint executor and endpoint handler, including the M1 public
  `LOCAL_PROXY` scope gate before request context creation.
- authorization facade and metrics hooks.
- startup service-registration seam for a future standalone admin application.
- internal peer gRPC service and static peer transport for coordinator
  experiments without modifying public protobuf APIs.
