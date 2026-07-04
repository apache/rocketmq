# RIP-2 Proxy Admin M1 Design

## Goals

RIP-2 Proxy Admin M1 adds the first internal building block for online client
query through RocketMQ Proxy. The milestone focuses on a local proxy read model
that is updated by the existing gRPC client lifecycle and can later be exposed
through an admin gRPC adapter.

M1 should support:

- Listing online clients connected to the current proxy.
- Describing one online client by client id.
- Listing online clients by consumer group.
- Listing online clients by topic.
- Stable ordering by client id and bounded pagination.
- Local-only semantics using `LOCAL_PROXY`.

## Non-Goals

M1 does not change `rocketmq-apis`, generate new public protobuf stubs, or define
the final cross-proxy protocol. It does not add a new public admin endpoint,
persistence, or a distributed client registry. The current branch includes
internal ACL and read-model metric hooks, but those hooks are not exposed as a
new public admin API until the adapter and protobuf ownership are resolved.

## Current Proxy and gRPC Structure

The current gRPC v2 entry point is `DefaultGrpcMessagingActivity`. It owns shared
instances of `GrpcClientSettingsManager` and `GrpcChannelManager`, then delegates
client lifecycle requests to `ClientActivity`.

`ClientActivity` handles:

- `telemetry`: receives client settings and registers producer or consumer
  channels.
- `heartbeat`: refreshes broker-side producer or consumer registration from
  cached client settings.
- `notifyClientTermination`: unregisters producer or consumer channels and
  removes cached settings.
- producer and consumer unregister listeners: remove local gRPC channels when
  existing broker-side managers report unregister events.

The existing broker-facing registration still uses `MessagingProcessor`,
`ProducerManager`, `ConsumerManager`, and `ClientChannelInfo`. M1 observes these
successful lifecycle paths and updates an independent read model without changing
the registration contract.

## API Draft

The public API shape is intentionally a draft because the protobuf ownership and
compatibility process should be discussed before changing `rocketmq-apis`.

## Public API Decision Record

The recommended public API direction is to add a standalone
`ProxyAdminService` instead of extending the existing `MessagingService`. Client
query is an administrative capability with different authorization, error, and
operational expectations from normal messaging traffic. Keeping it in a
separate service also lets Proxy register or gate admin endpoints independently
from client-facing messaging RPCs.

M1 should accept only `LOCAL_PROXY`. Requests for future scopes such as
`ALL_PROXIES` or `PROXY_ID` should return `BAD_REQUEST` until cross-proxy
semantics are designed and implemented.

Pagination tokens should be treated as opaque by public clients. The current
local implementation can continue using the last returned client id as the token
because all M1 results are sorted by client id, but the protobuf contract should
not promise that representation.

This fork should not directly modify `rocketmq-apis` for M1. The branch should
first carry the internal read model, authorization, metrics, and adapter seams,
plus this API proposal, so the final protobuf ownership and compatibility
decision can be discussed with the community before generated public stubs are
introduced.

## Implemented Internal Adapter Preparation

The branch now includes a proto-independent internal admin adapter surface. It
does not register a public gRPC service yet, but it gives the future endpoint a
small and tested boundary to call:

- `ProxyClientAdminActivity` owns the request execution boundary for client
  admin queries. It accepts `ProxyContext`, calls `AuthorizingClientAdminService`,
  and returns `ProxyClientAdminResult<T>` with an `apache.rocketmq.v2.Status`
  plus an optional body.
- `ProxyClientAdminResult` preserves the public status/body split expected by a
  gRPC endpoint while keeping the internal service API simple.
- `ProxyClientAdminClientView` and `ProxyClientAdminPageView` are public-facing
  response views. They avoid exposing the mutable internal read-model classes as
  the eventual protobuf adapter contract.
- `ProxyClientAdminListClientsRequest`,
  `ProxyClientAdminDescribeClientRequest`,
  `ProxyClientAdminListClientsByGroupRequest`, and
  `ProxyClientAdminListClientsByTopicRequest` mirror the proposed public request
  fields without importing generated admin protobuf classes.
- `ProxyClientAdminEndpointHandler` centralizes the future unary endpoint
  response flow: execute an activity action, convert thrown exceptions through
  `ResponseBuilder`, build a response from `Status` and optional body, and write
  it through `ResponseWriter`.
- `GrpcRequestPipelineFactory` extracts the existing gRPC context,
  authentication, authorization, and subject pipeline so a future standalone
  admin service can share the same request initialization behavior as
  `GrpcMessagingApplication`.
- `GrpcMessagingApplication.createDefaultActivity` and the shared-activity
  `create` overload make it possible for startup code to instantiate one
  `DefaultGrpcMessagingActivity`, register the existing messaging service, and
  pass the same activity/admin adapter to a future proxy admin service.

The request DTOs convert pagination, client type, and scope into
`ProxyClientQuery`. Page tokens are preserved as opaque strings at this layer.
The default scope is `LOCAL_PROXY`; unsupported future scopes are intentionally
carried through to the activity/service validation path so they produce the same
`BAD_REQUEST` semantics as direct internal calls.

The future generated endpoint should only translate protobuf messages to these
DTOs, call `ProxyClientAdminActivity`, and translate the result view back to a
protobuf response. Authorization, error mapping, metrics, pagination bounds, and
read-model queries should remain behind the existing activity/service boundary.
The generated unary methods should use `ProxyClientAdminEndpointHandler` for
the common result-to-`StreamObserver` flow.

### ListClients

Request:

```text
ListClientsRequest {
  string page_token;
  int32 page_size;
  ClientType client_type;
  ProxyScope scope; // M1: LOCAL_PROXY only
}
```

Response:

```text
ListClientsResponse {
  repeated Client clients;
  string next_page_token;
}
```

### DescribeClient

Request:

```text
DescribeClientRequest {
  string client_id;
  ProxyScope scope; // M1: LOCAL_PROXY only
}
```

Response:

```text
DescribeClientResponse {
  Client client;
}
```

### ListClientsByGroup

Request:

```text
ListClientsByGroupRequest {
  string group;
  string page_token;
  int32 page_size;
  ClientType client_type;
  ProxyScope scope; // M1: LOCAL_PROXY only
}
```

Response:

```text
ListClientsByGroupResponse {
  repeated Client clients;
  string next_page_token;
}
```

### ListClientsByTopic

Request:

```text
ListClientsByTopicRequest {
  string topic;
  string page_token;
  int32 page_size;
  ClientType client_type;
  ProxyScope scope; // M1: LOCAL_PROXY only
}
```

Response:

```text
ListClientsByTopicResponse {
  repeated Client clients;
  string next_page_token;
}
```

## Data Model

The internal model uses `ProxyClientInfo`:

- `clientId`: stable client identity from `ProxyContext`.
- `clientType`: gRPC `ClientType`.
- `groups`: consumer groups associated with the client.
- `topics`: producer publishing topics and consumer subscription topics.
- `language`: client language from request metadata.
- `remoteAddress` and `localAddress`: connection addresses.
- `clientVersion`: client version from request metadata.
- `connectTimeMillis`: first observed local connection time.
- `lastActiveTimeMillis`: most recent successful telemetry or heartbeat time.

`ProxyClientPage` returns a list of `ProxyClientInfo` plus a `nextPageToken`.
`ProxyClientQuery` carries optional group, topic, client type, page size, and
page token filters.

## Read Model and Indexes

`ProxyClientReadService` is an in-memory local read model. It owns:

- `clientId -> ProxyClientInfo`
- `group -> sorted clientId set`
- `topic -> sorted clientId set`
- `clientType -> sorted clientId set`

All listing results are ordered by client id. Upsert first removes the old
client's index entries and then writes the new snapshot. Remove deletes the
client record and all index entries. This makes repeated telemetry idempotent and
keeps group/topic changes from leaving stale index entries.

The service is synchronized in M1. That keeps the implementation simple and
consistent while lifecycle updates and admin reads are still local to one proxy
process. A future high-scale implementation can replace the internal maps with
lock-striped or immutable-snapshot indexes without changing the service API.

## Pagination

Pagination is bounded by `ProxyClientQuery.MAX_PAGE_SIZE`. Non-positive page
sizes use `DEFAULT_PAGE_SIZE`. Page tokens are based on the last client id
returned by the previous page. When a token is supplied, it must exist in the
filtered candidate set; otherwise `ProxyClientReadService` throws
`IllegalArgumentException`.

This gives stable pagination for the local snapshot and avoids offset-based
scans. If clients connect or disconnect between pages, a token can become
invalid for the filtered view and the caller should restart the query.

## Multi-Proxy Semantics

M1 supports `LOCAL_PROXY` only. A query observes clients connected to the current
proxy process and does not fan out to other proxies.

Future scopes can add:

- `ALL_PROXIES`: fan out to peer proxies and merge sorted pages.
- `PROXY_ID`: query one named proxy instance.
- broker-assisted discovery: query broker-side client metadata where available.

The M1 read model intentionally stores only local lifecycle state so cross-proxy
semantics can be added without changing the local index contract.

## ACL Plan

M1 should reuse existing cluster-level admin permissions:

- list operations require cluster-level `LIST`.
- describe operations require cluster-level `GET`.

The current internal implementation provides `ClientAdminAuthPolicy`,
`DefaultClientAdminAuthorizationService`, and `AuthorizingClientAdminService`.
The gRPC request pipeline also copies the authenticated access key into
`ProxyContext` as a `Subject`, and `ClientAdminRequestContext.from` derives the
admin request context from `ProxyContext`. This lets the future public adapter
authorize before delegating to read-model queries while keeping the first admin
surface consistent with existing management actions.
Topic-level or group-level ACL can be discussed later if the community wants
more granular visibility controls.

## Metrics Plan

The current internal implementation exposes read-model gauges for:

- current local online client count.
- current local online client count by `clientType`.
- current local group/topic index count.

It also records read-model upsert/remove mutation counters.

The current internal admin service wrapper records:

- admin query counters by operation and result code.
- admin query latency histograms.

The public adapter should reuse these low-cardinality operation and result labels
when the API surface is finalized.

## Error Semantics

Internal M1 errors:

- missing client id: `IllegalArgumentException`.
- invalid page token: `IllegalArgumentException`.
- client not found: `NoSuchElementException` from the admin service.
- missing internal request DTO: `IllegalArgumentException`.

Draft public adapter mapping:

- missing required field: `BAD_REQUEST`.
- invalid page token: `BAD_REQUEST`.
- unknown client id for `DescribeClient`: `NOT_FOUND`.
- unsupported scope: `BAD_REQUEST` until multi-proxy scopes are implemented.
- authorization failure: `UNAUTHORIZED`.
- internal failures: `INTERNAL_SERVER_ERROR`.

## Compatibility

M1 is additive. It does not change existing gRPC client behavior, public protobuf
definitions, broker registration, or client settings semantics. The original
three-argument `ClientActivity` constructor remains available. The new
four-argument constructor only allows tests and the default gRPC activity to
share a read model instance.

The read model is process-local and in-memory; restart behavior is unchanged.
After a proxy restart, clients repopulate the model through telemetry and
heartbeat.

## Test Plan

M1 tests cover:

- stable client id ordering and pagination.
- index refresh when upserting changed group/topic/type data.
- client and index deletion on remove.
- invalid page token rejection.
- consumer telemetry updating `ProxyClientReadService`.

Follow-up lifecycle tests should cover:

- producer telemetry updates. Done.
- heartbeat preserves `connectTimeMillis` and updates `lastActiveTimeMillis`.
  Done.
- termination removes client and indexes. Done.
- producer unregister listener removes client and indexes. Done.
- consumer unregister listener removes client and indexes. Done.

Internal adapter tests cover:

- request DTO conversion to `ProxyClientQuery`.
- default `LOCAL_PROXY` scope and opaque page-token pass-through.
- activity overloads for request DTOs.
- response view conversion.
- endpoint handler success response writing, error response writing, and thrown
  action error mapping.
- cross-package shared wiring for future admin gRPC application access to
  `ProxyClientAdminActivity`.
- missing request DTO, missing identifiers, not found, unsupported scope,
  authorization failure, and unexpected runtime error mapping.

## Public Endpoint Landing Route

After `rocketmq-apis` ownership and compatibility are confirmed, add or consume
generated admin protobuf classes for a standalone `ProxyAdminService`. The proxy
implementation should then add a dedicated `GrpcProxyAdminApplication extends
ProxyAdminServiceGrpc.ProxyAdminServiceImplBase`; it should not add admin RPCs
to the existing messaging application.

`ProxyStartup` can register the new application beside the existing messaging
service because `GrpcServerBuilder` already supports repeated `addService(...)`
calls for both `BindableService` and `ServerServiceDefinition`. The intended
startup shape is:

```text
GrpcServerBuilder.newBuilder(...)
  .addService(GrpcMessagingApplication...)
  .addService(GrpcProxyAdminApplication...)
  .addService(ChannelzService...)
  .addService(ProtoReflectionService...)
```

The endpoint adapter should stay thin:

- read headers and build `ProxyContext` through the existing request pipeline.
- translate proto requests to the internal request DTOs.
- call `ProxyClientAdminActivity`.
- translate `ProxyClientAdminPageView` and `ProxyClientAdminClientView` into
  proto responses.
- use `ProxyClientAdminEndpointHandler` to copy the `Status` from
  `ProxyClientAdminResult`, write the response, and keep exception-to-status
  behavior consistent with the internal adapter.

Cross-proxy fan-out, new ACL granularity, and new metrics labels should be added
behind the same service/activity boundary instead of being embedded into the
generated gRPC application.

## Implementation Order

1. Add the internal read model and focused unit tests. Done.
2. Wire the read model into gRPC client lifecycle paths. Done.
3. Add the internal admin service API. Done:
   `listClients`, `describeClient`, `listClientsByGroup`,
   `listClientsByTopic`.
4. Add admin service error mapping for missing ids, not found, and invalid page
   tokens. Done.
5. Add more lifecycle tests around producer telemetry, heartbeat timestamps,
   termination, and unregister listeners. Done.
6. Add the proto-independent internal admin activity, response views, request
   DTOs, and activity overloads. Done.
7. Add shared gRPC request-pipeline, activity wiring, and endpoint-handler seams
   for the future standalone admin gRPC application. Done.
8. Discuss public protobuf ownership before changing `rocketmq-apis`.
9. Add the public admin gRPC/protobuf adapter.
10. Wire the adapter through `AuthorizingClientAdminService`; internal ACL policy,
   request context propagation, and service are already in place.
11. Extend metrics with admin query counters and latency histograms. Done.
12. Add a synthetic 1M-client benchmark or simulation. Done.
