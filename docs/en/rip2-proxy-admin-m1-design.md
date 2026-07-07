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
- telemetry stream closure or gRPC status errors: removes the local read-model
  entry when the stream completes or reports a `StatusRuntimeException`.

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

The first public API should accept only `LOCAL_PROXY` unless the community also
accepts the internal cross-proxy coordinator semantics. Requests for future
scopes such as `ALL_PROXIES` or `PROXY_ID` should return `BAD_REQUEST` from a
public endpoint until that endpoint is explicitly wired to a reviewed peer
transport and coordinator contract.

Pagination tokens should be treated as opaque by public clients. The current
local read model continues using the last returned client id internally because
all M1 results are sorted by client id, while the public adapter wraps that value
in a versioned `v1:` base64url token. Legacy bare client-id tokens are still
accepted by the internal adapter to keep tests and early local callers
compatible, but the protobuf contract should not promise that representation.

This fork should not directly modify `rocketmq-apis` for M1. The branch should
first carry the internal read model, authorization, metrics, and adapter seams,
plus this API proposal, so the final protobuf ownership and compatibility
decision can be discussed with the community before generated public stubs are
introduced.

The documentation-only API sketch is captured in
`docs/en/rip2-proxy-admin-m1-public-api-draft.proto`; it is not part of Maven or
protobuf generation.

A community-ready discussion draft is captured in
`docs/en/rip2-proxy-admin-public-api-discussion.md`. It summarizes the proposed
standalone service, M1 local-only behavior, page-token contract, endpoint
implementation shape, compatibility notes, and open questions for the
`rocketmq-apis` ownership decision.

## Implemented Internal Adapter Preparation

The branch now includes a proto-independent internal admin adapter surface. It
does not register a public gRPC service yet, but it gives the future endpoint a
small and tested boundary to call:

- `ProxyClientAdminActivity` owns the request execution boundary for client
  admin queries. It accepts `ProxyContext`, calls `AuthorizingClientAdminService`,
  and returns `ProxyClientAdminResult<T>` with an `apache.rocketmq.v2.Status`
  plus an optional body. It also enforces the M1 `LOCAL_PROXY` scope before
  invoking authorization or read-model queries, and canonicalizes low-level
  `ProxyClientQuery` overloads so accidental `proxyId` filters are removed
  before authorization, metrics, or delegate execution.
- `ProxyClientAdminResult` preserves the public status/body split expected by a
  gRPC endpoint while keeping the internal service API simple.
- `ProxyClientAdminClientView` and `ProxyClientAdminPageView` are public-facing
  response views. They avoid exposing the mutable internal read-model classes as
  the eventual protobuf adapter contract. The views require a nonblank client
  id, reject null client entries in pages, snapshot collections, trim nullable
  string metadata to empty public strings, normalize repeated `groups` and
  `topics` entries by trimming, de-duplicating, and dropping blank values, and
  normalize blank public next-page tokens to an empty string.
- `ProxyClientAdminListClientsRequest`,
  `ProxyClientAdminDescribeClientRequest`,
  `ProxyClientAdminListClientsByGroupRequest`, and
  `ProxyClientAdminListClientsByTopicRequest` mirror the proposed public request
  fields without importing generated admin protobuf classes. They normalize
  request string fields at the adapter boundary so surrounding whitespace is
  trimmed and blank strings become missing values before validation. They also
  require a nonblank `proxy_id` for the explicit `PROXY_ID` scope before context
  creation, and preserve `proxy_id` only for that scope. Direct DTO use and future
  protobuf conversion therefore share the same M1 local and broadcast-scope
  semantics.
- `DefaultClientAdminService` also canonicalizes `LOCAL_PROXY` queries before
  reading the model, dropping accidental `proxyId` filters from direct internal
  callers while still rejecting future non-local scopes in M1.
- `ProxyClientAdminRequestConverter` centralizes the future proto-to-internal
  DTO mapping from public scalar fields, so generated unary methods can keep
  request conversion out of the RPC method bodies once `ProxyAdminService`
  lands. It drops `proxy_id` for the default or explicit public `LOCAL_PROXY`
  scope and for `ALL_PROXIES`, preserving it only with the future `PROXY_ID`
  scope so broadcast-style queries cannot accidentally carry a target-proxy
  filter into coordinator page tokens. The future public endpoint shell still
  rejects non-`LOCAL_PROXY` scopes before request context creation for M1; the
  converted cross-proxy DTOs are reserved for the internal scope router and for a
  later public rollout after the coordinator contract is accepted.
- `ProxyClientAdminPageTokenCodec` is the adapter boundary for public pagination
  tokens. M1 encodes internal last-client-id tokens as versioned `v1:`
  base64url public tokens, accepts legacy bare client-id tokens only for early
  internal compatibility, rejects unknown versioned public tokens, normalizes
  blank public tokens to "no token", rejects versioned tokens whose decoded
  read-model token is not already in canonical trimmed form, rejects versioned
  public tokens that are not the canonical no-padding encoding produced by the
  codec, and normalizes blank internal next tokens to an empty public string.
- `ProxyClientAdminScopeMapper` is the adapter boundary for public proxy scope
  values. It maps missing or `PROXY_SCOPE_UNSPECIFIED` public scope values to
  internal `LOCAL_PROXY`, maps prefixed public values such as
  `PROXY_SCOPE_ALL_PROXIES` and `PROXY_SCOPE_PROXY_ID` into the internal request
  model, rejects unprefixed internal enum names at the public adapter boundary,
  and rejects unknown scope names before they reach the service layer.
- `ProxyClientAdminEndpointHandler` centralizes the future unary endpoint
  response flow: execute an activity action, convert thrown exceptions through
  `ResponseBuilder`, build a response from `Status` and optional body, and write
  it through `ResponseWriter`.
- `ProxyClientAdminEndpointExecutor` is a proto-independent shell for generated
  unary admin methods. It adapts the proto request to the internal request DTO
  before creating the `ProxyContext`, so malformed public request fields such as
  invalid page tokens and M1-disabled public cross-proxy scopes fail at the
  adapter boundary without running the admin request pipeline. Once the DTO is
  built, it creates the `ProxyContext`, delegates to
  `ProxyClientAdminEndpointHandler`, and routes context or request-adapter
  failures through the same status conversion path. It offers explicit-header
  overloads for tests and adapter seams, plus no-header
  overloads that read `GrpcConstants.METADATA` from
  `Context.current()` to match normal generated gRPC method bodies. It also
  requires the context factory to return a non-null `ProxyContext` before the
  endpoint handler can run, so broken admin context initialization is reported as
  a status response instead of leaking into activity execution.
- `ProxyClientAdminContextFactory` runs the admin gRPC context pipeline and
  builds a `ProxyContext` for future admin RPCs without applying the messaging
  RPC client-id requirement or generic messaging authorization. Admin
  authorization is based on the authenticated subject carried in the context
  and is performed by `AuthorizingClientAdminService`.
- `GrpcRequestPipelineFactory` extracts the existing gRPC context,
  authentication, and subject pipeline so a future standalone admin service can
  share the same request initialization behavior as `GrpcMessagingApplication`
  while keeping admin authorization behind the client-admin service facade. It
  also exposes
  `createProxyClientAdminContextFactory(...)` so the admin endpoint wiring can
  reuse the shared pipeline without depending on messaging RPC client-id
  validation.
- `GrpcMessagingApplication.createDefaultActivity` and the shared-activity
  `create` overload make it possible for startup code to instantiate one
  `DefaultGrpcMessagingActivity`, register the existing messaging service, and
  pass the same activity/admin adapter to a future proxy admin service. The
  default activity also exposes the shared `ProxyClientAdminEndpointExecutor`
  that future generated admin service methods should call after proto classes
  are available. The shared-activity factory rejects a missing activity so future
  dual-service wiring fails during startup construction instead of running with a
  null messaging/admin activity.
- `ProxyClientAdminPeerMessageCodec` serializes internal peer requests and
  page/client peer responses as JSON messages. This is not the public admin API;
  it is a proto-free internal transport payload so the coordinator and peer
  executor can be tested and wired before `rocketmq-apis` ownership is settled.
  Successful peer responses must carry exactly the expected page or client body,
  and peer error responses must remain status-only without page/client bodies.
- `ProxyClientAdminPeerMessageClient` adapts the object-level peer-client
  contract to a raw message transport. `ProxyClientAdminPeerMessageHandler`
  adapts raw messages back to the local peer executor. The in-process message
  transport keeps local multi-proxy simulations on the same serialized boundary
  that a real transport will use.
- `ProxyClientAdminPeerGrpcService` exposes an internal unary gRPC service using
  `StringValue` request and response bodies that carry the peer JSON payload. It
  builds `ProxyContext` through the same admin context factory and delegates to
  the peer message handler. `ProxyClientAdminPeerGrpcTransport` is the matching
  client-side raw message transport over `Channel` and the service method
  descriptor. These classes do not add or modify any public RocketMQ protobuf
  service definitions.
- When cross-proxy query support is enabled, `DefaultGrpcMessagingActivity`
  creates the internal peer gRPC service beside the local coordinator peer
  client. The default coordinator still uses the local in-process message
  transport when `proxyClientAdminPeerGrpcTargets` is blank. If static peer
  targets are configured, the coordinator uses `ProxyClientAdminPeerGrpcTransport`
  backed by per-peer gRPC channels while preserving the same proto-free peer
  message boundary. The peer transport forwards the current admin
  `ProxyContext` into gRPC metadata, including the authenticated user subject
  and request address/client attributes, so the peer-side context factory sees
  the same admin caller context as the coordinator. Static peer gRPC channels
  are shut down gracefully with a bounded wait when the shared activity stops,
  and are forced closed if they do not terminate in time. Dynamic discovery,
  secure channel options, and deeper production channel tuning remain follow-up
  work. Static target lists must include the local `proxyName`; otherwise
  startup rejects the configuration so `ALL_PROXIES` queries do not silently omit
  clients connected to the coordinator proxy itself.
- `ProxyStartup.createGrpcBindableServices(...)` now has a tested package-private
  overload for appending additional `BindableService` instances after the
  messaging service while reusing the same `DefaultGrpcMessagingActivity`. It
  also appends the internal peer gRPC service when cross-proxy query support is
  enabled. The future public `GrpcProxyAdminApplication` can use the same
  multi-service startup seam once generated protobuf classes are available.

The request DTOs convert pagination, client type, scope, and optional proxy id
into `ProxyClientQuery`. Required identifiers such as client id, group, and
topic, plus optional page token and proxy id, are trimmed at this boundary; blank
values are treated as absent. The adapter rejects missing `proxy_id` immediately
for explicit `PROXY_ID` scope before building `ClientAdminRequestContext` or
entering coordinator/peer code. Page tokens pass through the dedicated token codec,
which encodes the
read-model last-client-id token as a versioned opaque public token and decodes
versioned or legacy bare tokens back to the internal token.
Public scope names pass through the scope mapper so future generated protobuf
adapters can translate prefixed enum names such as
`PROXY_SCOPE_LOCAL_PROXY`, `PROXY_SCOPE_ALL_PROXIES`, and
`PROXY_SCOPE_PROXY_ID` without importing the generated admin service in this
branch. The default internal scope is `LOCAL_PROXY`; request DTOs preserve
`proxy_id` only for the explicit `PROXY_ID` scope and require it for that scope,
while dropping it for `LOCAL_PROXY` and `ALL_PROXIES`, so broadcast-style queries
cannot accidentally carry a single-proxy filter into coordinator-owned tokens.
Unsupported future scopes
are still carried through the DTO and query objects so they can be validated by
the activity before authorization. The service layer revalidates the same scope
to keep direct internal calls consistent. This preserves `BAD_REQUEST`
semantics for unsupported scopes while keeping the adapter contract ready for
future `PROXY_ID` support. The protobuf default `CLIENT_TYPE_UNSPECIFIED` is
normalized to no client type filter, while `UNRECOGNIZED` client type values
are rejected as `BAD_REQUEST`.

The future generated endpoint should only translate protobuf messages to these
DTOs, call `ProxyClientAdminActivity`, and translate the result view back to a
protobuf response. Authorization, error mapping, metrics, pagination bounds, and
read-model queries should remain behind the existing activity/service boundary.
The generated unary methods should use `ProxyClientAdminEndpointHandler` for
the common result-to-`StreamObserver` flow and should use
`ProxyClientAdminEndpointExecutor` when they need the shared request conversion
and context-pipeline boundary. Request adapter failures are returned as status
responses before `ProxyClientAdminContextFactory` is invoked. Response factories
must return a non-null response; a null response is treated as an internal
adapter error and mapped through the same status response path. Non-`OK` results
are normalized to a status-only response with a null body so error responses
cannot accidentally carry stale success data. Missing response observers are
rejected before executing the admin action. A missing injected admin activity is
treated as a server-side wiring error.

### ListClients

Request:

```text
ListClientsRequest {
  string page_token;
  int32 page_size;
  ClientType client_type; // CLIENT_TYPE_UNSPECIFIED means no type filter; UNRECOGNIZED is rejected.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsResponse {
  Status status;
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
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
DescribeClientResponse {
  Status status;
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
  ClientType client_type; // CLIENT_TYPE_UNSPECIFIED means no type filter; UNRECOGNIZED is rejected.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsByGroupResponse {
  Status status;
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
  ClientType client_type; // CLIENT_TYPE_UNSPECIFIED means no type filter; UNRECOGNIZED is rejected.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsByTopicResponse {
  Status status;
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
- `proxyId`: source proxy node id. M1 records the local `proxyName` and
  normalizes blank values to `DEFAULT_PROXY`, matching the local peer routing
  id; future `ALL_PROXIES` responses can use the same field to distinguish
  fan-out results.
- `connectTimeMillis`: first observed local connection time.
- `lastActiveTimeMillis`: most recent successful telemetry or heartbeat time.

The internal public-view adapter trims nullable string metadata such as language,
addresses, client version, and proxy id, and normalizes missing or blank values
to empty strings before a future protobuf adapter writes response fields. A
missing or `UNRECOGNIZED` public-view client type is normalized to
`CLIENT_TYPE_UNSPECIFIED` so generated protobuf responses never need to write a
null or unknown enum value. Internally, `CLIENT_TYPE_UNSPECIFIED` snapshots and
`UNRECOGNIZED` snapshots are normalized to a missing client type before indexing,
so read-model metrics do not expose artificial unspecified or unrecognized
client-type series. Public response views trim group and topic values, drop
blank values, and de-duplicate repeated values while preserving the adapter
input order. The read-model response converter supplies those values in
lexicographic order so public responses remain deterministic even though the
internal read-model snapshots use sets.

`ProxyClientPage` returns a list of `ProxyClientInfo` plus a `nextPageToken`.
`ProxyClientQuery` carries optional group, topic, client type, page size, page
token, scope, and proxy id filters. `CLIENT_TYPE_UNSPECIFIED` is normalized to a
missing client type filter in `ProxyClientQuery`, so direct read-model callers
and future public request adapters share the same no-filter semantics.

## Read Model and Indexes

`ProxyClientReadService` is an in-memory local read model. It owns:

- `clientId -> ProxyClientInfo`
- `sorted clientId index`
- `group -> sorted clientId set`
- `topic -> sorted clientId set`
- `clientType -> sorted clientId set`
- `proxyId -> sorted clientId set`

All listing results are ordered by client id. Upsert first removes the old
client's index entries and then writes the new snapshot. Remove deletes the
client record, the sorted client id entry, and all secondary index entries. This
makes repeated telemetry idempotent and keeps group/topic/proxy changes from
leaving stale index entries.

The read model also exposes an internal inactive-client cleanup helper that
removes clients whose `lastActiveTimeMillis` is at or below a caller-provided
cutoff. It reuses the same remove path as lifecycle close, unregister, and
termination cleanup, so the client table, all secondary indexes, and read-model
operation metrics remain consistent. `ProxyClientReadServiceCleaner` wraps that
helper behind a `StartAndShutdown` maintenance component with explicit timeout,
interval, executor, and clock dependencies. The default gRPC activity wires this
cleaner behind the disabled-by-default `enableProxyClientReadServiceCleaner`
switch. When enabled, `proxyClientReadServiceCleanerInactiveTimeoutMillis`
controls the stale cutoff and `proxyClientReadServiceCleanerIntervalMillis`
controls the fixed-delay maintenance interval.

Unfiltered scans iterate the maintained sorted client id index directly instead
of copying all client ids on every request. Filtered scans collect only the
requested secondary indexes, copy the smallest candidate index, and intersect the
remaining candidates. This keeps the M1 read model simple while avoiding
unnecessary full-snapshot copies for common paginated reads and selective
group/topic/type/proxy queries.

The read model normalizes client ids by trimming surrounding whitespace before
storing, looking up, or removing entries. Group and topic index values are also
trimmed, de-duplicated by the index sets, and blank values are ignored. Proxy
ids are indexed with the same sorted-set structure so future `PROXY_ID` and
fan-out merge paths can restrict a query to one source proxy without changing
pagination order.

The service is synchronized in M1. That keeps the implementation simple and
consistent while lifecycle updates and admin reads are still local to one proxy
process. A future high-scale implementation can replace the internal maps with
lock-striped or immutable-snapshot indexes without changing the service API.

## Pagination

Pagination is bounded by `ProxyClientQuery.MAX_PAGE_SIZE`. Non-positive page
sizes use `DEFAULT_PAGE_SIZE`. Page tokens are based on the last client id
returned by the previous page. Unfiltered pages advance through the maintained
sorted client id index. Filtered pages advance through the sorted candidate set
after group/topic/type indexes have been intersected. When a token is supplied,
it must exist in that candidate set; otherwise `ProxyClientReadService` throws
`IllegalArgumentException`.

The public adapter treats page tokens as opaque values. The current M1 codec
accepts canonical `v1:` tokens and legacy bare read-model tokens, rejects
unknown or non-canonical versioned tokens, and caps incoming public page tokens
at 4096 characters to bound decode work before the future public endpoint is
registered.

This gives stable pagination for the local snapshot and avoids offset-based
scans. If clients connect or disconnect between pages, a token can become
invalid for the filtered view and the caller should restart the query.

## Multi-Proxy Semantics

M1 supports `LOCAL_PROXY` only. A query observes clients connected to the current
proxy process and does not fan out to other proxies.

Remote channel registration events can still synchronize raw gRPC client
settings for existing broker-side behavior, but M1 does not promote those remote
settings into `ProxyClientReadService`. The local read model is written only by
clients that connect to this proxy process through telemetry, heartbeat, explicit
termination, or local unregister/stream-close lifecycle paths. This keeps
`LOCAL_PROXY` query results aligned with the current proxy's online connection
surface and avoids mixing future cross-proxy state into the M1 indexes.

Future scopes can add:

- `ALL_PROXIES`: fan out to peer proxies and merge sorted pages.
- `PROXY_ID`: query one named proxy instance.
- broker-assisted discovery: query broker-side client metadata where available.

The M1 read model intentionally stores only local lifecycle state so cross-proxy
semantics can be added without changing the local index contract.

The existing `HeartbeatSyncer` path is not a complete cross-proxy admin query
mechanism. It broadcasts consumer register/unregister events through the proxy
system-message topic and replays remote channels into broker-side
`ConsumerManager` state. That data flow is useful for existing consumer
management behavior, but it is not a pageable admin read path: it does not carry
all producer and consumer telemetry fields, does not define a request/response
protocol, does not provide a consistent merged snapshot, and does not define
cross-proxy page-token ownership. A future `ALL_PROXIES` implementation should
therefore add a dedicated proxy-admin fan-out protocol or a separately maintained
cluster-wide index instead of trying to page directly over heartbeat sync
messages.

## M2 Cross-Proxy Query Decision Record

The next milestone should keep M1's `LOCAL_PROXY` behavior unchanged and add
cross-proxy query support behind a separate internal boundary before exposing it
through public protobuf APIs. This keeps the current read model stable while the
community decides where the public admin service lives.

The recommended M2 direction is a dedicated proxy-admin fan-out layer:

- Keep each proxy responsible for its own `ProxyClientReadService` local
  snapshot.
- Add an internal peer query protocol for `ListClients`, `DescribeClient`,
  `ListClientsByGroup`, and `ListClientsByTopic`.
- Let the receiving proxy execute the same authorized local query path that M1
  already uses, but with an explicit internal caller context.
- Merge page results at the coordinator proxy by sorted `client_id`, carrying
  per-peer continuation state inside an opaque external page token.
- Preserve `LOCAL_PROXY` as the default. Cross-proxy scopes should stay gated
  until the fan-out layer, peer discovery, timeout, and page-token ownership are
  validated.

Rejected M2 alternatives:

- Reusing `HeartbeatSyncer` messages as the query source. This path is designed
  for consumer registration replay into broker-side state, not for complete
  producer/consumer online-client inspection, request/response semantics, or
  consistent pagination.
- Querying broker-side consumer managers only. That would miss producer
  telemetry and proxy-local connection attributes, and it would not cover the
  client lifecycle fields already captured by M1.
- Adding `ALL_PROXIES` by reading remote proxy indexes directly from the local
  process. The local process has no durable ownership of peer snapshots, so page
  tokens would become ambiguous during peer restart, scale-out, or network
  partitions.

The coordinator should encode a future external page token as an opaque value
that contains:

- a version prefix.
- the requested scope and filters.
- the last emitted global cursor: `client_id` plus `proxy_id`, matching the
  coordinator merge order.
- a per-peer cursor map keyed by stable proxy id.
- a token creation timestamp for bounded retention and diagnostics.

The token must not expose raw implementation details to users. The current M1
`v1:` token codec is intentionally local and should not be reused as the final
cross-proxy token format without wrapping it in a coordinator-owned token.
This branch now includes an internal `cp1:` coordinator-token codec for that
future contract. The codec carries the requested scope, filters, last emitted
global cursor (`client_id` plus `proxy_id`), per-peer page tokens, and creation
time. The decoder rejects non-canonical `cp1:` inputs that are not the exact
no-padding encoding emitted by the codec, so equivalent JSON payloads cannot
create multiple public cursor representations. The response adapter preserves
canonical `cp1:` tokens instead of wrapping them in the local read-model `v1:`
token codec. It is not wired into the M1 `LOCAL_PROXY` endpoints and does not
change the public local `v1:` token behavior. The local token codec rejects bare
coordinator-owned `cpN:` tokens, `v1:` tokens whose decoded read-model cursor is
coordinator-owned, and attempts to encode coordinator-prefixed read-model
cursors. This keeps `LOCAL_PROXY` and `PROXY_ID` requests from accidentally
treating a cross-proxy cursor as a read-model client-id cursor. Request DTOs
preserve `cp1:` tokens only for `ALL_PROXIES`, where the coordinator owns
decoding and validation. A non-empty coordinator token must include the last
emitted global cursor (`client_id` plus `proxy_id`) and at least one peer cursor;
otherwise the coordinator rejects it as an incomplete progress token instead of
restarting from the first peer page or interpreting duplicate client ids
ambiguously.

When the coordinator builds a next token, it preserves a peer's own next-page
token after that peer's returned page has been fully emitted. If the global
merge stops in the middle of a peer page, it stores the last emitted client id
for that peer so the next coordinator request can replay the remaining peer
page without skipping clients. If peer responses indicate more data but the
coordinator cannot emit any client for the current global page, it fails the
request as an internal pagination error instead of returning an empty terminal
page and silently dropping peer progress. When a coordinator token carries a
per-peer cursor, the next peer page must only return client ids after that peer
cursor; otherwise the coordinator treats the peer response as stale or
misrouted and returns an internal routing error before merging the page. For
untokened peers, progress is checked against the global `(client_id, proxy_id)`
cursor so duplicate client ids on different proxies can be paginated in stable
proxy-id order. Every peer page must also be strictly ordered by increasing
`client_id`; otherwise the coordinator rejects it before building global
pagination state.

Recommended partial-failure behavior:

- `LOCAL_PROXY`: unchanged, fail or succeed as a single local request.
- `PROXY_ID`: return the target proxy result if reachable; return an error if
  that proxy cannot be reached or does not own the requested page token.
- `ALL_PROXIES`: prefer fail-fast in the first public version unless the API adds
  an explicit partial-result field. Silent omission would make online-client
  inspection misleading during incidents.

Recommended implementation order after public API ownership is confirmed:

1. Define internal peer request/response DTOs that mirror the existing admin
   activity DTOs, including scope, filters, page size, and page token.
   This branch includes the initial proto-free peer request/response DTOs and
   keeps peer execution local by converting peer requests into `LOCAL_PROXY`
   read-model queries. Peer requests now reject coordinator scopes and only
   carry `LOCAL_PROXY` execution semantics; the coordinator lowers
   `ALL_PROXIES` and `PROXY_ID` requests to local peer requests before fan-out.
   It also includes a local peer executor seam
   that wraps existing admin activity results into proto-free peer responses,
   converts activity failures into peer internal-error responses,
   stamps the local proxy id into successful peer page and client bodies,
   plus an in-process peer client adapter that rejects empty executor maps,
   normalizes and rejects duplicate peer ids, requires each map key to match the
   local proxy id stamped by its executor, exposes stable peer ids, and delegates
   target requests to local executors.
2. Add a peer transport adapter that can call another proxy process without
   depending on public client-facing protobuf classes. This branch now includes
   that proto-free transport layer: peer requests and responses are encoded as
   raw JSON messages, `ProxyClientAdminPeerMessageClient` and
   `ProxyClientAdminPeerMessageHandler` adapt the object-level peer API to that
   raw boundary, `ProxyClientAdminInProcessPeerMessageTransport` keeps local
   simulation on the serialized path, and `ProxyClientAdminPeerGrpcService` plus
   `ProxyClientAdminPeerGrpcTransport` provide the internal unary gRPC
   server/client boundary using `StringValue` payloads. The gRPC peer service is
   registered through `ProxyStartup` only when cross-proxy query support is
   enabled. It remains an internal peer endpoint, not the public
   `ProxyAdminService` API.
3. Add a coordinator service that fans out local-page requests, merges results in
  `(client_id, proxy_id)` order, and emits coordinator-owned opaque page tokens.
   This branch includes the first proto-free coordinator slice for
   `ALL_PROXIES` list queries and `PROXY_ID` target queries: `ALL_PROXIES`
   list queries fan out `ListClients`, `ListClientsByGroup`, and
  `ListClientsByTopic` to the peer-client boundary, merge peer pages by stable
  `(client_id, proxy_id)` order, fail fast on peer errors, and store per-peer
  cursors in the internal `cp1:` coordinator token. `DescribeClient` with
  `ALL_PROXIES`
   scans discovered peers in stable proxy-id order, ignores per-peer
   `NOT_FOUND` responses until a match is found, and returns `NOT_FOUND` only
   after all peers miss. `PROXY_ID` list queries and `DescribeClient` route
   directly to the requested proxy id, reuse the same peer error mapping, and
   preserve the target peer's page token. The
   coordinator trims, validates, and sorts discovered peer ids before fan-out,
   rejects empty, blank, or duplicate peer ids as internal discovery errors,
   rejects coordinator page tokens that reference peers outside the current
   discovery set as bad requests, and validates each peer response `proxyId`
   before accepting its body so a misrouted or stale peer transport result is
   returned as an internal routing error instead of being merged into the admin
   response. Peer wire responses reject mixed success bodies, so page responses
   cannot also carry a client body and describe responses cannot also carry a
   page body. Peer page bodies are also validated before merge so malformed peer
   results and peer client bodies without usable client ids become stable
   internal errors instead of leaking as merge-time exceptions or successful
   malformed describe results. Describe responses must also return the exact
   requested `client_id`; a peer response that carries a different client id is
   treated as an internal routing or peer-corruption error for both `PROXY_ID`
   and `ALL_PROXIES`. Single-proxy `PROXY_ID` page responses are checked against
   the decoded peer page token, so a stale peer page cannot return client ids at
   or before the requested cursor and make pagination move backwards.
   It also includes a proto-free scope router that keeps `LOCAL_PROXY` on the
   existing admin activity and routes `ALL_PROXIES`/`PROXY_ID` requests to the
   coordinator, plus optional endpoint-handler wiring for that router, without
   changing the public endpoint registration yet. `DefaultGrpcMessagingActivity`
   now creates the shared scope router for the default local-only path without
   allocating a peer client. When `enableProxyClientAdminCrossProxyQuery` is
   enabled, it wires a single local in-process peer; a future cross-proxy
   transport can replace the peer client without changing endpoint or activity
   semantics. The peer client is wrapped by
   `TimedProxyClientAdminPeerClient` so peer discovery and coordinator fan-out
   have a bounded wait; timed discovery must return a non-empty peer list, and
   `proxyClientAdminPeerRequestTimeoutMillis` controls the timeout. Timed-out or
   interrupted waits cancel the submitted peer work before returning an error or
   restoring the interrupt flag. Enabling the coordinator scope flag also
   requires a nonblank `proxyName`, which becomes the
   stable local peer id; the default local-only `DEFAULT_PROXY` fallback is not
   used for coordinator scopes. The in-process peer client converts local executor
   failures into peer error responses so coordinator fan-out receives a bounded
   peer result instead of an exception escaping the peer-client boundary. The
   default local peer executor now delegates peer-local work directly to the
   shared `ClientAdminService` instead of re-entering the public admin activity,
   so coordinator fan-out reuses local read semantics without duplicating the
   public admin authorization or metrics boundary. The older activity-backed
   constructor remains available for tests and alternate embeddings.
4. Gate `ALL_PROXIES` and `PROXY_ID` behind explicit config until peer discovery,
   timeout, retry, and partial-failure semantics are validated.
   This branch keeps those coordinator scopes disabled by default through
   `enableProxyClientAdminCrossProxyQuery`; while disabled, the internal scope
   router rejects coordinator scopes before any peer client or peer timeout
   configuration is touched. Enabling the flag lets the internal scope router
   use the current single-node in-process message peer transport when no static
   targets are configured, or the internal gRPC peer transport when
   `proxyClientAdminPeerGrpcTargets` is set. Static target mode requires the
   target list to include the local `proxyName`. It also registers the internal
   peer gRPC service and requires an explicit `proxyName` so future multi-proxy
   discovery and page tokens do not inherit an ambiguous default proxy id. Real
   multi-node discovery is still separate follow-up work.
5. Wire the public `ProxyAdminService` adapter to the coordinator service while
   keeping M1 `LOCAL_PROXY` as the default.

## ACL Plan

M1 should reuse existing cluster-level admin permissions:

- list operations require cluster-level `LIST`.
- describe operations require cluster-level `GET`.

The current internal implementation provides `ClientAdminAuthPolicy`,
`DefaultClientAdminAuthorizationService`, and `AuthorizingClientAdminService`.
The admin gRPC request pipeline copies the authenticated access key into
`ProxyContext` as a `Subject`, and `ClientAdminRequestContext.from` derives the
admin request context from `ProxyContext`. The source IP used for ACL is
normalized from the gRPC remote address by parsing the host portion, including
bracketed IPv6 host-and-port values, matching the existing remoting-side source
address intent. The admin pipeline intentionally does not run generic messaging
authorization; this lets the future public adapter reject unsupported M1 scopes
before ACL, then authorize once through `AuthorizingClientAdminService` before
delegating to read-model queries while keeping the first admin surface
consistent with existing management actions. When authorization is enabled, a
missing authenticated subject is rejected at the admin authorization boundary
and mapped to `UNAUTHORIZED`.
For internal coordinator scopes, the scope router performs admin authorization
once before peer discovery or fan-out. A denied `ALL_PROXIES` or `PROXY_ID`
request therefore never reaches the peer client. When coordinator scopes are
disabled by configuration, the router rejects those scopes as `BAD_REQUEST`
before authorization or peer discovery, preserving the local-only gate while
still treating the attempt as one public admin operation for metrics.
Topic-level or group-level ACL can be discussed later if the community wants
more granular visibility controls.

## Metrics Plan

The current internal implementation exposes read-model gauges for:

- current local online client count.
- current local online client count by `clientType`.
- current local group/topic/proxy index count.

It also records read-model upsert/remove mutation counters.

The current internal admin service wrapper records:

- admin query counters by operation and result code.
- admin query latency histograms.

Metrics are recorded around the authorizing admin service, not only around the
read-model service. This means ACL denials are reported as `UNAUTHORIZED`, while
successful reads, bad requests, not-found responses, and unexpected internal
errors are still counted once at the public admin operation boundary.

Internal coordinator scopes use the same one-operation boundary at the scope
router. The router records exactly one operation metric for a coordinator-scope
request after mapping the final status to `OK`, `BAD_REQUEST`, `NOT_FOUND`,
`UNAUTHORIZED`, `TIMEOUT`, `TOO_MANY_REQUESTS`, `NOT_IMPLEMENTED`, or
`INTERNAL_ERROR`. That includes the `BAD_REQUEST` result when cross-proxy scopes
are disabled by configuration, the `TIMEOUT` result when peer fan-out or
discovery exceeds its bounded wait, and explicit peer throttling or
not-implemented results from the internal gRPC transport. Peer-local execution
is deliberately routed through the shared `ClientAdminService`, not the public
activity wrapper, so a coordinator request is not counted again as a nested local
public admin request.

Metric recording is best effort. Read-model mutation recorder failures and
admin query metrics recorder failures are logged but do not mask successful
lifecycle/admin operations or the original service exception.

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
- unexpected successful query result without a response body:
  `INTERNAL_SERVER_ERROR`.
- internal failures: `INTERNAL_SERVER_ERROR`.

The internal peer gRPC transport maps peer call `StatusRuntimeException`s before
encoding a peer error response: invalid-argument style statuses become
`BAD_REQUEST`, `NOT_FOUND` stays `NOT_FOUND`, authentication and permission
statuses become `UNAUTHORIZED`, resource exhaustion becomes `TOO_MANY_REQUESTS`,
unimplemented methods become `NOT_IMPLEMENTED`, and peer deadlines or
unavailable peer channels become `PROXY_TIMEOUT`. Coordinator peer-discovery
timeouts also surface as `PROXY_TIMEOUT` instead of a generic internal error.
The admin gRPC error writer preserves explicit gRPC status exceptions before
they reach the transport mapper. Unknown transport failures remain
`INTERNAL_SERVER_ERROR`.

## Compatibility

M1 is additive. It does not change existing gRPC client behavior, public protobuf
definitions, broker registration, or client settings semantics. The original
three-argument `ClientActivity` constructor remains available. The new
four-argument constructor only allows tests and the default gRPC activity to
share a non-null read model instance.

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
- heartbeat with missing cached client settings removes stale client and
  indexes before returning the error status. Done.
- heartbeat with an unrecognized cached client type removes stale client and
  indexes before returning the error status. Done.
- termination removes client and indexes. Done.
- producer unregister listener removes client and indexes. Done.
- consumer unregister listener removes client and indexes. Done.
- telemetry completion, gRPC status errors, and non-status stream errors remove
  client and indexes. Done.
- telemetry settings validation failures remove stale client and indexes before
  returning the stream error. Done.
- termination with an unrecognized cached client type removes stale client and
  indexes before returning the error status. Done.
- termination unregister failures remove stale client and indexes before
  completing the response future exceptionally. Done.
- inactive read-model cleanup removes stale clients and all secondary indexes.
  Done.

Internal adapter tests cover:

- request DTO conversion to `ProxyClientQuery`.
- request DTO string normalization for client id, group, topic, page token, and
  proxy id.
- default `LOCAL_PROXY` scope, opaque page-token encode/decode, and proxy id
  pass-through for future scoped queries.
- activity-level `LOCAL_PROXY` query canonicalization before authorization and
  delegate calls.
- service-level `LOCAL_PROXY` query canonicalization that drops accidental
  proxy-id filters before querying the local read model.
- activity overloads for request DTOs.
- activity-level rejection of unsupported M1 scopes before ACL or delegate
  invocation.
- response view conversion, stable collection snapshots, and null-safe string
  metadata normalization.
- endpoint handler success response writing, error response writing, and thrown
  action error mapping.
- cross-package shared wiring for future admin gRPC application access to
  `ProxyClientAdminActivity`.
- coordinator pagination rejecting peer pages that go backward relative to the
  per-peer cursor stored in a coordinator-owned page token.
- activity-level static peer gRPC fan-out wiring for `ALL_PROXIES`, covering
  `DefaultGrpcMessagingActivity` construction with configured peer targets and
  real internal peer gRPC services.
- static peer gRPC channel shutdown from `DefaultGrpcMessagingActivity`,
  including bounded graceful termination, forced close on timeout, and forced
  close plus interrupt preservation when graceful termination is interrupted.
- coordinator pagination preserving duplicate client ids across proxies by
  using the last emitted `(client_id, proxy_id)` as the global cursor.
- coordinator pagination rejecting peer pages that are not strictly ordered by
  `client_id`.
- real peer gRPC fan-out from coordinator through `ProxyClientAdminPeerGrpcTransport`
  into two in-process Netty peer services, verifying merged `ALL_PROXIES`
  results and proxy id stamping.
- missing request DTO, missing identifiers, not found, unsupported scope,
  authorization failure, and unexpected runtime error mapping.

### Validation Snapshot

On 2026-07-07 Asia/Shanghai time, the branch was revalidated with this focused
RIP-2 suite:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  '-Dtest=ProxyClientReadServiceTest,ProxyClientReadServiceCleanerTest,ProxyClientInfoTest,ProxyClientQueryTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,ProxyMetricsManagerTest,ProxyClientAdmin*Test,GrpcProxyAdminWiringTest,DefaultGrpcMessagingActivityTest,GrpcRequestPipelineFactoryTest,ProxyStartupTest,ClientActivityTest#testProducerTelemetryUpdatesProxyClientReadService+testConsumerTelemetryUpdatesProxyClientReadService+testHeartbeatPreservesConnectTimeAndUpdatesLastActiveTime+testNotifyClientTerminationRemovesProxyClientReadServiceIndexes' \
  -DfailIfNoTests=false test -DskipITs
```

The proxy module reported `Tests run: 403, Failures: 0, Errors: 0, Skipped: 0`
and the Maven reactor ended with `BUILD SUCCESS`. Under JDK 17, JaCoCo 0.8.5
still prints instrumentation stack traces for JDK and Mockito-generated classes;
those logs are treated as environment noise only when Surefire reports zero
failures/errors and Maven exits successfully.

## Synthetic Benchmark

The read model includes a JMH benchmark in
`ProxyClientReadServiceBenchmark`. It builds synthetic in-memory client
metadata and measures the steady-state query paths that M1 exposes:

- unfiltered first-page listing.
- unfiltered next-page listing.
- group-filtered listing.
- topic-filtered listing.
- proxy-id-filtered listing.
- direct client lookup.

The default benchmark parameters model 1,000,000 clients, 1,000 groups, 10,000
topics, and 100 proxy ids. The benchmark annotations run one fork, three
one-second warmup iterations, five five-second measurement iterations, and four
worker threads.

Use the focused unit test to verify the benchmark setup and guard the synthetic
data assumptions:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  -Dtest=ProxyClientReadServiceBenchmarkTest \
  -DfailIfNoTests=false test -DskipITs
```

Use a short JMH smoke run to verify the launcher and classpath without spending
time on the full 1M-client scenario:

```bash
export JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -pl proxy -am -DskipTests -DskipITs clean test-compile
mvn -pl proxy -DskipTests -DskipITs dependency:build-classpath \
  -Dmdep.includeScope=test \
  -Dmdep.outputFile=/tmp/rocketmq-proxy-test-classpath.txt
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark \
  -p clientCount=1000 -p groupCount=10 -p topicCount=20 -p proxyCount=5 \
  -wi 0 -i 1 -r 100ms -w 100ms -f 1 -t 1
```

The clean compile step is intentional. On 2026-07-06, an incremental
`test-compile` left stale JMH-generated classes under `proxy/target`, and JMH
forks reported unresolved benchmark methods even though the real benchmark
class contained those methods. After `clean test-compile`,
`ProxyClientReadServiceBenchmark_jmhType_B1` correctly extended
`ProxyClientReadServiceBenchmark`, and the smoke run completed all six
benchmarks:

- `describeClient`: sample 2378, about 0.0001 ms/op.
- `listByGroupPage`: sample 3223, about 0.003 ms/op.
- `listByProxyIdPage`: sample 3442, about 0.009 ms/op.
- `listByTopicPage`: sample 3119, about 0.003 ms/op.
- `listFirstPage`: sample 1763, about 0.022 ms/op.
- `listNextPage`: sample 3805, about 0.001 ms/op.

These numbers are a launcher and classpath sanity check for the small
1000-client smoke scenario, not a formal performance claim.

Use the same classpath preparation and omit the `-p`, `-wi`, `-i`, `-r`, `-w`,
`-f`, and `-t` overrides for the full default 1M-client run:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceBenchmark
```

The Maven `exec:java` path is useful for non-forked debugging, but the
classpath-file launcher above is preferred for benchmark runs because JMH
forked VMs inherit the complete test runtime classpath.

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

This branch keeps that path proto-free through a default-empty
`ProxyAdminServiceFactory` seam in `ProxyStartup.createGrpcBindableServices`.
The factory receives the same `DefaultGrpcMessagingActivity` used by
`GrpcMessagingApplication`; once generated admin stubs are available, it should
return a `GrpcProxyAdminApplication` built from that shared activity so the
public endpoint and messaging lifecycle observe the same read model.

The endpoint adapter should stay thin:

- read headers and build `ProxyContext` through
  `GrpcRequestPipelineFactory.createProxyClientAdminContextFactory(...)`.
  Admin RPCs should use `ProxyClientAdminContextFactory` instead of the
  messaging application's client-id validation and generic authorization path.
- translate proto requests to the internal request DTOs through
  `ProxyClientAdminRequestConverter` and `ProxyClientAdminEndpointExecutor`.
- call `ProxyClientAdminActivity` through the shared endpoint handler.
- translate `ProxyClientAdminPageView` and `ProxyClientAdminClientView` into
  proto responses.
- use `ProxyClientAdminEndpointHandler` to copy the `Status` from
  `ProxyClientAdminResult`, write the response, and keep exception-to-status
  behavior consistent with the internal adapter.
- keep non-OK responses status-only. The scope router and endpoint handler drop
  bodies attached to error results before the future protobuf response is built.

### Public Endpoint Rollout Checklist

The remaining public endpoint work should start only after the community agrees
where the protobuf API lives. Once that is settled, the implementation can land
as a narrow adapter over the internal code already in this branch:

Current branch status: after fetching `upstream/develop` at commit `2af604f3a`
on 2026-07-06, `git grep` still finds no upstream `ProxyAdminService`,
`ProxyScope`, `ListClientsByGroup`, or `ListClientsByTopic` protobuf API to
consume. The upstream tree also contains no `.proto` source files; the proxy
module consumes generated `apache.rocketmq.v2.MessagingServiceGrpc` classes from
the external `rocketmq-apis` project instead. The documentation-only draft
remains under `docs/en`, and this fork should continue to avoid modifying
`rocketmq-apis` until that ownership decision is explicit.

The internal code is ready for a thin generated endpoint adapter once that
decision is made: lifecycle writes, read-model queries, request DTOs, scope and
page-token adapters, authorization, metrics, status mapping, context propagation,
and startup service-registration seams are already covered in this branch.

1. Decide the `rocketmq-apis` file location and whether the service should live
   beside the existing v2 messaging APIs or in a dedicated admin file.
2. Confirm the standalone service name `ProxyAdminService` and the four unary
   methods from the draft: `ListClients`, `DescribeClient`,
   `ListClientsByGroup`, and `ListClientsByTopic`.
3. Confirm field numbers in `rip2-proxy-admin-m1-public-api-draft.proto` before
   generating Java classes. Field numbers should not be reshuffled after public
   review starts.
4. Keep public page tokens opaque. The M1 adapter should encode read-model
   last-client-id tokens as versioned `ProxyClientAdminPageTokenCodec` tokens
   and decode them back at the request DTO boundary.
5. Keep public enum values prefixed with `PROXY_SCOPE_...`; generated adapters
   should pass the enum name to `ProxyClientAdminScopeMapper`.
6. Preserve M1 `LOCAL_PROXY` behavior. Generated public adapters should expose
   cross-proxy scopes only when the internal coordinator scope router is enabled
   and backed by configured peer targets or later peer discovery; otherwise they
   should continue to reject `PROXY_SCOPE_ALL_PROXIES` and
   `PROXY_SCOPE_PROXY_ID` with `BAD_REQUEST`.
7. Reject missing `proxy_id` at the adapter/request DTO boundary whenever the
   public request selects `PROXY_SCOPE_PROXY_ID`, before creating request context
   or invoking coordinator/peer code. Continue to ignore `proxy_id` for
   `PROXY_SCOPE_LOCAL_PROXY` and `PROXY_SCOPE_ALL_PROXIES`.
8. Require an explicit, nonblank `proxyName` before enabling cross-proxy
   coordinator scopes. `LOCAL_PROXY` can keep the default local-only fallback, but
   `ALL_PROXIES` and `PROXY_ID` must use stable, configured peer ids.
9. Register `GrpcProxyAdminApplication` beside `GrpcMessagingApplication` in
   `ProxyStartup.createGrpcBindableServices`, using the same
   `DefaultGrpcMessagingActivity` instance so lifecycle writes, read-model
   queries, ACL, metrics, and context propagation share one in-process state
   holder.
10. Keep endpoint methods free of business logic. They should adapt protobuf
   requests and responses only; authorization, validation, pagination, metrics,
   and error mapping should stay behind `ProxyClientAdminEndpointExecutor` and
   `ProxyClientAdminEndpointHandler`.

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
8. Harden admin activity scope validation and place admin metrics around the
   authorizing request boundary. Done.
9. Document the latest upstream public API status and the M1 remote-sync
   boundary. Done.
10. Discuss public protobuf ownership before changing `rocketmq-apis`.
11. Add the public admin gRPC/protobuf adapter.
12. Wire the adapter through `AuthorizingClientAdminService`; internal ACL policy,
   request context propagation, and service are already in place.
13. Extend metrics with admin query counters and latency histograms. Done.
14. Add a synthetic 1M-client benchmark or simulation. Done.
