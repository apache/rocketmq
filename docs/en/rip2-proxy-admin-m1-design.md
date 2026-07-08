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

The public contest-facing method names should follow the RIP-2 tracking issue:
`ListClients`, `DescribeClient`, `ListClientsByGroup`, and
`ListClientsByTopic`. These names now match the internal service boundary used
by this branch, so the future generated endpoint adapter can stay thin and avoid
renaming logic around the read model.

The first public API should accept only `LOCAL_PROXY` unless the community also
accepts the internal cross-proxy coordinator semantics. Requests for future
scopes such as `ALL_PROXIES` or `PROXY_ID` should return `BAD_REQUEST` from a
public endpoint until that endpoint is explicitly wired to a reviewed peer
transport and coordinator contract.

The RIP-2 issue lists `pageNum` and `pageSize` for the public API and caps
`pageSize` at 100. This branch carries those semantics through
`ProxyClientQuery`, `ProxyClientReadService`, and the proto-free admin request
DTOs. The existing opaque `pageToken` support remains an internal compatibility
path for coordinator experiments and early adapter tests. Public clients must
not depend on the internal page-token representation when that compatibility
path is present.

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

## Contest Requirement Alignment

The publicly available RIP-2 tracking issue
`apache/rocketmq#10599` describes the contest target as a Proxy Admin gRPC
surface for online client query. This branch currently satisfies the internal
service foundation, but the following items remain before the work is a complete
contest submission:

| RIP-2 requirement | Current branch state | Next action |
| --- | --- | --- |
| Public admin gRPC surface with `ListClients`, `DescribeClient`, `ListClientsByGroup`, and `ListClientsByTopic` | Internal proto-free activity and endpoint executor exist; no generated public `ProxyAdminService` is registered. | Keep a standalone `ProxyAdminService` draft in docs and wire the real endpoint after `rocketmq-apis` ownership is confirmed. |
| Filters: `clientId`, `clientIdPrefix`, `group`, `topic`, `clientLanguage`, `connectTimeStart`, `connectTimeEnd`, `pageNum`, and `pageSize` | Internal queries, the read model, and proto-free request DTOs support these fields. | Map the same fields from generated public protobuf requests once `rocketmq-apis` ownership is confirmed. |
| `pageSize <= 100` | Client-admin query page size is capped at 100 in `ProxyClientQuery`. | Keep the generated public endpoint and benchmark scenarios on the same cap. |
| Response fields: `clientId`, `language`, `version`, `localAddress`, `remoteAddress`, and `connectionTime` | Response views already expose the equivalent metadata, including client version and connect time. | Preserve the existing response view and align public proto field names with the contest wording. |
| Error codes compatible with RocketMQ gRPC status codes | `ResponseBuilder` and admin endpoint handler already map internal exceptions to v2 `Status`. | Extend tests for new validation errors and the future public endpoint. |
| Independent ACL resource `proxy.admin.client` | Client-admin authorization now uses the logical resource name `proxy.admin.client`, encoded as `Admin:proxy.admin.client` in the RocketMQ ACL resource model. | Keep the future public endpoint on the same LIST/GET policy. |
| Separate query thread pool | The proto-free endpoint executor now runs admin queries on a dedicated bounded executor by default: 4 threads, queue capacity 10000, and the `ProxyClientAdminQueryThread_` thread-name prefix. | Keep the future generated public endpoint on this executor boundary. |
| Admin service and port isolation | Startup seams exist for adding admin bindable services, but the public admin service and separate admin gRPC port are not registered yet. | Add a standalone public admin server/port after generated protobuf classes are available. |
| OpenTelemetry metrics, traces, and logs | Admin service metrics now include operation, result, scope, status, page size, filter presence, result size, and duration. The proto-free endpoint also writes matching trace attributes and structured failure logs without sensitive identifiers. | Keep the future public endpoint on the same low-cardinality labels and attributes. |
| Unit tests and E2E tests | Unit and internal peer tests exist; public endpoint E2E is blocked by generated stubs. | Add unit coverage for new filters now and add in-process public gRPC E2E once generated stubs are available. |
| English and Chinese documentation | English design, English user guide, Chinese user guide, and public API discussion docs exist. | Keep all docs synchronized with the final public endpoint and benchmark results before final submission. |
| 1M client query benchmark | Read-model and coordinator JMH benchmarks cover unfiltered, group, topic, proxy-id, prefix, language, connect-time range, and `pageSize=100` scenarios. | Run full 1M JMH locally before final submission and attach representative results. |

## Final Acceptance Matrix

This branch should be considered complete contest material only when the
following acceptance points are all true:

| Acceptance point | Required final state |
| --- | --- |
| Public service contract | A reviewed standalone `ProxyAdminService` contract exposes `ListClients`, `DescribeClient`, `ListClientsByGroup`, and `ListClientsByTopic` without extending the data-plane `MessagingService`. |
| Public endpoint implementation | A generated public gRPC adapter is registered only after `rocketmq-apis` ownership is settled; until then the repo carries a documentation-only draft and an endpoint-ready internal adapter. |
| Isolation | Admin queries are isolated from data-plane traffic by service boundary, ACL resource, dedicated query executor, and a separate admin gRPC server/port when the public endpoint is enabled. |
| Query semantics | All official filters are pushed into the read model or service layer, `pageNum` is 1-based, `pageSize` is capped at 100, and M1 public scope is `LOCAL_PROXY`. |
| Security | List-style RPCs require `LIST` on `proxy.admin.client`; `DescribeClient` requires `GET` on `proxy.admin.client`; missing or unauthorized subjects are rejected before query execution. |
| Observability | Admin operations record low-cardinality metrics, trace attributes, and failure logs for operation, scope, status/result, duration, filters, page size, and result size. |
| Validation | Unit, integration, and public in-process gRPC E2E tests cover success, filtering, pagination, ACL deny, not found, bad request, and shutdown behavior. |
| Performance | A benchmark report demonstrates paginated query P99 latency below 1s at 1M synthetic clients on a documented local environment. |
| Documentation | English and Chinese docs describe API fields, configuration, ACL, metrics, errors, examples, LOCAL_PROXY limitations, and the public API ownership gate. |

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
  Describe-client identifiers also reuse the read-model client-id validator at
  the activity boundary, so malformed ids are rejected before ACL or delegate
  execution.
- `ProxyClientAdminResult` preserves the public status/body split expected by a
  gRPC endpoint while keeping the internal service API simple.
- `ProxyClientAdminClientView` and `ProxyClientAdminPageView` are public-facing
  response views. They avoid exposing the mutable internal read-model classes as
  the eventual protobuf adapter contract. The views require a nonblank client
  id, reject overlong ids and reserved coordinator page-token prefixes, reject
  null client entries in pages, snapshot collections, trim nullable
  string metadata to empty public strings, cap response `proxyId` values at
  255 characters, normalize repeated `groups` and `topics` entries by trimming,
  bounding them to the existing RocketMQ group/topic length limits,
  de-duplicating, and dropping blank values, and normalize blank public
  next-page tokens to an empty string. Public `nextPageToken` values are capped
  at 4096 characters at the response-view boundary so direct adapter or peer
  conversion cannot emit a token larger than the future public request decoder
  accepts.
- `ProxyClientAdminListClientsRequest`,
  `ProxyClientAdminDescribeClientRequest`,
  `ProxyClientAdminListClientsByGroupRequest`, and
  `ProxyClientAdminListClientsByTopicRequest` are the existing internal
  compatibility DTOs that let the branch exercise the admin adapter before
  generated public protobuf classes are available. They normalize request string
  fields at the adapter boundary so surrounding whitespace is trimmed and blank
  strings become missing values before validation. Group and topic request
  identifiers are bounded by `Validators.GROUP_MAX_LENGTH` and
  `Validators.TOPIC_MAX_LENGTH` before query construction, and describe-client
  ids are bounded by `Validators.CHARACTER_MAX_LENGTH` and reject the reserved
  coordinator page-token prefix. They also require a
  nonblank `proxy_id` for the explicit `PROXY_ID` scope before context creation,
  and preserve `proxy_id` only for that scope. Direct DTO use and future protobuf
  conversion therefore share the same M1 local and broadcast-scope semantics.
- `DefaultClientAdminService` also canonicalizes `LOCAL_PROXY` queries before
  reading the model, dropping accidental `proxyId` filters from direct internal
  callers while still rejecting future non-local scopes in M1. Direct
  `DescribeClient` calls reuse the read-model client-id validator so overlong
  ids and coordinator page-token prefixes are rejected as malformed requests
  before lookup rather than being reported as missing clients.
- `ProxyClientAdminScopeRouter` keeps the default `LOCAL_PROXY` path on
  `ProxyClientAdminActivity` and routes explicitly enabled coordinator scopes to
  `ProxyClientAdminCoordinatorService` after authorization. Router-level
  exception-to-status conversion preserves the interrupted flag before returning
  status-only error results, including interrupts wrapped by async adapter
  exceptions.
- `ProxyClientAdminCoordinatorService` owns the proto-free cross-proxy fan-out
  and merge semantics for `ALL_PROXIES` and `PROXY_ID`. Coordinator-level
  exception-to-status conversion also preserves the interrupted flag, including
  direct or async-wrapped peer-discovery or fan-out interruption before a peer
  response is available.
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
  it through `ResponseWriter`. Handler-level action and response-conversion
  failures also preserve the interrupted flag before they are converted to
  status responses, including interrupts wrapped by async adapter exceptions.
- `ProxyClientAdminEndpointExecutor` is a proto-independent shell for generated
  unary admin methods. It adapts the proto request to the internal request DTO
  before creating the `ProxyContext`, so malformed public request fields such as
  invalid page tokens and M1-disabled public cross-proxy scopes fail at the
  adapter boundary without running the admin request pipeline. Once the DTO is
  built, it creates the `ProxyContext`, delegates to
  `ProxyClientAdminEndpointHandler`, and routes context or request-adapter
  failures through the same status conversion path. If an adapter or context
  factory boundary surfaces an interrupt, including one wrapped by async adapter
  exceptions, the executor restores the thread interrupted flag before writing
  the status response. It offers explicit-header
  overloads for tests and adapter seams, plus no-header
  overloads that read `GrpcConstants.METADATA` from
  `Context.current()` to match normal generated gRPC method bodies. It also
  requires the context factory to return a non-null `ProxyContext` before the
  endpoint handler can run, so broken admin context initialization is reported as
  a status response instead of leaking into activity execution.
  The default gRPC activity wires this executor to a dedicated admin query
  thread pool with 4 threads, queue capacity 10000, and thread names prefixed by
  `ProxyClientAdminQueryThread_`. The pool is configurable through
  `proxyClientAdminQueryThreadPoolNums` and
  `proxyClientAdminQueryThreadPoolQueueCapacity`, is shut down with the default
  activity lifecycle, and maps executor rejection to the normal admin status
  response path.
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
  Successful peer responses must carry exactly the expected page or client body
  without error fields, and peer error responses must remain status-only without
  page/client bodies.
  Raw peer JSON request and response messages are capped at 1 MiB before JSON
  parsing so malformed or oversized peer payloads fail at the transport boundary.
  Peer error codes are capped at 255 characters and peer error messages are
  capped at 4096 characters at the peer-response DTO boundary. Error messages
  use a fixed `...(truncated)` suffix when shortened, so local exception text
  cannot expand an otherwise bounded peer error payload. Successful peer page
  responses also validate `nextPageToken` as a local read-model client-id cursor,
  rejecting overlong values and reserved coordinator page-token prefixes before
  the response leaves the raw peer boundary.
  The gRPC peer transport validates outbound request messages before invoking a
  peer and validates inbound response messages before returning them to the
  message client.
- `ProxyClientAdminPeerMessageClient` adapts the object-level peer-client
  contract to a raw message transport. It validates the target `proxyId` before
  encoding a JSON request or invoking the transport, so overlong target ids fail
  before any raw peer payload is sent. `ProxyClientAdminPeerMessageHandler`
  adapts raw messages back to the local peer executor. Raw transport failures
  are converted into peer error responses, and interrupted raw peer calls restore
  the thread interrupted flag before returning that error, including interrupts
  wrapped by async adapter exceptions. The in-process message transport keeps
  local multi-proxy simulations on the same serialized boundary that a real
  transport will use, including applying the peer request-message bound before
  invoking the local handler and the peer response-message bound before
  returning the handler output, and applies the same interrupt preservation rule
  when local handler execution is interrupted or wrapped by an async adapter
  exception.
- `ProxyClientAdminPeerGrpcService` exposes an internal unary gRPC service using
  `StringValue` request and response bodies that carry the peer JSON payload. It
  validates inbound peer request messages before creating the proxy context,
  builds `ProxyContext` through the same admin context factory, delegates to the
  peer message handler, validates outbound peer response messages before
  returning them, and restores the interrupted flag when service-side
  handler execution is interrupted or wrapped by an async adapter exception
  before being written as a gRPC error.
  `ProxyClientAdminPeerGrpcTransport` is the matching client-side raw message
  transport over `Channel` and the service method descriptor. The transport maps
  peer gRPC status failures into peer error responses and restores the
  interrupted flag when the underlying invocation is interrupted directly or via
  an async adapter wrapper. Async-wrapped gRPC status failures are unwrapped
  before mapping, so deadline or unavailable peer calls still surface as
  `PROXY_TIMEOUT` peer errors instead of generic internal errors. Oversized or
  blank outbound request messages are returned as `BAD_REQUEST` peer errors
  before any network call; oversized or blank inbound response messages are
  normalized as `INTERNAL_SERVER_ERROR` peer errors. These classes do not add or
  modify any public RocketMQ protobuf service definitions.
- When cross-proxy query support is enabled, `DefaultGrpcMessagingActivity`
  creates the internal peer gRPC service beside the local coordinator peer
  client. The default coordinator still uses the local in-process message
  transport when `proxyClientAdminPeerGrpcTargets` is blank. If static peer
  targets are configured, the coordinator uses `ProxyClientAdminPeerGrpcTransport`
  backed by per-peer gRPC channels while preserving the same proto-free peer
  message boundary. The peer transport forwards the current admin
  `ProxyContext` into gRPC metadata, including the authenticated user subject
  and request address/client attributes, so the peer-side context factory sees
  the same admin caller context as the coordinator. Known admin metadata keys
  from the ambient gRPC context are cleared before the `ProxyContext` values are
  written, so stale authorization or request identity headers cannot leak into a
  peer call; unrelated metadata can still pass through. Static peer gRPC
  channels are shut down gracefully with a bounded wait when the shared activity
  stops, and are forced closed if they do not terminate in time. Dynamic discovery,
  secure channel options, and deeper production channel tuning remain follow-up
  work. Static target lists must include the local `proxyName`; otherwise
  startup rejects the configuration so `ALL_PROXIES` queries do not silently omit
  clients connected to the coordinator proxy itself. Static peer target parsing
  also caps configured proxy ids and host names at 255 characters before a
  target can enter the channel map, keeping configuration-derived peer ids out
  of unbounded coordinator tokens and peer error payloads.
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
  string client_id; // Optional exact-id filter.
  string client_id_prefix;
  string group;
  string topic;
  string client_language;
  int64 connect_time_start_millis;
  int64 connect_time_end_millis;
  int32 page_num; // 1-based.
  int32 page_size; // M1 public cap: 100.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsResponse {
  Status status;
  repeated Client clients;
  bool has_more;
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
  string client_id; // Optional exact-id filter.
  string client_id_prefix;
  string client_language;
  int64 connect_time_start_millis;
  int64 connect_time_end_millis;
  int32 page_num; // 1-based.
  int32 page_size; // M1 public cap: 100.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsByGroupResponse {
  Status status;
  repeated Client clients;
  bool has_more;
}
```

### ListClientsByTopic

Request:

```text
ListClientsByTopicRequest {
  string topic;
  string client_id; // Optional exact-id filter.
  string client_id_prefix;
  string client_language;
  int64 connect_time_start_millis;
  int64 connect_time_end_millis;
  int32 page_num; // 1-based.
  int32 page_size; // M1 public cap: 100.
  ProxyScope scope; // M1: LOCAL_PROXY only
  string proxy_id; // Required for future PROXY_ID scope; ignored for LOCAL_PROXY/ALL_PROXIES.
}
```

Response:

```text
ListClientsByTopicResponse {
  Status status;
  repeated Client clients;
  bool has_more;
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
addresses, and client version, trims the proxy id, caps response proxy ids at
255 characters, and normalizes missing or blank values to empty strings before a
future protobuf adapter writes response fields. A
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
`ProxyClientQuery` carries optional client id, client id prefix, group, topic,
client language, connect-time range, client type, page number, page size, page
token, scope, and proxy id filters. `CLIENT_TYPE_UNSPECIFIED` is normalized to a
missing client type filter in `ProxyClientQuery`, so direct read-model callers
and future public request adapters share the same no-filter semantics.
`UNRECOGNIZED` client type values are rejected at query construction, keeping
direct service calls aligned with the public request adapter and coordinator
validation boundaries.

## Read Model and Indexes

`ProxyClientReadService` is an in-memory local read model. It owns:

- `clientId -> ProxyClientInfo`
- `sorted clientId index`
- `group -> sorted clientId set`
- `topic -> sorted clientId set`
- `clientType -> sorted clientId set`
- `proxyId -> sorted clientId set`
- `clientLanguage -> sorted clientId set`
- `connectTimeMillis -> sorted clientId set`

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
requested secondary indexes or sorted client-id range, copy the smallest
candidate index, and intersect the remaining candidates. This keeps the M1 read
model simple while avoiding unnecessary full-snapshot copies for common
paginated reads and selective client id, prefix, group, topic, language,
connect-time, type, and proxy queries.

The read model normalizes client ids by trimming surrounding whitespace before
storing, looking up, or removing entries. Client ids are bounded by
`Validators.CHARACTER_MAX_LENGTH` before indexing. Group and topic index values
are also trimmed, de-duplicated by the index sets, and blank values are ignored.
Group and topic values are bounded by the existing RocketMQ client limits before
indexing or query lookup: group names use `Validators.GROUP_MAX_LENGTH` and
topic names use `Validators.TOPIC_MAX_LENGTH`. Proxy ids and client languages
are indexed with the same sorted-set structure so future `PROXY_ID` and fan-out
merge paths can restrict a query to one source proxy without changing pagination
order. Connect-time range queries read a bounded range from a sorted
connect-time index and then merge its client-id sets back into stable client-id
order.
Client ids with the `cp<digits>:` prefix are rejected because that namespace is
reserved for coordinator-owned page tokens; accepting them into the local read
model would make a local next-page token ambiguous or unencodable.

The service is synchronized in M1. That keeps the implementation simple and
consistent while lifecycle updates and admin reads are still local to one proxy
process. A future high-scale implementation can replace the internal maps with
lock-striped or immutable-snapshot indexes without changing the service API.

## Pagination

Pagination is bounded by `ProxyClientQuery.MAX_PAGE_SIZE`, which is 100 to match
the RIP-2 public API requirement. Non-positive page sizes use
`DEFAULT_PAGE_SIZE`. Public request DTOs use one-based `pageNum` plus `pageSize`;
the read model applies the page-number offset after all requested filters have
been intersected in stable client-id order.

Page tokens are still supported as an internal compatibility path. Local page
tokens are based on the last client id returned by the previous page. Because
`LOCAL_PROXY` read-model page tokens are client-id cursors, `ProxyClientQuery`
trims them, rejects overlong values using the same
`Validators.CHARACTER_MAX_LENGTH` bound as client ids, and rejects the reserved
coordinator `cp<digits>:` prefix before lookup. Coordinator scopes preserve
coordinator-owned tokens for the coordinator token codecs to decode and
validate. When a token is supplied, it takes precedence over `pageNum` and must
exist in the filtered candidate set; otherwise `ProxyClientReadService` throws
`IllegalArgumentException`.

Unfiltered pages advance through the maintained sorted client id index.
Filtered pages advance through the sorted candidate set after the requested
client id, prefix, group, topic, language, connect-time, type, and proxy indexes
have been intersected.

The public adapter treats page tokens as opaque values. The current M1 codec
accepts canonical `v1:` tokens and legacy bare read-model tokens, rejects
unknown or non-canonical versioned tokens, and caps incoming public page tokens
at 4096 characters to bound decode work before the future public endpoint is
registered. The encoder enforces the same cap so the adapter does not emit a
next-page token that the next request would reject.

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
create multiple public cursor representations. The encoder enforces the same
4096-character cap as the decoder so a large peer cursor map does not produce an
unusable next-page token. The coordinator validates the token creation time
before peer discovery and rejects expired or future-dated tokens as
`BAD_REQUEST`; `proxyClientAdminCoordinatorPageTokenTtlMillis` controls the
retention window and defaults to five minutes. The response adapter preserves
canonical `cp1:` tokens instead of wrapping them in the local read-model `v1:`
token codec. It is not wired into the M1 `LOCAL_PROXY` endpoints and does not
change the public local `v1:` token behavior. The local token codec rejects bare
coordinator-owned `cpN:` tokens, `v1:` tokens whose decoded read-model cursor is
coordinator-owned, and attempts to encode coordinator-prefixed read-model
cursors. This keeps `LOCAL_PROXY` and `PROXY_ID` requests from accidentally
treating a cross-proxy cursor as a read-model client-id cursor. Request DTOs
preserve `cp1:` tokens only for `ALL_PROXIES`, where the coordinator owns
decoding and validation. A non-empty coordinator token must include the last
emitted global cursor (`client_id` plus `proxy_id`), token creation time, and at
least one peer cursor;
otherwise the coordinator rejects it as an incomplete progress token instead of
restarting from the first peer page or interpreting duplicate client ids
ambiguously. The coordinator token object also validates the embedded
`lastClientId` and per-peer page token values as local client-id cursors,
rejecting overlong values and reserved coordinator `cp<digits>:` prefixes before
encoding, decoded token reuse, or peer fan-out. Embedded group and topic filters
are bounded to the same RocketMQ group/topic length limits used by request DTOs
and `ProxyClientQuery`.

When the coordinator builds a next token, it preserves a peer's own next-page
token after that peer's returned page has been fully emitted. If the global
merge stops in the middle of a peer page, it stores the last emitted client id
for that peer so the next coordinator request can replay the remaining peer
page without skipping clients. If peer responses indicate more data but the
coordinator cannot emit any client for the current global page, it fails the
request as an internal pagination error instead of returning an empty terminal
page and silently dropping peer progress. When a coordinator token carries a
per-peer cursor, the next peer page must only return client ids after that peer
cursor and after the coordinator's global `(client_id, proxy_id)` cursor;
otherwise the coordinator treats the peer response as stale or misrouted and
returns an internal routing error before merging the page. For untokened peers,
progress is checked against the same global cursor so duplicate client ids on
different proxies can be paginated in stable proxy-id order. Every peer page
must also be strictly ordered by increasing `client_id`; otherwise the
coordinator rejects it before building global pagination state. A peer's
returned next-page token must also make forward progress: it cannot be at or
before the page token sent to that peer, and it cannot sort before the last
client id returned in that peer page. This prevents the coordinator from
emitting an opaque token that would make the next request repeat or rewind a
peer page.

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
   carry `LOCAL_PROXY` execution semantics; they also reject explicit `proxyId`
   values because peer targeting is owned by the coordinator and peer client.
   Peer `DescribeClient` requests also reject malformed client ids, including
   reserved coordinator page-token prefixes, before converting into the local
   activity request model. Peer list requests treat `pageToken` as a local
   read-model client-id cursor and reject overlong values or reserved
   coordinator page-token prefixes before the request reaches the peer-local
   executor.
   The coordinator lowers `ALL_PROXIES` and `PROXY_ID` requests to local peer
   requests before fan-out.
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
   `ProxyAdminService` API. The raw peer request codec preserves and rejects
   any inbound `proxyId` field instead of silently dropping it at the JSON
   boundary, and the message client rejects overlong target proxy ids before
   raw request encoding or transport invocation.
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
   rejects empty, blank, overlong, or duplicate peer ids as internal discovery errors,
   rejects coordinator page tokens that reference peers outside the current
   discovery set as bad requests, and validates each peer response `proxyId`
   before accepting its body so a misrouted or stale peer transport result is
   returned as an internal routing error instead of being merged into the admin
   response. Peer wire responses reject mixed success bodies, so page responses
   cannot also carry a client body and describe responses cannot also carry a
   page body; successful page responses must carry an explicit `clients` array
   so a malformed peer cannot be silently decoded as an empty page. Peer page
   bodies are also validated before merge so malformed peer results and peer
   client bodies without usable client ids, or with malformed peer
   `nextPageToken` values, become stable internal errors instead of leaking as
   merge-time exceptions or successful malformed describe results.
   Describe responses must also return the exact
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
   restoring the interrupt flag; delegate failures that wrap an
   `InterruptedException` restore the caller interrupt flag before being mapped
   to the existing discovery or peer error response. Coordinator page-token
   retention is also bounded by `proxyClientAdminCoordinatorPageTokenTtlMillis`;
   expired or future-dated `cp1:` tokens are rejected before peer discovery so
   stale or non-monotonic cursors do not trigger remote fan-out. Enabling the
   coordinator scope flag also requires a nonblank
   `proxyName`, which becomes the
   stable local peer id; the default local-only `DEFAULT_PROXY` fallback is not
   used for coordinator scopes. The in-process peer client converts local executor
   failures into peer error responses so coordinator fan-out receives a bounded
   peer result instead of an exception escaping the peer-client boundary, while
   preserving the interrupted flag when local executor work is interrupted
   directly or through an async wrapper. The default local peer executor now
   delegates peer-local work directly to the shared `ClientAdminService` instead
   of re-entering the public admin activity,
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

The contest-facing admin API should authorize against a dedicated client-query
resource:

- list operations require `LIST` on `proxy.admin.client`.
- get operations require `GET` on `proxy.admin.client`.

The current internal implementation provides `ClientAdminAuthPolicy`,
`DefaultClientAdminAuthorizationService`, and `AuthorizingClientAdminService`.
`ClientAdminAuthPolicy` maps that logical resource to
`Admin:proxy.admin.client` so the permission is independent from the configured
cluster name while still using the existing typed RocketMQ ACL model.
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
Coordinator-scope requests also validate required identifiers before
authorization: `DescribeClient` requires `client_id`, group queries require
`group`, and topic queries require `topic`. This keeps malformed cross-proxy
requests from touching ACL state or peer routing.
List-style coordinator-scope requests also decode and validate page tokens and
client-type filters before authorization, so malformed opaque tokens are
reported as `BAD_REQUEST` without invoking ACL or peers.
Topic-level or group-level ACL can be discussed later if the community wants
more granular visibility controls.

## Metrics Plan

The current internal implementation exposes read-model gauges for:

- current local online client count.
- current local online client count by `clientType`.
- current local group/topic/proxy index count.

It also records read-model upsert/remove mutation counters.

The current internal admin service wrapper records:

- admin query counters by operation, result code, proxy query scope, status,
  page size, filter presence, and result size.
- admin query latency histograms by operation, result code, proxy query scope,
  status, page size, filter presence, and result size.

Metrics are recorded around the authorizing admin service, not only around the
read-model service. This means ACL denials are reported as `UNAUTHORIZED`, while
successful reads, bad requests, not-found responses, and unexpected internal
errors are still counted once at the public admin operation boundary.

The `scope` and `filters` labels are intentionally low cardinality. `scope`
records `local_proxy`, `all_proxies`, or `proxy_id`, but it does not include the
target proxy id. `filters` records only filter names such as
`client_id_prefix`, `client_language`, or `connect_time_range`; it does not
include any client, group, topic, proxy, or subject identifier.

The proto-free endpoint records OpenTelemetry span attributes for operation,
scope, status, page size, filter presence, and result size. It also writes a
structured warning for failed admin requests with operation, status, result,
scope, filters, page size, and result size. The failure log deliberately omits
client ids, group names, topic names, proxy ids, and auth subjects.

Internal coordinator scopes use the same one-operation boundary at the scope
router. The router records exactly one operation metric for a coordinator-scope
request after mapping the final status to `OK`, `BAD_REQUEST`, `NOT_FOUND`,
`UNAUTHORIZED`, `TIMEOUT`, `TOO_MANY_REQUESTS`, `NOT_IMPLEMENTED`, or
`INTERNAL_ERROR`. That includes the `BAD_REQUEST` result when cross-proxy scopes
are disabled by configuration, missing required identifiers, or malformed page
tokens, the `TIMEOUT` result when peer fan-out or discovery exceeds its bounded
wait, and explicit peer throttling or not-implemented results from the internal
gRPC transport.
Wrapped asynchronous exceptions are classified by their cause chain so
bad-request, not-found, authorization, and timeout outcomes are not counted as
internal errors.
Peer-local execution is deliberately routed through the shared
`ClientAdminService`, not the public activity wrapper, so a coordinator request
is not counted again as a nested local public admin request.

Metric recording is best effort. Read-model mutation recorder failures and
admin query metrics recorder failures are logged but do not mask successful
lifecycle/admin operations or the original service exception.

The public adapter should reuse these low-cardinality metric labels, trace
attributes, and structured log fields when the API surface is finalized.

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

`ResponseBuilder` maps gRPC `StatusRuntimeException` and `StatusException`
instances into the same public status vocabulary the admin endpoint expects:
invalid-argument style statuses become `BAD_REQUEST`, `NOT_FOUND` stays
`NOT_FOUND`, authentication and permission statuses become `UNAUTHORIZED`,
resource exhaustion becomes `TOO_MANY_REQUESTS`, unimplemented methods become
`NOT_IMPLEMENTED`, and gRPC deadlines or unavailable channels become
`PROXY_TIMEOUT`. Coordinator peer-discovery timeouts also surface as
`PROXY_TIMEOUT` instead of a generic internal error.

The internal peer gRPC transport applies the same mapping before encoding a
peer error response so coordinator fan-out sees one normalized status model.
It also maps invalid outbound peer request payloads to `BAD_REQUEST` before the
network call, while malformed or oversized peer response payloads are converted
to peer `INTERNAL_SERVER_ERROR` results at the transport boundary.
The admin gRPC error writer preserves explicit gRPC status exceptions, including
those wrapped by async adapters, before they reach the transport mapper. Unknown
transport failures remain
`INTERNAL_SERVER_ERROR`.
For coordinator scopes, required identifiers are checked by the scope router
before authorization and fan-out, so malformed `ALL_PROXIES` or `PROXY_ID`
requests map to `BAD_REQUEST` without touching ACL state or peer routing.
List-style coordinator requests also validate page tokens and client-type
filters before authorization, keeping request-shape errors on the same
pre-authorization path.

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
- reserved coordinator page-token client id prefix rejection.
- read-model client metadata rejection for overlong client ids before indexing.
- read-model client metadata rejection for overlong proxy ids before indexing.
- read-model query rejection for overlong proxy id filters before lookup.
- read-model query rejection for overlong page tokens before lookup.
- local read-model query rejection for reserved coordinator page-token prefixes
  before lookup while coordinator scopes preserve coordinator-owned tokens for
  coordinator validation.
- read-model query rejection for `UNRECOGNIZED` client type filters before
  service or read-model execution.
- read-model client metadata rejection for overlong group/topic names before
  indexing.
- read-model query rejection for overlong group/topic filters before lookup.
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
- request DTO rejection for overlong `PROXY_ID` proxy ids before routing.
- request DTO and request-converter rejection for overlong group/topic
  identifiers before query construction.
- request DTO, request-converter, and response-view rejection for overlong
  client ids before endpoint handling or protobuf response construction.
- describe-client request DTO rejection for reserved coordinator page-token
  client-id prefixes before endpoint handling.
- response-view rejection for reserved coordinator page-token client-id prefixes
  before protobuf response construction.
- default `LOCAL_PROXY` scope, opaque page-token encode/decode, and proxy id
  pass-through for future scoped queries.
- activity-level `LOCAL_PROXY` query canonicalization before authorization and
  delegate calls.
- service-level `LOCAL_PROXY` query canonicalization that drops accidental
  proxy-id filters before querying the local read model.
- service-level `DescribeClient` rejection for overlong client ids and reserved
  coordinator page-token prefixes before read-model lookup.
- activity overloads for request DTOs.
- activity-level rejection of unsupported M1 scopes before ACL or delegate
  invocation.
- activity-level `DescribeClient` rejection for reserved coordinator page-token
  client-id prefixes before ACL or delegate invocation.
- public endpoint-executor pre-validation for overlong group/topic filters
  before `ProxyContext` creation or endpoint handling.
- public endpoint-executor pre-validation for overlong and reserved-prefix
  `DescribeClient` client ids before `ProxyContext` creation or endpoint
  handling.
- response view conversion, stable collection snapshots, and null-safe string
  metadata normalization.
- endpoint handler success response writing, error response writing, and thrown
  action error mapping.
- cross-package shared wiring for future admin gRPC application access to
  `ProxyClientAdminActivity`.
- coordinator pagination rejecting peer pages that go backward relative to the
  per-peer cursor stored in a coordinator-owned page token.
- coordinator pagination rejecting tokened peer pages that are after their
  per-peer cursor but still behind the global coordinator cursor.
- coordinator pagination rejecting stale peer next-page tokens that do not
  advance beyond the input peer cursor or that rewind behind the returned page.
- scope-router coordinator pre-validation for missing client id, group, and
  topic before authorization or peer routing.
- scope-router coordinator pre-validation for malformed page tokens before
  authorization or peer routing.
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
- coordinator page-token TTL rejection before peer discovery.
- coordinator page-token rejection for overlong embedded proxy ids before
  encoding or decoded token reuse.
- coordinator page-token rejection for overlong or reserved embedded client-id
  cursor values before encoding, decoded token reuse, or peer fan-out.
- coordinator page-token rejection for overlong embedded group/topic filters
  before encoding, decoded token reuse, or peer fan-out.
- query construction rejection for overlong `PROXY_ID` proxy ids before
  coordinator peer routing.
- real peer gRPC fan-out from coordinator through `ProxyClientAdminPeerGrpcTransport`
  into two in-process Netty peer services, verifying merged `ALL_PROXIES`
  results and proxy id stamping.
- peer gRPC metadata propagation from `ProxyContext` while clearing stale known
  admin metadata keys from the ambient gRPC context.
- local peer executor rejection for overlong configured local proxy ids before
  response stamping.
- peer gRPC transport request and response payload bounds before and after the
  network call.
- malformed outbound peer request payload rejection before both local handler
  invocation and gRPC peer calls.
- malformed peer response payload mapping at both in-process and gRPC transport
  boundaries.
- peer error response message truncation before peer payload encoding.
- static peer gRPC target rejection for overlong proxy ids and host names.
- peer client and transport discovery rejection for overlong proxy ids before
  coordinator page-token construction.
- peer `DescribeClient` request rejection for reserved coordinator page-token
  client-id prefixes before peer-local execution.
- peer list request rejection for overlong local page tokens and reserved
  coordinator page-token prefixes before peer-local execution.
- peer page response rejection for overlong local next-page tokens and reserved
  coordinator page-token prefixes before coordinator token construction.
- in-process peer transport request/response bounds around local handler
  invocation.
- peer error response code length rejection before peer payload encoding and at
  the peer response boundary.
- peer error response code rejection when `success=false` uses `OK` or
  `UNRECOGNIZED`.
- peer error response code rejection when the code is not a known RocketMQ
  `Code`.
- peer response envelope rejection when the peer `proxyId` is missing.
- peer response envelope rejection when the peer `proxyId` exceeds the bounded
  proxy-id length.
- peer client-body rejection when the client `proxyId` exceeds the bounded
  proxy-id length.
- peer client-body rejection when the client `proxyId` does not match the
  response-envelope `proxyId`.
- peer error response envelope rejection when `success=false` omits `errorCode`.
- peer success response rejection when a peer payload also carries error fields.
- operation-aware peer response validation that rejects successful responses
  without the expected page or client body at service and transport boundaries.
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

On 2026-07-08 Asia/Shanghai time, after refreshing `upstream/develop` to commit
`0e4ccf1b6`, adding admin metrics scope labels, and hardening peer gRPC request
and response payload bounds including service-side request and response
validation, peer error-message truncation, static peer-target length bounds,
in-process peer transport request/response bounds, plus coordinator
stale-peer-page and stale-peer-next-token checks, the admin endpoint,
coordinator, startup wiring, metrics, and authorization suite was revalidated
with:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  '-Dtest=ProxyClientAdmin*Test,TimedProxyClientAdminPeerClientTest,GrpcProxyAdminWiringTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest' \
  -DfailIfNoTests=false test -DskipITs
```

The run reported `Tests run: 487, Failures: 0, Errors: 0, Skipped: 0` and ended
with `BUILD SUCCESS`. The same JDK 17 JaCoCo instrumentation noise appeared in
the log, but Maven exited successfully.

On 2026-07-08 Asia/Shanghai time, the public endpoint-executor boundary was
hardened before `ProxyContext` creation: group/topic filters now reject overlong
values, and `DescribeClient` now rejects overlong or reserved coordinator
client-id prefixes even if a future adapter or test double bypasses request DTO
builders. The focused red/green checks first failed with `BAD_REQUEST` expected
but `INTERNAL_SERVER_ERROR` returned from the context-creation path, then passed
after the executor reused the shared identifier validators.

The final broad verification for this checkpoint used:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  '-Dtest=ProxyClientAdmin*Test,TimedProxyClientAdminPeerClientTest,GrpcProxyAdminWiringTest,DefaultGrpcMessagingActivityTest,ProxyStartupTest,GrpcRequestPipelineFactoryTest,ProxyMetricsManagerTest,DefaultClientAdminServiceTest,AuthorizingClientAdminServiceTest,DefaultClientAdminAuthorizationServiceTest,ClientAdminAuthPolicyTest,MeteredClientAdminServiceTest,MeteredAuthorizingClientAdminServiceTest,ProxyClientInfoTest,ProxyClientQueryTest,ProxyClientReadServiceTest,ClientActivityTest#testConsumerTelemetryUpdatesProxyClientReadService+testProducerTelemetryUpdatesProxyClientReadService' \
  -DfailIfNoTests=false test -DskipITs
```

After `5564a8606`, Maven reported
`Tests run: 607, Failures: 0, Errors: 0, Skipped: 0` and ended with
`BUILD SUCCESS`. JDK 17 JaCoCo instrumentation stack traces remained
environment noise because Surefire and Maven both completed successfully.

## Synthetic Benchmark

The read model includes a JMH benchmark in
`ProxyClientReadServiceBenchmark`. It builds synthetic in-memory client
metadata and measures the steady-state local query paths that M1 exposes:

- unfiltered first-page listing.
- unfiltered next-page listing.
- group-filtered listing.
- topic-filtered listing.
- proxy-id-filtered listing.
- client-id-prefix-filtered listing.
- client-language-filtered listing.
- connect-time-range-filtered listing.
- direct client lookup.

The cross-proxy coordinator experiment also includes
`ProxyClientAdminCoordinatorServiceBenchmark`. It builds synthetic per-proxy
read models behind a real `ProxyClientAdminCoordinatorService` and
`ProxyClientAdminPeerLocalExecutor` fan-out path, then measures:

- all-proxies first-page listing.
- all-proxies next-page listing.
- all-proxies group-filtered listing.
- all-proxies topic-filtered listing.
- all-proxies client-id-prefix-filtered listing.
- all-proxies client-language-filtered listing.
- all-proxies connect-time-range-filtered listing.
- targeted `PROXY_ID` listing.
- all-proxies client lookup.

The default benchmark parameters model 1,000,000 clients, 1,000 groups, 10,000
topics, 100 proxy ids, and a coordinator page size of 100. The benchmark
annotations run one fork, three one-second warmup iterations, five five-second
measurement iterations, and four worker threads.

Use the focused unit test to verify the benchmark setup and guard the synthetic
data assumptions:

```bash
JAVA_HOME=/Users/shuaimaoer/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home \
  mvn -pl proxy -am \
  -Dtest=ProxyClientReadServiceBenchmarkTest,ProxyClientAdminCoordinatorServiceBenchmarkTest \
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

Use the same launcher shape for the coordinator benchmark:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminCoordinatorServiceBenchmark \
  -p clientCount=1000 -p groupCount=10 -p topicCount=20 -p proxyCount=5 -p pageSize=100 \
  -wi 0 -i 1 -r 100ms -w 100ms -f 1 -t 1
```

The clean compile step is intentional. On 2026-07-06, an incremental
`test-compile` left stale JMH-generated classes under `proxy/target`, and JMH
forks reported unresolved benchmark methods even though the real benchmark
class contained those methods. After `clean test-compile`,
`ProxyClientReadServiceBenchmark_jmhType_B1` correctly extended
`ProxyClientReadServiceBenchmark`, and the smoke run completed all local
benchmarks:

- `describeClient`: sample 2378, about 0.0001 ms/op.
- `listByGroupPage`: sample 3223, about 0.003 ms/op.
- `listByClientIdPrefixPage`: prefix-filtered page scan.
- `listByConnectTimeRangePage`: connect-time-range-filtered page scan.
- `listByLanguagePage`: client-language-filtered page scan.
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

To run the full default coordinator scenario, use:

```bash
"$JAVA_HOME/bin/java" \
  -cp "proxy/target/test-classes:proxy/target/classes:$(cat /tmp/rocketmq-proxy-test-classpath.txt)" \
  org.openjdk.jmh.Main \
  org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminCoordinatorServiceBenchmark
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

Current branch status: after fetching `upstream/develop` at commit `0e4ccf1b6`
on 2026-07-08, `git grep` still finds no upstream `ProxyAdminService`,
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
4. Keep public contest pagination on `pageNum/pageSize`, with `pageSize` capped
   at 100. The existing `ProxyClientAdminPageTokenCodec` should remain an
   internal compatibility and coordinator-experiment boundary unless the
   community later chooses an opaque public token contract.
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
8. Preserve coordinator-scope pre-validation before authorization: missing
   `client_id`, `group`, `topic`, malformed page tokens, or unsupported client
   type filters should return `BAD_REQUEST` without invoking ACL or peer fan-out.
9. Require an explicit, nonblank `proxyName` before enabling cross-proxy
   coordinator scopes. `LOCAL_PROXY` can keep the default local-only fallback, but
   `ALL_PROXIES` and `PROXY_ID` must use stable, configured peer ids.
10. Register `GrpcProxyAdminApplication` beside `GrpcMessagingApplication` in
   `ProxyStartup.createGrpcBindableServices`, using the same
   `DefaultGrpcMessagingActivity` instance so lifecycle writes, read-model
   queries, ACL, metrics, and context propagation share one in-process state
   holder.
11. Keep endpoint methods free of business logic. They should adapt protobuf
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
