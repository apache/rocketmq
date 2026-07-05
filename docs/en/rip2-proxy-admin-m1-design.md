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

M1 should accept only `LOCAL_PROXY`. Requests for future scopes such as
`ALL_PROXIES` or `PROXY_ID` should return `BAD_REQUEST` until cross-proxy
semantics are designed and implemented.

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

## Implemented Internal Adapter Preparation

The branch now includes a proto-independent internal admin adapter surface. It
does not register a public gRPC service yet, but it gives the future endpoint a
small and tested boundary to call:

- `ProxyClientAdminActivity` owns the request execution boundary for client
  admin queries. It accepts `ProxyContext`, calls `AuthorizingClientAdminService`,
  and returns `ProxyClientAdminResult<T>` with an `apache.rocketmq.v2.Status`
  plus an optional body. It also enforces the M1 `LOCAL_PROXY` scope before
  invoking authorization or read-model queries.
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
  trimmed and blank strings become missing values before validation.
- `ProxyClientAdminRequestConverter` centralizes the future proto-to-internal
  DTO mapping from public scalar fields, so generated unary methods can keep
  request conversion out of the RPC method bodies once `ProxyAdminService`
  lands. It drops `proxy_id` for the default or explicit public `LOCAL_PROXY`
  scope and preserves it only with future non-local scopes, keeping public M1
  local-scope behavior independent from the internal read-model proxy-id index.
- `ProxyClientAdminPageTokenCodec` is the adapter boundary for public pagination
  tokens. M1 encodes internal last-client-id tokens as versioned `v1:`
  base64url public tokens, accepts legacy bare client-id tokens only for early
  internal compatibility, rejects unknown versioned public tokens, normalizes
  blank public tokens to "no token", rejects versioned tokens whose decoded
  read-model token is not already in canonical trimmed form, and normalizes
  blank internal next tokens to an empty public string.
- `ProxyClientAdminScopeMapper` is the adapter boundary for public proxy scope
  values. It maps missing or `PROXY_SCOPE_UNSPECIFIED` public scope values to
  internal `LOCAL_PROXY`, maps prefixed public values such as
  `PROXY_SCOPE_ALL_PROXIES` and `PROXY_SCOPE_PROXY_ID` into the internal request
  model, keeps the shorter internal names accepted for tests and direct
  adapters, and rejects unknown scope names before they reach the service layer.
- `ProxyClientAdminEndpointHandler` centralizes the future unary endpoint
  response flow: execute an activity action, convert thrown exceptions through
  `ResponseBuilder`, build a response from `Status` and optional body, and write
  it through `ResponseWriter`.
- `ProxyClientAdminEndpointExecutor` is a proto-independent shell for generated
  unary admin methods. It adapts the proto request to the internal request DTO
  before creating the `ProxyContext`, so malformed public request fields such as
  invalid page tokens fail at the adapter boundary without running the admin
  request pipeline. Once the DTO is built, it creates the `ProxyContext`,
  delegates to `ProxyClientAdminEndpointHandler`, and routes context or
  request-adapter failures through the same status conversion path. It offers
  explicit-header overloads for tests and adapter seams, plus no-header
  overloads that read `GrpcConstants.METADATA` from
  `Context.current()` to match normal generated gRPC method bodies.
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
  are available.
- `ProxyStartup.createGrpcBindableServices(...)` now has a tested package-private
  overload for appending additional `BindableService` instances after the
  messaging service while reusing the same `DefaultGrpcMessagingActivity`. The
  future `GrpcProxyAdminApplication` can use that seam once generated protobuf
  classes are available.

The request DTOs convert pagination, client type, scope, and optional proxy id
into `ProxyClientQuery`. Required identifiers such as client id, group, and
topic, plus optional page token and proxy id, are trimmed at this boundary; blank
values are treated as absent and are rejected later when the operation requires
them. Page tokens pass through the dedicated token codec, which encodes the
read-model last-client-id token as a versioned opaque public token and decodes
versioned or legacy bare tokens back to the internal token.
Public scope names pass through the scope mapper so future generated protobuf
adapters can translate prefixed enum names such as
`PROXY_SCOPE_LOCAL_PROXY`, `PROXY_SCOPE_ALL_PROXIES`, and
`PROXY_SCOPE_PROXY_ID` without importing the generated admin service in this
branch. The default internal scope is `LOCAL_PROXY`; unsupported future scopes
and their proxy id are intentionally carried through the DTO and query objects
so they can be validated by the activity before authorization. The service layer
revalidates the same scope to keep direct internal calls consistent. This
preserves `BAD_REQUEST` semantics for unsupported scopes while keeping the
adapter contract ready for future `PROXY_ID` support. The protobuf default
`CLIENT_TYPE_UNSPECIFIED` is normalized to no client type filter, while
`UNRECOGNIZED` client type values are rejected
as `BAD_REQUEST`.

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
  string proxy_id; // Reserved for future PROXY_ID scope.
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
  string proxy_id; // Reserved for future PROXY_ID scope.
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
  string proxy_id; // Reserved for future PROXY_ID scope.
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
  string proxy_id; // Reserved for future PROXY_ID scope.
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
- `proxyId`: source proxy node id. M1 records the local `proxyName`; future
  `ALL_PROXIES` responses can use the same field to distinguish fan-out
  results.
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
operation metrics remain consistent. M1 does not schedule this cleanup by
default; it is a bounded maintenance hook for future online-client stale-entry
guardrails.

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
- activity overloads for request DTOs.
- activity-level rejection of unsupported M1 scopes before ACL or delegate
  invocation.
- response view conversion, stable collection snapshots, and null-safe string
  metadata normalization.
- endpoint handler success response writing, error response writing, and thrown
  action error mapping.
- cross-package shared wiring for future admin gRPC application access to
  `ProxyClientAdminActivity`.
- missing request DTO, missing identifiers, not found, unsupported scope,
  authorization failure, and unexpected runtime error mapping.

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
mvn -pl proxy -am -DskipTests -DskipITs test-compile
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

### Public Endpoint Rollout Checklist

The remaining public endpoint work should start only after the community agrees
where the protobuf API lives. Once that is settled, the implementation can land
as a narrow adapter over the internal code already in this branch:

Current branch status: after fetching `upstream/develop` at commit `8242c1e9d`
on 2026-07-06, `git grep` still finds no upstream `ProxyAdminService`,
`ProxyScope`, `ListClientsByGroup`, or `ListClientsByTopic` protobuf API to
consume. The documentation-only draft remains under `docs/en`, and this fork
should continue to avoid modifying `rocketmq-apis` until that ownership decision
is explicit.

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
6. Preserve M1 `LOCAL_PROXY` behavior. `PROXY_SCOPE_ALL_PROXIES` and
   `PROXY_SCOPE_PROXY_ID` should continue to return `BAD_REQUEST` until the
   cross-proxy query protocol is implemented.
7. Register `GrpcProxyAdminApplication` beside `GrpcMessagingApplication` in
   `ProxyStartup.createGrpcBindableServices`, using the same
   `DefaultGrpcMessagingActivity` instance so lifecycle writes, read-model
   queries, ACL, metrics, and context propagation share one in-process state
   holder.
8. Keep endpoint methods free of business logic. They should adapt protobuf
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
