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
`DefaultClientAdminAuthorizationService`, and `AuthorizingClientAdminService` so
the future public adapter can authorize before delegating to read-model queries.
This keeps the first admin surface consistent with existing management actions.
Topic-level or group-level ACL can be discussed later if the community wants
more granular visibility controls.

## Metrics Plan

The current internal implementation exposes read-model gauges for:

- current local online client count.
- current local online client count by `clientType`.

Planned follow-up metrics are:

- read model upsert/remove counters.
- admin query counters by operation and result code.
- admin query latency histograms.

Admin-query metrics should be added after the public adapter is stable so label
cardinality is controlled.

## Error Semantics

Internal M1 errors:

- missing client id: `IllegalArgumentException`.
- invalid page token: `IllegalArgumentException`.
- client not found: empty result from the read model; the future admin service
  should map this to a not-found admin error.

Draft public adapter mapping:

- missing required field: `BAD_REQUEST`.
- invalid page token: `BAD_REQUEST`.
- unknown client id for `DescribeClient`: `NOT_FOUND`.
- unsupported scope: `BAD_REQUEST` until multi-proxy scopes are implemented.
- internal failures: `INTERNAL_ERROR`.

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

- producer telemetry updates.
- heartbeat preserves `connectTimeMillis` and updates `lastActiveTimeMillis`.
- termination removes client and indexes.
- producer unregister listener removes client and indexes.
- consumer unregister listener removes client and indexes.

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
6. Discuss public protobuf ownership before changing `rocketmq-apis`.
7. Add the public admin gRPC/protobuf adapter.
8. Wire the adapter through `AuthorizingClientAdminService`; internal ACL policy
   and service are already in place.
9. Extend metrics with admin query counters and latency histograms.
10. Add a synthetic 1M-client benchmark or simulation.
