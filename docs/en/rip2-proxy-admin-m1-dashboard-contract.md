# RIP-2 Proxy Admin M1 Dashboard Contract

This document is the handoff contract for RIP-1 Dashboard CLIENT-01
integration. It describes what a dashboard can consume from the generated public
`apache.rocketmq.v2.ProxyAdminService` endpoint in the RIP-2 M1 branch.

The branch already verifies these fields through generated gRPC tests:

- `GrpcProxyAdminApplicationTest#listClientsReturnsDashboardTableFieldsThroughGeneratedGrpcService`
  covers the table-row payload from `ListClients`.
- `GrpcProxyAdminApplicationTest#describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService`
  covers the detail payload from `DescribeClient`.

The remaining gate is an external joint E2E run in an environment that contains
the RIP-1 Dashboard client.

## Endpoint

Run Proxy with the public admin server enabled:

```properties
enableProxyAdminGrpcServer=true
proxyAdminGrpcServerPort=8082
```

Dashboard clients should call the admin gRPC server, not the data-plane
`MessagingService` server.

M1 public scope is process-local:

- omitted `scope` and `PROXY_SCOPE_LOCAL_PROXY` are accepted.
- `PROXY_SCOPE_ALL_PROXIES` and `PROXY_SCOPE_PROXY_ID` are rejected until the
  community accepts multi-proxy discovery, authorization, timeout, and
  pagination ownership semantics.

## Recommended Dashboard Flow

Use `ListClients` for the table view:

```bash
grpcurl -plaintext \
  -d '{"page_num":1,"page_size":100}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/ListClients
```

Use filters for dashboard search:

- `client_id` for exact client lookup.
- `client_id_prefix` for incremental search.
- `group` for consumer-group filtering.
- `topic` for topic filtering.
- `client_language` for language filtering.
- `connect_time_start_millis` and `connect_time_end_millis` for connection-time
  windows.

Use `DescribeClient` when opening a row detail panel:

```bash
grpcurl -plaintext \
  -d '{"client_id":"client-dashboard"}' \
  127.0.0.1:8082 \
  apache.rocketmq.v2.ProxyAdminService/DescribeClient
```

Use `ListClientsByGroup` and `ListClientsByTopic` for shortcut navigation from
group and topic pages.

## Field Mapping

| Dashboard field | Proto field | Meaning |
| --- | --- | --- |
| Client ID | `ProxyClient.client_id` | Stable client identity used for row keys and `DescribeClient`. |
| Client type | `ProxyClient.client_type` | RocketMQ client role, for example `PRODUCER` or `PUSH_CONSUMER`. |
| Groups | `ProxyClient.groups` | Groups associated with the client. The list is sorted for deterministic display. |
| Topics | `ProxyClient.topics` | Topics associated with the client. The list is sorted for deterministic display. |
| Language | `ProxyClient.language` | Client language from telemetry metadata, for example `JAVA`. |
| Remote address | `ProxyClient.remote_address` | Client-side remote socket address as observed by Proxy. |
| Local address | `ProxyClient.local_address` | Proxy-side local socket address for the connection. |
| Version | `ProxyClient.version` | Client version string from telemetry metadata. |
| Connect time | `ProxyClient.connect_time_millis` | Epoch milliseconds when Proxy first observed the client connection. |
| Last active time | `ProxyClient.last_active_time_millis` | Epoch milliseconds of the latest heartbeat or telemetry activity. |
| Proxy ID | `ProxyClient.proxy_id` | Local Proxy id that owns the process-local view. |
| Pagination | `has_more` plus `page_num` / `page_size` | Public list responses use page-number pagination and cap `page_size` at 100. |
| Status | `status.code`, `status.message` | `OK`, `BAD_REQUEST`, `NOT_FOUND`, `UNAUTHORIZED`, or `INTERNAL_SERVER_ERROR`. |

Sparse telemetry metadata is returned as protobuf defaults: strings are empty,
lists are empty, enum fields may be `CLIENT_TYPE_UNSPECIFIED`, and timestamp
fields are `0` when the read model has no value.

## Example Detail Response

The generated public gRPC tests seed Dashboard-facing clients and assert every
field through a real generated gRPC server and channel. A representative
`DescribeClient` detail payload is:

```json
{
  "status": {
    "code": "OK"
  },
  "client": {
    "clientId": "client-dashboard",
    "clientType": "PUSH_CONSUMER",
    "groups": [
      "dashboard-group"
    ],
    "topics": [
      "dashboard-topic"
    ],
    "language": "JAVA",
    "remoteAddress": "127.0.0.1:8080",
    "localAddress": "192.168.0.1:8080",
    "version": "V5_0_0",
    "connectTimeMillis": "100",
    "lastActiveTimeMillis": "200",
    "proxyId": "proxy-a"
  }
}
```

## Dashboard Acceptance Checklist

For the external RIP-1 Dashboard CLIENT-01 E2E, verify:

- Dashboard can connect to the independent admin gRPC server on the configured
  admin port.
- `ListClients` renders at least client id, client type, groups, topics,
  language, remote/local address, version, connect time, last active time, and
  proxy id.
- `DescribeClient` opens a detail view for the selected `client_id`.
- Group and topic filters call `ListClientsByGroup` and `ListClientsByTopic`,
  or call `ListClients` with the corresponding filter.
- Pagination uses `page_num >= 1` and `1 <= page_size <= 100`.
- Empty list responses are rendered as a valid state when no gRPC clients are
  currently online.
- `BAD_REQUEST`, `NOT_FOUND`, and `UNAUTHORIZED` statuses are surfaced without
  assuming a result body is present.
- `PROXY_SCOPE_ALL_PROXIES` and `PROXY_SCOPE_PROXY_ID` are not exposed as public
  UI controls for M1.
