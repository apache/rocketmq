# RIP-2 Proxy Admin — Least Privilege Configuration Guide

The RIP-2 admin surface authorizes every RPC against dedicated `proxy.admin.*`
ACL 2.0 resources (see `docs/rip-2-proxy-admin.md`, decision D2). This guide
shows the minimum-permission policy for each operational role.

## 1. Resource & Action Model

Resources (ACL 2.0 keys; modeled as cluster-typed literals with reserved names):

| Resource key | Protects |
|---|---|
| `cluster:proxy.admin.client` | online client query & client diagnostics |
| `cluster:proxy.admin.config` | runtime config query & hot update |
| `cluster:proxy.admin.connection` | kick/disconnect clients, telemetry commands (HIGH) |
| `cluster:proxy.admin.quota` | quota query & adjustment (adjust = HIGH) |
| `cluster:proxy.admin.route` | route topology & route event stream |
| `cluster:proxy.admin.ops` | broker-facing ops: stats/topic status/message query (read) and reset offset / delete subscription / admin send (HIGH) |

Action classes:

- Read-only: `Get`, `List`
- High privilege (mutating / disruptive): `Update`, `Delete`, `Pub`

The server maps every RPC to exactly one (resource, action) pair; granting a
read-only action can never authorize a high-privilege RPC.

## 2. Role Templates

All commands run against any broker/namesrv of the cluster (ACL 2.0 storage).
Create users first:

```bash
sh mqadmin createUser -n <namesrv-addr> -u <username> -p <password>
```

### Role A — Read-only observer (dashboard service account)

Online clients, subscriptions, accumulation, diagnostics, config/route views.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.quota,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List \
  -d Allow
```

Note: `Get,List` on `proxy.admin.ops` covers the read-only broker-facing RPCs;
the mutating ops RPCs require `Update`/`Delete`/`Pub` and stay denied.

### Role B — On-call operator (observer + connection control)

Role A plus the ability to kick misbehaving clients.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-oncall \
  -r cluster:proxy.admin.connection \
  -a Update \
  -d Allow
# plus the Role A grant above
```

### Role C — Admin (full control, break-glass)

Config hot update, quota adjustment, offset reset, subscription deletion,
admin send.

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-admin \
  -r cluster:proxy.admin.client,cluster:proxy.admin.config,cluster:proxy.admin.connection,cluster:proxy.admin.quota,cluster:proxy.admin.route,cluster:proxy.admin.ops \
  -a Get,List,Update,Delete,Pub \
  -d Allow
```

(Keep Role C accounts to a minimum; every use is recorded in the auth audit
log with the `[PROXY-ADMIN-AUDIT]` prefix.)

### Environment restriction (recommended)

Restrict admin access to the operations network via the `-i` sourceIp option:

```bash
sh mqadmin updateAcl -n <namesrv-addr> \
  -s user:rip2-ro \
  -r cluster:proxy.admin.client \
  -a Get,List \
  -d Allow \
  -i 10.0.0.0/8
```

## 3. Fail-Closed Mode

By default the admin server follows the cluster-wide authentication switch
(same behavior as the data plane). To require credentials unconditionally:

```properties
# proxy.json / -D proxyAdminRequireAuth=true
proxyAdminRequireAuth: true
```

With `proxyAdminRequireAuth=true` and the cluster authentication disabled, all
admin requests are rejected (fail-closed) — use this when the admin port cannot
be network-isolated.

## 4. Disabling the Surface

```properties
proxyAdminEnabled: false   # admin gRPC server is not started at all
```

or set `adminGrpcPort` to 0 / negative.

## 5. Audit

Every served admin RPC logs subject (Console login user / AK), method, resource,
action and source IP to the auth audit logger; denied requests are logged by the
ACL 2.0 engine itself. This satisfies the four-tuple audit requirement
(Console user + AK + resource + operation).
