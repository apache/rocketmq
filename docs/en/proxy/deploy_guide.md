# RocketMQ Proxy Deployment Guide

## Overview

RocketMQ Proxy supports two deployment modes: `Local` and `Cluster`.

## Configuration

The configuration file applies to both `Cluster` and `Local` mode, whose default path is
distribution/conf/rmq-proxy.json.

## `Cluster` Mode

* Set configuration field `nameSrvAddr`.
* Set configuration field `proxyMode` to `cluster` (case insensitive).

Run the command below.

```shell
nohup sh mqproxy &
```

The command will only launch the `Proxy` component itself. It assumes that `Namesrv` nodes are already running at the address specified `nameSrvAddr`, and broker nodes, registering themselves with `nameSrvAddr`, are running too.

## `Local` Mode

* Set configuration field `nameSrvAddr`.
* Set configuration field `proxyMode` to `local` (case insensitive).

Run the command below.

```shell
nohup sh mqproxy &
```

The previous command will launch the `Proxy`, with `Broker` in the same process. It assumes `Namesrv` nodes are running at the address specified by `nameSrvAddr`.
