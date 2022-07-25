# RocketMQ Proxy Deployment Guide

## Overview

RocketMQ Proxy supports two deployment modes, `Local` mode and `Cluster` mode.

## Configuration

The configuration applies to both the `Cluster` mode and `Local` mode, whose default path is
distribution/conf/rmq-proxy.json directory.

## `Cluster` mode

* Set configuration field `nameSrvAddr`.
* Set configuration field `proxyMode` to `cluster` (case insensitive).

Run the command below.

```shell
nohup sh mqproxy &
```

The command will only run `Proxy` itself. It requires `Namesrv` and `Broker` components running.

## `Local` mode

* Set configuration field `nameSrvAddr`.
* Set configuration field `proxyMode` to `local` (case insensitive).

Run the command below.

```shell
nohup sh mqproxy &
```

The command will not only run `Proxy`, but also run `Broker`. It requires `Namesrv` only and there's no need for
extra `Broker`.