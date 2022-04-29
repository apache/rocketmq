# RocketMQ Proxy Deployment Guide

## Overview
RocketMQ Proxy supports two deployment modes, `Local` mode and `Cluster` mode. 

With `Local` mode, `Proxy` deployed with `Broker` in the same process with inter-process communication.

While `Cluster` mode, `Proxy` is a single cluster who communicate `Broker` with remote procedure call. In this way, `Proxy` acts as a stateless computing components while `Broker` is a stateful components with local storage. This form of deployment makes RocketMQ an architecture of separation of computing and storage.

## Configuration
The configuration applies to both the `Cluster` mode and `Local` mode, whose default path is distribution/conf/rmq-proxy.json directory. 

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
The command will not only run `Proxy`, but also run `Broker`. It requires `Namesrv` only and there's no need for extra `Broker`.