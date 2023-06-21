# RocketMQ Proxy部署指南

## 概述

RocketMQ Proxy 支持两种代理模式: `Local` and `Cluster`。

## 配置

该配置适用于 `Cluster` 和 `Local` 两种模式, 默认路径为 `distribution/conf/rmq-proxy.json`。

## `Cluster` 模式

* 设置 `nameSrvAddr`
* 设置 `proxyMode` 为 `cluster` (不区分大小写)

运行以下命令:

```shell
nohup sh mqproxy &
```

该命令仅会启动 `Proxy` 组件本身。它假设已经在指定的 `nameSrvAddr` 地址上运行着 `Namesrv` 节点，同时也有 broker 节点通过 `nameSrvAddr` 注册自己并运行。

## `Local` 模式

* 设置 `nameSrvAddr`
* 设置 `proxyMode` 为 `local` (不区分大小写)

运行以下命令:

```shell
nohup sh mqproxy &
```

上面的命令将启动`Proxy`，并在同一进程中运行`Broker`。它假设`Namesrv`节点正在按照`nameSrvAddr`指定的地址运行。
