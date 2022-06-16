# Controller mode quick start

## 前言

该文档主要介绍如何快速构建和部署基于 Controller 的可以自动切换的 RocketMQ 集群。

详细的新集群部署和旧集群升级指南请参考 [部署指南](deploy_guide.md)。

## 编译 RocketMQ 源码

`git clone https://github.com/apache/rocketmq.git`

`cd rocketmq`

`git checkout -b controller_mode origin/5.0.0-beta-dledger-controller `

`	mvn -Prelease-all -DskipTests clean install -U`

## 快速部署

在构建成功后

`cd distribution/target/apache-rocketmq`

`sh bin/controller/fast-try.sh start`

如果上面的步骤执行成功，可以通过运维命令查看集群状态。

至此, 启动成功，现在可以向集群收发消息，并进行切换测试了。

如果需要关闭快速集群，可以执行：

`sh bin/controller/fast-try.sh stop`

对于快速部署，默认配置在 conf/controller里面，默认的存储路径在 /tmp/rmqstore, 且会开启一个 Controller (嵌入在 Namesrv) 和两个 Broker。

### 查看 SyncStateSet

可以通过运维工具查看 SyncStateSet:

`sh bin/mqadmin getSyncStateSet -a localhost:9878 -b broker-a`

-a 代表的是任意一个 Controller 的地址

如果顺利的话, 可以看到以下内容:

![image-20220605205259913](../image/controller/quick-start/syncstateset.png)

### 查看 BrokerEpoch

可以通过运维工具查看 BrokerEpochEntry:

`sh bin/mqadmin getBrokerEpoch -n localhost:9876 -b broker-a`

-n 代表的是任意一个 Namesrv 的地址

如果顺利的话, 可以看到以下内容:

![image-20220605205247476](../image/controller/quick-start/epoch.png)

## 切换

部署成功后，现在尝试进行 Master 切换。

首先, kill 掉原 Master 的进程, 在上文的例子中, 就是使用端口 30911 的进程:

```
查找端口:
ps -ef|grep java|grep BrokerStartup|grep ./conf/controller/quick-start/broker-n0.conf|grep -v grep|awk '{print $2}'
杀掉 master:
kill -9 PID
```

接着, 用 SyncStateSet admin 脚本查看:

`sh bin/mqadmin getSyncStateSet -a localhost:9878 -b broker-a`

可以发现 Master 已经发生了切换。

![image-20220605211244128](../image/controller/quick-start/changemaster.png)
