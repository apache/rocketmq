### 前言
该文档主要介绍如何快速构建和部署基于 DLedger 的可以自动容灾切换的 RocketMQ 集群。


### 源码构建
构建分为两个部分，需要先构建 DLedger，然后 构建 RocketMQ

#### 构建 DLedger

`git clone https://github.com/openmessaging/openmessaging-storage-dledger.git`

`cd openmessaging-storage-dledger`

`mvn clean install -DskipTests`

#### 构建 RocketMQ

`git clone https://github.com/apache/rocketmq.git`

`cd rocketmq`

`git checkout -b store_with_dledger origin/store_with_dledger`

`mvn -Prelease-all -DskipTests clean install -U`

### 快速部署

在构建成功后

`cd distribution/target/apache-rocketmq`

`sh bin/dledger/fast-try.sh start`

如果上面的步骤执行成功，可以通过 mqadmin 运维命令查看集群状态。

`sh bin/mqadmin clusterList -n 127.0.0.1:9876`

顺利的话，会看到如下内容：

![ClusterList](https://img.alicdn.com/5476e8b07b923/TB11Z.ZyCzqK1RjSZFLXXcn2XXa)

启动成功，现在可以向集群收发消息进行验证了。

关闭快速集群，可以执行：

`sh bin/dledger/fast-try.sh stop`

快速部署，默认配置在 conf/dledger 里面，默认的存储路径在 /tmp/rmqstore。


### 



