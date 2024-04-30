# BrokerContainer

## 背景

在RocketMQ 4.x 版本中，一个进程只有一个broker，通常会以主备或者DLedger（Raft）的形式部署，但是一个进程中只有一个broker，而slave一般只承担冷备或热备的作用，节点之间角色的不对等导致slave节点资源没有充分被利用。
因此在RocketMQ 5.x 版本中，提供一种新的模式BrokerContainer，在一个BrokerContainer进程中可以加入多个Broker（Master Broker、Slave Broker、DLedger Broker），来提高单个节点的资源利用率，并且可以通过各种形式的交叉部署来实现节点之间的对等部署。
该特性的优点包括：

1. 一个BrokerContainer进程中可以加入多个broker，通过进程内混部来提高单个节点的资源利用率
2. 通过各种形式的交叉部署来实现节点之间的对等部署，增强单节点的高可用能力
3. 利用BrokerContainer可以实现单进程内多CommitLog写入，也可以实现单机的多磁盘写入
4. BrokerContainer中的CommitLog天然隔离的，不同的CommitLog（broker）可以采取不同作用，比如可以用来比如创建单独的broker做不同TTL的CommitLog。

## 架构

### 单进程视图

![](https://s4.ax1x.com/2022/01/26/7LMZHP.png)

相比于原来一个Broker一个进程，RocketMQ 5.0将增加BrokerContainer概念，一个BrokerContainer可以存放多个Broker，每个Broker拥有不同的端口，但它们共享同一个传输层（remoting层），而每一个broker在功能上是完全独立的。BrokerContainer也拥有自己端口，在运行时可以通过admin命令来增加或减少Broker。

### 对等部署形态

在BrokerContainer模式下，可以通过各种形式的交叉部署完成节点的对等部署

- 二副本对等部署

![](https://s4.ax1x.com/2022/01/26/7LQi5T.png)

二副本对等部署情况下，每个节点都会有一主一备，资源利用率均等。另外假设图中Node1宕机，由于Node2的broker_2可读可写，broker_1可以备读，因此普通消息的收发不会收到影响，单节点的高可用能力得到了增强。

- 三副本对等部署

![](https://s4.ax1x.com/2022/01/26/7LQMa6.png)

三副本对等部署情况下，每个节点都会有一主两备，资源利用率均等。此外，和二副本一样，任意一个节点的宕机也不会影响到普通消息的收发。

### 传输层共享

![](https://s4.ax1x.com/2022/02/07/HMNIVs.png)

BrokerContainer中的所有broker共享同一个传输层，就像RocketMQ客户端中同进程的Consumer和Producer共享同一个传输层一样。

这里为NettyRemotingServer提供SubRemotingServer支持，通过为一个RemotingServer绑定另一个端口即可生成SubRemotingServer，其共享NettyRemotingServer的Netty实例、计算资源、以及协议栈等，但拥有不同的端口以及ProcessorTable。另外同一个BrokerContainer中的所有的broker也会共享同一个BrokerOutAPI（RemotingClient）。

## 启动方式和配置

![](https://s4.ax1x.com/2022/01/26/7LQ1PO.png)

像Broker启动利用BrokerStartup一样，使用BrokerContainerStartup来启动BrokerContainer。我们可以通过两种方式向BrokerContainer中增加broker，一种是通过启动时通过在配置文件中指定

BrokerContainer配置文件内容主要是Netty网络层参数（由于传输层共享），BrokerContainer的监听端口、namesrv配置，以及最重要的brokerConfigPaths参数，brokerConfigPaths是指需要向BrokerContainer内添加的brokerConfig路径，多个config间用“:”分隔，不指定则只启动BrokerContainer，具体broker可通过mqadmin工具添加

broker-container.conf（distribution/conf/container/broker-container.conf）:

```
#配置端口，用于接收mqadmin命令
listenPort=10811
#指定namesrv
namesrvAddr=127.0.0.1:9876
#或指定自动获取namesrv
fetchNamesrvAddrByAddressServer=true
#指定要向BrokerContainer内添加的brokerConfig路径，多个config间用“:”分隔；
#不指定则只启动BrokerContainer，具体broker可通过mqadmin工具添加
brokerConfigPaths=/home/admin/broker-a.conf:/home/admin/broker-b.conf
```
broker的配置和以前一样，但在BrokerContainer模式下broker配置文件中下Netty网络层参数和nameserver参数不生效，均使用BrokerContainer的配置参数。

完成配置文件后，可以以如下命令启动
```
sh mqbrokercontainer -c broker-container.conf
```
mqbrokercontainer脚本路径为distribution/bin/mqbrokercontainer。

## 运行时增加或较少Broker

当BrokerContainer进程启动后，也可以通过Admin命令来增加或减少Broker。

AddBrokerCommand
```
usage: mqadmin addBroker -b <arg> -c <arg> [-h] [-n <arg>]
 -b,--brokerConfigPath <arg>      Broker config path
 -c,--brokerContainerAddr <arg>   Broker container address
 -h,--help                        Print help
 -n,--namesrvAddr <arg>           Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876
```

RemoveBroker Command
```
usage: mqadmin removeBroker -b <arg> -c <arg> [-h] [-n <arg>]
 -b,--brokerIdentity <arg>        Information to identify a broker: clusterName:brokerName:brokerId
 -c,--brokerContainerAddr <arg>   Broker container address
 -h,--help                        Print help
 -n,--namesrvAddr <arg>           Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876
```

## 存储变化

storePathRootDir，storePathCommitLog路径依然为MessageStoreConfig中配置值，需要注意的是同一个brokerContainer中的broker不能使用相同的storePathRootDir，storePathCommitLog，否则不同的broker占用同一个存储目录，发生数据混乱。

在文件删除策略上，仍然单个Broker的视角来进行删除，但MessageStoreConfig新增replicasPerDiskPartition参数和logicalDiskSpaceCleanForciblyThreshold。

replicasPerDiskPartition表示同一磁盘分区上有多少个副本，即该broker的存储目录所在的磁盘分区被几个broker共享，默认值为1。该配置用于计算当同一节点上的多个broker共享同一磁盘分区时，各broker的磁盘配额

e.g. replicasPerDiskPartition==2且broker所在磁盘空间为1T时，则该broker磁盘配额为512G，该broker的逻辑磁盘空间利用率基于512G的空间进行计算。

logicalDiskSpaceCleanForciblyThreshold，该值只在replicasPerDiskPartition大于1时生效，表示逻辑磁盘空间强制清理阈值，默认为0.80（80%）， 逻辑磁盘空间利用率为该broker在自身磁盘配额内的空间利用率，物理磁盘空间利用率为该磁盘分区总空间利用率。由于在BrokerContainer实现中，考虑计算效率的情况下，仅统计了commitLog+consumeQueue（+ BCQ）+indexFile作为broker的存储空间占用，其余文件如元数据、消费进度、磁盘脏数据等未统计在内，故在多个broker存储空间达到动态平衡时，各broker所占空间可能有相差，以一个BrokerContainer中有两个broker为例，两broker存储空间差异可表示为：
![](https://s4.ax1x.com/2022/01/26/7L14v4.png)
其中，R_logical为logicalDiskSpaceCleanForciblyThreshold，R_phy为diskSpaceCleanForciblyRatio，T为磁盘分区总空间，x为除上述计算的broker存储空间外的其他文件所占磁盘总空间比例，可见，当
![](https://s4.ax1x.com/2022/01/26/7L1TbR.png)
时，可保证BrokerContainer各Broker存储空间在达到动态平衡时相差无几。

eg.假设broker获取到的配额是500g（根据replicasPerDiskPartition计算获得），logicalDiskSpaceCleanForciblyThreshold为默认值0.8，则默认commitLog+consumeQueue（+ BCQ）+indexFile总量超过400g就会强制清理文件。

其他清理阈值（diskSpaceCleanForciblyRatio、diskSpaceWarningLevelRatio），文件保存时间（fileReservedTime）等逻辑与之前保持一致。

注意：当以普通broker方式启动而非brokerContainer启动时，且replicasPerDiskPartition=1（默认值）时，清理逻辑与之前完全一致。replicasPerDiskPartition>1时，逻辑磁盘空间强制清理阈值logicalDiskSpaceCleanForciblyThreshold将会生效。


## 日志变化

在BrokerContainer模式下日志的默认输出路径将发生变化，具体为：

```
{user.home}/logs/rocketmqlogs/${brokerCanonicalName}/
```

其中 `brokerCanonicalName` 为 `{BrokerClusterName_BrokerName_BrokerId}`。