# 部署和升级指南

## Controller部署

若需要保证Controller具备容错能力，Controller部署需要三副本及以上（遵循Raft的多数派协议）。

> Controller若只部署单副本也能完成Broker Failover，但若该单点Controller故障，会影响切换能力，但不会影响存量集群的正常收发。

Controller部署有两种方式。一种是嵌入于NameServer进行部署，可以通过配置enableControllerInNamesrv打开（可以选择性打开，并不强制要求每一台NameServer都打开），在该模式下，NameServer本身能力仍然是无状态的，也就是内嵌模式下若NameServer挂掉多数派，只影响切换能力，不影响原来路由获取等功能。另一种是独立部署，需要单独部署Controller组件。

### 嵌入NameServer部署

嵌入NameServer部署时只需要在NameServer的配置文件中设置enableControllerInNamesrv=true，并填上Controller的配置即可。

```
enableControllerInNamesrv = true
controllerDLegerGroup = group1
controllerDLegerPeers = n0-127.0.0.1:9877;n1-127.0.0.1:9878;n2-127.0.0.1:9879
controllerDLegerSelfId = n0
controllerStorePath = /home/admin/DledgerController
enableElectUncleanMaster = false
notifyBrokerRoleChanged = true
```

参数解释：

- enableControllerInNamesrv：Nameserver中是否开启controller，默认false。
- controllerDLegerGroup：DLedger Raft Group的名字，同一个DLedger Raft Group保持一致即可。
- controllerDLegerPeers：DLedger Group 内各节点的端口信息，同一个 Group 内的各个节点配置必须要保证一致。
- controllerDLegerSelfId：节点 id，必须属于 controllerDLegerPeers 中的一个；同 Group 内各个节点要唯一。
- controllerStorePath：controller日志存储位置。controller是有状态的，controller重启或宕机需要依靠日志来恢复数据，该目录非常重要，不可以轻易删除。
- enableElectUncleanMaster：是否可以从SyncStateSet以外选举Master，若为true，可能会选取数据落后的副本作为Master而丢失消息，默认为false。
- notifyBrokerRoleChanged：当broker副本组上角色发生变化时是否主动通知，默认为true。
- scanNotActiveBrokerInterval：扫描 Broker是否存活的时间间隔。

其他一些参数可以参考ControllerConfig代码。

参数设置完成后，指定配置文件启动Nameserver即可。

### 独立部署

独立部署执行以下脚本即可

```shell
sh bin/mqcontroller -c controller.conf
```
mqcontroller脚本在distribution/bin/mqcontroller，配置参数与内嵌模式相同。

## Broker Controller模式部署

Broker启动方法与之前相同，增加以下参数

- enableControllerMode：Broker controller模式的总开关，只有该值为true，controller模式才会打开。默认为false。
- controllerAddr：controller的地址，两种方式填写。
  - 直接填写多个Controller IP地址，多个controller中间用分号隔开，例如`controllerAddr = 127.0.0.1:9877;127.0.0.1:9878;127.0.0.1:9879`。注意由于Broker需要向所有controller发送心跳，因此请填上所有的controller地址。
  - 填写域名，然后设置fetchControllerAddrByDnsLookup为true，则Broker去自动解析域名后面的多个真实controller地址。
- fetchControllerAddrByDnsLookup：controllerAddr填写域名时，如果设置该参数为true，会自动获取所有controller的地址。默认为false。
- controllerHeartBeatTimeoutMills：Broker和controller之间心跳超时时间，心跳超过该时间判断Broker不在线。
- syncBrokerMetadataPeriod：向controller同步Broker副本信息的时间间隔。默认5000（5s）。
- checkSyncStateSetPeriod：检查SyncStateSet的时间间隔，检查SyncStateSet可能会shrink SyncState。默认5000（5s）。
- syncControllerMetadataPeriod：同步controller元数据的时间间隔，主要是获取active controller的地址。默认10000（10s）。
- haMaxTimeSlaveNotCatchup：表示slave没有跟上Master的最大时间间隔，若在SyncStateSet中的slave超过该时间间隔会将其从SyncStateSet移除。默认为15000（15s）。
- storePathEpochFile：存储epoch文件的位置。epoch文件非常重要，不可以随意删除。默认在store目录下。
- allAckInSyncStateSet：若该值为true，则一条消息需要复制到SyncStateSet中的每一个副本才会向客户端返回成功，可以保证消息不丢失。默认为false。
- syncFromLastFile：若slave是空盘启动，是否从最后一个文件进行复制。默认为false。
- asyncLearner：若该值为true，则该副本不会进入SyncStateSet，也就是不会被选举成Master，而是一直作为一个learner副本进行异步复制。默认为false。
- inSyncReplicas：需保持同步的副本组数量，默认为1，allAckInSyncStateSet=true时该参数无效。
- minInSyncReplicas：最小需保持同步的副本组数量，若SyncStateSet中副本个数小于minInSyncReplicas则putMessage直接返回PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH，默认为1。

在Controller模式下，Broker配置必须设置enableControllerMode=true，并填写controllerAddr。

### 重要参数解析

1.写入副本参数

其中inSyncReplicas、minInSyncReplicas等参数在普通Master-Salve部署、SlaveActingMaster模式、自动主从切换架构有重叠和不同含义，具体区别如下

|                      | inSyncReplicas                                                      | minInSyncReplicas                                                        | enableAutoInSyncReplicas                    | allAckInSyncStateSet                                          | haMaxGapNotInSync                     | haMaxTimeSlaveNotCatchup                          |
|----------------------|---------------------------------------------------------------------|--------------------------------------------------------------------------|---------------------------------------------|---------------------------------------------------------------|---------------------------------------|---------------------------------------------------|
| 普通Master-Salve部署     | 同步复制下需要ACK的副本数，异步复制无效                                               | 无效                                                                       | 无效                                          | 无效                                                            | 无效                                    | 无效                                                |
| 开启SlaveActingMaster （slaveActingMaster=true） | 不自动降级情况下同步复制下需要ACK的副本数                                              | 自动降级后，需要ACK最小副本数                                                         | 是否开启自动降级，自动降级后，ACK最小副本数降级到minInSyncReplicas | 无效                                                            | 判断降级依据：Slave与Master Commitlog差距值，单位字节 | 无效                                                |
| 自动主从切换架构（enableControllerMode=true） | 不开启allAckInSyncStateSet下，同步复制下需要ACK的副本数，开启allAckInSyncStateSet后该值无效 | SyncStateSet可以降低到最小的副本数，如果SyncStateSet中副本个数小于minInSyncReplicas则直接返回副本数不足 | 无效                                          | 若该值为true，则一条消息需要复制到SyncStateSet中的每一个副本才会向客户端返回成功，该参数可以保证消息不丢失 | 无效                             | SyncStateSet收缩时，Slave最小未跟上Master的时间差，详见[RIP-44](https://shimo.im/docs/N2A1Mz9QZltQZoAD) |

总结来说：
- 普通Master-Slave下无自动降级能力，除了inSyncReplicas其他参数均无效，inSyncReplicas表示同步复制下需要ACK的副本数。
- slaveActingMaster模式下开启enableAutoInSyncReplicas有降级能力，最小可降级到minInSyncReplicas副本数，降级判断依据是主备Commitlog高度差（haMaxGapNotInSync）以及副本存活情况，参考[slaveActingMaster模式自动降级](../QuorumACK.md)。
> SlaveActingMaster为其他高可用部署方式，该模式下如果不使用可不参考
- 自动主从切换（Controller模式）依赖SyncStateSet的收缩进行自动降级，SyncStateSet副本数最小收缩到minInSyncReplicas仍能正常工作，小于minInSyncReplicas直接返回副本数不足，收缩依据之一是Slave跟上的时间间隔（haMaxTimeSlaveNotCatchup）而非Commitlog高度。
- 自动主从切换（Controller模式）正常情况是要求保证不丢消息的，只需设置allAckInSyncStateSet = true 即可，不需要考虑inSyncReplicas参数（该参数无效），如果副本较多、距离较远对延迟有要求，可以参考设置部分副本设置为asyncLearner。

2.SyncStateSet收缩检查配置

checkSyncStateSetPeriod 参数决定定时检查SyncStateSet是否需要收缩的时间间隔
haMaxTimeSlaveNotCatchup 参数决定备跟不上主的时间

当allAckInSyncState = true时（保证不丢消息），
- haMaxTimeSlaveNotCatchup 值越小，对SyncStateSet收缩越敏感，比如主备之间网络抖动就可能导致SyncStateSet收缩，造成不必要的集群抖动。
- haMaxTimeSlaveNotCatchup 值越大，对SyncStateSet收缩虽然不敏感，但是可能加大SyncStateSet收缩时的RTO时间。该RTO时间可以按照 checkSyncStateSetPeriod/2 + haMaxTimeSlaveNotCatchup 估算。

3.消息可靠性配置

保证 allAckInSyncStateSet = true 以及 enableElectUncleanMaster = false

4.延迟

当 allAckInSyncStateSet = true 后，一条消息要复制到SyncStateSet所有副本才能确认返回，假设SyncStateSet有3副本，其中1副本距离较远，则会影响到消息延迟。可以设置延迟最高距离最远的副本为asyncLearner，该副本不会进入SyncStateSet，只会进行异步复制，该副本作为冗余副本。

## 兼容性

该模式未对任何客户端层面 API 进行新增或修改，不存在客户端的兼容性问题。

Nameserver本身能力未做任何修改，Nameserver不存在兼容性问题。如开启enableControllerInNamesrv且controller参数配置正确，则开启controller功能。

Broker若设置enableControllerMode=false，则仍然以之前方式运行。若设置enableControllerMode=true，则需要部署controller且参数配置正确才能正常运行。

具体行为如下表所示：

|                         | 旧版Nameserver | 旧版Nameserver+独立部署Controller | 新版Nameserver开启controller功能 | 新版Nameserver关闭controller功能 |
|-------------------------|--------------|-----------------------------|----------------------------|----------------------------|
| 旧版Broker                | 正常运行，无法切换    | 正常运行，无法切换                   | 正常运行，无法切换                  | 正常运行，无法切换                  |
| 新版Broker开启Controller模式  | 无法正常上线       | 正常运行，可以切换                   | 正常运行，可以切换                  | 无法正常上线                        |
| 新版Broker不开启Controller模式 | 正常运行，无法切换    | 正常运行，无法切换                   |正常运行，无法切换 | 正常运行，无法切换                  |

## 升级注意事项

从上述兼容性表述可以看出，NameServer正常升级即可，无兼容性问题。在不想升级Nameserver情况，可以独立部署Controller组件来获得切换能力。

针对Broker升级，分为两种情况：

（1）Master-Slave部署升级成Controller切换架构

可以带数据进行原地升级，对于每组Broker，停机主、备Broker，**保证主、备的Commitlog对齐**（可以在升级前禁写该组Broker一段时间，或则通过拷贝方式保证一致），升级包后重新启动即可。

> 若主备commitlog不对齐，需要保证主上线以后再上线备，否则可能会因为数据截断而丢失消息。

（2）原DLedger模式升级到Controller切换架构

由于原DLedger模式消息数据格式与Master-Slave下数据格式存在区别，不提供带数据原地升级的路径。在部署多组Broker的情况下，可以禁写某一组Broker一段时间（只要确认存量消息被全部消费即可，比如根据消息的保存时间来决定），然后清空store目录下除config/topics.json、subscriptionGroup.json下（保留topic和订阅关系的元数据）的其他文件后，进行空盘升级。

### 持久化BrokerID版本的升级注意事项

目前版本支持采用了新的持久化BrokerID版本，详情可以参考[该文档](persistent_unique_broker_id.md)，从该版本前的5.x升级到当前版本需要注意如下事项。

4.x版本升级遵守上述正常流程即可。
5.x非持久化BrokerID版本升级到持久化BrokerID版本按照如下流程:

**升级Controller**

1. 将旧版本Controller组停机。
2. 清除Controller数据，即默认在`~/DLedgerController`下的数据文件。
3. 上线新版Controller组。

> 在上述升级Controller流程中，Broker仍可正常运行，但无法切换。

**升级Broker**

1. 将Broker从节点停机。
2. 将Broker主节点停机。
3. 将所有的Broker的Epoch文件删除，即默认为`~/store/epochFileCheckpoint`和`~/store/epochFileCheckpoint.bak`。
4. 将原先的主Broker先上线，等待该Broker当选为master。(可使用`admin`命令的`getSyncStateSet`来观察)
5. 将原来的从Broker全部上线。

> 建议停机时先停从再停主，上线时先上原先的主再上原先的从，这样可以保证原来的主备关系。
> 若需要改变升级前后主备关系，则需要停机时保证主、备的CommitLog对齐，否则可能导致数据被截断而丢失。