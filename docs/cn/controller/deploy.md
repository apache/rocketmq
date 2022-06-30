# 部署和升级指南

## Controller部署

若需要保证Controller具备容错能力，Controller部署需要三副本及以上（遵循Raft的多数派协议）。

> Controller若只部署单副本也能完成Broker Failover，只是若该单点Controller故障，会影响切换能力，但不会影响存量集群的正常收发。

Controller部署有两种方式。一种是嵌入于NameServer进行部署，可以通过配置enableControllerInNamesrv打开（可以选择性打开，并不强制要求每一台NameServer都打开），在该模式下，NameServer本身能力仍然是无状态的，也就是内嵌模式下若NameServer挂掉多数派，只影响切换能力，不影响原来路由获取等功能。另一种是独立部署，需要单独部署Controller组件。

### 嵌入NameServer部署

嵌入NameServer部署时只需要在NameServer的配置文件中设置enableControllerInNamesrv=true，并填上Controller的配置即可。

```
enableControllerInNamesrv = true
controllerDLegerGroup = group1
controllerDLegerPeers = n0-127.0.0.1:9877;n1-127.0.0.1:9878;n1-127.0.0.1:9879
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

其他一些参数可以参考ControllerConfig代码。

### 独立部署

独立部署执行以下脚本即可

```shell
sh bin/mqcontroller -c controller.conf
```
mqcontroller脚本在distribution/bin/mqcontroller，配置参数与内嵌模式相同。

## Broker Controller模式部署

Broker启动方法与之前相同，增加以下参数

- enableControllerMode：Broker controller模式的总开关，只有该值为true，controller模式才会打开。默认为false。
- controllerAddr：controller的地址，多个controller中间用分号隔开。例如`controllerAddr = 127.0.0.1:9877;127.0.0.1:9878;127.0.0.1:9879`
- controllerDeployedStandAlone：controller是否独立部署，若controller为独立部署，则为true，内嵌Nameserver部署，则为false。默认为false。
- syncBrokerMetadataPeriod：向controller同步Broker副本信息的时间间隔。默认5000（5s）。
- checkSyncStateSetPeriod：检查SyncStateSet的时间间隔，检查SyncStateSet可能会shrink SyncState。默认5000（5s）。
- syncControllerMetadataPeriod：同步controller元数据的时间间隔，主要是获取active controller的地址。默认10000（10s）。
- haMaxTimeSlaveNotCatchup：表示slave没有跟上Master的最大时间间隔，若在SyncStateSet中的slave超过该时间间隔会将其从SyncStateSet移除。默认为15000（15s）。
- storePathEpochFile：存储epoch文件的位置。epoch文件非常重要，不可以随意删除。默认在store目录下。
- allAckInSyncStateSet：若该值为true，则一条消息需要复制到SyncStateSet中的每一个副本才会向客户端返回成功，可以保证消息不丢失。默认为false。
- syncFromLastFile：若slave是空盘启动，是否从最后一个文件进行复制。默认为false。
- asyncLearner：若该值为true，则该副本不会进入SyncStateSet，也就是不会被选举成Master，而是一直作为一个learner副本进行异步复制。默认为false。

在Controller模式下，Broker配置必须设置enableControllerMode=true，并填写controllerAddr。若controller为独立部署，还需要将controllerDeployedStandAlone设置为true。

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