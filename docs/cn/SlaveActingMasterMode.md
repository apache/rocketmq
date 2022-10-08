# Slave Acting Master模式

## 背景

![](https://s4.ax1x.com/2022/02/05/HnW3CQ.png)

上图为当前RocketMQ Master-Slave冷备部署，在该部署方式下，即使一个Master掉线，发送端仍然可以向其他Master发送消息，对于消费端而言，若开启备读，Consumer会自动重连到对应的Slave机器，不会出现消费停滞的情况。但也存在以下问题：

1. 一些仅限于在Master上进行的操作将无法进行，包括且不限于：

- searchOffset 
- maxOffset 
- minOffset 
- earliestMsgStoreTime 
- endTransaction

所有锁MQ相关操作，包括lock，unlock，lockBatch，unlockAll

具体影响为：
- 客户端无法获取位于该副本组的mq的锁，故当本地锁过期后，将无法消费该组的顺序消息 
- 客户端无法主动结束处于半状态的事务消息，只能等待broker回查事务状态 
- Admin tools或控制中依赖查询offset及earliestMsgStoreTime等操作在该组上无法生效

2. 故障Broker组上的二级消息消费将会中断，该类消息特点依赖Master Broker上的线程扫描CommitLog上的特殊Topic，并将满足要求的消息投放回CommitLog，如果Master Broker下线，会出现二级消息的消费延迟或丢失。具体会影响到当前版本的延迟消息消费、事务消息消费、Pop消费。

3. 没有元数据的反向同步。Master重新被人工拉起后，容易造成元数据的回退，如Master上线后将落后的消费位点同步给备，该组broker的消费位点回退，造成大量消费重复。

![](https://s4.ax1x.com/2022/02/05/HnWwUU.png)

上图为DLedger（Raft）架构，其可以通过选主一定程度上规避上述存在的问题，但可以看到DLedger模式下当前需要强制三副本及以上。

提出一个新的方案，Slave代理Master模式，作为Master-Slave部署模式的升级。在原先Master-Slave部署模式下，通过备代理主、轻量级心跳、副本组信息获取、broker预上线机制、二级消息逃逸等方式，当同组Master发生故障时，Slave将承担更加重要的作用，包括：

- 当Master下线后，该组中brokerId最小的Slave会承担备读 以及 一些 客户端和管控会访问 但却只能在Master节点上完成的任务。包括且不限于searchOffset、maxOffset、minOffset、earliestMsgStoreTime、endTransaction以及所有锁MQ相关操作lock，unlock，lockBatch，unlockAll。
- 当Master下线后，故障Broker组上的二级消息消费将不会中断，由该组中该组中brokerId最小的Slave承担起该任务，定时消息、Pop消息、事务消息等仍然可以正常运行。
- 当Master下线后，在Slave代理Master一段时间主后，然后当Master再次上线后，通过预上线机制，Master会自动完成元数据的反向同步后再上线，不会出现元数据回退，造成消息大量重复消费或二级消息大量重放。

## 架构

### 备代理主

Master下线后Slave能正常消费，且在不修改客户端代码情况下完成只能在Master完成的操作源自于Namesrv对“代理”Master的支持。此处“代理”Master指的是，当副本组处于无主状态时，Namesrv将把brokerId最小的存活Slave视为“代理”Master，具体表现为在构建TopicRouteData时，将该Slave的brokerId替换为0，并将brokerPermission修改为4（Read-Only），从而使得该Slave在客户端视图中充当只读模式的Master的角色。

此外，当Master下线后，brokerId最小的Slave会承担起二级消息的扫描和重新投递功能，这也是“代理”的一部分。

```java
//改变二级消息扫描状态
public void changeSpecialServiceStatus(boolean shouldStart) {
	……

    //改变延迟消息服务的状态
    changeScheduleServiceStatus(shouldStart);

    //改变事务消息服务的状态
    changeTransactionCheckServiceStatus(shouldStart);

    //改变Pop消息服务状态
    if (this.ackMessageProcessor != null) {
        LOG.info("Set PopReviveService Status to {}", shouldStart);
        this.ackMessageProcessor.setPopReviveServiceStatus(shouldStart);
    }
}
```

### 轻量级心跳

如上文所述，brokerId最小的存活Slave在Master故障后开启自动代理Master模式，因此需要一种机制，这个机制需要保证：

1. Nameserver能及时发现broker上下线并完成路由替换以及下线broker的路由剔除。

2. Broker能及时感知到同组Broker的上下线情况。

针对1，Nameserver原本就存在判活机制，定时会扫描不活跃的broker使其下线，而原本broker与nameserver的“心跳”则依赖于registerBroker操作，而这个操作涉及到topic信息上报，过于“重”，而且注册间隔过于长，因此需要一个轻量级的心跳机制，RoccketMQ 5.0在nameserver和broker间新增BrokerHeartbeat请求，broker会定时向nameserver发送心跳，若nameserver定时任务扫描发现超过心跳超时时间仍未收到该broker的心跳，将unregister该broker。registerBroker时会完成心跳超时时间的设置，并且注册时如果发现broker组内最小brokerId发生变化，将反向通知该组所有broker，并在路由获取时将最小brokerId的Slave路由替换使其充当只读模式的Master的角色

针对2，通过两个机制来及时感知同组broker上下线情况，1是上文中介绍的当nameserver发现该broker组内最小brokerId发生变化，反向通知该组所有broker。2是broker自身会有定时任务，向nameserver同步本broker组存活broker的信息，RoccketMQ 5.0会新增GetBrokerMemberGroup请求来完成该工作。

Slave Broker发现自己是该组中最小的brokerId，将会开启代理模式，而一旦Master Broker重新上线，Slave Broker同样会通过Nameserver反向通知或自身定时任务同步同组broker的信息感知到，并自动结束代理模式。

### 二级消息逃逸

代理模式开启后，brokerId最小的Slave会承担起二级消息的扫描和重新投递功能。

二级消息一般分为两个阶段，发送或者消费时会发送到一个特殊topic中，后台会有线程会扫描，最终的满足要求的消息会被重新投递到Commitlog中。我们可以让brokerId最小的Slave进行扫描，但如果扫描之后的消息重新投递到本Commitlog，那将会破坏Slave不可写的语义，造成Commitlog分叉。因此RoccketMQ 5.0提出一种逃逸机制，将重放的二级消息远程或本地投放到其他Master的Commitlog中。

- 远程逃逸

![](https://s4.ax1x.com/2022/02/05/HnWWVK.png)

如上图所示，假设Region A发生故障，Region B中的节点2将会承担二级消息的扫描任务，同时将最终的满足要求的消息通过EscapeBridge远程发送到当前Broker集群中仍然存活的Master上。

- 本地逃逸

![](https://s4.ax1x.com/2022/02/05/HnWfUO.png)

本地逃逸需要在BrokerContainer下进行，如果BrokerContainer中存在存活的Master，会优先向同进程的Master Commitlog中逃逸，避免远程RPC。

#### 各类二级消息变化

**延迟消息**

Slave代理Master时，ScheduleMessageService将启动，时间到期的延迟消息将通过EscapeBridge优先往本地Master逃逸，若没有则向远程的Master逃逸。该broker上存量的时间未到期的消息将会被逃逸到存活的其他Master上，数据量上如果该broker上有大量的延迟消息未到期，远程逃逸会造成集群内部会有较大数据流转，但基本可控。


**POP消息**

1. CK/ACK拼key的时候增加brokerName属性。这样每个broker能在扫描自身commitlog的revive topic时抵消其他broker的CK/ACK消息。

2. Slave上的CK/ACK消息将被逃逸到其他指定的Master A上（需要同一个Master，否则CK/ACK无法抵消，造成消息重复），Master A扫描自身Commitlog revive消息并进行抵消，若超时，则将根据CK消息中的信息向Slave拉取消息（若本地有则拉取本地，否则远程拉取），然后投放到本地的retry topic中。

数据量上，如果是远程投递或拉取，且有消费者大量通过Pop消费存量的Slave消息，并且长时间不ACK，则在集群内部会有较大数据流转。

### 预上线机制

![](https://s4.ax1x.com/2022/02/05/HnW5Pe.png)

当Master Broker下线后，Slave Broker将承担备读的作用，并对二级消息进行代理，因此Slave Broker中的部分元数据包括消费位点、定时消息进度等会比下线的Master Broker更加超前。如果Master Broker重新上线，Slave Broker元数据将被Master Broker覆盖，该组Broker元数据将发生回退，可能造成大量消息重复。因此，需要一套预上线机制来完成元数据的反向同步。

需要为consumerOffset和delayOffset等元数据增加版本号（DataVersion）的概念，并且为了防止版本号更新太频繁，增加更新步长的概念，比如对于消费位点来说，默认每更新位点超过500次，版本号增加到下一个版本。

如上图所示，Master Broker启动前会进行预上线，再预上线之前，对外不可见（Broker会有isIsolated标记自己的状态，当其为true时，不会像nameserver注册和发送心跳），因此也不会对外提供服务，二级消息的扫描流程也不会进行启动，具体预上线机制如下：

1. Master Broker向NameServer获取Slave Broker地址（GetBrokerMemberGroup请求），但不注册
2. Master Broker向Slave Broker发送自己的状态信息和地址
3. Slave Broker得到Master Broker地址后和状态信息后，建立HA连接，并完成握手，进入Transfer状态
4. Master Broker再完成握手后，反向获取备的元数据，包括消费位点、定时消息进度等，根据版本号决定是否更新。
5. Master Broker对broker组内所有Slave Broker都完成1-4步操作后，正式上线，向NameServer注册，正式对外提供服务。

### 锁Quorum

当Slave代理Master时，外部看到的是“只读”的Master，因此顺序消息仍然可以对队列上锁，消费不会中断。但当真的Master重新上线后，在一定的时间差内可能会造成多个consumer锁定同一个队列，比如一个consumer仍然锁着代理的备某一个队列，一个consumer锁刚上线的主的同一队列，造成顺序消息的乱序和重复。

因此在lock操作时要求，需锁broker副本组的大多数成员（quorum原则）均成功才算锁成功。但两副本下达不到quorum的原则，所以提供了lockInStrictMode参数，表示消费端消费顺序消息锁队列时是否使用严格模式。严格模式即对单个队列而言，需锁副本组的大多数成员（quorum原则）均成功才算锁成功，非严格模式即锁任意一副本成功就算锁成功，该参数默认为false。当对消息顺序性高于可用性时，需将该参数设置为false。

## 配置更新

Nameserver

- scanNotActiveBrokerInterval：扫描不活跃broker间隔，每次扫描将判断broker心跳是否超时，默认5s。
- supportActingMaster：nameserver端是否支持Slave代理Master模式，开启后，副本组在无master状态下，brokerId==1的slave将在TopicRoute中被替换成master（即brokerId=0），并以只读模式对客户端提供服务，默认为false。

Broker

- enableSlaveActingMaster：broker端开启slave代理master模式总开关，默认为false。
- enableRemoteEscape：是否允许远程逃逸，默认为false。
- brokerHeartbeatInterval：broker向nameserver发送心跳间隔（不同于注册间隔），默认1s。
- brokerNotActiveTimeoutMillis：broker不活跃超时时间，超过此时间nameserver仍未收到broker心跳，则判定broker下线，默认10s。
- sendHeartbeatTimeoutMillis：broker发送心跳请求超时时间，默认1s。
- lockInStrictMode：消费端消费顺序消息锁队列时是否使用严格模式，默认为false，上文已介绍。
- skipPreOnline：broker跳过预上线流程，默认为false。
- compatibleWithOldNameSrv：是否以兼容模式访问旧nameserver，默认为true。

## 兼容性方案

新版nameserver和旧版broker：新版nameserver可以完全兼容旧版broker，无兼容问题。

旧版nameserver和新版Broker：新版Broker开启Slave代理Master，会向Nameserver发送 BROKER_HEARTBEAT以及GET_BROKER_MEMBER_GROUP请求，但由于旧版本nameserver无法处理这些请求。因此需要在brokerConfig中配置compatibleWithOldNameSrv=true，开启对旧版nameserver的兼容模式，在该模式下，broker的一些新增RPC将通过复用原有RequestCode实现，具体为：
新增轻量级心跳将通过复用QUERY_DATA_VERSION实现
新增获取BrokerMemberGroup数据将通过复用GET_ROUTEINFO_BY_TOPIC实现，具体实现方式是每个broker都会新增rmq_sys_{brokerName}的系统topic，通过获取该系统topic的路由来获取该副本组的存活信息。
但旧版nameserver无法提供代理功能，Slave代理Master的功能将无法生效，但不影响其他功能。

客户端对新旧版本的nameserver和broker均无兼容性问题。


参考文档：[原RIP](https://github.com/apache/rocketmq/wiki/RIP-32-Slave-Acting-Master-Mode)