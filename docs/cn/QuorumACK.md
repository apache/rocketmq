# Quorum Write和自动降级

## 背景

![](https://s4.ax1x.com/2022/02/05/HnWo2d.png)

在RocketMQ中，主备之间的复制模式主要有同步复制和异步复制，如上图所示，Slave1的复制是同步的，在向Producer报告成功写入之前，Master需要等待Slave1成功复制该消息并确认，Slave2的复制是异步的，Master不需要等待Slave2的响应。在RocketMQ中，发送一条消息，如果一切都顺利，那最后会返回给Producer客户端一个PUT_OK的状态，如果是Slave同步超时则返回FLUSH_SLAVE_TIMEOUT状态，如果是Slave不可用或者Slave与Master之间CommitLog差距超过一定的值（默认是256MB），则返回SLAVE_NOT_AVAILABLE，后面两个状态并不会导致系统异常而无法写入下一条消息。

同步复制可以保证Master失效后，数据仍然能在Slave中找到，适合可靠性要求较高的场景。异步复制虽然消息可能会丢失，但是由于无需等待Slave的确认，效率上要高于同步复制，适合对效率有一定要求的场景。但是只有两种模式仍然不够灵活，比如在三副本甚至五副本且对可靠性要求高场景中，采用异步复制无法满足需求，但采用同步复制则需要每一个副本确认后才会返回，在副本数多的情况下严重影响效率。另一方面，在同步复制的模式下，如果副本组中的某一个Slave出现假死，整个发送将一直失败直到进行手动处理。

因此，RocketMQ 5 提出了副本组的quorum write，在同步复制的模式下，用户可以在broker端指定发送后至少需要写入多少副本数后才能返回，并且提供自适应降级的方式，可以根据存活的副本数以及CommitLog差距自动完成降级。

## 架构和参数

### Quorum Write

通过增加两个参数来支持quorum write。

- **totalReplicas**：副本组broker总数。默认为1。
- **inSyncReplicas**：正常情况需保持同步的副本组数量。默认为1。

通过这两个参数，可以在同步复制的模式下，灵活指定需要ACK的副本数。

![](https://s4.ax1x.com/2022/02/05/HnWHKI.png)

如上图所示，在两副本情况下，如果inSyncReplicas为2，则该条消息需要在Master和Slave中均复制完成后才会返回给客户端；在三副本情况下，如果inSyncReplicas为2，则该条消息除了需要复制在Master上，还需要复制到任意一个slave上，才会返回给客户端。在四副本情况下，如果inSyncReplicas为3，则条消息除了需要复制在Master上，还需要复制到任意两个slave上，才会返回给客户端。通过灵活设置totalReplicas和inSyncReplicas，可以满足用户各类场景的需求。

### 自动降级

自动降级的标准是

- 当前副本组的存活副本数
- Master Commitlog和Slave CommitLog的高度差

> **注意：自动降级只在slaveActingMaster模式开启后才生效**

通过Nameserver的反向通知以及GetBrokerMemberGroup请求可以获取当前副本组的存活信息，而Master与Slave的Commitlog高度差也可以通过HA服务中的位点记录计算出来。将增加以下参数完成自动降级：

- **minInSyncReplicas**：最小需保持同步的副本组数量，仅在enableAutoInSyncReplicas为true时生效，默认为1
- **enableAutoInSyncReplicas**：自动同步降级开关，开启后，若当前副本组处于同步状态的broker数量（包括master自身）不满足inSyncReplicas指定的数量，则按照minInSyncReplicas进行同步。同步状态判断条件为：slave commitLog落后master长度不超过haSlaveFallBehindMax。默认为false。
- **haMaxGapNotInSync**：slave是否与master处于in-sync状态的判断值，slave commitLog落后master长度超过该值则认为slave已处于非同步状态。当enableAutoInSyncReplicas打开时，该值越小，越容易触发master的自动降级，当enableAutoInSyncReplicas关闭，且totalReplicas==inSyncReplicas时，该值越小，越容易导致在大流量时发送请求失败，故在该情况下可适当调大haMaxGapNotInSync。默认为256K。

注意：在RocketMQ 4.x中存在haSlaveFallbehindMax参数，默认256MB，表明Slave与Master的CommitLog高度差多少后判定其为不可用，在RIP-34中该参数被取消。

```java
//计算needAckNums
int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                              this.defaultMessageStore.getHaService().inSyncSlaveNums(currOffset) + 1);
needAckNums = calcNeedAckNums(inSyncReplicas);
if (needAckNums > inSyncReplicas) {
    // Tell the producer, don't have enough slaves to handle the send request
    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
}

private int calcNeedAckNums(int inSyncReplicas) {
    int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
    if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
        needAckNums = Math.min(needAckNums, inSyncReplicas);
        needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
    }
    return needAckNums;
}
```

当enableAutoInSyncReplicas=true是开启自适应降级模式，当副本组中存活的副本数减少或Master和Slave Commitlog高度差过大时，都会进行自动降级，最小降级到minInSyncReplicas副本数。比如在两副本中，如果设置totalReplicas=2，InSyncReplicas=2，minInSyncReplicas=1，enableAutoInSyncReplicas=true，正常情况下，两个副本均会处于同步复制，当Slave下线或假死时，会进行自适应降级，producer只需要发送到master即成功。

## 兼容性

** 用户需要设置正确的参数才能完成正确的向后兼容。举个例子，假设用户原集群为两副本同步复制，在没有修改任何参数的情况下，升级到RocketMQ 5的版本，由于totalReplicas、inSyncReplicas默认都为1，将降级为异步复制，如果需要和以前行为保持一致，则需要将totalReplicas和inSyncReplicas均设置为2。**


参考文档：[原RIP](https://github.com/apache/rocketmq/wiki/RIP-34-Support-quorum-write-and-adaptive-degradation-in-master-slave-architecture)