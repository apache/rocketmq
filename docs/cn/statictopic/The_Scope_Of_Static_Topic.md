### Version 记录
| 时间 | 主要内容 | 作者 |
| --- | --- | --- |
| 2021-11-01 | 初稿，探讨Static Topic的Scope视线范围 | dongeforever |


中文文档在描述特定专业术语时，仍然使用英文。

### 需求背景
RocketMQ的集群设计，是一个多集群、动态、零耦合的设计，具体体现在以下地方：
- 一个 Nameserver 可以管理多个 Cluster
- Broker 与 Cluster 之间是弱关联，Cluster仅仅只是一个标识符，主要在运维时使用来界定Topic的创建范围
- 开发用户对 Cluster 无感知
- 不同 Broker 之间没有任何关联

这样的设计，在运维时带来了极大的便利，但也带来了一个问题：
- Topic 的队列数无法固定

基于 Logic Queue 技术而实现的 Static Topic，就是用来解决『固定队列数量』的问题。

但这个『固定』要到何种范围呢？是一个值得探讨的问题。

从理论上可以分析出来，有以下三种情况：
- 单集群固定
- 多集群固定
- 全网固定

#### 单集群固定
一个 Static Topic，固定在一个 Cluster 内漂移。
不同的 Cluster 内，可以拥有相同的 Static Topic。
对应MessageQueue的Broker 命名规范为：
```
__logic__{clusterName}
```
#### 多集群固定
一个 Static Topic，固定在特定的几个 Cluster 内漂移。
没有交集的Cluster集合之间，可以拥有相同的 Static Topic。
对应MessageQueue的Broker 命名规范为：
```
__logic__{cluster1}_{cluster2}_{xxx}
```
#### 全网固定
全网是指『同一个Nameserver内』。
一个 Static Topic，不与特定Cluster绑定，同一个Nameserver内，全网漂移。
同一个Nameserver内，只有一个同名的 Static Topic。
对应MessageQueue的Broker 命名规范为：
```
__logic__global
```
#### 为什么要引入Scope
直接全网固定不就好了吗，为啥还要引入Scope呢？
主要原因是，不想完全放弃 RocketMQ 『多集群、动态、零耦合』的设计优势。
而全网固定，则意味着彻底失去了这个优势。

举1个『多活保序』的场景：
- ClusterA 部署在 SiteA 内，创建 Static Topic 『TopicTest』，有50个队列。
- ClusterB 部署在 SiteB 内，创建 Static Topic 『TopicTest』，有50个队列。

对Nameserver稍作修改，支持传入标识符(比如为scope或者unitName)，来获取特定范围内的 Topic Route。

正常情况下：
- SiteA 的Producer和Consumer 只能看见 ClusterA 的 MessageQueue，brokerName为 "__logic__clusterA"。
- SiteB 的Producer和Consumer 只能看见 ClusterB 的 MessageQueue，brokerName为 "__logic__clusterB"。
- 机房内就近访问，且机房内严格保序。

假设 SiteA 宕机，此时对Nameserver发指令允许全网读，也即忽略客户端传入的 Scope或者unitName 标识符：
- SiteB 的 Producer 仍然看见并写入 ClusterB 的 MessageQueue，brokerName为 "__logic__clusterB"
- SiteB 的 Consumer 可以同时看见并读取 ClusterA 的 MessageQueue 和 ClusterB MessageQueue, brokerName为 "__logic__clusterB" 和 "__logic__clusterA
- 在这种场景下，Consumer 在消费 ClusterB 数据的同时，同时去消费 ClusterA 未消费完的数据

不同地域的客户端，看见不同Scope的元数据，从而访问不同Scope的节点。

#### 全球容灾集群
RocketMQ 多个集群的元数据可以无缝在Nameserver处汇聚，同时又可以无缝地根据标识符拆分给不同地域的Producer和Consumer。
这样一个『元数据可分可合』的设计优势，是其它消息中间件所不具备的，应该值得挖掘一下。  
引入以下概念：
- 融合集群，共享同一个Nameserver的集群之和
- 单元集群，clusterName名字一样的集群，不同单元集群之间，物理隔离
- namespace，租户，逻辑隔离，只是命名的区别

如果单元集群部署在异地，所形成的『融合集群』，就是全球容灾集群：
- 客户端引入 scope 或者 unitName 字段，默认情况，不同 scope或者unitName 获取的都是单元集群的元数据
- 顺序性，关键在于 固定Producer端可见的队列，单元内的队列是固定的，因此可以保证单元内是顺序的
- Consumer 端按照队列消费，天然是顺序的
- 正常情况下，单元内封闭，也即单元产生的数据在同单元内消费掉
- 灾难发生时，改变元数据的可见性，允许读其它 单元集群 未消费完的数据，也即跨单元读
- 跨单元读，是指读『其它clusterName』的队列，不一定是远程读，如果本单元有相应的Slave节点，则直接本地读

### 设计目标
Static Topic 实现 单集群固定 和 全网固定 两种Scope，以适配『全球容灾集群』。
多集群，暂时没有必要。

一期只实现 全网固定 这个Scope，但在格式上注意兼容

#### SOT 增加 Scope 字段
```
{
"version":"1",
"scope": "clusterA",
"bname": "broker02" //标记这份数据的原始存储位置，如果发送误拷贝，可以利用这个字段来进行标识
"epoch": 0, //标记修改版本，用来做一致性校验
"totalQueues":"50",  //当前Topic 总共有多少 LogicQueues
}
```

scope字段：
- 单集群固定，则就是 Cluster 名字
- 全网固定，则为常量『global』








