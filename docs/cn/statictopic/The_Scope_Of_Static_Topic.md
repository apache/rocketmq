### Version 记录
| 时间 | 主要内容 | 作者 |
| --- | --- | --- |
| 2021-11-01 | 初稿，探讨Static Topic的视线范围 | dongeforever |


中文文档在描述特定专业术语时，仍然使用英文。

### 需求背景
RocketMQ的集群设计，是一个多集群、动态、零耦合的设计，具体体现在以下地方：
- 一个 Nameserver 可以管理多个 Cluster
- Broker 与 Cluster 之间是弱关联，Cluster仅仅只是一个标识符，主要在运维时使用来界定Topic的创建范围
- 开发用户对 Cluster 无感知
- 不同 Broker 之间没有任何关联

这样的设计，在运维时带来了极大的便利，但也带来了一个问题:
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

举1个多活保序的场景。
ClusterA 部署在 SiteA 内，创建 Static Topic 『TopicTest』，有50个队列。
ClusterB 部署在 SiteB 内，创建 Static Topic 『TopicTest』，有50个队列。

对Nameserver稍作修改，支持传入 Cluster 标识符，来获取Topic Route。

正常情况下：
- SiteA 的Producer和Consumer 都只能访问到 ClusterA 的 MessageQueue。
- SiteB 的Producer和Consumer 都只能访问到 ClusterB 的 MessageQueue。
— 机房内就近访问，且机房内严格保序。

假设 SiteA 宕机，此时对Nameserver发指令允许全网读：
- SiteB 的 Producer 仍然写入 ClusterB 的 MessageQueue。
- SiteB 的 Consumer 可以同时读到 ClusterA 的Topic 和 ClusterB MessageQueue。
- 在这种场景下，读取端可以读到所有队列，相当于『Static Topic』 + 『Dynamic Cluster』的融合设计。

Static 的 Scope 限定在 Cluster 内，而 Dynamic 允许在不同 Cluster之间。


### 设计目标
实现 单集群固定 和 全网固定 两种Scope。
多集群，暂时没有必要。

一期只实现 单集群固定 这个Scope。

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








