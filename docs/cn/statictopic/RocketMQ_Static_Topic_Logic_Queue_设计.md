### Version 记录
| 时间 | 主要内容 | 作者 |
| --- | --- | --- |
| 2021-11-01 | 初稿，包括背景、目标、SOT定义与持久化、SOT生命周期、SOT的使用、API逻辑修改、问题与风险 | dongeforever |
| 2021-11-15 | 修改 LogicQueue 的定义，不要引入新字段，完全复用旧的MessageQueue；RemappingStaticTopic时，不要迁移『位点』『幂等数据』等，而是采用Double-Check-Read 的机制| dongforever |
| 2021-12-01 | 更新问题与风险，增加关于一致性、OutOfRange、拉取中断的详细说明| dongforever |
| 2021-12-03 | 增加代码走读的说明| dongforever |
| 2021-12-10 | 引入Scope概念，保留『多集群动态零耦合』的集群设计模型 | dongforever |
| 2021-12-23 | 梳理待完成事项；讨论Admin接口的适配方式 | dongforever |
| 2021-01-05 | Offset存储改成『转换制』，以更好适配原有逻辑 | dongforever |




中文文档在描述特定专业术语时，仍然使用英文。

### 需求背景
StaticTopic/LogicQueue 本质上是解决『固定队列数量』的需求。
这个需求是不是必需的呢，如果是做应用集成，则可能不是必需的，但如果是做数据集成，则是必需的。

固定队列数量，首先可以解决『顺序性』的问题。
在应用集成场景下，应用是无需感知到队列的，只要MQ能保证按顺序投递给应用即可，MQ底层队列数量如何变化，对应用来说是不关心。比如，MQ之前的那套『禁读禁写』就是可以玩转的。

但在数据集成场景中，队列或者叫『分片』，是要暴露给客户端的，客户端所有的数据计算场景，都是基于『分片』来进行的，如果『分片』里的数据，发生了错乱，则计算结果都是错误的。比如，计算WordCount，源数据经过预处理之后，按key写入清洗后的Topic，然后计算侧根据清洗的结果，按照分片来并行计算。如果分片发生变化，则整个清洗逻辑，需要重新处理。

有人可能会反驳，说计算组件清洗后，可以以批的方式写入其它存储组件。这当然是可以的，但如果是这样，MQ的价值就纯粹是一个『源头』价值，而不是『通道』价值。

MQ要想成为一个『数据通道』，则必需要具备可以让计算组件『回写』数据的能力，具备存储『Clean Data』的能力，这样才让MQ有可能在数据集成领域站稳脚跟。

如果是 RocketMQ Streams 这种轻量化的组件，则『回写』会更频繁，更重要。

除此之外，『固定队列数据』对于，RocketMQ 自身后续的发展，也是至关重要的：

- compact topic，如果不能做到严格按key hash，则这个KV系统是有问题的
- 事务或者其它Coordinator的实现，采用『固定队列数量』，可以选取到准确的Broker来充当协调器
- Metadata 的存储，按key hash，那么就可以在Broker上，存储千万级的 Topic

『固定队列数量』对于RocketMQ挺进『数据集成』这个领域，有着不可或缺的作用。
LogicQueue的思路就是为了解决这一问题。

### 设计目标
#### 总体目标
提供『Static Topic』的特性。
引入以下核心概念：
- physical message queue, physical queue for short, a shard bound to a specified broker.
- logic message queue, logic queue for short, a shard vertically composed by physical queues.
- dynamic sharded topic, dynamic topic for short, which has queues increasing with the broker numbers.
- static sharded topic, static topic for short, which has fixed queues, implemented with logic queues.

『Static Topic』拥有固定的分片数量，每个分片称之为『Logic Queue』。
每个『Logic Queue』由多个『Physical Queue』进行纵向分段映射组成。

引入以下非核心概念，对用户无感知，但对于讨论问题非常重要：
- Leader Queue， 某个『Logic Queue』最新映射的『Physical Queue』，也即可写的那个Queue
- Second Leader Queue，某个『Logic Queue』次新映射的『Physical Queue』，也即最新一次切换之前的『Leader Queue』

#### Scope 目标
单集群固定 和 全网固定，参考 [The_Scope_Of_Static_Topic](The_Scope_Of_Static_Topic.md)。


#### LogicQueue 目标
在客户端，LogicQueue 与 Physical Queue 使用体感上没有任何区别，使用一样的概念和对象，遵循一样的语义。
在服务端，针对 LogicQueue 去适配相关的API。

#### 队列语义
RocketMQ Physical Queue 含有以下语义：

- 队列内的Offset，单调递增且连续
- 属于同一个 Broker 上的队列，编号单调递增且连续

LogicQueue 需要保障的语义：

- 队列内的offset，单调递增

LogicQueue 可以不保障的语义：

- 队列内的 offset 连续
- 属于同一个 Broker 上的队列，编号单调递增且连续

offset连续，是一个应该尽量保证的语义，可以允许有少量空洞，但不应该出现大面积不连续的位点。
offset不连续最直接的问题就是：

- 计算Lag会比较麻烦
- 不方便客户端进行各种优化计算（比如切批等）

但只要空洞不是大量频繁出现的，那么也是问题不大的。

单机队列编号连续，除了在注册元数据时，可以简约部分字节外，没有其它实际用处，可以不保证。
当前客户端使用到『单机队列编号连续』这个特点的场景主要有：

- 客户端在获取到TopicRouteData后，转化成MessageQueue时，利用编号进行遍历


#### LogicQueue 定义
当前 MessageQueue 的定义如下
```
private String topic;
private String brokerName;
private int queueId;
```


LogicQueue需要对客户直接暴露，为了保证使用习惯一致，采用同样的定义，其中 queueId相当于全局Id，而brokerName 固定如下：
```
MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME = "__logical_queue_broker__";
```
此时，brokerName没有实际含义，但可以用来识别是否是LogicQueue。

采用此种定义，对于客户端内部的实现习惯改变如下：

- **无法直接根据 brokerName 找到addr，而是需要根据 MessageQueue 找到 addr**

具体改法是MQClientInstance中维护一个映射关系
```
private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<>();
```


基本目标与定义清楚了，接下来的设计，从 Source of Truth 开始。

### SOT 定义和持久化
LogicQueue 的 Source of Truth 就是 LogicQueue 到 Physical Queue 的映射关系。
只要这个映射关系不丢失，则整个系统的状态都是可以恢复的。
反之，整个系统可能陷入混乱。

这个SOT，命名为 TopicQueueMapping。

#### Mapping Schema
```
{
"version":"1",
"bname": "broker02" //标记这份数据的原始存储位置，如果发送误拷贝，可以利用这个字段来进行标识
"epoch": 0, //标记修改版本，用来做一致性校验
"totalQueues":"50",  //当前Topic 总共有多少 LogicQueues
"hostedQueues": {    //当前Broker 所拥有的 LogicQueues
"3" : [
       {
        "queue":"0",
        "bname":"broker01"
        "gen":"0",  //标记切换代次
        "logicOffset":"0", //logicOffset的起始位置
        "startOffset":"0",   // 物理offset的起始位置
        "endOffset":"1000" // 可选，物理offset的最大位置，可以根据上下文算出来
        "timeOfStart":"1561018349243" //可选，用来优化时间搜索
        "timeOfEnd":"1561018349243"   //可选，用来优化时间搜索
        "updateTime":"1561018349243" //可选，记录更新的时间
        },  
        {
        "queue":"0",
        "bname":"broker02",
        "gen":"1",  //可选，标记切换代次
        "logicOffset":"1000", //logicOffset的起始位置
        "startOffset":"0",   // 物理offset的起始位置
        "endOffset":"-1" // 可选，物理offset的最大位置，可以根据上下文算出来，最新的一个应该是活跃的
        "timeOfStart":"1561018349243" //可选，用来优化时间搜索
        "timeOfEnd":"1561018349243"   //可选，用来优化时间搜索
        "updateTime":"1561018349243" //可选，记录更新的时间
        }
      ]
  }
}
```
上述示例的含义是：
* broker02 拥有 LogicQueue 3
* LogicQueue 3 由 2 个 Physical Queue 组成
* 位点范围『0-1000』映射到 Physical Queue 『broker01-0』上面
* 位点范围『1000-』映射到 Physical Queue 『broker02-0』上面

『拥有』的定义是指，Leader Queue 在当前Broker。注意，在实现时，也会把Second Leader Queue存储下来作为备份。

注意以下要点：

- 这个数据量会很大，后续需要考虑进行压缩优化（大部分字段可以压缩）
- 如果将来利用 LogicQueue 去做 Serverless 弹缩，则这个数据会加速膨胀，对这个数据的利用要谨慎
- 要写上 bname，以具备自我识别能力

#### Leader Completeness
RocketMQ 没有中心化的元数据存储，那就遵循『Leader Completeness』原则。
对于每个逻辑队列，把所有映射关系存储在『最新队列所在的Broker上面』，最新队列，其实也是可写队列。
Leader Completeness，避免了数据的切割，对于后续其它操作有极大的便利。

#### Global Epoch Check
对于每个Static Topic，在每个Broker都应该拥有一份『TopicQueueMapping』，每份都带有Epoch。
在创建和更新时，要对已有数据进行完备性校验，如果发现不完备，则说明上次操作失败，或者部分Broker数据丢失，应该先修复再操作。

注意：
即使当前Broker不拥有任何 LogicQueue 或者 PhysicalQueue，也应该存储一份，以做校验。
假设某个Static Topic只拥有1个Logic Queue，而对应的Broker整好宕机，则此时可以根据其它Broker的信息判断出该Topic不完备。


#### File Isolation
由于 RocketMQ 很多的运维习惯，都是直接拷贝 Topics.json 到别的机器进行部署的。
而  TopicQueueMapping 是 Broker 相关的，如果把 TopicQueueMapping 从一个Broker拷贝到另一个Broker，则会造成SOT冲突。

在设计上，TopicQueueMapping 采取独立文件，避免冲突。
在格式上，queue 里面要写上 bname，以具备自我识别能力，这样即使误拷贝到另一台机器，可以识别并报错，进行忽略即可。


### SOT 生命周期
#### 创建和更新
映射关系的创建，第一期应该只由 MQAdmin 来进行操作。
后续，可以考虑引入自动化组件。
这里的要点是：

- TopicConfig 和 TopicQueueMapping 分开存储，但写入时，需要先写 TopicQueueMapping 再写 TopicConfig（SOT先写）
- 【加强校验】需要在 TopicConfig 里面加上一个字段来标识『LogicQueue』的 Topic
- 【加强校验】允许单独更新 TopicConfig，但要带上 TotalQueues 这些基础数据
- 允许更新单 LogicQueue


更多细节在API逻辑修改里面
#### 存储
按照 『Leader Completeness』原则进行存储。
#### 切换
如果为了保证严格顺序，则应该采取『禁旧再切新』的原则：
- 从旧 Leader 所在 Broker 获取信息，进行计算
- 写入旧 Leader，也即禁写旧 Leader
- 写入新 Leader

如果为了保证最高可用性，则应该采取『切新禁旧再切新』：
- 从旧 Leader 所在 Broker 获取信息，进行计算
- 写入新 Leader，保证新 Leader 可写，此时 logicOffset 未定
- 写入旧 Leader，禁写旧 Leader
- 更新新 Leader，确定 logicOffset

切换失败处理：
- 不管哪种方式，数据存储至少成功了1份，后续可以手工恢复


#### 清除
有两部分信息需要清除

- 旧 Broker 上超过2代的映射关系进行清除
- 对于单个LogicQueue，清除已经过期的 Broker Queue 映射项目


### SOT 的使用
#### Broker 注册数据
SOT存储在Broker上，所以使用从 Broker开始。

在 RegisterBrokerBody 中，需要带上两个信息：

- 对于每个Topic，带上本机队列编号和逻辑队列编号的映射关系，也即queueMap
- 对于每个Topic，需要带上 totalQueueNum 这个信息



异常情况需要考虑，假如本 Broker 不拥有任何 LogicQueue 呢？依然需要带上 totalQueueNum 这个信息。
注意，不需要带上所有的映射关系，否则Nameserver很快会被打爆。


#### Nameserver 组装数据
原先的 QueueData  增加2个字段：

- totalQueues，标识总逻辑队列数
- queueMap，也即本机队列编号和逻辑队列编号的映射关系


如果 QueueData 里面 totalQueues 的值 > 0 则认为是逻辑队列，在客户端解析时要进行判断。


遗留问题：
是否需要尊重 readQueueNums 和 writeQueueNums ？
在LogicQueue这里，这个场景是没有意义的，但依然保持尊重。

#### Client 解析数据
改动两个方法即可：

- topicRouteData2TopicPublishInfo
- topicRouteData2TopicSubscribeInfo


注意，逻辑队列要求队列数是固定，如果发现，解析完之后，存在部分队列空洞，要用虚拟Broker值进行补全。
Producer 侧如果要对无 key 场景进行优化，可以通过虚拟Broker值来判断，当前队列是不可用的。
对于key场景，应该让客户端报错。

### API 逻辑修改
#### Tools
LogicQueue是为了解决『Static Sharding』的问题。对于客户来说，『LogicQueue』是手段，『Static』才是目的。本着『用户知晓目的，开发者才需要关心手段』的原则，对用户应该只暴露『Static』的概念。所有QueueMapping的生命周期维护，应该都对用户透明。

#### UpdateStaticTopic
新增UpdateStaticTopic命令，对应RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC=513，主要参数是：

- -t，topic 名字
- -qn，总队列数量
- -c cluster列表 或者 -b broker列表



UpdateStaticTopic 命令会自动计算预期的分布情况，包括但不限于执行以下逻辑：

- 检测Topic的命名冲突
- 检测旧数据的一致性
- 创建必要的物理队列
- 创建必要的映射关系


#### RemappingStaticTopic
迁移动作不引入新命令，计算好之后，执行UPDATE_AND_CREATE_STATIC_TOPIC即可.
主要参数：

- -t， topic 名字
- -c cluster列表 或者 -b broker列表

基本操作流程：

- 1 获取旧Leader，计算映射关系
- 2 迁移『位点』『幂等数据』等
- 3 映射关系写入新/旧 Leader

其中第二步，数据量可能会很大，导致迁移周期非常长，且不是并发安全的。
但这些数据，都是覆盖型的，因此可以改造成不迁移的方式，而是在API层做兼容，也即『Double-Read-Check』机制：

- 读取数据时，先从 Leader 读，如果Leader没有，则从Sub-Leader读取。
- 提交数据，直接在 Leader 层面操作即可，覆盖旧数据。


将来实现的幂等逻辑，也是类似。

#### UpdateTopic
服务端判断『StaticTopic』，禁止该命令进行修改。

#### DeleteTopic
复用现有逻辑，对于 StaticTopic，执行必要的清除动作。

#### TopicStatus
复用现有逻辑，同时展现『Logical Queue』和『Physical Queue』。

#### Broker
#### pullMessage
分段映射，执行远程读，在返回消息时，不进行offset转换，而是返回 OffsetDelta 变量，由客户端进行转换。
这里的方式，类似于Batch。

#### getMinOffset
寻找映射关系，读最早的队列的MinOffset

#### getMaxOffset
本机读，转换成logicOffset即可。


#### getOffsetByTime
需要分段查找。
如果要优化查找速度，应该在映射关系里面，插入时间戳。


#### consumerOffsets 系列
Offset的存储，进行转换，存储在对应PhysicalQueue 所在的 Broker上面。  
读取时，采取『Double-Read-Check』机制，并进行转换。  
这样可以最大程度与 PhysicalQueue 的相关逻辑进行适配，比如 ConsumerProgress 可以看到『最近拉取时间』。 

#### Client

- MQClientInstance.topicRouteData2TopicXXXInfo，修改解析 TopicRouteData的逻辑
- Consumer解压消息时，需要加上OffsetDelta逻辑

#### SDK 兼容性分析
如果要使用StaticTopic，则需要升级Client、Broker、Nameserver。

### 问题与风险
#### 数据一致性问题
RocketMQ 没有引入中心化的存储组件，那么如何保证 SOT 的全局一致性呢？
主要利用两个信息
* TopicQueueMapping 带上的 epoch
* TopicQueueMapping 带上 totalQueues  

在更新或者切换时，获取所有Broker上的 TopicQueueMapping，校验 epoch 和 totalQueues，并且根据 TopicQueueMapping 可以完整地构建出对应的Logic Queue，则说明数据是完整一致的。

如果发现数据不一致，可能是以下因素引入的：
* 集群中有Broker宕机
* 上次更新没有完全成功

应该要先修复数据，再执行 更新或切换 操作

#### No Target Brokers
Target Brokers 是指拥有 LogicQueue 的 Broker。  
考察1个场景，如果某个Topic 只有1个 LogicQueue，而拥有这个 LogicQueue 的 Broker 正好宕机了。此时去更新 Topic，会不会误认为该 Topic 不存在？  
解决这个问题的办法是引入 No Target Brokers，也即集群中除去『Target Brokers』之外的 Broker。  
对于 No Target Broker，依然需要写入一份 TopicQueueMapping，带上 epoch 和 totalQueues，但不拥有任何 LogicQueue。  
有了这个信息之后，在进行一致性校验时，就可以识别出上述场景。  

尤其要注意，如果 Nameserver 中没有任何信息，则需要主动去所有 Broker 拉取一遍。

#### 切换时最新 LogicQueueMappingItem 的 logicOffset 决策问题
logicOffset的决策，依赖于上一个 PhysicalQueue 的最大位点。  
此时，要么跳跃位点，要么等待上一个 PhysicalQueue 确保已经禁写。  
当前实现，为了保障高可用，采用『切新禁旧再切新』的方式，同时跳跃位点。  

#### logicOffset 为 -1 时的处理
此时，可以写，但返回给 客户端的 offset 也是-1。  
此时，不可以读最新 PhysicalQueue。  
需要非常小心触发位点被重置：  
- 忽略logicOffset为 -1 的item
- 计算staticOffset时，如果发现logicOffset为-1，则报错

目前只允许，SendMessage和GetMin时，返回-1。其余场景，要严格校验并报错。
  

#### 队列重复映射
如果允许1个 PhysicalQueue 被重复利用，也即多段映射给多个 LogicQueue，或者从非0开始映射。  
会带来以下麻烦：
* 所有位点相关的API，需要考虑 MappingItem endOffset，因为超过了 endOffset 可能已经不属于 当前 LogicQueue 了
* 新建 MappingItem，需要先获取 旧 MappingItem 的 endOffset  

当前实现，为了保证简洁，禁止 PhysicalQueue 被重复利用，每次更新映射都会让物理层面的 writeQueues++ 和 readQueues++。  
后续实现，可以考虑复用已经被清除掉的Physical，也即已经没有数据，位点从0开始。

#### 备机更新映射
当前，admin操作都是要求在Master操作的。因此，没有这个问题。  
Command操作时，提前预判Master是否存在，如果不存在，则提前报错，减少中间失败率。  

#### 拉取消息时 OutOfRange 的判断
以下情况会影响 OutOfRange 的判断
* 从备机拉取消息（默认不会返回OFFSET_MOVED）
* 中间 MappingItem 因为Commitlog的提前删除导致 PhysicalQueue Offset Truncation

所以，OutOfRange 的判断，遵循以下原则：
* 从 Leader Item 拉取，只有requestOffset > maxOffset，尊重 OFFSET_MOVED
* 从 Earliest Item 拉取，只有 requestOffset < minOffset，尊重 OFFSET_MOVED
* 其它情况，都忽略 OFFSET_MOVED

如果没有恰当地处理 OFFSET_MOVED，可能造成位点被重置。

需要注意，这个地方，产生了对 PullMessageResponseHeader 中 minOffset 和 maxOffset 的强依赖。
在次此之前，这两个信息，只对客户端的限流有作用，对业务没有实际的影响。

#### 拉取消息时的 中断问题
当1个 PhysicalQueue 被拉取干净时，需要修正 nextBeginOffset 到下一个 PhysicalQueue。
如果没有处理好，则直接会导致拉取中断，无法前进。
#### pullResult 位点由谁设置的问题
类似于Batch，由客户端设置，避免服务端解开消息：  
在PullResultExt中新增字段 offsetDelta。
#### Admin接口与User接口的适配方式区别
User 接口，使用范围广泛如多语言等，应该尽可能简单，把适配逻辑做在服务端，对『客户端』透明。  
那么 Admin 接口呢，比如 examineTopicStats，适配逻辑是做在『服务端』还是『客户端』？  
一个 Admin 接口，通常需要访问所有 Broker 的所有队列。
如果做在服务端，则可能产生交叉访问，在极端情况下，性能会非常差，举个例子：  
100 个 Broker，相互交叉映射过一遍，则Admin客户端首先要向 100 个 Broker 发请求，然后这 100 个 Broker 为了获取远程信息，各自向其余 Broker 发请求。
其实际网络请求数就是 100 * 100 = 10000 个网络请求。放大效应十分明显。  
同时，考虑到 Admin 接口，使用范围是有限的，无需考虑多语言适配等问题，可以把适配逻辑做在 Admin 客户端。

#### 远程读的性能问题
从实战经验来看，性能损耗几乎不计。
#### 使用习惯的改变
利用新的创建命令进行隔离。
#### 消费SendBack问题
目前的实现里，消费Send Back，是直接传回Commitlog Pos，这个在LogicQueue里行不通。
需要修改API，改成传回『Logic Queue Offset』。
#### 二阶消息的兼容性
二阶消息，也即『原始消息』存储在『系统Topic』中，需要经过一轮『Read-ReWrite』逻辑才会被用户看见的消息。
例如，定时消息，事务消息。
二阶消息需要支持远程读写操作。
一期的LogicQueue不支持『二阶消息』。

### 待完成事项
#### 阻止旧客户端的请求
旧的客户端无法解析逻辑路由，但可以识别物理路由。如果错误使用，则会影响映射关系的准确性。
#### 阻止Pop模式、事务消息、定时消息使用 LogicQueue
不兼容 事务消息和定时消息。  
LogicQueue 当前不支持Pop模式消费。
#### Nameserver 相关生命周期完善
目前没有处理Nameserver中Mapping数据的生命周期
#### ConsumeQueue 的 correctMinOffset 逻辑存在缺陷
可能导致 LogicQueue 无法清除已经过期的 MappingItem。
#### getOffsetInQueueByTime 语义有缺陷
可能导致 LogicQueue 时间搜索不准确。需要专项修复。
#### Metadata 更新机制
当前的更新机制太慢。且访问『不存在Broker』时，会频繁访问Nameserver，有打爆Nameserver的风险。
#### examineConsumeStats 接口获取不到『最近消费时间』
位点相关的消息可能不在本机，需要远程访问。
#### resetOffset 需要适配
当前没有适配。重置位点，可能会得到不符合预期的结果。
#### MessageQueue 没有被物理清除
当前只是产生遗留数据，占用一点点存储空间，没有太大影响。  
如果将来要实现 物理 Queue 复用，则需要先完善相关逻辑。

### 代码走读要点
#### Admin 入口
主要看两个类：  
UpdateStaticTopicSubCommand   
RemappingStaticTopicSubCommand   
#### Metadata 入口
主要看：  
TopicQueueMappingManager
#### Client 入口
重点关注：  
MQClientInstance.updateTopicRouteInfoFromNameServer
#### Server 入口
以 SendMessageProcessor 为例，插桩代码普遍是以下风格：
```
TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, true);
RemotingCommand rewriteResult =  rewriteRequestForStaticTopic(requestHeader, mappingContext);
if (rewriteResult != null) {
    return CompletableFuture.completedFuture(rewriteResult);
}
```
其它Processor类似
#### 测试入口
rocketmq-test模块，statictopic目录。





