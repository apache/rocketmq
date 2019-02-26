## DefaultMQPushConsumer
---
### 类简介

`public class DefaultMQPushConsumer 
extends ClientConfig 
implements MQPushConsumer`

`MQPushConsumer`从技术上讲，这个push客户机实际上是底层pull服务的包装器。具体地说,在从代理提取的消息到达时，它大致调用注册的回调处理程序来提供消息。

`MQPushConsumer`类是rocketmq客户端消费者实现，从名字上已经可以看出其消息获取方式为broker往消费端推送数据，其内部实现了流控，消费位置上报等等。

[快速上手代码示例](https://rocketmq.apache.org/docs/simple-example/)

**注意：**该类是线程安全的。初始化后，实例可视为线程安全。

### 字段摘要
|类型|字段名称|描述|
|------|-------|-------|
|DefaultMQPushConsumerImpl|defaultMQPushConsumerImpl|消费者的内部默认实现|
|String|consumerGroup|消费者分组|
|MessageModel|messageModel|消息模型，默认为CLUSTERING（集群）|
|ConsumeFromWhere|consumeFromWhere|用户引导上的消费点，默认为CONSUME_FROM_LAST_OFFSET（消费者客户端从它之前停止的地方开始）|
|String|consumeTimestamp|消费时间戳|
|AllocateMessageQueueStrategy|allocateMessageQueueStrategy|指定如何将消息队列分配给每个使用者客户机的队列分配算法|
|Map<String, String>|subscription|订阅关系|
|MessageListener|messageListener|消息监听|
|OffsetStore|offsetStore|抵消存储|
|int|consumeThreadMin|最小用户线程数，默认（20）|
|int|consumeThreadMax|最大用户线程数，默认（64）|
|long|adjustThreadPoolNumsThreshold|用于动态调整线程池数量的阈值，默认（100000）|
|int|consumeConcurrentlyMaxSpan|同时最大跨距偏移量。它对顺序消费没有影响，默认（2000）|
|int|pullThresholdForQueue|队列级流控制阈值，每个消息队列默认缓存最多1000条消息|
|int|pullThresholdSizeForQueue|在队列级别上限制缓存的消息大小，默认情况下每个消息队列最多缓存100条MiB消息|
|int|pullThresholdForTopic|主题级的流控制阈值，默认值为-1(无限制)|
|int|pullThresholdSizeForTopic|在主题级别上限制缓存的消息大小，默认值是-1 MiB(无限制)|
|long|pullInterval|消息拉取间隔|
|int|consumeThreadMax|最大用户线程数，默认（64）|
|int|consumeMessageBatchMaxSize|批量消费规模|
|int|pullBatchSize|批处理拉取大小|
|boolean|postSubscriptionWhenPull|每次拉动时是否更新订阅关系|
|boolean|unitMode|是否订阅组的单位|
|int|maxReconsumeTimes|最大重复消费时间|
|long|suspendCurrentQueueTimeMillis|对于需要缓慢牵拉的情况，如流量控制，暂停牵拉时间|
|long|consumeTimeout|消费超时时间|
|TraceDispatcher|traceDispatcher|接口的异步传输数据|
### 构造方法摘要

|方法名称|方法描述|
|-------|------------|
|DefaultMQPushConsumer()|由默认参数值创建一个生产者|
|DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,AllocateMessageQueueStrategy allocateMessageQueueStrategy)|构造函数，指定使用者组、RPC钩子和消息队列分配算法|
|DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic)|构造函数指定使用者组、RPC钩子、消息队列分配算法、启用msg跟踪标志和自定义跟踪主题名称|
|DefaultMQPushConsumer(RPCHook rpcHook)|指定RPC钩子的构造函数|
|DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace)|构造函数，指定使用者组并启用msg跟踪标志|
|DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic)|构造函数指定使用者组、启用msg跟踪标志和自定义跟踪主题名称|
|DefaultMQPushConsumer(final String consumerGroup)|指定使用者组的构造函数|

### 使用方法摘要

|返回值|方法名称|方法描述|
|-------|-------|------------|
|void|createTopic(String key, String newTopic, int queueNum)|在broker上创建指定的topic|
|void|createTopic(String key, String newTopic, int queueNum, int topicSysFlag)|在broker上创建指定的topic|
|long|searchOffset(MessageQueue mq, long timestamp)|查找指定时间的消息队列的物理offset|
|long|maxOffset(MessageQueue mq)|查询给定消息队列的最大offset|
|long|minOffset(MessageQueue mq)|查询给定消息队列的最小offset|
|long|earliestMsgStoreTime(MessageQueue mq)|查询最早的消息存储时间|
|MessageExt|viewMessage(String offsetMsgId)|根据给定的msgId查询消息|
|QueryResult|queryMessage(String topic, String key, int maxNum, long begin, long end)|按关键字查询消息|
|MessageExt|public MessageExt viewMessage(String topic, String msgId)|根据物理offset查询消息???(PS:入参定义和实现有些不一致)|
|void|sendMessageBack(MessageExt msg, int delayLevel)(String topic)|将消息发送回代理，代理将在未来重新发送|
|void|sendMessageBack(MessageExt msg, int delayLevel, String brokerName)|将消息发送回名称为brokerName的代理，该消息将在将来重新传递|
|set<MessageQueue>|fetchSubscribeMessageQueues(String topic)|获取订阅topic的消息队列|
|void|start()|启动消费者|
|void|shutdown()|关闭当前消费者实例并释放相关资源|
|void|registerMessageListener(MessageListener messageListener)|注册消息监听|
|void|registerMessageListener(MessageListenerConcurrently messageListener)|注册一个回调函数，以便在消息到达时执行并发消费|
|void|registerMessageListener(MessageListenerOrderly messageListener)|注册一个回调函数，以便在消息到达时执行，以便进行有序消费|
|void|subscribe(String topic, String subExpression)|将主题订阅到消费订阅|
|void|subscribe(String topic, String fullClassName, String filterClassSource)|将主题订阅到消费订阅|
|void|subscribe(final String topic, final MessageSelector messageSelector)|通过消息选择器订阅主题|
|void|unsubscribe(String topic)|从订阅中取消订阅指定的主题|
|void|updateCorePoolSize(int corePoolSize)|更新使用线程核心池大小的消息|
|void|suspend()|暂停提取新消息|
|void|resume()|再次拉取|

### 字段详细信息
- defaultMQPushConsumerImpl

	`protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl`
	
	内部实现。这里的大多数功能都委托给了它。

- [consumerGroup](http://rocketmq.apache.org/docs/core-concept/)

	`private String consumerGroup`

	相同角色的使用者需要具有完全相同的订阅和consumerGroup才能正确地实现负载平衡。它是必需的，并且需要是全球唯一的。

- messageModel

	`private MessageModel messageModel = MessageModel.CLUSTERING`

	消息模型定义了如何将消息传递给每个消费者客户端。RocketMQ支持两种消息模型:集群和广播。如果设置了集群，则使用相同的{@link #consumerGroup}将只使用订阅的消息的碎片，从而实现加载平衡;相反，如果设置了广播，每个使用者客户机将使用所有订阅的消息分开。
    此字段默认为集群。

- consumeFromWhere

	`private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET`

	消费者引导上的消费点。
    有三个消费点:
    CONSUME_FROM_LAST_OFFSET:消费者客户端从它之前停止的地方开始。
    如果它是一个新启动的消费客户端，根据消费组的老化，有两个
    例:
    如果使用者组是最近创建的，则订阅的最早消息尚未出现过期，这意味着消费者组代表最近启动的业务，即consumer will从头开始;如果最早订阅的消息已过期，则消费将从最新消息开始消息，即在启动时间戳之前生成的消息将被忽略。
    CONSUME_FROM_FIRST_OFFSET:消费者客户端将从可用的最早的消息开始。
    CONSUME_FROM_TIMESTAMP: Consumer客户机将从指定的时间戳开始，这意味着在{@link #consumeTimestamp}之前生成的消息将被忽略。

- consumeTimestamp

	`private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30))`

	以秒精度回溯消耗时间。时间格式是20131223171201，意味着2013年12月23日是1712又01秒，默认回溯消费时间半小时前。

- allocateMessageQueueStrategy

	`private AllocateMessageQueueStrategy allocateMessageQueueStrategy`

	指定如何将消息队列分配给每个使用者客户机的队列分配算法。

- subscription

	`private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>()`

	订阅关系。

- messageListener

	`private MessageListener messageListener`

	消息监听。

- offsetStore

	`OffsetStore offsetStore`

	抵消存储。

- consumeThreadMin

	`private int consumeThreadMin = 20`

	最小用户线程数。

- consumeThreadMax

	`private int consumeThreadMax = 64`

	最大用户线程数。
	
- adjustThreadPoolNumsThreshold 

	`private long adjustThreadPoolNumsThreshold = 100000`

	用于动态调整线程池数量的阈值。
	
- consumeConcurrentlyMaxSpan

	`private int consumeConcurrentlyMaxSpan = 2000`

	同时最大跨距偏移量。它对顺序消费没有影响。

- pullThresholdForQueue

	`private int pullThresholdForQueue = 1000`

	流控制阈值在队列级别，每个消息队列默认缓存最多1000条消息，
	注意：{@code pullBatchSize}，瞬时值可能超过限制。

- pullThresholdSizeForQueue

	`private int pullThresholdSizeForQueue = 100`

	在队列级别上限制缓存的消息大小，默认情况下每个消息队列最多缓存100条MiB消息，
    注意：{@code pullBatchSize}，瞬时值可能会超过限制
       
    消息的大小仅由消息体度量，因此不准确。

- pullThresholdForTopic

	`private int pullThresholdForTopic = -1`

	主题级的流控制阈值，默认值为-1(无限制)。
    如果{@code pullThresholdForQueue}的值不是无限制的，那么它的值将被重写并基于{@code pullThresholdForTopic}进行计算
    例如，如果pullThresholdForTopic 的值是1000，并且为该使用者分配了10个消息队列，那么pullThresholdForQueue 将设置为100。
	
- pullThresholdSizeForTopic

	`private int pullThresholdSizeForTopic = -1`

	在主题级别上限制缓存的消息大小，默认值为-1 MiB(无限制)。	
	{@code pullThresholdSizeForQueue}的值将被覆盖，并根据其进行计算{@code pullThresholdSizeForTopic}如果不是无限的。
	例如，如果pullThresholdSizeForTopic 的值是1000 MiB，而10个消息队列的值是1000 MiB 分配给这个消费者，然后pullThresholdSizeForQueue 将被设置为100 MiB。
	
- pullInterval

	`private long pullInterval = 0`

	消息拉取间隔。
	
- consumeMessageBatchMaxSize

	`private int consumeMessageBatchMaxSize = 1`

	批量消费规模。
	
- pullBatchSize

	`private int pullBatchSize = 32`

	批量处理拉取大小。
	
- postSubscriptionWhenPull

	`private boolean postSubscriptionWhenPull = false`

	每次拉动时是否更新订阅关系。
	
- unitMode

	`private boolean unitMode = false`

	是否订阅组的单位。
	
- maxReconsumeTimes

	`private int maxReconsumeTimes = -1`

	最大重复消费时间。-1表示16次。
    如果消息在成功之前被重复使用的次数超过{@link #maxReconsumeTimes}，那么它将被定向到正在等待删除的队列。。
	
- suspendCurrentQueueTimeMillis

	`private long suspendCurrentQueueTimeMillis = 1000`

	对于需要缓慢牵拉的情况，如流量控制，暂停牵拉时间。
	
- consumeTimeout

	`private long consumeTimeout = 15`

	消息可能阻塞正在使用的线程的最大时间(以分钟为单位)。
	
- traceDispatcher

	`private TraceDispatcher traceDispatcher = null`

	接口的异步传输数据。

### 构造方法详细信息

1. DefaultMQPushConsumer
	
	`DefaultMQPushConsumer()`

	创建一个新的消费者。

2. DefaultMQPushConsumer
	
	`DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy)`

	构造函数，指定使用者组、RPC hook和消息队列分配算法。

	- 入参描述：
	
	    参数名 | 类型 | 是否必须 | 缺省值 |描述
        ---|---|---|---|---
        consumerGroup | String | 是 | DEFAULT_CONSUMER | 消费队列组名称
        rpcHook | RPCHook | 是 |  | 每个远程命令执行后会回掉rpcHook
        allocateMessageQueueStrategy | AllocateMessageQueueStrategy | 是 | null | 消息队列分配算法

3. DefaultMQPushConsumer
	
	`DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic)`

	构造函数指定使用者组、RPC hook、消息队列分配算法、启用msg跟踪标志和自定义跟踪主题名称。

	- 入参描述：
    
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         consumerGroup | String | 是 | DEFAULT_CONSUMER | 消费队列组名称
         rpcHook | RPCHook | 是 |  | 每个远程命令执行后会回掉rpcHook
         allocateMessageQueueStrategy | AllocateMessageQueueStrategy | 是 |  | 消息队列分配算法
         enableMsgTrace | boolean | 是 | false | 切换标记实例用于消息跟踪
         customizedTraceTopic | String | 是 |  | 定制消息跟踪主题的名称。如果不进行配置，可以使用默认跟踪主题名称

4. DefaultMQPushConsumer

	`DefaultMQPushConsumer(RPCHook rpcHook)`

	使用指定的hook创建一个消费者。

	- 入参描述：
    
        参数名 | 类型 | 是否必须 | 缺省值 |描述
        ---|---|---|---|---       
        rpcHook | RPCHook | 是 |  | 每个远程命令执行后会回掉rpcHook

5. DefaultMQPushConsumer

	`DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace)`

	构造函数，指定使用者组并启用msg跟踪标志。
	
	- 入参描述：
        
        参数名 | 类型 | 是否必须 | 缺省值 |描述
        ---|---|---|---|---   
        consumerGroup | String | 是 | DEFAULT_CONSUMER | 消费队列组名称
        enableMsgTrace | boolean | 是 | false | 切换标记实例用于消息跟踪
		
6. DefaultMQPushConsumer

	`DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic)`

	构造函数指定使用者组、启用msg跟踪标志和自定义跟踪主题名称。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         consumerGroup | String | 是 | DEFAULT_CONSUMER | 消费队列组名称
         enableMsgTrace | boolean | 是 | false | 切换标记实例用于消息跟踪
         customizedTraceTopic | String | 是 |  | 定制消息跟踪主题的名称。如果不进行配置，可以使用默认跟踪主题名称
		
7. DefaultMQPushConsumer

	`DefaultMQPushConsumer(final String consumerGroup)`

	指定使用者组的构造函数。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         consumerGroup | String | 是 | DEFAULT_CONSUMER | 消费队列组名称

### 使用方法详细信息

1.  createTopic

	`public void createTopic(String key, String newTopic, int queueNum) `

	在broker上创建一个topic。
	
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         key | String | 是 |  | 访问的key
         newTopic | String  | 是 |  | 新建topic的名称
         queueNum | int | 是 |  | topic的队列数量
		
	- 返回值描述：
		void
	- 异常描述：
		MQClientException - 生产者状态非Running；未找到broker等客户端异常。

2.  createTopic

	`public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)`

	在broker上创建一个topic。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         key | String | 是 |  | 访问的key
         newTopic | String  | 是 |  | 新建topic的名称
         queueNum | int | 是 |  | topic的队列数量
         topicSysFlag | int | 是 |  | topic的系统属性
		
	- 返回值描述：
		void
	- 异常描述：
		MQClientException - 生产者状态非Running；未找到broker等客户端异常。

3. searchOffset

	`public long searchOffset(MessageQueue mq, long timestamp)`

	查找指定时间的消息队列的物理offset。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         mq | MessageQueue | 是 |  | 要查询的消息队列
         timestamp | long  | 是 |  | 指定的时间戳
			
	- 返回值描述：
		指定时间的消息队列的物理offset。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
		
4. maxOffset

	`public long maxOffset(MessageQueue mq)`

	查询给定消息队列的最大offset。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         mq | MessageQueue | 是 |  | 要查询的消息队列
			
	- 返回值描述：
		给定消息队列的最大offset。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

5. minOffset

	`public long minOffset(MessageQueue mq)`

	查询给定消息队列的最小offset。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         mq | MessageQueue | 是 |  | 要查询的消息队列
			
	- 返回值描述：
		给定消息队列的最小offset。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

6. earliestMsgStoreTime

	`public long earliestMsgStoreTime(MessageQueue mq)`

	查询最早的消息存储时间。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         mq | MessageQueue | 是 |  | 要查询的消息队列
			
	- 返回值描述：
		要查询队列最早存储时间的时间戳。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。
		
7. viewMessage

	`public MessageExt viewMessage(String offsetMsgId)`

	根据给定的msgId查询消息。

	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         offsetMsgId | String | 是 |  | 消息ID
	
	- 返回值描述：
		返回`MessageExt`，包含：topic名称，消息题，消息ID，消费次数，生产者host等信息。
	- 异常描述：
		RemotingException - 网络层发生错误。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 线程被中断。<br>
		MQClientException - 生产者状态非Running；msgId非法等。
		
8. queryMessage

	`public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)`

	按关键字查询消息。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | topic名称
         key | String | 是 |  | 消息的key，生产消息时设置的key
         maxNum | int | 是 |  | 返回消息的最大数量
         begin | long | 是 |  | 开始查询时间戳，单位：毫秒
         end | long | 是 |  | 结束查询时间戳，单位：毫秒
	
	- 返回值描述：
		查询到的消息集合。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常等客户端异常客户端异常。<br>
		InterruptedException - 线程中断。

9. viewMessage

	`public MessageExt viewMessage(String topic, String msgId)`

	根据给定的msgId查询消息。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | topic名称
         msgId | String | 是 |  | 消息ID

	- 返回值描述：
		返回`MessageExt`，包含：topic名称，消息题，消息ID，消费次数，生产者host等信息。
	- 异常描述：
		RemotingException - 网络层发生错误。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 线程被中断。<br>
		MQClientException - 生产者状态非Running；msgId非法等。

10. sendMessageBack

	`public void sendMessageBack(MessageExt msg, int delayLevel)`

	将消息发送回代理，代理将在未来重新发送。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         msg | MessageExt | 是 |  | 消息发送回来
         delayLevel | int | 是 |  | 延迟级别延迟级别

	- 返回值描述：
		void
	- 异常描述：
		RemotingException - 网络异常。<br>
        MQBrokerException - broker发生错误。<br>
        InterruptedException - 发送线程中断。<br>
        MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

11. sendMessageBack

	` public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)`

	将消息发送回名称为brokerName的代理，在未来消息将被重新传递进来。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         msg | MessageExt | 是 |  | 消息发送回来
         delayLevel | int | 是 |  | 延迟级别延迟级别
         brokerName | String | 是 |  | 代理名称

	- 返回值描述：
		void
	- 异常描述：
		RemotingException - 网络异常。<br>
        MQBrokerException - broker发生错误。<br>
        InterruptedException - 发送线程中断。<br>
        MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。	
        	
12. fetchSubscribeMessageQueues

	`public Set<MessageQueue> fetchSubscribeMessageQueues(String topic)`

	获取订阅主题topic的消息队列。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         msg | MessageExt | 是 |  | 消息发送回来

	- 返回值描述：
		topic下的消息队列。
	- 异常描述：
		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

13. start

	`public void start()`

	启动消费者实例。它执行了许多内部初始化，比如：检查配置、与namesrv建立连接、启动一系列心跳等定时任务等。实例必须在配置之后调用此方法。

	- 入参描述：
		无。
	- 返回值描述：
		void
	- 异常描述：
		MQClientException - 初始化过程中出现失败。

14. shutdown

	`public void shutdown()`

	关闭当前生产者实例并释放相关资源。

	- 入参描述：
		无。
	- 返回值描述：
		void
	- 异常描述：
		无声明异常



15. registerMessageListener

	`public void registerMessageListener(MessageListener messageListener)`

	该方法已标识弃用。注册一个回调函数。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         messageListener | MessageListener | 是 |  | 消息处理回调

	- 返回值描述：
		void。
	- 异常描述：
		

16. registerMessageListener

	`public void registerMessageListener(MessageListenerConcurrently messageListener)`

	注册一个回调函数，以便在消息到达时执行并发消费。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         messageListener | MessageListener | 是 |  | 消息处理回调

	- 返回值描述：
		void
	- 异常描述：
	
17. registerMessageListener

	`public void registerMessageListener(MessageListenerOrderly messageListener)`

	注册一个回调函数，以便在消息到达时执行，以便进行有序消费。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         messageListener | MessageListener | 是 |  | 消息处理回调

	- 返回值描述：
		void
	- 异常描述：	
		
18. subscribe

	`public void subscribe(String topic, String subExpression)`

	将主题订阅到消费订阅。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | 订阅主题
         subExpression | String | 是 |  | 订阅表达式。如果为空或*表达式，则只支持或操作“tag1 || bb2 || tag3”，即全部订阅

	- 返回值描述：
		void
	- 异常描述：
		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。

19. subscribe

	`public void subscribe(String topic, String fullClassName, String filterClassSource)`

	将主题订阅到消费订阅。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | 订阅主题
         fullClassName | String | 是 |  | 完整的类名，必须扩展 org.apache.rocketmq.common.filter.MessageFilter
         filterClassSource | String | 是 |  | 类源代码，使用UTF-8文件编码，必须对您的代码安全负责

	- 返回值描述：
		void
	- 异常描述：
		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。
		
20. subscribe

	`public void subscribe(final String topic, final MessageSelector messageSelector)`

	通过消息选择器订阅主题。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | 订阅主题
         messageSelector | MessageSelector | 是 |  | 消息选择器

	- 返回值描述：
		void
	- 异常描述：
		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。

21. unsubscribe

	`public void unsubscribe(String topic)`

	从订阅中取消订阅指定的主题。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         topic | String | 是 |  | 订阅主题

	- 返回值描述：
		void
	- 异常描述：
		

22. updateCorePoolSize

	`public void updateCorePoolSize(int corePoolSize)`

	更新使用线程核心池大小的消息。
	- 入参描述：
    	
    	 参数名 | 类型 | 是否必须 | 缺省值 |描述
         ---|---|---|---|---
         corePoolSize | int | 是 |  | 线程池大小

	- 返回值描述：
		void
	- 异常描述：

23. suspend

	`public void suspend()`

	暂停提取新消息。

	- 入参描述：
		
	- 返回值描述：
		void
	- 异常描述：

24. resume

	`public void resume()`

	再次拉取。

	- 入参描述：
		
	- 返回值描述：
		void
	- 异常描述：

