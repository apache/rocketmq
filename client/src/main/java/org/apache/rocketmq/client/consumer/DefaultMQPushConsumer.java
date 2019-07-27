/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * In most scenarios, this is the mostly recommended class to consume messages.
 * 在大多数情况下，这是最推荐使用消费消息的类。
 * </p>
 *
 * Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
 * arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
 * 从技术上讲，这个push客户机实际上是底层pull服务的包装器。具体地说，当从代理中提取的消息到达时，它大致调用注册的回调处理程序来提供消息。
 * </p>
 *
 * See quickstart/Consumer in the example module for a typical usage.
 * 有关典型用法，请参阅示例模块中的QuickStart/Consumer。
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
 * 线程安全：初始化后，可以将实例视为线程安全的。
 * </p>
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    private final InternalLogger log = ClientLogger.getLog();


    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * 相同角色的使用者必须拥有完全相同的订阅和使用者组才能正确地实现负载平衡。它是必需的，需要全球独一无二。
     * </p>
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    private String consumerGroup;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * 消息模型定义了如何将消息传递给每个客户机。
     * </p>
     *
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * Rocketmq支持两种消息模型：集群和广播。
     * 如果设置了集群，具有相同消息的客户机将只使用订阅的消息的碎片，从而实现负载平衡；
     * 相反，如果设置了广播，则每个客户机将分别使用所有订阅的消息。
     * </p>
     *
     * This field defaults to clustering.
     * 此字段默认为群集。
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Consuming point on consumer booting.
     * 消费引导上的消费点。
     * </p>
     *
     * There are three consuming points:
     * 有三个消费点：
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * 消费者客户会在它之前停止的地方找到它。如果它是一个新启动的消费客户，根据消费群体的老化，有两个：
     * cases: 例如：
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * 如果用户组是最近创建的，以至于订阅的最早消息尚未过期，这意味着用户组代表一个最近启动的业务，那么消费将从头开始；
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * 如果订阅的最早消息已过期，则使用将从最新消息开始，这意味着在启动时间戳之前生成的消息将被忽略。
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * CONSUME_FROM_FIRST_OFFSET ：使用者客户端将从可用的最早消息开始。
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * CONSUME_FROM_TIMESTAMP：使用者客户端将从指定的时间戳开始，这意味着将忽略之前生成的消息
     * </li>
     * </ul>
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201<br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     *
     * 二次精度回溯消耗时间。时间格式为20131223171201，意味着2013年12月23日17分12秒01秒
     * 半小时前的默认回溯消耗时间。
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     * 队列分配算法，指定如何将消息队列分配给每个使用者客户端
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     * 订阅关系
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * Message listener
     * 消息监听
     */
    private MessageListener messageListener;

    /**
     * Offset Storage
     * 偏移量存储
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     * 最小消费者线程数
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     * 最大消费者线程数
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     * 线程池数动态调整阈值
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     * 并发最大跨距偏移。它对连续消耗没有影响
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * 队列级别上的流控制阈值，默认情况下，每个消息队列最多缓存1000条消息，考虑到pullBatchSize，瞬时值可能超过极限
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     *
     * 限制队列级别的缓存消息大小，默认情况下，每个消息队列最多缓存100MB消息，考虑到pullBatchSize，瞬时值可能超过极限
     *
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     * 消息的大小只能通过消息体来测量，因此不准确
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * Flow control threshold on topic level, default value is -1(Unlimited)
     * 主题级别的流控制阈值，默认值为-1（无限制）
     * <p>
     * The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
     * {@code pullThresholdForTopic} if it is't unlimited
     * pullThresholdForQueue 的值将被重写，并根据 pullThresholdForTopic 的值将被重写
     * <p>
     * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
     * then pullThresholdForQueue will be set to 100
     * 例如，如果 pullThresholdForTopic 的值为1000，并且向此使用者分配了10个消息队列，则 pullThresholdForQueue 将设置为100
     */
    private int pullThresholdForTopic = -1;

    /**
     * Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
     * 限制主题级别的缓存邮件大小，默认值为-1 mib（无限制）
     * <p>
     * The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
     * {@code pullThresholdSizeForTopic} if it is't unlimited
     * pullThresholdSizeForQueue的值将被重写，并根据pullThresholdSizeForTopic如果它不是无限的
     * <p>
     * For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
     * assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * Message pull Interval
     * 消息拉取间隔
     */
    private long pullInterval = 0;

    /**
     * Batch consumption size
     * 批量消费量
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Batch pull size
     * 批量拉取大小
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     * 每次拉取时是否更新订阅关系
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     * 订阅组单位
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times. -1 means 16 times.
     * 最大再消费团队。-1代表16个。
     * </p>
     *
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
     * queue waiting.
     * 如果消息在成功之前被重复使用超过 maxReconsumeTimes 它将被定向到一个正在等待的删除队列
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     * 对于需要缓慢拉取的情况，如流量控制场景，暂停拉取时间。
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     * 消息可能阻塞消耗线程的最长时间（分钟）
     */
    private long consumeTimeout = 15;

    /**
     * Interface of asynchronous transfer data
     * 异步传输数据接口
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     * 默认构造函数
     */
    public DefaultMQPushConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, RPC hook and message queue allocating algorithm.
     * 指定使用者组、RPC挂钩和消息队列分配算法的构造函数。
     *
     * @param consumerGroup Consume queue.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup Consume queue.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(customizedTraceTopic, rpcHook);
                dispatcher.setHostConsumer(this.getDefaultMQPushConsumerImpl());
                traceDispatcher = dispatcher;
                this.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                    new ConsumeMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }


    /**
     * Constructor specifying consumer group and enabled msg trace flag.
     *
     * @param consumerGroup Consumer group.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, null);
    }

    /**
     * Constructor specifying consumer group, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup Consumer group.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(mq, timestamp);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(mq);
    }

    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(topic, msgId);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    public void setSubscription(Map<String, String> subscription) {
        this.subscription = subscription;
    }

    /**
     * Send message back to broker which will be re-delivered in future.
     * 将消息发送回代理，该代理将在将来重新传递。
     *
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * Send message back to the broker whose name is <code>brokerName</code> and the message will be re-delivered in
     * future.
     * 将消息发送回名为 brokerName 的代理，该消息将在将来重新传递。
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @param brokerName broker name.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(topic);
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     * 这种方法使内部基础设施易于服务。实例必须在配置后调用此方法。
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void start() throws MQClientException {
        this.defaultMQPushConsumerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * Shut down this client and releasing underlying resources.
     * 关闭此客户端并释放基础资源。
     */
    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for concurrent consuming.
     * 注册一个回调，以便在消息到达时执行以进行并发消费。
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     * 注册一个回调，以便在消息到达时执行，以便有序地使用。
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Subscribe a topic to consuming subscription.
     * 订阅主题以使用订阅。
     * @param topic topic to subscribe.
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     * if null or * expression,meaning subscribe all
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
    }

    /**
     * Subscribe a topic to consuming subscription.
     * 订阅主题以使用订阅。
     * @param topic topic to consume.
     * @param fullClassName full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
     */
    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource);
    }

    /**
     * Subscribe a topic by message selector.
     * 通过消息选择器订阅主题。
     * @param topic topic to consume.
     * @param messageSelector {@link org.apache.rocketmq.client.consumer.MessageSelector}
     * @see org.apache.rocketmq.client.consumer.MessageSelector#bySql
     * @see org.apache.rocketmq.client.consumer.MessageSelector#byTag
     */
    @Override
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, messageSelector);
    }

    /**
     * Un-subscribe the specified topic from subscription.
     * 从订阅中取消订阅指定主题。
     * @param topic message topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }

    /**
     * Update the message consuming thread core pool size.
     * 更新消耗线程核心池大小的消息。
     * @param corePoolSize new core pool size.
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    /**
     * Suspend pulling new messages.
     * 暂停拉取新消息
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * Resume pulling.
     * 继续拉
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }
}
