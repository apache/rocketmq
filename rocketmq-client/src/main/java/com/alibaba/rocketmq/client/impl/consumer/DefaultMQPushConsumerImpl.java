/**
 * $Id: DefaultMQPushConsumerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-15
 */
public class DefaultMQPushConsumerImpl implements MQPushConsumer, MQConsumerInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientFactory mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;

    private volatile boolean pause = false;

    // 消费进度存储
    private OffsetStore offsetStore;

    // 消息存储相关
    private ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable =
            new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);

    // 消费消息服务
    private ConsumeMessageService consumeMessageService;

    // 是否顺序消费消息
    private boolean consumeOrderly = false;

    // 可订阅的信息（定时从Name Server更新最新版本）
    private final ConcurrentHashMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
            new ConcurrentHashMap<String, Set<MessageQueue>>();

    // 订阅关系，用户配置的原始数据
    private final ConcurrentHashMap<String /* topic */, SubscriptionData> subscriptionInner =
            new ConcurrentHashMap<String, SubscriptionData>();
    private MessageListener messageListenerInner;


    @Override
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.checkConfig();

            // 复制订阅关系
            this.copySubscription();

            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(this.defaultMQPushConsumer);

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPushConsumer.getConsumerGroup());

            // 广播消费/集群消费
            switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                this.offsetStore =
                        new LocalFileOffsetStore(this.mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup());
                break;
            case CLUSTERING:
                this.offsetStore =
                        new RemoteBrokerOffsetStore(this.mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup());
                break;
            default:
                break;
            }

            // 加载消费进度
            this.offsetStore.load();

            // 启动消费消息服务
            if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                this.consumeOrderly = true;
                this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this,
                            (MessageListenerOrderly) this.getMessageListenerInner());
            }
            else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                this.consumeOrderly = false;
                this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this,
                            (MessageListenerConcurrently) this.getMessageListenerInner());
            }

            this.consumeMessageService.start();

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                this.consumeMessageService.shutdown();
                throw new MQClientException("The consumer group["
                        + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has created already, specifed another name please."
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mQClientFactory.start();
            log.info("the consumer [{}] start OK", this.defaultMQPushConsumer.getConsumerGroup());
            break;
        case RUNNING:
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }

        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        this.mQClientFactory.rebalanceImmediately();
    }


    @Override
    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            this.consumeMessageService.shutdown();
            this.persistConsumerOffset();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, " + this.serviceState, null);
        }
    }


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset);
    }


    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException,
            InterruptedException {
        this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(msg,
            this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 3000);
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.topicSubscribeInfoTable.get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.topicSubscribeInfoTable.get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return result;
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, boolean order)
            throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, order);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    @Override
    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getMaxOffset(mq);
    }


    @Override
    public long getMinOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getMinOffset(mq);
    }


    @Override
    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getEarliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }


    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.subscriptionInner.put(topic, subscriptionData);
            // 发送心跳，将变更的订阅关系注册上去
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    @Override
    public void unsubscribe(String topic) {
        this.subscriptionInner.remove(topic);
    }


    @Override
    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    @Override
    public void resume() {
        this.pause = false;
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    @Override
    public String getGroupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }


    @Override
    public MessageModel getMessageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }


    @Override
    public ConsumeType getConsumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }


    @Override
    public Set<SubscriptionData> getMQSubscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.subscriptionInner.values());

        return subSet;
    }


    /**
     * 稍后再执行这个PullRequest
     */
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }


    /**
     * 立刻执行这个PullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    private final long PullTimeDelayMillsWhenException = 3000;
    private final long PullTimeDelayMillsWhenFlowControl = 1000;
    private final long PullTimeDelayMillsWhenSuspend = 1000;


    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDroped()) {
            log.info("the pull request[{}] is droped.", pullRequest.toString());
            return;
        }

        // 检测Consumer是否启动
        try {
            this.makeSureStateOK();
        }
        catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            return;
        }

        // 检测Consumer是否被挂起
        if (this.isPause()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenSuspend);
            return;
        }

        // 流量控制，队列中消息总数
        long size = processQueue.getMsgCount().get();
        if (size > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
            log.warn("the consumer message buffer is full, so do flow control, {} {}", size, pullRequest);
            return;
        }

        // 流量控制，队列中消息最大跨度
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
                log.warn("the queue's messages, span too long, so do flow control, {} {}", size, pullRequest);
                return;
            }
        }

        // 查询订阅关系
        final SubscriptionData subscriptionData =
                this.subscriptionInner.get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            // 由于并发关系，即使找不到订阅关系，也要重试下，防止丢失PullRequest
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult =
                            DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(
                                pullRequest.getMessageQueue(), pullResult, subscriptionData);
                    pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                    switch (pullResult.getPullStatus()) {
                    case FOUND:
                        processQueue.putMessage(pullResult.getMsgFoundList());
                        DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                            pullResult.getMsgFoundList(), processQueue, pullRequest.getMessageQueue());

                        // 流控
                        if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                            DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                        }
                        // 立刻拉消息
                        else {
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        }

                        break;
                    case NO_NEW_MSG:
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case NO_MATCHED_MSG:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case OFFSET_ILLEGAL:
                        log.warn("the pull request offset illegal, {} {}",//
                            pullRequest.toString(), pullResult.toString());
                        if (pullRequest.getNextOffset() < pullResult.getMinOffset()) {
                            pullRequest.setNextOffset(pullResult.getMinOffset());
                        }
                        else if (pullRequest.getNextOffset() > pullResult.getMaxOffset()) {
                            pullRequest.setNextOffset(pullResult.getMaxOffset());
                        }

                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(
                            pullRequest.getMessageQueue(), pullRequest.getNextOffset());

                        log.warn("fix the pull request offset, {}", pullRequest);
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    default:
                        break;
                    }
                }
            }


            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                    PullTimeDelayMillsWhenException);
            }
        };

        int sysFlag = PullSysFlag.buildSysFlag(//
            false, // commitOffset
            true, // suspend
            false// subscription
            );
        try {
            this.pullAPIWrapper.pullKernelImpl(//
                pullRequest.getMessageQueue(), // 1
                null, // 2
                subscriptionData.getSubVersion(), // 3
                pullRequest.getNextOffset(), // 4
                this.defaultMQPushConsumer.getPullBatchSize(), // 5
                sysFlag, // 6
                0,// 7
                BrokerSuspendMaxTimeMillis, // 8
                ConsumerTimeoutMillisWhenSuspend, // 9
                CommunicationMode.ASYNC, // 10
                pullCallback// 11
                );
        }
        catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
        }
    }

    // 长轮询模式，Consumer连接在Broker挂起最长时间
    private static final long BrokerSuspendMaxTimeMillis = 1000 * 15;
    // 长轮询模式，Consumer超时时间（必须要大于brokerSuspendMaxTimeMillis）
    private static final long ConsumerTimeoutMillisWhenSuspend = 1000 * 30;


    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.processQueueTable.keySet();
            if (allocateMq != null) {
                mqs.addAll(allocateMq);
            }

            this.offsetStore.persistAll(mqs);
        }
        catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup()
                    + " persistConsumerOffset exception", e);
        }
    }


    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    /**
     * 计算从哪里开始拉消息，如果出错返回-1
     */
    private long computePullFromWhere(final MessageQueue mq) {
        long result = -1;
        switch (this.defaultMQPushConsumer.getConsumeFromWhere()) {
        case CONSUME_FROM_LAST_OFFSET: {
            long lastOffset = this.offsetStore.readOffset(mq, true);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else {
                result = Long.MAX_VALUE;
            }
            break;
        }
        case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST: {
            long lastOffset = this.offsetStore.readOffset(mq, true);
            if (lastOffset >= 0) {
                result = lastOffset;
            }
            else {
                result = 0L;
            }
            break;
        }
        case CONSUME_FROM_MAX_OFFSET:
            result = Long.MAX_VALUE;
            break;
        case CONSUME_FROM_MIN_OFFSET:
            result = 0L;
            break;
        default:
            break;
        }

        return result;
    }


    private void updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet) {
        // 将多余的队列删除
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    ProcessQueue pq = this.processQueueTable.remove(mq);
                    if (pq != null) {
                        pq.setDroped(true);
                        log.info("doRebalance, {}, remove unnecessary mq, {}",
                            this.defaultMQPushConsumer.getConsumerGroup(), mq);
                    }
                }
            }
        }

        // 增加新增的队列
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                PullRequest pullRequest = new PullRequest();
                pullRequest.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                pullRequest.setMessageQueue(mq);
                pullRequest.setProcessQueue(new ProcessQueue());

                // 这个需要根据策略来设置
                long nextOffset = this.computePullFromWhere(mq);
                pullRequest.setNextOffset(nextOffset);
                pullRequestList.add(pullRequest);
                this.processQueueTable.put(mq, pullRequest.getProcessQueue());
                log.info("doRebalance, {}, add a new mq, {}", this.defaultMQPushConsumer.getConsumerGroup(),
                    mq);
            }
        }

        // 派发PullRequest
        for (PullRequest pullRequest : pullRequestList) {
            this.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}",
                this.defaultMQPushConsumer.getConsumerGroup(), pullRequest);
        }
    }


    private void rebalanceByTopic(final String topic) {
        switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING: {
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            if (mqSet != null) {
                this.updateProcessQueueTableInRebalance(topic, mqSet);
            }
            else {
                log.warn("doRebalance, {}, but the topic[{}] not exist.",
                    this.defaultMQPushConsumer.getConsumerGroup(), topic);
            }
            break;
        }
        case CLUSTERING: {
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            List<String> cidAll =
                    this.mQClientFactory.findConsumerIdList(topic,
                        this.defaultMQPushConsumer.getConsumerGroup());
            if (null == mqSet) {
                if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.",
                        this.defaultMQPushConsumer.getConsumerGroup(), topic);
                }
            }

            if (null == cidAll) {
                log.warn("doRebalance, {}, get consumer id list failed",
                    this.defaultMQPushConsumer.getConsumerGroup());
            }

            if (mqSet != null && cidAll != null) {
                List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                mqAll.addAll(mqSet);

                // 排序
                Collections.sort(mqAll);
                Collections.sort(cidAll);

                AllocateMessageQueueStrategy strategy =
                        this.defaultMQPushConsumer.getAllocateMessageQueueStrategy();

                // 执行分配算法
                List<MessageQueue> allocateResult = null;
                try {
                    allocateResult = strategy.allocate(this.mQClientFactory.getClientId(), mqAll, cidAll);
                }
                catch (Throwable e) {
                    log.error("AllocateMessageQueueStrategy.allocate Exception", e);
                }

                Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                if (allocateResult != null) {
                    allocateResultSet.addAll(allocateResult);
                }

                // 更新本地队列
                this.updateProcessQueueTableInRebalance(topic, allocateResultSet);
            }
            break;
        }
        default:
            break;
        }
    }


    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDroped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}",
                        this.defaultMQPushConsumer.getConsumerGroup(), mq);
                }
            }
        }
    }


    @Override
    public void doRebalance() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic);
                }
                catch (Exception e) {
                    log.warn("rebalanceByTopic Exception", e);
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }


    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }


    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }


    private void copySubscription() throws MQClientException {
        try {
            // 复制用户初始设置的订阅关系
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.subscriptionInner.put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                break;
            case CLUSTERING:
                // 默认订阅消息重试Topic
                final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                SubscriptionData subscriptionData =
                        FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                this.subscriptionInner.put(retryTopic, subscriptionData);
                break;
            default:
                break;
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    private void checkConfig() throws MQClientException {
        // consumerGroup
        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException("consumerGroup is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumerGroup
        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException("consumerGroup can not equal "//
                    + MixAll.DEFAULT_CONSUMER_GROUP //
                    + ", please specify another one."//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // messageModel
        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException("messageModel is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeFromWhereOffset
        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException("consumeFromWhere is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException("allocateMessageQueueStrategy is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException("subscription is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException("messageListener is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently =
                this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently" //
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1 //
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000//
                || this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer
                    .getConsumeThreadMax()//
        ) {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1
                || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0
                || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException("pullInterval Out of range [0, 65535]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1
                || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException("pullBatchSize Out of range [1, 1024]" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                null);
        }
    }


    public boolean isPause() {
        return pause;
    }


    public void setPause(boolean pause) {
        this.pause = pause;
    }


    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }


    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }


    @Override
    public ConsumeFromWhere getConsumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }
}
