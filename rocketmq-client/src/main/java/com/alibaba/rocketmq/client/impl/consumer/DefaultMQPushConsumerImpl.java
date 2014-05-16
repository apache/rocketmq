/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.Validators;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.hook.ConsumeMessageHook;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.stat.ConsumerStatManager;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Push方式的Consumer实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-15
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    // 拉消息异常时，延迟一段时间再拉
    private static final long PullTimeDelayMillsWhenException = 3000;
    // 本地内存队列慢，流控间隔时间
    private static final long PullTimeDelayMillsWhenFlowControl = 50;
    // 被挂起后，下次拉取间隔时间
    private static final long PullTimeDelayMillsWhenSuspend = 1000;
    // 长轮询模式，Consumer连接在Broker挂起最长时间
    private static final long BrokerSuspendMaxTimeMillis = 1000 * 15;
    // 长轮询模式，Consumer超时时间（必须要大于brokerSuspendMaxTimeMillis）
    private static final long ConsumerTimeoutMillisWhenSuspend = 1000 * 30;
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    // Rebalance实现
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ConsumerStatManager consumerStatManager = new ConsumerStatManager();
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    // 是否暂停接收消息 suspend/resume
    private volatile boolean pause = false;
    // 是否顺序消费消息
    private boolean consumeOrderly = false;
    // 消费消息监听器
    private MessageListener messageListenerInner;
    // 消费进度存储
    private OffsetStore offsetStore;
    // 消费消息服务
    private ConsumeMessageService consumeMessageService;

    // 消息过滤 hook
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();


    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    /**
     * 消费每条消息会回调
     */
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
    }


    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }


    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum);
    }


    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return result;
    }


    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }


    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }


    public OffsetStore getOffsetStore() {
        return offsetStore;
    }


    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }


    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }


    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }


    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }


    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }


    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance();
        }
    }


    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
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


    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }


    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }


    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }


    /**
     * 通过Tag过滤时，会存在offset不准确的情况，需要纠正
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        // 说明本地没有可消费的消息
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    private long flowControlTimes1 = 0;
    private long flowControlTimes2 = 0;


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
            log.warn("consumer was paused, execute pull request later. instanceName={}",
                this.defaultMQPushConsumer.getInstanceName());
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenSuspend);
            return;
        }

        // 流量控制，队列中消息总数
        long size = processQueue.getMsgCount().get();
        if (size > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
            if ((flowControlTimes1++ % 1000) == 0) {
                log.warn("the consumer message buffer is full, so do flow control, {} {} {}", size,
                    pullRequest, flowControlTimes1);
            }
            return;
        }

        // 流量控制，队列中消息最大跨度
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
                if ((flowControlTimes2++ % 1000) == 0) {
                    log.warn("the queue's messages, span too long, so do flow control, {} {} {}",
                        processQueue.getMaxSpan(), pullRequest, flowControlTimes2);
                }
                return;
            }
        }

        // 查询订阅关系
        final SubscriptionData subscriptionData =
                this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            // 由于并发关系，即使找不到订阅关系，也要重试下，防止丢失PullRequest
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult =
                            DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(
                                pullRequest.getMessageQueue(), pullResult, subscriptionData);

                    switch (pullResult.getPullStatus()) {
                    case FOUND:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        DefaultMQPushConsumerImpl.this.getConsumerStatManager().getConsumertat()
                            .getPullTimesTotal().incrementAndGet();
                        DefaultMQPushConsumerImpl.this.getConsumerStatManager().getConsumertat()
                            .getPullRTTotal().addAndGet(pullRT);

                        boolean dispathToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                        DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(//
                            pullResult.getMsgFoundList(), //
                            processQueue, //
                            pullRequest.getMessageQueue(), //
                            dispathToConsume);

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
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

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

                        // 第一步、缓存队列里的消息全部废弃
                        pullRequest.getProcessQueue().setDroped(true);
                        // 第二步、等待10s后再执行，防止Offset更新后又被覆盖
                        DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    // 第三步、纠正内部Offset
                                    DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(
                                        pullRequest.getMessageQueue(), pullRequest.getNextOffset(), false);

                                    // 第四步、将最新的Offset更新到服务器
                                    DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest
                                        .getMessageQueue());

                                    // 第五步、丢弃当前PullRequest，并且从Rebalabce结果里删除，等待下次Rebalance时，取纠正后的Offset
                                    DefaultMQPushConsumerImpl.this.rebalanceImpl
                                        .removeProcessQueue(pullRequest.getMessageQueue());

                                    log.warn("fix the pull request offset, {}", pullRequest);
                                }
                                catch (Throwable e) {
                                    log.error("executeTaskLater Exception", e);
                                }
                            }
                        }, 10000);
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

        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue =
                    this.offsetStore.readOffset(pullRequest.getMessageQueue(),
                        ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd =
                this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        int sysFlag = PullSysFlag.buildSysFlag(//
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null,// subscription
            classFilter // class filter
            );
        try {
            this.pullAPIWrapper.pullKernelImpl(//
                pullRequest.getMessageQueue(), // 1
                subExpression, // 2
                subscriptionData.getSubVersion(), // 3
                pullRequest.getNextOffset(), // 4
                this.defaultMQPushConsumer.getPullBatchSize(), // 5
                sysFlag, // 6
                commitOffsetValue,// 7
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


    /**
     * 立刻执行这个PullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }


    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }


    /**
     * 稍后再执行这个PullRequest
     */
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }


    public boolean isPause() {
        return pause;
    }


    public void setPause(boolean pause) {
        this.pause = pause;
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }


    public ConsumerStatManager getConsumerStatManager() {
        return consumerStatManager;
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }


    public void resume() {
        this.pause = false;
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        try {
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(msg,
                this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 3000);
        }
        catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            Message newMsg =
                    new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()),
                        msg.getBody());

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }


    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.consumeMessageService.shutdown();
            this.persistConsumerOffset();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.START_FAILED;

            this.checkConfig();

            // 复制订阅关系
            this.copySubscription();

            if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                this.defaultMQPushConsumer.changeInstanceNameToPID();
            }

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer);

            // 初始化Rebalance变量
            this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
            this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
            this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer
                .getAllocateMessageQueueStrategy());
            this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
            // 每次拉消息之后，都会进行一次过滤。
            this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

            if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
            }
            else {
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
                        + "] has been created before, specify another name please."
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mQClientFactory.start();
            log.info("the consumer [{}] start OK", this.defaultMQPushConsumer.getConsumerGroup());
            this.serviceState = ServiceState.RUNNING;
            break;
        case RUNNING:
        case START_FAILED:
        case SHUTDOWN_ALREADY:
            throw new MQClientException("The PushConsumer service state not OK, maybe started once, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        default:
            break;
        }

        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        this.mQClientFactory.rebalanceImmediately();
    }


    private void checkConfig() throws MQClientException {
        // consumerGroup 有效性检查
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

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

        // 校验回溯时间戳格式是否正确
        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.yyyyMMddHHmmss);
        if (null == dt) {
            throw new MQClientException("consumeTimestamp is invalid, yyyyMMddHHmmss" //
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


    private void copySubscription() throws MQClientException {
        try {
            // 复制用户初始设置的订阅关系
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData =
                            FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                                topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
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
                        FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                            retryTopic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                break;
            default:
                break;
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
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


    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData =
                    FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(),//
                        topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            // 发送心跳，将变更的订阅关系注册上去
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        }
        catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }


    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }


    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }


    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }


    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }


    public void resetOffsetByTimeStamp(long timeStamp) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            for (MessageQueue mq : mqs) {
                long offset = searchOffset(mq, timeStamp);
                offsetTable.put(mq, offset);
            }
            this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
        }
    }


    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }


    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }
}
