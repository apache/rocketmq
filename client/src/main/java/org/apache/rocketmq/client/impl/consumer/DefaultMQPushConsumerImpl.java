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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION = 3000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final Logger log = ClientLogger.getLog();
    /**
     * defaultMQPushConsumer
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**
     * 均衡器
     */
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private final RPCHook rpcHook;
    /**
     * 服务状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    /**
     * MQClient对象
     */
    private MQClientInstance mQClientFactory;
    /**
     * 消息拉取API封装
     */
    private PullAPIWrapper pullAPIWrapper;
    /**
     * 是否暂停
     */
    private volatile boolean pause = false;
    /**
     * 是否顺序消费
     */
    private boolean consumeOrderly = false;
    /**
     * 消息监听器
     */
    private MessageListener messageListenerInner;
    private OffsetStore offsetStore;
    /**
     * 消费消息服务
     */
    private ConsumeMessageService consumeMessageService;
    /**
     * this.defaultMQPushConsumer.getPullThresholdForQueue()
     */
    private long flowControlTimes1 = 0;
    /**
     *
     */
    private long flowControlTimes2 = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
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
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
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

    /**
     * 拉取消息
     *
     * @param pullRequest 拉取消息请求
     */
    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        // 设置队列最后拉取消息时间
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        // 判断consumer状态是否运行中。如果不是，则延迟拉取消息。
        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
            return;
        }

        // 判断是否暂停中。
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        // 判断是否超过最大持有消息数量。默认最大值为1000。
        long size = processQueue.getMsgCount().get();
        if (size > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL); // 提交延迟消息拉取请求。50ms。
            if ((flowControlTimes1++ % 1000) == 0) {
                log.warn(
                    "the consumer message buffer is full, so do flow control, minOffset={}, maxOffset={}, size={}, pullRequest={}, flowControlTimes={}",
                    processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), size, pullRequest, flowControlTimes1);
            }
            return;
        }

        if (!this.consumeOrderly) { // 判断消息跨度是否过大。
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL); // 提交延迟消息拉取请求。50ms。
                if ((flowControlTimes2++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, flowControlTimes2);
                }
                return;
            }
        } else { // TODO 顺序消费
            if (processQueue.isLocked()) {
                if (!pullRequest.isLockedFirst()) {
                    final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                        pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                            pullRequest, offset);
                    }

                    pullRequest.setLockedFirst(true);
                    pullRequest.setNextOffset(offset);
                }
            } else {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        // 获取Topic 对应的订阅信息。若不存在，则延迟拉取消息
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                        subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            // 设置下次拉取消息队列位置
                            long prevRequestOffset = pullRequest.getNextOffset();
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 统计
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                // 统计
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                // 提交拉取到的消息到消息处理队列
                                boolean dispathToConsume = processQueue.putMessage(pullResult.getMsgFoundList());

                                // 提交消费请求
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(//
                                    pullResult.getMsgFoundList(), //
                                    processQueue, //
                                    pullRequest.getMessageQueue(), //
                                    dispathToConsume);

                                // 提交下次拉取消息请求
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            // 下次拉取消费队列位置小于上次拉取消息队列位置 或者 第一条消息的消费队列位置小于上次拉取消息队列位置，则判定为BUG，输出log
                            if (pullResult.getNextBeginOffset() < prevRequestOffset//
                                || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}", //
                                    pullResult.getNextBeginOffset(), //
                                    firstMsgOffset, //
                                    prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                            // 设置下次拉取消息队列位置
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 持久化消费进度
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            // 立即提交拉取消息请求
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case NO_MATCHED_MSG:
                            // 设置下次拉取消息队列位置
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 持久化消费进度
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            // 提交立即拉取消息请求
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}", //
                                pullRequest.toString(), pullResult.toString());
                            // 设置下次拉取消息队列位置
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 设置消息处理队列为dropped
                            pullRequest.getProcessQueue().setDropped(true);

                            // 提交延迟任务，进行消费处理队列移除。 // TODO 疑问：为什么不立即移除
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        // 更新消费进度，同步消费进度到Broker
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                            pullRequest.getNextOffset(), false);
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        // 移除消费处理队列
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
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

                // 提交延迟拉取消息请求
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
            }
        };

        // 集群消息模型下，计算提交的消费进度。
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        // 计算请求的 订阅表达式 和 是否进行filtersrv过滤消息
        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        // 计算拉取消息系统标识
        int sysFlag = PullSysFlag.buildSysFlag(//
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
        );

        // 执行拉取。如果拉取请求发生异常时，提交延迟拉取消息请求。
        try {
            this.pullAPIWrapper.pullKernelImpl(//
                pullRequest.getMessageQueue(), // 1
                subExpression, // 2
                subscriptionData.getSubVersion(), // 3
                pullRequest.getNextOffset(), // 4
                this.defaultMQPushConsumer.getPullBatchSize(), // 5
                sysFlag, // 6
                commitOffsetValue, // 7
                BROKER_SUSPEND_MAX_TIME_MILLIS, // 8
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, // 9
                CommunicationMode.ASYNC, // 10
                pullCallback// 11
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION);
        }
    }

    /**
     * 校验Consumer是否处于运行{@link ServiceState#RUNNING}状态。
     *
     * @throws MQClientException 当不处于运行状态抛出client异常
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "//
                + this.serviceState//
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    /**
     * 当处理消息队列为0时，更新消费进度为拉取消息请求队列开始位置
     *
     * @param pullRequest 拉取消息请求
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
        InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * 注册消息监听器
     *
     * @param messageListener 消息监听器
     */
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**
     * 发回消息
     *
     * @param msg 消息
     * @param delayLevel 延迟级别
     * @param brokerName brokerName
     * @throws RemotingException 当远程调用发生异常
     * @throws MQBrokerException 当Broker发生异常
     * @throws InterruptedException 当线程中断
     * @throws MQClientException 当Client发生异常时
     */
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            // Consumer发回消息
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) { // TODO 疑问：什么情况下会发生异常
            // 异常时，使用Client内置Producer发回消息
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
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
                this.rebalanceImpl.destroy();
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
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                    this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                this.serviceState = ServiceState.START_FAILED;

                // 检查配置
                this.checkConfig();

                // 复制订阅数据
                this.copySubscription();

                // 设置instanceName
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                // 获取MQClient对象
                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                // 设置负载均衡器
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                // 拉取API封装
                this.pullAPIWrapper = new PullAPIWrapper(
                    mQClientFactory,
                    this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                // TODO 待读：store
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                }
                this.offsetStore.load();

                // TODO 待读：监听器
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }
                this.consumeMessageService.start();

                // 设置MQClient对象
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown();
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());

                // 设置服务状态
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        // 更新 Topic路由信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        // 通过心跳同步Consumer信息
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        // 重新均衡
        this.mQClientFactory.rebalanceImmediately();
    }

    /**
     * 校验配置
     * - 消费分组不为空
     * - 消费分布不能为MixAll.DEFAULT_CONSUMER_GROUP
     * - 消息模式不能为空
     * - 消费开启点不能为空
     * - 回溯时间不能为空 && 格式正确
     * - 消息分配策略不能为空
     * - 订阅关系不能为空
     * - 消息监听器不能为空 && 监听器类型为（并发或者顺序消息监听器中的一种）
     * - 消费线程数量：consumeThreadMin && consumeThreadMax 范围正常
     * - consumeConcurrentlyMaxSpan 范围正常
     * - 轮询间隔：pullInterval 范围正常
     * - pullThresholdForQueue不能为空
     * - 拉取批量数量：pullBatchSize 范围正常
     * @throws MQClientException 当校验不符合规则时
     */
    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) { // TODO 疑问：这个是为毛
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                "consumeFromWhere is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYY_MMDD_HHMMSS);
        if (null == dt) {
            throw new MQClientException(
                "consumeTimestamp is invalid, YYYY_MMDD_HHMMSS"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                "subscription is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                "messageListener is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
            || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000
            || this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                "consumeThreadMin Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                "consumeThreadMax Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
            || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                "pullThresholdForQueue Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                "pullInterval Out of range [0, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
            || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                "consumeMessageBatchMaxSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                "pullBatchSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    /**
     * 复制订阅关系
     * important！！！当集群消费时，加入当前消费分组重试消息的订阅
     * @throws MQClientException 当解析订阅数据失败时
     */
    private void copySubscription() throws MQClientException {
        try {
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) { // TODO 疑问：why？
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), //
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
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), //
                        retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    /**
     * 当 订阅信息 改变时，更新 Topic路由信息
     */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**
     * 订阅 Topic
     *
     * @param topic Topic
     * @param subExpression 订阅表达式
     * @throws MQClientException 当client发生异常
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            // 创建订阅数据
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), //
                topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            // 通过心跳同步Consumer信息到Broker
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), //
                topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
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

    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
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

    public void resetOffsetByTimeStamp(long timeStamp)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
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

    /**
     * 进行平衡
     */
    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
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

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }
}
