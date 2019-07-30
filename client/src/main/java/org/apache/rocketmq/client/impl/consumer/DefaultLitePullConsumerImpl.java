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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;

import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;

    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mQClientFactory;

    private PullAPIWrapper pullAPIWrapper;

    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private enum SubscriptionType {
        NONE, SUBSCRIBE, ASSIGN
    }

    private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running.";

    private static final String SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE = "Cannot select two subscription types at the same time.";
    /**
     * the type of subscription
     */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;
    /**
     * Delay some time when exception occur
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION = 1000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;

    private DefaultLitePullConsumer defaultLitePullConsumer;

    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;

    private long nextAutoCommitDeadline = -1L;

    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {

        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;

    }

    private void checkServiceState() {
        if (!(this.serviceState == ServiceState.RUNNING))
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE);
    }

    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(assignedMessageQueue);
        updatePullTask(topic, assignedMessageQueue);
    }

    private void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        startPullTask(mqNewSet);
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    updateAssignedMessageQueue(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    private int nextPullBatchNums() {
        return Math.min(this.defaultLitePullConsumer.getPullBatchNums(), consumeRequestCache.remainingCapacity());
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                scheduledThreadPoolExecutor.shutdown();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                this.copySubscription();

                if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultLitePullConsumer.changeInstanceNameToPID();
                }

                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);

                this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                this.pullAPIWrapper = new PullAPIWrapper(
                    mQClientFactory,
                    this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultLitePullConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
                } else {
                    switch (this.defaultLitePullConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
                }

                this.offsetStore.load();

                boolean registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;

                    throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                mQClientFactory.start();

                final String group = this.defaultLitePullConsumer.getConsumerGroup();

                this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
                    this.defaultLitePullConsumer.getPullThreadNumbers(),
                    new ThreadFactoryImpl("PullMsgThread-" + group)
                );
                if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                    updateTopicSubscribeInfoWhenSubscriptionChanged();
                }
                if (subscriptionType == SubscriptionType.ASSIGN) {
                    updateAssignPullTask(assignedMessageQueue.messageQueues());
                }

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
    }

    private void checkConfig() throws MQClientException {
        // check consumerGroup
        Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

        // consumerGroup
        if (null == this.defaultLitePullConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumerGroup
        if (this.defaultLitePullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // messageModel
        if (null == this.defaultLitePullConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // allocateMessageQueueStrategy
        if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    private void copySubscription() throws MQClientException {
        try {
            Set<String> registerTopics = this.defaultLitePullConsumer.getRegisterTopics();
            if (registerTopics != null) {
                for (final String topic : registerTopics) {
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultLitePullConsumer.getConsumerGroup(),
                        topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    private void startPullTask(Collection<MessageQueue> mqSet) {
        for (MessageQueue messageQueue : mqSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void updateAssignPullTask(Collection<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (!mqNewSet.contains(next.getKey())) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }

        startPullTask(mqNewSet);
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(),
                topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public synchronized void unsubscribe(final String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        //can be delete
        removePullTaskCallback(topic);
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        setSubscriptionType(SubscriptionType.ASSIGN);
        assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
        if (serviceState == ServiceState.RUNNING) {
            updateAssignPullTask(messageQueues);
        }
    }

    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitInterval() * 1000;
        }
    }

    public List<MessageExt> poll(long timeout) {
        try {
            checkServiceState();
            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;
            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if ((endTime - System.currentTimeMillis()) <= 0)
                    break;
            }
            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                return messages;
            }
        } catch (InterruptedException ignore) {

        }
        return null;
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.pause(messageQueues);
    }

    public void resume(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.resume(messageQueues);
    }

    public synchronized void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        if (offset < minOffset(messageQueue) || offset > maxOffset(messageQueue))
            throw new MQClientException("Seek offset illegal", null);
        try {
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            updateConsumeOffset(messageQueue, offset);
            updateConsumeOffsetToBroker(messageQueue, offset, false);
        } catch (Exception e) {
            log.error("Seek offset failed.", e);
        }
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public void removePullTaskCallback(final String topic) {
        removePullTask(topic);
    }

    public void removePullTask(final String topic) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
    }

    public synchronized void commitSync() {
        try {
            for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
                long consumerOffset = assignedMessageQueue.getConusmerOffset(messageQueue);
                if (consumerOffset != -1) {
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    long preConsumerOffset = this.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
                    if (processQueue != null && !processQueue.isDropped() && consumerOffset != preConsumerOffset) {
                        updateConsumeOffset(messageQueue, consumerOffset);
                        updateConsumeOffsetToBroker(messageQueue, consumerOffset, false);
                    }
                }
            }
        } catch (Exception e) {
            log.error("An error occurred when update consume offset synchronously.", e);
        }
    }

    public synchronized void commitAll() {
        try {
            for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
                long consumerOffset = assignedMessageQueue.getConusmerOffset(messageQueue);
                if (consumerOffset != -1) {
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    long preConsumerOffset = this.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
                    if (processQueue != null && !processQueue.isDropped() && consumerOffset != preConsumerOffset) {
                        updateConsumeOffset(messageQueue, consumerOffset);
                        updateConsumeOffsetToBroker(messageQueue, consumerOffset, true);
                    }
                }
            }
        } catch (Exception e) {
            log.error("An error occurred when update consume offset Automatically.");
        }
    }

    private void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        if (assignedMessageQueue.getSeekOffset(remoteQueue) == -1) {
            assignedMessageQueue.updateNextOffset(remoteQueue, nextPullOffset);
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException ex) {
            log.error("Submit consumeRequest error", ex);
        }
    }

    private long fetchConsumeOffset(MessageQueue mq, boolean fromStore) {
        checkServiceState();
        return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }

    private long nextPullOffset(MessageQueue remoteQueue) {
        long offset = -1;
        long seekOffset = assignedMessageQueue.getSeekOffset(remoteQueue);
        if (seekOffset != -1) {
            offset = seekOffset;
            assignedMessageQueue.setSeekOffset(remoteQueue, -1);
            assignedMessageQueue.updateNextOffset(remoteQueue,offset);
        } else {
            offset = assignedMessageQueue.getNextOffset(remoteQueue);
            if (offset == -1) {
                offset = fetchConsumeOffset(remoteQueue, false);
                assignedMessageQueue.updateNextOffset(remoteQueue, offset);
                assignedMessageQueue.updateConsumeOffset(remoteQueue, offset);
            }
        }

        return offset;
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);

            if (processQueue == null && processQueue.isDropped()) {
                log.info("the message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                return;
            }

            if (consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchNums() > defaultLitePullConsumer.getPullThresholdForAll()) {
                scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                if ((consumeRequestFlowControlTimes++ % 1000) == 0)
                    log.warn("the consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                return;
            }

            long cachedMessageCount = processQueue.getMsgCount().get();
            long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

            if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                if ((queueFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                        defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                }
                return;
            }

            if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                if ((queueFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                        defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                }
                return;
            }

            if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                }
                return;
            }

            if (!this.isCancelled()) {
                if (assignedMessageQueue.isPaused(messageQueue)) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }
                String subExpression = null;
                if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                    String topic = this.messageQueue.getTopic();
                    subExpression = rebalanceImpl.getSubscriptionInner().get(topic).getSubString();
                }
                long offset = nextPullOffset(messageQueue);
                long pullDelayTimeMills = 0;
                try {
                    PullResult pullResult = pull(messageQueue, subExpression, offset, nextPullBatchNums());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            processQueue.putMessage(pullResult.getMsgFoundList());
                            submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                            pullDelayTimeMills = 0;
                            break;
                        case NO_NEW_MSG:
                            pullDelayTimeMills = 100;
                        case OFFSET_ILLEGAL:
                            //TODO
                            log.warn("the pull request offset illegal, {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                } catch (Throwable e) {
                    pullDelayTimeMills = PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION;
                    e.printStackTrace();
                    log.error("An error occurred in pull message process.", e);
                }

                if (!this.isCancelled()) {
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                }
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    private PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subExpression, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, subExpression);
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
    }

    private PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, messageSelector, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        SubscriptionData subscriptionData = getSubscriptionData(mq, messageSelector);
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, false, timeout);
    }

    private SubscriptionData getSubscriptionData(MessageQueue mq, String subExpression)
        throws MQClientException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        try {
            return FilterAPI.buildSubscriptionData(this.defaultLitePullConsumer.getConsumerGroup(),
                mq.getTopic(), subExpression);
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }
    }

    private SubscriptionData getSubscriptionData(MessageQueue mq, MessageSelector messageSelector)
        throws MQClientException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        try {
            return FilterAPI.build(mq.getTopic(),
                messageSelector.getExpression(), messageSelector.getExpressionType());
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }
    }

    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
        boolean block,
        long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
            mq,
            subscriptionData.getSubString(),
            subscriptionData.getExpressionType(),
            isTagType ? 0L : subscriptionData.getSubVersion(),
            offset,
            maxNums,
            sysFlag,
            0,
            this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
            timeoutMillis,
            CommunicationMode.SYNC,
            null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        //If namespace not null , reset Topic without namespace.
        this.resetTopic(pullResult.getMsgFoundList());
        if (!this.consumeMessageHookList.isEmpty()) {
            ConsumeMessageContext consumeMessageContext = null;
            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setNamespace(defaultLitePullConsumer.getNamespace());
            consumeMessageContext.setConsumerGroup(this.groupName());
            consumeMessageContext.setMq(mq);
            consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
            consumeMessageContext.setSuccess(false);
            this.executeHookBefore(consumeMessageContext);
            consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            consumeMessageContext.setSuccess(true);
            this.executeHookAfter(consumeMessageContext);
        }
        return pullResult;
    }

    private void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        //If namespace not null , reset Topic without namespace.
        for (MessageExt messageExt : msgList) {
            if (null != this.defaultLitePullConsumer.getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
            }
        }

    }

    public void subscriptionAutomatically(final String topic) {
        if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
            try {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultLitePullConsumer.getConsumerGroup(),
                    topic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
            } catch (Exception ignore) {
            }
        }
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        checkServiceState();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> result = new HashSet<SubscriptionData>();

        Set<String> topics = this.defaultLitePullConsumer.getRegisterTopics();
        if (topics != null) {
            synchronized (topics) {
                for (String t : topics) {
                    SubscriptionData ms = null;
                    try {
                        ms = FilterAPI.buildSubscriptionData(this.groupName(), t, SubscriptionData.SUB_ALL);
                    } catch (Exception e) {
                        log.error("parse subscription error", e);
                    }
                    ms.setSubVersion(0L);
                    result.add(ms);
                }
            }
        }

        return result;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            checkServiceState();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultLitePullConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, this.defaultLitePullConsumer.getConsumerGroup());
    }

    private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            if (UtilAll.isBlank(consumerGroup)) {
                consumerGroup = this.defaultLitePullConsumer.getConsumerGroup();
            }

            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup, delayLevel, 3000,
                this.defaultLitePullConsumer.getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultLitePullConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultLitePullConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(this.defaultLitePullConsumer.getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultLitePullConsumer.getNamespace()));
        }
    }

    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        // check if has info in memory, otherwise invoke api.
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        }

        return parseMessageQueues(result);
    }

    private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue messageQueue : queueSet) {
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                this.defaultLitePullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    public class ConsumeRequest {
        private final List<MessageExt> messageExts;
        private final MessageQueue messageQueue;
        private final ProcessQueue processQueue;
        private long startConsumeTimeMillis;

        public ConsumeRequest(final List<MessageExt> messageExts, final MessageQueue messageQueue,
            final ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public long getStartConsumeTimeMillis() {
            return startConsumeTimeMillis;
        }

        public void setStartConsumeTimeMillis(final long startConsumeTimeMillis) {
            this.startConsumeTimeMillis = startConsumeTimeMillis;
        }
    }
}
