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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;

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
    /**
     * Delay some time when no new message
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_NO_NEW_MSG = 0;

    private DefaultLitePullConsumer defaultLitePullConsumer;

    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private final ScheduledExecutorService scheduledExecutorService;

    private Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<String, TopicMessageQueueChangeListener>();

    private Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<String, Set<MessageQueue>>();

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;

    private long nextAutoCommitDeadline = -1L;

    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            this.defaultLitePullConsumer.getPullThreadNumbers(),
            new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
        );
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MonitorMessageQueueChangeThread");
            }
        });
    }

    private void checkServiceState() {
        if (this.serviceState != ServiceState.RUNNING)
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE);
    }

    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
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
                    updatePullTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    updatePullTask(topic, mqDivided);
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

                if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                    updateTopicSubscribeInfoWhenSubscriptionChanged();
                }
                if (subscriptionType == SubscriptionType.ASSIGN) {
                    updateAssignPullTask(assignedMessageQueue.messageQueues());
                }

                scheduledExecutorService.scheduleAtFixedRate(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                fetchTopicMessageQueuesAndCompare();
                            } catch (Exception e) {
                                log.error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
                            }
                        }
                    }, 1000 * 20, 1000 * 30, TimeUnit.MILLISECONDS);

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
                    Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
                    messageQueuesForTopic.put(topic, messageQueues);
                }
                this.mQClientFactory.checkClientInBroker();
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

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    private void copySubscription() throws MQClientException {
        try {
            switch (this.defaultLitePullConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    /*
                     * Retry topic will be support in the future.
                     */
                    break;
                default:
                    break;
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
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        try {
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void unsubscribe(final String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        removePullTaskCallback(topic);
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.isEmpty()) {
            throw new IllegalArgumentException("Message queues can not be null or empty.");
        }
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
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;

            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            if (endTime - System.currentTimeMillis() > 0) {
                while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (endTime - System.currentTimeMillis() <= 0)
                        break;
                }
            }

            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //If namespace not null , reset Topic without namespace.
                this.resetTopic(messages);
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
        if (!assignedMessageQueue.messageQueues().contains(messageQueue)) {
            if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
            } else {
                throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
            }
        }
        long minOffset = minOffset(messageQueue);
        long maxOffset = maxOffset(messageQueue);
        if (offset < minOffset || offset > maxOffset)
            throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
        try {
            assignedMessageQueue.pause(Collections.singletonList(messageQueue));
            CountDownLatch2 pausedLatch = assignedMessageQueue.getPausedLatch(messageQueue);
            if (pausedLatch != null) {
                pausedLatch.await(2, TimeUnit.SECONDS);
            }
            ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
            if (processQueue != null) {
                processQueue.clear();
            }
            Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
            while (iter.hasNext()) {
                if (iter.next().getMessageQueue().equals(messageQueue))
                    iter.remove();
            }
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
        } catch (Exception e) {
            log.error("Seek offset failed.", e);
        } finally {
            assignedMessageQueue.resume(Collections.singletonList(messageQueue));
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
            if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING)
                offsetStore.persistAll(assignedMessageQueue.messageQueues());
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
            if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING)
                offsetStore.persistAll(assignedMessageQueue.messageQueues());
        } catch (Exception e) {
            log.error("An error occurred when update consume offset Automatically.");
        }
    }

    private void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        if (assignedMessageQueue.getSeekOffset(remoteQueue) == -1) {
            assignedMessageQueue.updatePullOffset(remoteQueue, nextPullOffset);
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException e) {
            log.error("Submit consumeRequest error", e);
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
            assignedMessageQueue.updatePullOffset(remoteQueue, offset);
        } else {
            offset = assignedMessageQueue.getPullOffset(remoteQueue);
            if (offset == -1) {
                offset = fetchConsumeOffset(remoteQueue, false);
                if (offset == -1 && defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING) {
                    offset = 0;
                }
                assignedMessageQueue.updatePullOffset(remoteQueue, offset);
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

            if (!this.isCancelled()) {

                if (assignedMessageQueue.isPaused(messageQueue)) {
                    CountDownLatch2 pasuedLatch = assignedMessageQueue.getPausedLatch(messageQueue);
                    if (pasuedLatch != null)
                        pasuedLatch.countDown();
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }

                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);

                if (processQueue == null && processQueue.isDropped()) {
                    log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                    return;
                }

                if (consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchNums() > defaultLitePullConsumer.getPullThresholdForAll()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((consumeRequestFlowControlTimes++ % 1000) == 0)
                        log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                    return;
                }

                long cachedMessageCount = processQueue.getMsgCount().get();
                long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

                if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                    }
                    return;
                }

                long offset = nextPullOffset(messageQueue);
                long pullDelayTimeMills = defaultLitePullConsumer.getPullDelayTimeMills();
                try {

                    SubscriptionData subscriptionData;
                    if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    } else{
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(),
                            topic, SubscriptionData.SUB_ALL);
                    }

                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, nextPullBatchNums());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty()) {
                                processQueue.putMessage(pullResult.getMsgFoundList());
                                submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("The pull request offset illegal, {}", pullResult.toString());
                            break;
                        case NO_NEW_MSG:
                            pullDelayTimeMills = PULL_TIME_DELAY_MILLS_WHEN_NO_NEW_MSG;
                            break;
                        default:
                            break;
                    }
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                } catch (Throwable e) {
                    pullDelayTimeMills = PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION;
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

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
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
        return pullResult;
    }

    private void resetTopic(List<MessageExt> msgList) {
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
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
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

    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        Set<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        return parseMessageQueues(result);
    }

    private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
            String topic = entry.getKey();
            TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();
            Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
            Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
            boolean isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
            if (isChanged) {
                messageQueuesForTopic.put(topic, newMessageQueues);
                if (topicMessageQueueChangeListener != null) {
                    topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                }
            }
        }
    }

    private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
        if (set1 == null && set2 == null) {
            return true;
        }

        if (set1 == null || set2 == null || set1.size() != set2.size()
            || set1.size() == 0 || set2.size() == 0) {
            return false;
        }

        Iterator iter = set2.iterator();
        boolean isEqual = true;
        while (iter.hasNext()) {
            if (!set1.contains(iter.next())) {
                isEqual = false;
            }
        }
        return isEqual;
    }

    public synchronized void registerTopicMessageQueueChangeListener(String topic,
        TopicMessageQueueChangeListener listener) throws MQClientException {
        if (topic == null || listener == null) {
            throw new MQClientException("Topic or listener is null", null);
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        if (this.serviceState == ServiceState.RUNNING) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
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

    }
}
