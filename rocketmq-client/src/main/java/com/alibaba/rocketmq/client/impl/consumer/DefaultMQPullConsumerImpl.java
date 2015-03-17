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

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.Validators;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class DefaultMQPullConsumerImpl implements MQConsumerInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private OffsetStore offsetStore;
    private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;


    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer, final RPCHook rpcHook) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.rpcHook = rpcHook;
    }


    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException {
        this.makeSureStateOK();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }


    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        this.makeSureStateOK();
        return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE
                : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }


    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        this.makeSureStateOK();
        if (null == topic) {
            throw new IllegalArgumentException("topic is null");
        }

        ConcurrentHashMap<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
        Set<MessageQueue> mqResult = new HashSet<MessageQueue>();
        for (MessageQueue mq : mqTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                mqResult.add(mq);
            }
        }

        return mqResult;
    }


    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }


    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
    }


    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    @Override
    public String groupName() {
        return this.defaultMQPullConsumer.getConsumerGroup();
    }


    @Override
    public MessageModel messageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
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

        Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
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
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPullConsumer.getConsumerGroup()
                    + " persistConsumerOffset exception", e);
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
        return this.defaultMQPullConsumer.isUnitMode();
    }


    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }


    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }


    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subExpression, offset, maxNums,
                this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }


    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, false, timeout);
    }


    private PullResult pullSyncImpl(MessageQueue mq, String subExpression, long offset, int maxNums,
                                    boolean block, long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        this.makeSureStateOK();

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

        SubscriptionData subscriptionData;
        try {
            subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),//
                    mq.getTopic(), subExpression);
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }

        long timeoutMillis =
                block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(//
                mq, // 1
                subscriptionData.getSubString(), // 2
                0L, // 3
                offset, // 4
                maxNums, // 5
                sysFlag, // 6
                0, // 7
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), // 8
                timeoutMillis, // 9
                CommunicationMode.SYNC, // 10
                null// 11
        );

        return this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
    }


    private void subscriptionAutomatically(final String topic) {
        if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
            try {
                SubscriptionData subscriptionData =
                        FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),//
                                topic, SubscriptionData.SUB_ALL);
                this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
            } catch (Exception e) {
            }
        }
    }


    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums,
                     PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        pull(mq, subExpression, offset, maxNums, pullCallback,
                this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }


    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums,
                     PullCallback pullCallback, long timeout) throws MQClientException, RemotingException,
            InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, false, timeout);
    }


    private void pullAsyncImpl(//
                               final MessageQueue mq,//
                               final String subExpression,//
                               final long offset,//
                               final int maxNums,//
                               final PullCallback pullCallback,//
                               final boolean block,//
                               final long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        if (null == pullCallback) {
            throw new MQClientException("pullCallback is null", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        try {
            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

            final SubscriptionData subscriptionData;
            try {
                subscriptionData =
                        FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),//
                                mq.getTopic(), subExpression);
            } catch (Exception e) {
                throw new MQClientException("parse subscription error", e);
            }

            long timeoutMillis =
                    block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

            this.pullAPIWrapper.pullKernelImpl(//
                    mq, // 1
                    subscriptionData.getSubString(), // 2
                    0L, // 3
                    offset, // 4
                    maxNums, // 5
                    sysFlag, // 6
                    0, // 7
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), // 8
                    timeoutMillis, // 9
                    CommunicationMode.ASYNC, // 10
                    new PullCallback() {

                        @Override
                        public void onException(Throwable e) {
                            pullCallback.onException(e);
                        }


                        @Override
                        public void onSuccess(PullResult pullResult) {
                            pullCallback.onSuccess(DefaultMQPullConsumerImpl.this.pullAPIWrapper
                                    .processPullResult(mq, pullResult, subscriptionData));
                        }
                    });
        } catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }


    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, true, this.getDefaultMQPullConsumer()
                .getConsumerPullTimeoutMillis());
    }


    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
                                    PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, true, this
                .getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumerGroup());
    }


    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr =
                    (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                            : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            if (UtilAll.isBlank(consumerGroup)) {
                consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();
            }

            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup,
                    delayLevel, 3000);
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPullConsumer.getConsumerGroup(), e);

            Message newMsg =
                    new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumerGroup()),
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
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
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

                this.copySubscription();

                if (this.defaultMQPullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPullConsumer.changeInstanceNameToPID();
                }

                this.mQClientFactory =
                        MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPullConsumer,
                                this.rpcHook);

                this.rebalanceImpl.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPullConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPullConsumer
                        .getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                this.pullAPIWrapper = new PullAPIWrapper(//
                        mQClientFactory,//
                        this.defaultMQPullConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPullConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPullConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPullConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore =
                                    new LocalFileOffsetStore(this.mQClientFactory,
                                            this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore =
                                    new RemoteBrokerOffsetStore(this.mQClientFactory,
                                            this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                }

                this.offsetStore.load();

                boolean registerOK =
                        mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;

                    throw new MQClientException("The consumer group["
                            + this.defaultMQPullConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please."
                            + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "//
                        + this.serviceState//
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
            default:
                break;
        }
    }


    private void copySubscription() throws MQClientException {
        try {
            Set<String> registerTopics = this.defaultMQPullConsumer.getRegisterTopics();
            if (registerTopics != null) {
                for (final String topic : registerTopics) {
                    SubscriptionData subscriptionData =
                            FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(),//
                                    topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }


    private void checkConfig() throws MQClientException {
        // check consumerGroup
        Validators.checkGroup(this.defaultMQPullConsumer.getConsumerGroup());

        // consumerGroup
        if (null == this.defaultMQPullConsumer.getConsumerGroup()) {
            throw new MQClientException("consumerGroup is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // consumerGroup
        if (this.defaultMQPullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException("consumerGroup can not equal "//
                    + MixAll.DEFAULT_CONSUMER_GROUP //
                    + ", please specify another one."//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // messageModel
        if (null == this.defaultMQPullConsumer.getMessageModel()) {
            throw new MQClientException("messageModel is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException("allocateMessageQueueStrategy is null" //
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.makeSureStateOK();
        this.offsetStore.updateOffset(mq, offset, false);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();


    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }


    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }


    public OffsetStore getOffsetStore() {
        return offsetStore;
    }


    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }


    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }


    public void setPullAPIWrapper(PullAPIWrapper pullAPIWrapper) {
        this.pullAPIWrapper = pullAPIWrapper;
    }


    public ServiceState getServiceState() {
        return serviceState;
    }


    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }


    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, this.consumerStartTimestamp);
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }


    public long getConsumerStartTimestamp() {
        return consumerStartTimestamp;
    }


    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}
