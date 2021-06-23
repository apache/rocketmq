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

import com.alibaba.fastjson.JSON;
import com.google.common.base.Objects;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQRedirectException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Optional.fromNullable;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.nanoTime());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        final PullResultExt pullResultExt = (PullResultExt) pullResult;

        LogicalQueueRouteData queueRouteData = null;
        PullResultWithLogicalQueues pullResultWithLogicalQueues = null;
        if (pullResultExt instanceof PullResultWithLogicalQueues) {
            pullResultWithLogicalQueues = (PullResultWithLogicalQueues) pullResultExt;
            queueRouteData = pullResultWithLogicalQueues.getQueueRouteData();
        }

        if (queueRouteData != null) {
            pullResultWithLogicalQueues.setOrigPullResultExt(new PullResultExt(pullResultExt.getPullStatus(),
                queueRouteData.toLogicalQueueOffset(pullResultExt.getNextBeginOffset()),
                queueRouteData.toLogicalQueueOffset(pullResultExt.getMinOffset()),
                // although this maxOffset may not belong to this queue route, but the actual value must be a larger one, and since maxOffset here is not an accurate value, we just do it to make things simple.
                queueRouteData.toLogicalQueueOffset(pullResultExt.getMaxOffset()),
                pullResultExt.getMsgFoundList(),
                pullResultExt.getSuggestWhichBrokerId(),
                pullResultExt.getMessageBinary()));
        }

        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            if (queueRouteData != null) {
                // prevent pulled data is out of current queue route, this happens when some commit log data is cleaned in the broker but still pull from it.
                msgList = queueRouteData.filterMessages(msgList);
            }

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
                msg.setQueueId(mq.getQueueId());
                if (queueRouteData != null) {
                    msg.setQueueOffset(queueRouteData.toLogicalQueueOffset(msg.getQueueOffset()));
                }
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResultExt;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    public PullResult pullKernelImpl(
        MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        long offset,
        final int maxNums,
        final int sysFlag,
        long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        if (MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME.equals(mq.getBrokerName())) {
            LogicalQueueContext logicalQueueContext = new LogicalQueueContext(mq, subExpression, expressionType, subVersion, offset, maxNums, sysFlag, commitOffset, brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, pullCallback);
            while (true) {
                try {
                    MessageQueue messageQueue = logicalQueueContext.getModifiedMessageQueue();
                    if (messageQueue == null) {
                        if (pullCallback != null) {
                            pullCallback.onSuccess(logicalQueueContext.getPullResult());
                            return null;
                        } else {
                            return logicalQueueContext.getPullResult();
                        }
                    }
                    PullResult pullResult = this.pullKernelImplWithoutRetry(messageQueue, subExpression, expressionType, subVersion, logicalQueueContext.getModifiedOffset(), maxNums, sysFlag, logicalQueueContext.getModifiedCommitOffset(), brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, logicalQueueContext.wrapPullCallback());
                    return logicalQueueContext.wrapPullResult(pullResult);
                } catch (MQRedirectException e) {
                    if (!logicalQueueContext.shouldRetry(e)) {
                        throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, "redirect");
                    }
                }
            }
        } else {
            return this.pullKernelImplWithoutRetry(mq, subExpression, expressionType, subVersion, offset, maxNums, sysFlag, commitOffset, brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, pullCallback);
        }
    }

    public PullResult pullKernelImplWithoutRetry(
        MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        long offset,
        final int maxNums,
        final int sysFlag,
        long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();

        FindBrokerResult findBrokerResult =
            this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(queueId);
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computePullFromWhichFilterServer(topic, brokerAddr);
            }

            return this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }


    /**
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
    //     * @param expressionType
    //     * @param expression
     * @param order
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                         long timeout, PopCallback popCallback, boolean poll, int initMode, boolean order, String expressionType, String expression)
        throws MQClientException, RemotingException, InterruptedException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setInitMode(initMode);
            requestHeader.setExpType(expressionType);
            requestHeader.setExp(expression);
            requestHeader.setOrder(order);
            //give 1000 ms for server response
            if (poll) {
                requestHeader.setPollTime(timeout);
                requestHeader.setBornTime(System.currentTimeMillis());
                // timeout + 10s, fix the too earlier timeout of client when long polling.
                timeout += 10 * 1000;
            }
            String brokerAddr = findBrokerResult.getBrokerAddr();
            this.mQClientFactory.getMQClientAPIImpl().popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, popCallback);
            return;
        }
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    private class LogicalQueueContext implements PullCallback {
        private final MessageQueue mq;
        private final String subExpression;
        private final String expressionType;
        private final long subVersion;
        private final long offset;
        private final int maxNums;
        private final int sysFlag;
        private final long commitOffset;
        private final long brokerSuspendMaxTimeMillis;
        private final long timeoutMillis;
        private final CommunicationMode communicationMode;
        private final PullCallback pullCallback;

        private volatile LogicalQueuesInfo logicalQueuesInfo;
        private volatile LogicalQueueRouteData logicalQueueRouteData;
        private volatile boolean isMaxReadableQueueRoute;

        private volatile PullResultExt pullResult = null;

        private final AtomicInteger retry = new AtomicInteger();

        public LogicalQueueContext(MessageQueue mq, String subExpression, String expressionType, long subVersion,
            long offset, int maxNums, int sysFlag, long commitOffset, long brokerSuspendMaxTimeMillis,
            long timeoutMillis, CommunicationMode communicationMode,
            PullCallback pullCallback) {
            this.mq = mq;
            this.subExpression = subExpression;
            this.expressionType = expressionType;
            this.subVersion = subVersion;
            this.offset = offset;
            this.maxNums = maxNums;
            this.sysFlag = sysFlag;
            this.commitOffset = commitOffset;
            this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
            this.timeoutMillis = timeoutMillis;
            this.communicationMode = communicationMode;
            this.pullCallback = pullCallback;

            this.buildLogicalQueuesInfo();
        }

        private boolean notUsingLogicalQueue() {
            return !Objects.equal(mq.getBrokerName(), MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME) || this.logicalQueuesInfo == null;
        }

        private void buildLogicalQueuesInfo() {
            TopicRouteData topicRouteData = PullAPIWrapper.this.mQClientFactory.queryTopicRouteData(mq.getTopic());
            if (topicRouteData != null) {
                this.logicalQueuesInfo = topicRouteData.getLogicalQueuesInfo();
            }
        }

        @Override public void onSuccess(PullResult pullResult) {
            this.pullCallback.onSuccess(this.wrapPullResult(pullResult));
        }

        @Override public void onException(Throwable t) {
            if (!this.shouldRetry(t)) {
                this.pullCallback.onException(t);
                return;
            }
            MessageQueue messageQueue = this.getModifiedMessageQueue();
            if (messageQueue == null) {
                this.pullCallback.onSuccess(this.getPullResult());
                return;
            }
            try {
                PullAPIWrapper.this.pullKernelImplWithoutRetry(messageQueue, subExpression, expressionType, subVersion, this.getModifiedOffset(), maxNums, sysFlag, this.getModifiedCommitOffset(), brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, this);
            } catch (Exception e) {
                this.pullCallback.onException(e);
            }
        }

        public MessageQueue getModifiedMessageQueue() {
            if (this.notUsingLogicalQueue()) {
                return this.mq;
            }
            this.logicalQueuesInfo.readLock().lock();
            try {
                List<LogicalQueueRouteData> queueRouteDataList = fromNullable(this.logicalQueuesInfo.get(this.mq.getQueueId())).or(Collections.<LogicalQueueRouteData>emptyList());
                LogicalQueueRouteData searchKey = new LogicalQueueRouteData();
                searchKey.setState(MessageQueueRouteState.Normal);
                searchKey.setLogicalQueueDelta(offset);
                // it's sorted after getTopicRouteInfoFromNameServer
                int startIdx = Collections.binarySearch(queueRouteDataList, searchKey);
                if (startIdx < 0) {
                    startIdx = -startIdx - 1;
                    // lower entry
                    startIdx -= 1;
                }
                this.logicalQueueRouteData = null;
                this.pullResult = null;
                LogicalQueueRouteData lastReadableLogicalQueueRouteData = null; // first item which delta > offset
                LogicalQueueRouteData minReadableLogicalQueueRouteData = null;
                LogicalQueueRouteData maxReadableLogicalQueueRouteData = null;
                for (int i = 0, size = queueRouteDataList.size(); i < size; i++) {
                    LogicalQueueRouteData queueRouteData = queueRouteDataList.get(i);
                    if (!queueRouteData.isReadable()) {
                        continue;
                    }
                    maxReadableLogicalQueueRouteData = queueRouteData;
                    if (minReadableLogicalQueueRouteData == null) {
                        minReadableLogicalQueueRouteData = queueRouteData;
                        if (i < startIdx) {
                            // must consider following `i++` operation when invoke `continue`, so decrease first
                            i = startIdx - 1;
                            continue;
                        }
                    }
                    if (queueRouteData.getLogicalQueueDelta() > offset) {
                        if (this.logicalQueueRouteData != null) {
                            if (this.logicalQueueRouteData.toLogicalQueueOffset(this.logicalQueueRouteData.getOffsetMax()) <= offset) {
                                this.logicalQueueRouteData = queueRouteData;
                            }
                            break;
                        } else {
                            if (lastReadableLogicalQueueRouteData == null) {
                                lastReadableLogicalQueueRouteData = queueRouteData;
                            }
                        }
                    } else {
                        this.logicalQueueRouteData = queueRouteData;
                    }
                }
                if (this.logicalQueueRouteData == null) {
                    if (lastReadableLogicalQueueRouteData != null) {
                        this.pullResult = new PullResultExt(PullStatus.OFFSET_ILLEGAL, lastReadableLogicalQueueRouteData.getLogicalQueueDelta(), minReadableLogicalQueueRouteData.getLogicalQueueDelta(), maxReadableLogicalQueueRouteData.getLogicalQueueDelta(), null, 0, null);
                        return null;
                    } else {
                        if (maxReadableLogicalQueueRouteData != null) {
                            this.logicalQueueRouteData = maxReadableLogicalQueueRouteData;
                        } else {
                            if (!queueRouteDataList.isEmpty()) {
                                this.logicalQueueRouteData = queueRouteDataList.get(queueRouteDataList.size() - 1);
                            } else {
                                pullResult = new PullResultExt(PullStatus.NO_NEW_MSG, 0, 0, 0, null, 0, null);
                                return null;
                            }
                        }
                    }
                }
                this.isMaxReadableQueueRoute = this.logicalQueueRouteData.isSameTo(maxReadableLogicalQueueRouteData);
                return this.logicalQueueRouteData.getMessageQueue();
            } finally {
                this.logicalQueuesInfo.readLock().unlock();
            }
        }

        public PullResultExt getPullResult() {
            return pullResult;
        }

        public PullCallback wrapPullCallback() {
            if (this.notUsingLogicalQueue()) {
                return this.pullCallback;
            }
            if (!CommunicationMode.ASYNC.equals(this.communicationMode)) {
                return this.pullCallback;
            }
            return this;
        }

        public long getModifiedOffset() {
            return this.logicalQueueRouteData.toMessageQueueOffset(this.offset);
        }

        public long getModifiedCommitOffset() {
            // TODO should this be modified too? If offset is not in current broker's range, how do we handle it?
            return this.commitOffset;
        }

        public void incrRetry() {
            this.retry.incrementAndGet();
        }

        public boolean shouldRetry(Throwable t) {
            this.incrRetry();
            if (this.retry.get() >= 3) {
                return false;
            }
            if (t instanceof MQRedirectException) {
                MQRedirectException e = (MQRedirectException) t;
                this.processResponseBody(e.getBody());
                return true;
            }
            return false;
        }

        public PullResult wrapPullResult(PullResult pullResult) {
            if (pullResult == null) {
                return null;
            }
            if (this.logicalQueueRouteData == null) {
                return pullResult;
            }
            if (!this.isMaxReadableQueueRoute && PullStatus.NO_MATCHED_MSG.equals(pullResult.getPullStatus())) {
                PullStatus status = PullStatus.OFFSET_ILLEGAL;
                if (pullResult instanceof PullResultExt) {
                    PullResultExt pullResultExt = (PullResultExt) pullResult;
                    pullResult = new PullResultExt(status, pullResultExt.getNextBeginOffset(), pullResultExt.getMinOffset(), pullResultExt.getMaxOffset(), pullResultExt.getMsgFoundList(), pullResultExt.getSuggestWhichBrokerId(), pullResultExt.getMessageBinary());
                } else {
                    pullResult = new PullResult(status, pullResult.getNextBeginOffset(), pullResult.getMinOffset(), pullResult.getMaxOffset(), pullResult.getMsgFoundList());
                }
            }
            // method PullAPIWrapper#processPullResult will modify queueOffset/nextBeginOffset/minOffset/maxOffset
            return new PullResultWithLogicalQueues(pullResult, this.logicalQueueRouteData);
        }

        public void processResponseBody(byte[] responseBody) {
            log.info("LogicalQueueContext.processResponseBody got redirect {}: {}", this.logicalQueueRouteData, responseBody != null ? new String(responseBody, MessageDecoder.CHARSET_UTF8) : null);
            if (responseBody != null) {
                try {
                    List<LogicalQueueRouteData> queueRouteDataList = JSON.parseObject(responseBody, MixAll.TYPE_LIST_LOGICAL_QUEUE_ROUTE_DATA);
                    this.logicalQueuesInfo.updateLogicalQueueRouteDataList(this.mq.getQueueId(), queueRouteDataList);
                    return;
                } catch (Exception e) {
                    log.warn("LogicalQueueContext.processResponseBody {} update exception, fallback to updateTopicRouteInfoFromNameServer", this.logicalQueueRouteData, e);
                }
            }
            PullAPIWrapper.this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic(), false, null, Collections.singleton(this.mq.getQueueId()));
            this.buildLogicalQueuesInfo();
        }
    }
}
