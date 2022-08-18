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

package org.apache.rocketmq.broker.failover;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.NamedForkJoinWorkerThreadFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

public class EscapeBridge {
    protected static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long SEND_TIMEOUT = 3000L;
    private static final long LOCK_TIMEOUT_MILLIS = 3000L;
    private static final int DEFAULT_RETRY_TIMES = 3;
    private static final long DEFAULT_PULL_TIMEOUT_MILLIS = 1000 * 10L;
    private final String innerProducerGroupName;
    private final String innerConsumerGroupName;

    private final BrokerController brokerController;

    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<>();
    private ExecutorService defaultAsyncSenderExecutor;
    private ScheduledExecutorService scheduledExecutorService;
    private static final long POLL_NAMESERVER_INTERVAL = 1000 * 30L;

    public EscapeBridge(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.innerProducerGroupName = "InnerProducerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
        this.innerConsumerGroupName = "InnerConsumerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
    }

    public void start() throws Exception {
        if (brokerController.getBrokerConfig().isEnableSlaveActingMaster() && brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            String nameserver = this.brokerController.getNameServerList();
            if (nameserver != null && !nameserver.isEmpty()) {
                this.defaultAsyncSenderExecutor = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                    new NamedForkJoinWorkerThreadFactory("AsyncEscapeBridgeSender_", brokerController.getBrokerIdentity()),
                    null,
                    false);

                initScheduledTask();

                LOG.info("init executor for escaping messages asynchronously success.");
            } else {
                throw new RuntimeException("nameserver address is null or empty");
            }
        }
    }

    private void initScheduledTask() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "EscapeBridgeScheduledThread");
            }
        });

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                LOG.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
            }
        }, 10, POLL_NAMESERVER_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void updateTopicRouteInfoFromNameServer() {
        if (null == topicRouteTable || topicRouteTable.isEmpty()) {
            return;
        }
        for (String topic : topicRouteTable.keySet()) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public void shutdown() {
        if (null != this.scheduledExecutorService) {
            this.scheduledExecutorService.shutdown();
        }
        if (null != this.defaultAsyncSenderExecutor) {
            this.defaultAsyncSenderExecutor.shutdown();
        }
    }

    public PutMessageResult putMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {

            try {
                messageExt.setWaitStoreMsgOK(false);
                final SendResult sendResult = putMessageToRemoteBroker(messageExt, DEFAULT_RETRY_TIMES);
                return transformSendResult2PutResult(sendResult);
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
    }

    private SendResult putMessageToRemoteBroker(MessageExtBrokerInner messageExt, final int retryTimes) {
        final TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(messageExt.getTopic());
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            LOG.warn("putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                messageExt.getTopic(), messageExt.getMsgId());
            return null;
        }

        final long invokeID = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        String lastBrokerName;
        MessageQueue mq = null;
        long endTimestamp;
        int times = 0;
        String[] brokersSent = new String[retryTimes];
        for (; times < retryTimes + 1; times++) {
            lastBrokerName = null == mq ? null : mq.getBrokerName();
            MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue(lastBrokerName);
            messageExt.setQueueId(mqSelected.getQueueId());
            mq = mqSelected;

            String brokerNameToSend = mq.getBrokerName();
            brokersSent[times] = brokerNameToSend;
            String brokerAddrToSend = this.findBrokerAddressInPublish(brokerNameToSend);
            final SendResult sendResult;
            try {
                beginTimestampPrev = System.currentTimeMillis();
                sendResult = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(
                    brokerAddrToSend, lastBrokerName,
                    messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT);
                if (null != sendResult && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                    return sendResult;
                }
            } catch (RemotingException | MQBrokerException e) {
                endTimestamp = System.currentTimeMillis();
                LOG.warn(String.format("putMessageToRemoteBroker exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s",
                    invokeID, endTimestamp - beginTimestampPrev, mq), e);
                LOG.warn(String.valueOf(messageExt));
            } catch (InterruptedException e) {
                endTimestamp = System.currentTimeMillis();
                LOG.warn(String.format("putMessageToRemoteBroker exception, return null sendResult, InvokeID: %s, RT: %sms, Broker: %s",
                    invokeID, endTimestamp - beginTimestampPrev, mq), e);
                LOG.warn(String.valueOf(messageExt));
                return null;
            }

        }

        LOG.error("Escaping failed! send {} times, cost {}ms, Topic: {}, MsgId: {}, BrokersSent: {}",
            times, System.currentTimeMillis() - beginTimestampFirst, messageExt.getTopic(),
            messageExt.getMsgId(), Arrays.toString(brokersSent));
        return null;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().asyncPutMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(messageExt.getTopic());
                final String producerGroup = getProducerGroup(messageExt);
                final CompletableFuture<SendResult> retryFuture = wrapRetryableFuture(
                    lastBroker -> {
                        MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue(lastBroker);
                        messageExt.setQueueId(mqSelected.getQueueId());

                        String brokerNameToSend = mqSelected.getBrokerName();
                        String brokerAddrToSend = this.findBrokerAddressInPublish(brokerNameToSend);
                        return this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(brokerAddrToSend,
                            brokerNameToSend, messageExt,
                            producerGroup, SEND_TIMEOUT);
                    },
                    DEFAULT_RETRY_TIMES,
                    null,
                    defaultAsyncSenderExecutor);

                return retryFuture.exceptionally(throwable -> null)
                    .thenApplyAsync(sendResult -> {
                        return transformSendResult2PutResult(sendResult);
                    }, this.defaultAsyncSenderExecutor)
                    .exceptionally(throwable -> {
                        return transformSendResult2PutResult(null);
                    });

            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true));
            }
        } else {
            LOG.warn("Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null));
        }
    }

    private static CompletableFuture<SendResult> wrapRetryableFuture(
        Function<String, CompletableFuture<SendResult>> action, int retryTimes, String lastBrokerName,
        Executor executor) {
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        action.apply(lastBrokerName).whenCompleteAsync((sendResult, throwable) -> {
            if (null != throwable || null == sendResult) {
                if (retryTimes <= 0) {
                    future.complete(null);
                } else {
                    retrySendMessageAsync(future, action, retryTimes - 1, lastBrokerName, executor);
                }
            } else {
                future.complete(sendResult);
            }
        }, executor);
        return future;
    }

    private static void retrySendMessageAsync(CompletableFuture<SendResult> future,
        Function<String, CompletableFuture<SendResult>> action, int leftRetryTimes, String lastBrokerName,
        Executor executor) {
        action.apply(lastBrokerName).whenCompleteAsync((sendResult, throwable) -> {
            if (null != throwable || null == sendResult) {
                if (leftRetryTimes <= 0) {
                    future.complete(null);
                } else {
                    LOG.info("retry escaping message async, sendRestult={}, current leftRetryTimes=", sendResult, leftRetryTimes);
                    retrySendMessageAsync(future, action, leftRetryTimes - 1, lastBrokerName, executor);
                }
            } else {
                LOG.info("escaping message async completed result={}, leftRetryTimes=", sendResult, leftRetryTimes);
                future.complete(sendResult);
            }
        }, executor);
    }

    private String getProducerGroup(MessageExtBrokerInner messageExt) {
        if (null == messageExt) {
            return this.innerProducerGroupName;
        }
        String producerGroup = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = this.innerProducerGroupName;
        }
        return producerGroup;
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            this.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        return topicPublishInfo;
    }

    private boolean updateTopicRouteInfoFromNameServer(String topic) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData = this.brokerController.getBrokerOuterAPI()
                        .getTopicRouteInfoFromNameServer(topic, SEND_TIMEOUT);
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = this.topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            LOG.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        if (changed) {

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                TopicPublishInfo publishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                this.updateTopicPublishInfo(topic, publishInfo);

                            }

                            TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
                            LOG.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        LOG.warn("EscapeBridge: updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}.", topic);
                    }
                } catch (RemotingException e) {
                    LOG.error("updateTopicRouteInfoFromNameServer Exception", e);
                } catch (MQBrokerException e) {
                    LOG.error("updateTopicRouteInfoFromNameServer Exception", e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
        return false;
    }

    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                LOG.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        final TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
        return null == prev || !prev.ok();
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = new TopicRouteData(olddata);
        TopicRouteData now = new TopicRouteData(nowdata);
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private String findBrokerAddressInPublish(String brokerName) {
        if (brokerName == null) {
            return null;
        }
        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public PutMessageResult putMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(messageExt.getTopic());
                List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();

                if (null == mqs || mqs.isEmpty()) {
                    return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
                }

                String id = messageExt.getTopic() + messageExt.getStoreHost();
                final int index = Math.floorMod(id.hashCode(), mqs.size());

                MessageQueue mq = mqs.get(index);
                messageExt.setQueueId(mq.getQueueId());

                String brokerNameToSend = mq.getBrokerName();
                String brokerAddrToSend = this.findBrokerAddressInPublish(brokerNameToSend);
                final SendResult sendResult = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(
                    brokerAddrToSend, brokerNameToSend,
                    messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT);

                return transformSendResult2PutResult(sendResult);
            } catch (Exception e) {
                LOG.error("sendMessageInFailover to remote failed", e);
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
            }
        } else {
            LOG.warn("Put message to specific queue failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                this.brokerController.getBrokerConfig().isEnableSlaveActingMaster(), this.brokerController.getBrokerConfig().isEnableRemoteEscape());
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
    }

    private PutMessageResult transformSendResult2PutResult(SendResult sendResult) {
        if (sendResult == null) {
            return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
        switch (sendResult.getSendStatus()) {
            case SEND_OK:
                return new PutMessageResult(PutMessageStatus.PUT_OK, null, true);
            case SLAVE_NOT_AVAILABLE:
                return new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, null, true);
            case FLUSH_DISK_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, null, true);
            case FLUSH_SLAVE_TIMEOUT:
                return new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null, true);
            default:
                return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
        }
    }

    public MessageExt getMessage(String topic, long offset, int queueId, String brokerName) {
        MessageStore messageStore = brokerController.getMessageStoreByBrokerName(brokerName);
        if (messageStore != null) {
            final GetMessageResult getMessageTmpResult = messageStore.getMessage(innerConsumerGroupName, topic, queueId, offset, 1, null);
            if (getMessageTmpResult == null) {
                LOG.warn("getMessageResult is null , innerConsumerGroupName {}, topic {}, offset {}, queueId {}", innerConsumerGroupName, topic, offset, queueId);
                return null;
            }
            List<MessageExt> list = decodeMsgList(getMessageTmpResult);
            if (list == null || list.isEmpty()) {
                LOG.warn("Can not get msg , topic {}, offset {}, queueId {}, result is {}", topic, offset, queueId, getMessageTmpResult);
                return null;
            } else {
                return list.get(0);
            }
        } else {
            return getMessageFromRemote(topic, offset, queueId, brokerName);
        }
    }

    protected List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        LOG.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb);
                    if (msgExt == null) {
                        LOG.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    protected MessageExt getMessageFromRemote(String topic, long offset, int queueId, String brokerName) {
        try {
            String brokerAddr = this.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);
            if (null == brokerAddr) {
                this.updateTopicRouteInfoFromNameServer(topic);
                brokerAddr = this.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);

                if (null == brokerAddr) {
                    LOG.warn("can't find broker address for topic {}", topic);
                    return null;
                }
            }

            PullResult pullResult = this.brokerController.getBrokerOuterAPI().pullMessageFromSpecificBroker(brokerName,
                brokerAddr, this.innerConsumerGroupName, topic, queueId, offset, 1, DEFAULT_PULL_TIMEOUT_MILLIS);

            if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                return pullResult.getMsgFoundList().get(0);
            }
        } catch (Exception e) {
            LOG.error("Get message from remote failed.", e);
        }

        return null;
    }

    private String findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean found = false;

        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            boolean slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            if (!found && !onlyThisBroker) {
                Map.Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                found = true;
            }
        }

        return brokerAddr;

    }
}
