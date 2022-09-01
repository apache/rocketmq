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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

public class EscapeBridge {
    protected static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long SEND_TIMEOUT = 3000L;
    private static final long DEFAULT_PULL_TIMEOUT_MILLIS = 1000 * 10L;
    private final String innerProducerGroupName;
    private final String innerConsumerGroupName;

    private final BrokerController brokerController;

    private ExecutorService defaultAsyncSenderExecutor;

    public EscapeBridge(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.innerProducerGroupName = "InnerProducerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
        this.innerConsumerGroupName = "InnerConsumerGroup_" + brokerController.getBrokerConfig().getBrokerName() + "_" + brokerController.getBrokerConfig().getBrokerId();
    }

    public void start() throws Exception {
        if (brokerController.getBrokerConfig().isEnableSlaveActingMaster() && brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            final BlockingQueue<Runnable> asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
            this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                asyncSenderThreadPoolQueue,
                new ThreadFactoryImpl("AsyncEscapeBridgeExecutor_", this.brokerController.getBrokerIdentity())
            );
            LOG.info("init executor for escaping messages asynchronously success.");
        }
    }

    public void shutdown() {
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
                final SendResult sendResult = putMessageToRemoteBroker(messageExt);
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

    private SendResult putMessageToRemoteBroker(MessageExtBrokerInner messageExt) {
        final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            LOG.warn("putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                messageExt.getTopic(), messageExt.getMsgId());
            return null;
        }

        final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue();
        messageExt.setQueueId(mqSelected.getQueueId());

        final String brokerNameToSend = mqSelected.getBrokerName();
        final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);

        final long beginTimestamp = System.currentTimeMillis();
        try {
            final SendResult sendResult = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(
                brokerAddrToSend, brokerNameToSend,
                messageExt, this.getProducerGroup(messageExt), SEND_TIMEOUT);
            if (null != sendResult && SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                return sendResult;
            } else {
                LOG.error("Escaping failed! cost {}ms, Topic: {}, MsgId: {}, Broker: {}",
                    System.currentTimeMillis() - beginTimestamp, messageExt.getTopic(),
                    messageExt.getMsgId(), brokerNameToSend);
            }
        } catch (RemotingException | MQBrokerException e) {
            LOG.error(String.format("putMessageToRemoteBroker exception, MsgId: %s, RT: %sms, Broker: %s",
                messageExt.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
        } catch (InterruptedException e) {
            LOG.error(String.format("putMessageToRemoteBroker interrupted, MsgId: %s, RT: %sms, Broker: %s",
                messageExt.getMsgId(), System.currentTimeMillis() - beginTimestamp, mqSelected), e);
            Thread.currentThread().interrupt();
        }

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

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                final String producerGroup = getProducerGroup(messageExt);

                final MessageQueue mqSelected = topicPublishInfo.selectOneMessageQueue();
                messageExt.setQueueId(mqSelected.getQueueId());

                final String brokerNameToSend = mqSelected.getBrokerName();
                final String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
                final CompletableFuture<SendResult> future = this.brokerController.getBrokerOuterAPI().sendMessageToSpecificBrokerAsync(brokerAddrToSend,
                    brokerNameToSend, messageExt,
                    producerGroup, SEND_TIMEOUT);

                return future.exceptionally(throwable -> null)
                    .thenApplyAsync(sendResult -> transformSendResult2PutResult(sendResult), this.defaultAsyncSenderExecutor)
                    .exceptionally(throwable -> transformSendResult2PutResult(null));

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


    public PutMessageResult putMessageToSpecificQueue(MessageExtBrokerInner messageExt) {
        BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (masterBroker != null) {
            return masterBroker.getMessageStore().putMessage(messageExt);
        } else if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()
            && this.brokerController.getBrokerConfig().isEnableRemoteEscape()) {
            try {
                messageExt.setWaitStoreMsgOK(false);

                final TopicPublishInfo topicPublishInfo = this.brokerController.getTopicRouteInfoManager().tryToFindTopicPublishInfo(messageExt.getTopic());
                List<MessageQueue> mqs = topicPublishInfo.getMessageQueueList();

                if (null == mqs || mqs.isEmpty()) {
                    return new PutMessageResult(PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL, null, true);
                }

                String id = messageExt.getTopic() + messageExt.getStoreHost();
                final int index = Math.floorMod(id.hashCode(), mqs.size());

                MessageQueue mq = mqs.get(index);
                messageExt.setQueueId(mq.getQueueId());

                String brokerNameToSend = mq.getBrokerName();
                String brokerAddrToSend = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInPublish(brokerNameToSend);
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
            String brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);
            if (null == brokerAddr) {
                this.brokerController.getTopicRouteInfoManager().updateTopicRouteInfoFromNameServer(topic, true, false);
                brokerAddr = this.brokerController.getTopicRouteInfoManager().findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, false);

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

}
