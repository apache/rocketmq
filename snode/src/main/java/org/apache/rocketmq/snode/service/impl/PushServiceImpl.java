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
package org.apache.rocketmq.snode.service.impl;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PushMessageHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.constant.SnodeConstant;
import org.apache.rocketmq.snode.service.PushService;

public class PushServiceImpl implements PushService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private SnodeController snodeController;
    private ExecutorService pushMessageExecutorService;

    public PushServiceImpl(final SnodeController snodeController) {
        this.snodeController = snodeController;
        pushMessageExecutorService = ThreadUtils.newThreadPoolExecutor(
            this.snodeController.getSnodeConfig().getSnodePushMessageMinPoolSize(),
            this.snodeController.getSnodeConfig().getSnodePushMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(this.snodeController.getSnodeConfig().getSnodeSendThreadPoolQueueCapacity()),
            "SnodePushMessageThread",
            false);
    }

    public class PushTask implements Runnable {
        private AtomicBoolean canceled = new AtomicBoolean(false);
        private final byte[] message;
        private final RemotingCommand sendMessageResponse;
        private final SendMessageRequestHeader sendMessageRequestHeader;

        public PushTask(final SendMessageRequestHeader sendMessageRequestHeader, final byte[] message,
            final RemotingCommand response) {
            this.message = message;
            this.sendMessageRequestHeader = sendMessageRequestHeader;
            this.sendMessageResponse = response;
        }

        private MessageExt buildMessageExt(final SendMessageResponseHeader sendMessageResponseHeader,
            final byte[] message, final SendMessageRequestHeader sendMessageRequestHeader) {
            MessageExt messageExt = new MessageExt();
            messageExt.setProperties(MessageDecoder.string2messageProperties(sendMessageRequestHeader.getProperties()));
            messageExt.setTopic(sendMessageRequestHeader.getTopic());
            messageExt.setMsgId(sendMessageResponseHeader.getMsgId());
            messageExt.setQueueId(sendMessageResponseHeader.getQueueId());
            messageExt.setQueueOffset(sendMessageResponseHeader.getQueueOffset());
            messageExt.setReconsumeTimes(sendMessageRequestHeader.getReconsumeTimes());
            messageExt.setCommitLogOffset(sendMessageResponseHeader.getCommitLogOffset());
            messageExt.setBornTimestamp(sendMessageRequestHeader.getBornTimestamp());
            messageExt.setBornHost(RemotingUtil.string2SocketAddress(sendMessageRequestHeader.getBornHost()));
            messageExt.setStoreHost(RemotingUtil.string2SocketAddress(sendMessageResponseHeader.getStoreHost()));
            messageExt.setStoreTimestamp(sendMessageResponseHeader.getStoreTimestamp());
            messageExt.setWaitStoreMsgOK(false);
            messageExt.setStoreSize(sendMessageResponseHeader.getStoreSize());
            messageExt.setSysFlag(sendMessageRequestHeader.getSysFlag());
            messageExt.setFlag(sendMessageRequestHeader.getFlag());
            messageExt.setBody(message);
            messageExt.setBodyCRC(UtilAll.crc32(message));
            log.info("MessageExt:{}", messageExt);
            return messageExt;
        }

        @Override
        public void run() {
            if (!canceled.get()) {
                try {
                    SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) sendMessageResponse.decodeCommandCustomHeader(SendMessageResponseHeader.class);
                    log.debug("sendMessageResponseHeader: {}", sendMessageResponseHeader);
                    MessageQueue messageQueue = new MessageQueue(sendMessageRequestHeader.getTopic(), sendMessageRequestHeader.getEnodeName(), sendMessageRequestHeader.getQueueId());
                    Set<RemotingChannel> consumerTable = snodeController.getSubscriptionManager().getPushableChannel(messageQueue);
                    if (consumerTable != null) {
                        PushMessageHeader pushMessageHeader = new PushMessageHeader();
                        pushMessageHeader.setQueueOffset(sendMessageResponseHeader.getQueueOffset());
                        pushMessageHeader.setTopic(sendMessageRequestHeader.getTopic());
                        pushMessageHeader.setQueueId(sendMessageResponseHeader.getQueueId());
                        pushMessageHeader.setEnodeName(sendMessageRequestHeader.getEnodeName());
                        RemotingCommand pushMessage = RemotingCommand.createRequestCommand(RequestCode.SNODE_PUSH_MESSAGE, pushMessageHeader);
                        MessageExt messageExt = buildMessageExt(sendMessageResponseHeader, message, sendMessageRequestHeader);
                        pushMessage.setBody(MessageDecoder.encode(messageExt, false));
                        for (RemotingChannel remotingChannel : consumerTable) {
                            log.info("Push message pushMessage:{} to remotingChannel: {}", pushMessage, remotingChannel);
                            Client client = null;
                            if (remotingChannel instanceof NettyChannelImpl) {
                                Channel channel = ((NettyChannelImpl) remotingChannel).getChannel();
                                Attribute<Client> clientAttribute = channel.attr(SnodeConstant.NETTY_CLIENT_ATTRIBUTE_KEY);
                                if (clientAttribute != null) {
                                    client = clientAttribute.get();
                                }
                            }
                            if (client != null) {
                                for (String consumerGroup : client.getGroups()) {
                                    Subscription subscription = snodeController.getSubscriptionManager().getSubscription(consumerGroup);
                                    if (subscription.getSubscriptionData(sendMessageRequestHeader.getTopic()) != null) {
                                        boolean slowConsumer = snodeController.getSlowConsumerService().isSlowConsumer(sendMessageResponseHeader.getQueueOffset(), sendMessageRequestHeader.getTopic(), sendMessageRequestHeader.getQueueId(), consumerGroup, sendMessageRequestHeader.getEnodeName());
                                        if (slowConsumer) {
                                            log.warn("[SlowConsumer]: {} is slow consumer", remotingChannel);
                                            snodeController.getSlowConsumerService().slowConsumerResolve(pushMessage, remotingChannel);
                                            continue;
                                        }
                                        pushMessageHeader.setConsumerGroup(consumerGroup);
                                        snodeController.getSnodeServer().push(remotingChannel, pushMessage, SnodeConstant.DEFAULT_TIMEOUT_MILLS);
                                    }
                                }
                            } else {
                                log.error("[NOTIFYME] Remoting channel: {} related client is null", remotingChannel.remoteAddress());
                            }
                        }
                    } else {
                        log.info("No online registered as push consumer and online for messageQueue: {} ", messageQueue);
                    }
                } catch (Exception ex) {
                    log.warn("Push message to topic: {} queueId: {}", sendMessageRequestHeader.getTopic(), sendMessageRequestHeader.getQueueId(), ex);
                }
            } else {
                log.info("Push message to topic: {} queueId: {} canceled!", sendMessageRequestHeader.getTopic(), sendMessageRequestHeader.getQueueId());
            }
        }

        public void setCanceled(AtomicBoolean canceled) {
            this.canceled = canceled;
        }

    }

    @Override
    public void pushMessage(final SendMessageRequestHeader requestHeader, final byte[] message,
        final RemotingCommand response) {
        PushTask pushTask = new PushTask(requestHeader, message, response);
        pushMessageExecutorService.submit(pushTask);
    }

    @Override
    public void shutdown() {
        this.pushMessageExecutorService.shutdown();
    }
}
