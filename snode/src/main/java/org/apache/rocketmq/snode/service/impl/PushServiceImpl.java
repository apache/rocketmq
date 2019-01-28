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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PushMessageHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.Subscription;
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
        private final Integer queueId;
        private final String topic;
        private final RemotingCommand response;
        private final String enodeName;

        public PushTask(final String topic, final Integer queueId, final byte[] message,
            final RemotingCommand response, final String enodeName) {
            this.message = message;
            this.queueId = queueId;
            this.topic = topic;
            this.response = response;
            this.enodeName = enodeName;
        }

        @Override
        public void run() {
            if (!canceled.get()) {
                try {
                    SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);
                    PushMessageHeader pushMessageHeader = new PushMessageHeader();
                    pushMessageHeader.setQueueOffset(sendMessageResponseHeader.getQueueOffset());
                    pushMessageHeader.setTopic(topic);
                    pushMessageHeader.setQueueId(queueId);
                    RemotingCommand pushMessage = RemotingCommand.createRequestCommand(RequestCode.SNODE_PUSH_MESSAGE, pushMessageHeader);
                    pushMessage.setBody(message);
                    MessageQueue messageQueue = new MessageQueue(topic, enodeName, queueId);
                    Set<RemotingChannel> consumerTable = snodeController.getSubscriptionManager().getPushableChannel(messageQueue);
                    if (consumerTable != null) {
                        for (RemotingChannel remotingChannel : consumerTable) {
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
                                    if (subscription.getSubscriptionData(topic) != null) {
                                        boolean slowConsumer = snodeController.getSlowConsumerService().isSlowConsumer(sendMessageResponseHeader.getQueueOffset(), topic, queueId, consumerGroup, enodeName);
                                        if (slowConsumer) {
                                            log.warn("[SlowConsumer]: {} closed as slow consumer", remotingChannel);
                                            remotingChannel.close();
                                            continue;
                                        }
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
                    log.warn("Push message to topic: {} queueId: {}", topic, queueId, ex);
                }
            } else {
                log.info("Push message to topic: {} queueId: {} canceled!", topic, queueId);
            }
        }

        public void setCanceled(AtomicBoolean canceled) {
            this.canceled = canceled;
        }

    }

    @Override
    public void pushMessage(final String enodeName, final String topic, final Integer queueId, final byte[] message,
        final RemotingCommand response) {
        PushTask pushTask = new PushTask(topic, queueId, message, response, enodeName);
        pushMessageExecutorService.submit(pushTask);
    }

    @Override
    public void shutdown() {
        this.pushMessageExecutorService.shutdown();
    }
}
