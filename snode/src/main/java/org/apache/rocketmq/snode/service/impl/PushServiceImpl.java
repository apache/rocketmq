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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PushMessageHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.ClientChannelInfo;
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

        public PushTask(final String topic, final Integer queueId, final byte[] message,
            final RemotingCommand response) {
            this.message = message;
            this.queueId = queueId;
            this.topic = topic;
            this.response = response;
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
                    ConcurrentHashMap consumerGroupTable = snodeController.getConsumerManager().getClientInfoTable(topic, queueId);
                    if (consumerGroupTable != null) {
                        Iterator<Map.Entry<String, ClientChannelInfo>> itChannel = consumerGroupTable.entrySet().iterator();
                        while (itChannel.hasNext()) {
                            Entry<String, ClientChannelInfo> clientChannelInfoEntry = itChannel.next();
                            RemotingChannel remotingChannel = clientChannelInfoEntry.getValue().getChannel();
                            if (remotingChannel.isWritable()) {
                                log.warn("Push message to topic: {} queueId: {} consumer group:{}, message:{}", topic, queueId, clientChannelInfoEntry.getKey(), pushMessage);
                                snodeController.getSnodeServer().push(remotingChannel, pushMessage, SnodeConstant.defaultTimeoutMills);
                            }
                        }
                    } else {
                        log.warn("Get client info to topic: {} queueId: {} is null", topic, queueId);
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
    public boolean registerPushSession(String consumerGroup) {
        return false;
    }

    @Override
    public void unregisterPushSession(String consumerGroup) {

    }

    @Override
    public void pushMessage(final String topic, final Integer queueId, final byte[] message,
        final RemotingCommand response) {
        ConcurrentHashMap<String, ClientChannelInfo> clientChannelInfoTable = this.snodeController.getConsumerManager().getClientInfoTable(topic, queueId);
        if (clientChannelInfoTable != null) {
            PushTask pushTask = new PushTask(topic, queueId, message, response);
            pushMessageExecutorService.submit(pushTask);
        } else {
            log.info("Topic: {} QueueId: {} no need to push", topic, queueId);
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
        this.pushMessageExecutorService.shutdown();
    }
}
