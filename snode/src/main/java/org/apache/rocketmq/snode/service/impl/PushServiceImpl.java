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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.PushMessageHeader;
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
            new ArrayBlockingQueue<Runnable>(this.snodeController.getSnodeConfig().getSnodeSendThreadPoolQueueCapacity()),
            "SnodePushMessageThread",
            false);
    }

    public class PushTask implements Runnable {
        private AtomicBoolean canceled;
        private final String messageId;
        private final byte[] message;
        private final Integer queueId;
        private final String topic;
        private final long queueOffset;

        public PushTask(final String messageId, final byte[] message, final Integer queueId, final String topic,
            final long queueOffset) {
            this.messageId = messageId;
            this.message = message;
            this.queueId = queueId;
            this.topic = topic;
            this.queueOffset = queueOffset;
        }

        @Override
        public void run() {
            if (!canceled.get()) {
                PushMessageHeader pushMessageHeader = new PushMessageHeader();
                pushMessageHeader.setMessageId(this.messageId);
                pushMessageHeader.setQueueOffset(queueOffset);
                pushMessageHeader.setTopic(topic);
                pushMessageHeader.setQueueId(queueId);
                RemotingCommand pushMessage = RemotingCommand.createResponseCommand(PushMessageHeader.class);
                pushMessage.setBody(message);
                pushMessage.setCustomHeader(pushMessageHeader);
                try {
                    ClientChannelInfo clientChannelInfo = snodeController.getPushSessionManager().getClientInfoTable(topic, queueId);
                    if (clientChannelInfo != null) {
                        RemotingChannel remotingChannel = clientChannelInfo.getChannel();
                        snodeController.getSnodeServer().push(remotingChannel, pushMessage, SnodeConstant.defaultTimeoutMills);
                    } else {
                        log.warn("Get client info to topic: {} queueId: {} is null", topic, queueId);
                    }
                } catch (Exception ex) {
                    log.warn("Push message to topic: {} queueId: {} ex:{}", topic, queueId, ex);
                }
            }
        }

        public AtomicBoolean getCanceled() {
            return canceled;
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
    public void pushMessage(final String messageId, final byte[] message, final Integer queueId, final String topic,
        final long queueOffset) {
        PushTask pushTask = new PushTask(messageId, message, queueId, topic, queueOffset);
        pushMessageExecutorService.submit(pushTask);
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
        this.pushMessageExecutorService.shutdown();
    }
}
