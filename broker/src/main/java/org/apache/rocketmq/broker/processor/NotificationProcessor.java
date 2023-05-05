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
package org.apache.rocketmq.broker.processor;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.NotificationRequest;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class NotificationProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private Random random = new Random(System.currentTimeMillis());
    private static final String BORN_TIME = "bornTime";
    private ConcurrentLinkedHashMap<String, ArrayBlockingQueue<NotificationRequest>> pollingMap = new ConcurrentLinkedHashMap.Builder<String, ArrayBlockingQueue<NotificationRequest>>().maximumWeightedCapacity(100000).build();
    private Thread checkNotificationPollingThread;

    public NotificationProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.checkNotificationPollingThread = new Thread(new AbstractBrokerRunnable(brokerController.getBrokerConfig()) {
            @Override
            public void run0() {
                while (true) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    try {
                        Thread.sleep(200L);
                        Collection<ArrayBlockingQueue<NotificationRequest>> pops = pollingMap.values();
                        for (ArrayBlockingQueue<NotificationRequest> popQ : pops) {
                            NotificationRequest tmPopRequest = popQ.peek();
                            while (tmPopRequest != null) {
                                if (tmPopRequest.isTimeout()) {
                                    tmPopRequest = popQ.poll();
                                    if (tmPopRequest == null) {
                                        break;
                                    }
                                    POP_LOGGER.info("timeout , wakeUp Notification : {}", tmPopRequest);
                                    wakeUp(tmPopRequest);
                                    tmPopRequest = popQ.peek();
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        POP_LOGGER.error("checkNotificationPolling error", e);
                    }
                }
            }
        });
        this.checkNotificationPollingThread.setDaemon(true);
        this.checkNotificationPollingThread.setName("checkNotificationPolling");
        this.checkNotificationPollingThread.start();
    }

    public void shutdown() {
        this.checkNotificationPollingThread.interrupt();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        return this.processRequest(ctx.channel(), request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public void notifyMessageArriving(final String topic, final int queueId) {
        notifyMessageArrivingForQueue(topic, -1);
        if (queueId > 0) {
            notifyMessageArrivingForQueue(topic, queueId);
        }
    }

    public void notifyMessageArrivingForQueue(final String topic, final int queueId) {
        ArrayBlockingQueue<NotificationRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingNotificationKey(topic, queueId));
        if (remotingCommands != null) {
            List<NotificationRequest> c = new ArrayList<>();
            remotingCommands.drainTo(c);
            for (NotificationRequest notificationRequest : c) {
                POP_LOGGER.info("new msg arrive , wakeUp : {}", notificationRequest);
                wakeUp(notificationRequest);
            }
        }
    }

    private void wakeUp(final NotificationRequest request) {
        if (request == null || !request.complete()) {
            return;
        }
        if (!request.getChannel().isActive()) {
            return;
        }
        Runnable run = () -> {
            try {
                final RemotingCommand response;
                response = NotificationProcessor.this.processRequest(request.getChannel(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (RemotingCommandException e) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e);
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request)
        throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(NotificationResponseHeader.class);
        final NotificationResponseHeader responseHeader = (NotificationResponseHeader) response.readCustomHeader();
        final NotificationRequestHeader requestHeader =
            (NotificationRequestHeader) request.decodeCommandCustomHeader(NotificationRequestHeader.class);

        response.setOpaque(request.getOpaque());

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] peeking message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }
        int randomQ = random.nextInt(100);
        boolean hasMsg = false;
        boolean needRetry = randomQ % 5 == 0;
        if (needRetry) {
            TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
            if (retryTopicConfig != null) {
                for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                    hasMsg = hasMsgFromQueue(true, requestHeader, queueId);
                    if (hasMsg) {
                        break;
                    }
                }
            }
        }
        if (!hasMsg) {
            if (requestHeader.getQueueId() < 0) {
                // read all queue
                for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                    int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                    hasMsg = hasMsgFromQueue(false, requestHeader, queueId);
                    if (hasMsg) {
                        break;
                    }
                }
            } else {
                int queueId = requestHeader.getQueueId();
                hasMsg = hasMsgFromQueue(false, requestHeader, queueId);
            }
            // if it doesn't have message, fetch retry again
            if (!needRetry && !hasMsg) {
                TopicConfig retryTopicConfig =
                    this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
                if (retryTopicConfig != null) {
                    for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
                        int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
                        hasMsg = hasMsgFromQueue(true, requestHeader, queueId);
                        if (hasMsg) {
                            break;
                        }
                    }
                }
            }
        }

        if (!hasMsg) {
            if (polling(channel, request, requestHeader)) {
                return null;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        responseHeader.setHasMsg(hasMsg);
        return response;
    }

    private boolean hasMsgFromQueue(boolean isRetry, NotificationRequestHeader requestHeader, int queueId) {
        if (this.brokerController.getConsumerOrderInfoManager().checkBlock(null, requestHeader.getTopic(), requestHeader.getConsumerGroup(), queueId, 0)) {
            return false;
        }
        String topic = isRetry ? KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()) : requestHeader.getTopic();
        long offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId);
        long restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset;
        return restNum > 0;
    }

    private long getPopOffset(String topic, String cid, int queueId) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(cid, topic, queueId);
        if (offset < 0) {
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        }
        long bufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService()
            .getLatestOffset(topic, cid, queueId);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return bufferOffset > offset ? bufferOffset : offset;
        }
    }

    private boolean polling(final Channel channel, RemotingCommand remotingCommand,
        final NotificationRequestHeader requestHeader) {
        if (requestHeader.getPollTime() <= 0) {
            return false;
        }

        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        final NotificationRequest request = new NotificationRequest(remotingCommand, channel, expired);
        boolean result = false;
        if (!request.isTimeout()) {
            String key = KeyBuilder.buildPollingNotificationKey(requestHeader.getTopic(), requestHeader.getQueueId());
            ArrayBlockingQueue<NotificationRequest> queue = pollingMap.get(key);
            if (queue == null) {
                queue = new ArrayBlockingQueue<>(this.brokerController.getBrokerConfig().getPopPollingSize());
                pollingMap.put(key, queue);
                result = queue.offer(request);
            } else {
                result = queue.offer(request);
            }
        }
        POP_LOGGER.info("polling {}, result {}", remotingCommand, result);
        return result;

    }
}
