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

import com.alibaba.fastjson2.JSON;
import com.github.benmanes.caffeine.cache.Cache;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.pop.PopConsumerContext;
import org.apache.rocketmq.broker.pop.PopConsumerService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

/**
 * Processes PopMessage requests from consumers.
 *
 * <p>This is the core processor for the Pop consumption mode. It handles:
 * <ul>
 *   <li>Validating the request (topic, group, queue, subscription, permissions)</li>
 *   <li>Routing to the {@link PopConsumerService} (KVStore path) or the
 *       inline file-based path</li>
 *   <li>Popping messages from normal and retry topics (V1/V2)</li>
 *   <li>Creating checkpoints and writing them to the revive topic</li>
 *   <li>Long-polling suspension via {@link PopLongPollingService}</li>
 *   <li>Transferring messages to the client (heap copy or zero-copy)</li>
 * </ul>
 *
 * <p>This class also owns the {@link PopLongPollingService},
 * {@link PopBufferMergeService}, and {@link QueueLockManager} instances
 * used by the file-based ack path.
 */
public class PopMessageProcessor implements NettyRequestProcessor {

    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final String BORN_TIME = "bornTime";

    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    private final String reviveTopic;

    private final PopLongPollingService popLongPollingService;
    private final PopBufferMergeService popBufferMergeService;
    private final QueueLockManager queueLockManager;
    private final AtomicLong ckMessageNumber;

    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(
            this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popLongPollingService = new PopLongPollingService(brokerController, this, false);
        this.queueLockManager = new QueueLockManager();
        this.popBufferMergeService = new PopBufferMergeService(this.brokerController, this);
        this.ckMessageNumber = new AtomicLong();
    }

    public void shutdown() throws Exception {
        popLongPollingService.shutdown();
        queueLockManager.shutdown();
        popBufferMergeService.shutdown();
    }

    protected String getReviveTopic() {
        return reviveTopic;
    }

    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }

    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
            + PopAckConstants.SPLIT + batchAckMsg.getQueueId()
            + PopAckConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
            + PopAckConstants.SPLIT + batchAckMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + batchAckMsg.getPopTime()
            + PopAckConstants.SPLIT + PopAckConstants.BATCH_ACK_TAG;
    }

    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public Cache<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return popLongPollingService.getPollingMap();
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) throws ConsumeQueueException {
        this.notifyLongPollingRequestIfNeed(
            topic, group, queueId, null, 0L, null, null);
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap,
        Map<String, String> properties) throws ConsumeQueueException {
        long popBufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        long offset = Math.max(popBufferOffset, consumerOffset);
        if (maxOffset > offset) {
            boolean notifySuccess = popLongPollingService.notifyMessageArriving(
                topic, -1, group, tagsCode, msgStoreTime, filterBitMap, properties);
            if (!notifySuccess) {
                // notify pop queue
                notifySuccess = popLongPollingService.notifyMessageArriving(
                    topic, queueId, group, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            this.brokerController.getNotificationProcessor().notifyMessageArriving(topic, queueId);
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                    topic, group, queueId, notifySuccess);
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        popLongPollingService.notifyMessageArrivingWithRetryTopic(
            topic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final String cid) {
        popLongPollingService.notifyMessageArriving(
            topic, queueId, cid, false, null, 0L, null, null);
    }

    /**
     * Process a PopMessage request.
     *
     * <p>This method handles the full Pop lifecycle:
     * <ol>
     *   <li>Validates the request (topic, group, permissions, subscription)</li>
     *   <li>Routes to the <b>KVStore path</b> (via {@link PopConsumerService#popAsync})
     *       or the <b>file-based path</b> (inline CompletableFuture chain)</li>
     *   <li>Pops messages from normal and retry topics (V1/V2)</li>
     *   <li>Creates checkpoints and appends them to the revive topic</li>
     *   <li>Suspends the request via {@link PopLongPollingService#polling} if
     *       no messages are available</li>
     *   <li>Transfers messages via heap copy or zero-copy ({@code FileRegion})</li>
     * </ol>
     *
     * @param ctx     the Netty channel handler context
     * @param request the incoming PopMessage request
     * @return the response, or {@code null} if the response is sent asynchronously
     *         (zero-copy path or long-polling suspension)
     * @throws RemotingCommandException if the request cannot be decoded
     */
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

        // init request and response
        final long beginTimeMills = this.brokerController.getMessageStore().now();

        Channel channel = ctx.channel();

        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());

        final PopMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(PopMessageRequestHeader.class, true);
        if (requestHeader.getBornTime() == 0) {
            request.addExtField(BORN_TIME, String.valueOf(beginTimeMills));
            requestHeader.setBornTime(beginTimeMills);
        }
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();

        // validation
        // Pop mode only supports consumption in cluster load balancing mode
        brokerController.getConsumerManager().compensateBasicConsumerInfo(
            requestHeader.getConsumerGroup(), ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }

        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        // init filter
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        SubscriptionData subscriptionData = null;
        ExpressionMessageFilter messageFilter = null;
        if (requestHeader.getExp() != null && !requestHeader.getExp().isEmpty()) {
            try {
                // origin topic
                subscriptionData = FilterAPI.build(
                    requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // retry topic
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                SubscriptionData retrySubscriptionData = FilterAPI.build(
                    retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);

                ConsumerFilterData consumerFilterData = null;
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(),
                        requestHeader.getExpType(), System.currentTimeMillis());
                    if (consumerFilterData == null) {
                        POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}",
                            requestHeader.getExp(), requestHeader.getConsumerGroup());
                        response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                        response.setRemark("parse the consumer's subscription failed");
                        return response;
                    }
                }
                messageFilter = new ExpressionMessageFilter(
                    subscriptionData, consumerFilterData, brokerController.getConsumerFilterManager());
            } catch (Exception e) {
                POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            try {
                // origin topic
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // retry topic
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);
            } catch (Exception e) {
                POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
            }
        }

        GetMessageResult getMessageResult = new GetMessageResult(requestHeader.getMaxMsgNums());
        ExpressionMessageFilter finalMessageFilter = messageFilter;
        SubscriptionData finalSubscriptionData = subscriptionData;

        // There are two type of ack mode:
        // 1. ack by KV service
        // 2. ack by file merge service, default mode
        if (brokerConfig.isPopConsumerKVServiceEnable()) {

            CompletableFuture<PopConsumerContext> popAsyncFuture = brokerController.getPopConsumerService().popAsync(
                RemotingHelper.parseChannelRemoteAddr(channel), beginTimeMills, requestHeader.getInvisibleTime(),
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(),
                requestHeader.getAttemptId(), requestHeader.getInitMode(), messageFilter);

            popAsyncFuture.thenApply(result -> {
                // callback
                try {
                    if (request.getCallbackList() != null) {
                        request.getCallbackList().forEach(CommandCallback::accept);
                        request.getCallbackList().clear();
                    }
                } catch (Throwable t) {
                    POP_LOGGER.error("PopProcessor execute callback error", t);
                }

                // long polling process
                if (result.isFound()) {
                    response.setCode(ResponseCode.SUCCESS);
                    getMessageResult.setStatus(GetMessageStatus.FOUND);
                    // recursive processing
                    if (result.getRestCount() > 0) {
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                } else {
                    POP_LOGGER.debug("Processor not found, polling request, popTime={}, restCount={}",
                        result.getPopTime(), result.getRestCount());

                    PollingResult pollingResult = popLongPollingService.polling(
                        ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);

                    if (PollingResult.POLLING_SUC == pollingResult) {
                        // recursive processing
                        if (result.getRestCount() > 0) {
                            popLongPollingService.notifyMessageArriving(
                                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                                null, 0L, null, null);
                        }
                        return null;
                    } else if (PollingResult.POLLING_FULL == pollingResult) {
                        response.setCode(ResponseCode.POLLING_FULL);
                    } else {
                        response.setCode(ResponseCode.POLLING_TIMEOUT);
                    }
                    getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
                }

                // format response
                responseHeader.setPopTime(result.getPopTime());
                responseHeader.setInvisibleTime(result.getInvisibleTime());
                responseHeader.setReviveQid(
                    requestHeader.isOrder() ? KeyBuilder.POP_ORDER_REVIVE_QUEUE : 0);
                responseHeader.setRestNum(result.getRestCount());
                responseHeader.setStartOffsetInfo(result.getStartOffsetInfo());
                responseHeader.setMsgOffsetInfo(result.getMsgOffsetInfo());
                if (requestHeader.isOrder() && !result.getOrderCountInfo().isEmpty()) {
                    responseHeader.setOrderCountInfo(result.getOrderCountInfo());
                }

                response.setRemark(getMessageResult.getStatus().name());
                if (response.getCode() != ResponseCode.SUCCESS) {
                    return response;
                }

                // add message
                result.getGetMessageResultList().forEach(temp -> {
                    for (int i = 0; i < temp.getMessageMapedList().size(); i++) {
                        getMessageResult.addMessage(temp.getMessageMapedList().get(i));
                    }
                });

                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    final byte[] r = this.readGetMessageResult(getMessageResult,
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                } else {
                    final GetMessageResult tmpGetMessageResult = getMessageResult;
                    try {
                        FileRegion fileRegion = new ManyMessageTransfer(
                            response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        channel.writeAndFlush(fileRegion)
                            .addListener((ChannelFutureListener) future -> {
                                tmpGetMessageResult.release();
                                RemotingMetricsManager remotingMetricsManager = brokerController.getBrokerMetricsManager().getRemotingMetricsManager();
                                Attributes attributes = remotingMetricsManager.newAttributesBuilder()
                                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                                    .put(LABEL_RESULT, remotingMetricsManager.getWriteAndFlushResult(future))
                                    .build();
                                remotingMetricsManager.getRpcLatency().record(
                                    request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                if (!future.isSuccess()) {
                                    POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                        channel.remoteAddress(), future.cause());
                                }
                            });
                    } catch (Throwable e) {
                        POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }
                    return null;
                }
                return response;
            }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result, null, brokerController.getBrokerMetricsManager().getRemotingMetricsManager()));
            return null;
        } // end of ack by kv service

        // start of ack by file merge service mode
        // init pop parameters
        int randomQ = random.nextInt(100);
        int reviveQid;
        if (requestHeader.isOrder()) {
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() %
                this.brokerController.getBrokerConfig().getReviveQueueNum());
        }

        // properties of rocketmq 4.x, useless in 5.x
        StringBuilder startOffsetInfo = new StringBuilder(64);
        // properties of rocketmq 4.x, useless in 5.x
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        // properties of rocketmq 4.x, useless in 5.x
        StringBuilder orderCountInfo = requestHeader.isOrder() ? new StringBuilder(64) : null;

        // Due to the design of the fields startOffsetInfo, msgOffsetInfo, and orderCountInfo,
        // a single POP request could only invoke the popMsgFromQueue method once
        // for either a normal topic or a retry topic's queue. Retry topics v1 and v2 are
        // considered the same type because they share the same retry flag in previous fields.
        // Therefore, needRetryV1 is designed as a subset of needRetry, and within a single request,
        // only one type of retry topic is able to call popMsgFromQueue.
        boolean usePriorityMode = TopicMessageType.PRIORITY.equals(topicConfig.getTopicMessageType())
            && !requestHeader.isOrder() && randomQ < subscriptionGroupConfig.getPriorityFactor();
        boolean needRetry = randomQ < (usePriorityMode ?
            brokerConfig.getPopFromRetryProbabilityForPriority() : brokerConfig.getPopFromRetryProbability());
        boolean needRetryV1 = false;
        if (brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
            needRetryV1 = randomQ % 2 == 0;
        }
        randomQ = usePriorityMode ? 0 : randomQ; // reset randomQ
        long popTime = System.currentTimeMillis();

        // pop message
        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        if (needRetry && !requestHeader.isOrder()) {
            if (needRetryV1) {
                String retryTopic = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }

        if (requestHeader.getQueueId() < 0) {
            // read all queue
            getMessageFuture = popMsgFromTopic(topicConfig, false, getMessageResult, requestHeader, reviveQid, channel,
                popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
        } else {
            int queueId = requestHeader.getQueueId();
            getMessageFuture = getMessageFuture.thenCompose(restNum ->
                popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), false,
                    getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                    startOffsetInfo, msgOffsetInfo, orderCountInfo));
        }

        // if not full , fetch retry again
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            if (needRetryV1) {
                String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopicV1, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }

        // async result handle
        final RemotingCommand finalResponse = response;
        getMessageFuture.thenApply(restNum -> {
            try {
                if (request.getCallbackList() != null) {
                    request.getCallbackList().forEach(CommandCallback::accept);
                    request.getCallbackList().clear();
                }
            } catch (Throwable t) {
                POP_LOGGER.error("PopProcessor execute callback error", t);
            }

            // long polling used in version 4.*, useless in 5.*
            if (!getMessageResult.getMessageBufferList().isEmpty()) {
                finalResponse.setCode(ResponseCode.SUCCESS);
                getMessageResult.setStatus(GetMessageStatus.FOUND);
                if (restNum > 0) {
                    // all queue pop can not notify specified queue pop, and vice versa
                    popLongPollingService.notifyMessageArriving(
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                        null, 0L, null, null);
                }
            } else {
                PollingResult pollingResult = popLongPollingService.polling(
                    ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);
                if (PollingResult.POLLING_SUC == pollingResult) {
                    if (restNum > 0) {
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                    return null;
                } else if (PollingResult.POLLING_FULL == pollingResult) {
                    finalResponse.setCode(ResponseCode.POLLING_FULL);
                } else {
                    finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
                }
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }

            // format response
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(reviveQid);
            responseHeader.setRestNum(restNum);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            if (requestHeader.isOrder() && orderCountInfo != null) {
                responseHeader.setOrderCountInfo(orderCountInfo.toString());
            }
            finalResponse.setRemark(getMessageResult.getStatus().name());

            // transfer msg by heap or zero copy,
            // zero copy used in 4.*, useless in 5.*
            switch (finalResponse.getCode()) {
                case ResponseCode.SUCCESS:
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        finalResponse.setBody(r);
                    } else { // zero copy
                        final GetMessageResult tmpGetMessageResult = getMessageResult;
                        try {
                            FileRegion fileRegion =
                                new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()),
                                    getMessageResult);
                            channel.writeAndFlush(fileRegion)
                                .addListener((ChannelFutureListener) future -> {
                                    tmpGetMessageResult.release();
                                    RemotingMetricsManager remotingMetricsManager = brokerController.getBrokerMetricsManager().getRemotingMetricsManager();
                                    Attributes attributes = remotingMetricsManager.newAttributesBuilder()
                                        .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                        .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                        .put(LABEL_RESULT, remotingMetricsManager.getWriteAndFlushResult(future))
                                        .build();
                                    remotingMetricsManager.getRpcLatency().record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                            channel.remoteAddress(), future.cause());
                                    }
                                });
                        } catch (Throwable e) {
                            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                            getMessageResult.release();
                        }

                        return null;
                    }
                    break;
                default:
                    return finalResponse;
            }
            return finalResponse;
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result, null, brokerController.getBrokerMetricsManager().getRemotingMetricsManager()));
        return null;
    }

    /**
     * Pop messages from every read queue of the given topic.
     *
     * <p>Queues are visited sequentially (respecting {@code priorityOrderAsc}).
     * For each queue a {@link #popMsgFromQueue} call is chained via
     * {@code CompletableFuture#thenCompose}. The chained future carries the
     * <em>remaining</em> number of messages still needed ({@code restNum}).
     *
     * <p>Early termination can occur inside {@link #popMsgFromQueue} when:
     * <ul>
     *   <li>the queue lock cannot be acquired</li>
     *   <li>too many in-flight (un-acked) messages exist</li>
     *   <li>an order queue is blocked</li>
     *   <li>the accumulated message count already reaches {@code maxMsgNums}</li>
     * </ul>
     *
     * @param topicConfig   topic configuration; {@code null} skips all queues
     * @param isRetry       whether the topic is a retry topic
     * @param getMessageResult accumulator for the messages popped so far
     * @param requestHeader pop request parameters
     * @param reviveQid     revive queue id
     * @param channel       netty channel of the requesting client
     * @param popTime       pop timestamp
     * @param messageFilter expression filter applied to each message
     * @param startOffsetInfo buffer for offset tracing info
     * @param msgOffsetInfo   buffer for per-message offset tracing info
     * @param orderCountInfo  buffer for order-consume count info
     * @param randomQ       random queue offset for round-robin load balancing
     * @param getMessageFuture future that carries the remaining message count
     * @return a future completing with the remaining number of messages needed
     */
    private CompletableFuture<Long> popMsgFromTopic(TopicConfig topicConfig, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        if (topicConfig != null) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                int index = (brokerController.getBrokerConfig().isPriorityOrderAsc() ?
                    topicConfig.getReadQueueNums() - 1 - i : i) + randomQ;
                int queueId = index % topicConfig.getReadQueueNums();
                getMessageFuture = getMessageFuture.thenCompose(restNum ->
                    popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), isRetry,
                        getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, messageFilter,
                        startOffsetInfo, msgOffsetInfo, orderCountInfo));
            }
        }
        return getMessageFuture;
    }

    private CompletableFuture<Long> popMsgFromTopic(String topic, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        return popMsgFromTopic(topicConfig, isRetry, getMessageResult, requestHeader, reviveQid, channel, popTime,
            messageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
    }

    /**
     * Pop messages from a specific queue of a topic.
     *
     * <p>This method is called as a step in a {@link CompletableFuture} chain
     * (see {@link #popMsgFromTopic}). The {@code restNum} argument is the
     * number of messages still needed — when it drops to {@code 0} or below,
     * subsequent calls in the chain may short-circuit early.
     *
     * <p>The method has several <b>early-termination</b> paths (all return
     * immediately with the current {@code restNum}):
     * <ul>
     *   <li>Queue lock cannot be acquired — skips this queue</li>
     *   <li>Too many in-flight (un-acked) messages for this
     *       {@code topic@group@queueId}</li>
     *   <li>Order queue is blocked by a previous un-acked message</li>
     *   <li>Already accumulated {@code >= maxMsgNums} messages</li>
     * </ul>
     *
     * <p>Otherwise, it asynchronously fetches messages from the store, handles
     * offset correction, updates order-consume tracking / checkpoint data, and
     * merges the results into {@code getMessageResult}.
     *
     * @param topic            topic name
     * @param attemptId        attempt id for idempotent consumption
     * @param isRetry          whether this is a retry topic
     * @param getMessageResult accumulator for messages popped so far
     * @param requestHeader    pop request parameters
     * @param queueId          target queue id
     * @param restNum          number of messages still needed before the batch
     *                         size is satisfied
     * @param reviveQid        revive queue id for checkpoint
     * @param channel          netty channel of the requesting client
     * @param popTime          pop invocation timestamp
     * @param messageFilter    expression filter applied to each message
     * @param startOffsetInfo  buffer for offset tracing info
     * @param msgOffsetInfo    buffer for per-message offset tracing info
     * @param orderCountInfo   buffer for order-consume count info
     * @return a future completing with the remaining number of messages needed
     */
    private CompletableFuture<Long> popMsgFromQueue(String topic, String attemptId, boolean isRetry,
        GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {

        // get pop offset
        String lockKey =
            topic + PopAckConstants.SPLIT + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + queueId;
        boolean isOrder = requestHeader.isOrder();
        long offset;
        try {
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                false, lockKey, false);
        } catch (ConsumeQueueException e) {
            CompletableFuture<Long> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }

        // try lock
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (!queueLockManager.tryLock(lockKey)) {
            try {
                if (!requestHeader.isOrder()) {
                    restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                }
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            return future;
        }
        future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));

        // check inflight message number
        if (isPopShouldStop(topic, requestHeader.getConsumerGroup(), queueId)) {
            POP_LOGGER.warn("Too much msgs unacked, then stop popping. topic={}, group={}, queueId={}",
                topic, requestHeader.getConsumerGroup(), queueId);
            try {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        // check orderly lock and max message number
        try {
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                true, lockKey, true);

            // Current requests would calculate the total number of messages
            // waiting to be filtered for new message arrival notifications in
            // the long-polling service, need disregarding the backlog in order
            // consumption scenario. If rest message num including the blocked
            // queue accumulation would lead to frequent unnecessary wake-ups
            // of long-polling requests, resulting unnecessary CPU usage.
            // When client ack message, long-polling request would be notifications
            // by AckMessageProcessor.ackOrderly() and message will not be delayed.
            if (isOrder) {
                if (brokerController.getConsumerOrderInfoManager().checkBlock(
                    attemptId, topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                    // should not add accumulation(max offset - consumer offset) here
                    future.complete(restNum);
                    return future;
                }
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic, requestHeader.getConsumerGroup(), queueId);
            }

            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }

        AtomicLong atomicRestNum = new AtomicLong(restNum);
        AtomicLong atomicOffset = new AtomicLong(offset);
        long finalOffset = offset;
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter)
            // result check and retry if offset is not correct
            .thenCompose(result -> {
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // maybe store offset is not correct.
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // commit offset, because the offset is not correct
                    // If offset in store is greater than cq offset, it will cause duplicate messages,
                    // because offset in PopBuffer is not committed.
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}",
                        lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                        queueId, result.getNextBeginOffset());
                    atomicOffset.set(result.getNextBeginOffset());
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            })
            // update order info or append checkpoint then format result
            .thenApply(result -> {
                if (result == null) {
                    try {
                        atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    } catch (ConsumeQueueException e) {
                        POP_LOGGER.error("Failed to get max offset in queue", e);
                    }
                    return atomicRestNum.get();
                }
                if (!result.getMessageMapedList().isEmpty()) {
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic,
                        result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic,
                        result.getBufferTotalSize());

                    Attributes attributes = this.brokerController.getBrokerMetricsManager().newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    this.brokerController.getBrokerMetricsManager().getMessagesOutTotal().add(result.getMessageCount(), attributes);
                    this.brokerController.getBrokerMetricsManager().getThroughputOutTotal().add(result.getBufferTotalSize(), attributes);

                    if (isOrder) {
                        this.brokerController.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic,
                            requestHeader.getConsumerGroup(),
                            queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(),
                            orderCountInfo, result);
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, queueId, finalOffset);
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    if (isOrder) {
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                            queueId, result.getNextBeginOffset());
                    } else {
                        popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                            requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
                    }
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // We should not recode buffer when popResponseReturnActualRetryTopic is true or topic is not retry topic
                    if (brokerController.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                                // Set retry message topic to origin topic and clear message store size to recode
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);

                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                return atomicRestNum.get();
            })
            // unlock queueLock
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                queueLockManager.unLock(lockKey);
            });
    }

    private boolean isPopShouldStop(String topic, String group, int queueId) {
        return brokerController.getBrokerConfig().isEnablePopMessageThreshold() &&
            brokerController.getPopInflightMessageCounter().getGroupPopInFlightMessageNum(topic, group, queueId) > brokerController.getBrokerConfig().getPopInflightMessageThreshold();
    }

    /**
     * get consume offset for pop mode
     * called by:
     *  - this.popMsgFromQueue()
     * functionality:
     *  - return resetOffset if exists
     *  - get offset if exists
     *  - init offset if not exists
     *  - get offset from popBufferMergeService
     *
     * @param topic topic
     * @param group group
     * @param queueId queueId
     * @param initMode initMode ConsumeInitMode.MAX for pop mode
     * @param init flag of whether commit offset the first time pop message
     * @param lockKey lockKey
     * @param checkResetOffset flag of whether resetPopOffset
     * @return offset
     */
    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) throws ConsumeQueueException {

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            //the first time consume, init offset by initMode
            offset = this.getInitOffset(topic, group, queueId, initMode, init);
        }

        // before lock checkResetOffset is false
        // after lock checkResetOffset is true
        // This is an admin related feature
        if (checkResetOffset) {
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                return resetOffset;
            }
        }

        long bufferOffset = this.popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return Math.max(bufferOffset, offset);
        }
    }

    /**
     * get offset from consume queue
     * If consume from min offset:
     *      - return min offset.
     * If consume from max offset:
     *      - get max offset
     *      - commit max offset if init is true.
     */
    public long getInitOffset(String topic, String group, int queueId, int initMode, boolean init)
        throws ConsumeQueueException {
        long offset;
        if (ConsumeInitMode.MIN == initMode || topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        } else {
            if (this.brokerController.getBrokerConfig().isInitPopOffsetByCheckMsgInMem() &&
                this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId) <= 0 &&
                this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
                offset = 0;
            } else {
                // pop last one,then commit offset.
                offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
                // max & no consumer offset
                if (offset < 0) {
                    offset = 0;
                }
            }
        }
        if (init) { // whichever initMode
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "getPopOffset", group, topic, queueId, offset);
        }
        return offset;
    }

    public MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // add check point msg to revive log
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            ck.addDiff((int) (msgQueueOffset - offset));
        }

        this.brokerController.getBrokerStatsManager().incBrokerCkNums(1);
        this.brokerController.getBrokerStatsManager().incGroupCkNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        final boolean addBufferSuc = this.popBufferMergeService.addCk(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );

        if (addBufferSuc) {
            return true;
        }
        return this.popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);
        if (resetOffset != null) {
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
            this.getPopBufferMergeService().clearOffsetQueue(lockKey);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        }
        return resetOffset;
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    static class TimedLock {
        private final AtomicBoolean lock;
        private volatile long lockTime;

        public TimedLock() {
            // init lock status, false means not locked
            this.lock = new AtomicBoolean(false);
            this.lockTime = System.currentTimeMillis();
        }

        public boolean tryLock() {
            boolean ret = lock.compareAndSet(false, true);
            if (ret) {
                this.lockTime = System.currentTimeMillis();
                return true;
            } else {
                return false;
            }
        }

        public void unLock() {
            lock.set(false);
        }

        public boolean isLock() {
            return lock.get();
        }

        public long getLockTime() {
            return lockTime;
        }
    }

    public class QueueLockManager extends ServiceThread {
        private final ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);

        public String buildLockKey(String topic, String consumerGroup, int queueId) {
            return topic + PopAckConstants.SPLIT + consumerGroup + PopAckConstants.SPLIT + queueId;
        }

        public boolean tryLock(String topic, String consumerGroup, int queueId) {
            return tryLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public boolean tryLock(String key) {
            TimedLock timedLock = ConcurrentHashMapUtils.computeIfAbsent(expiredLocalCache, key, k -> new TimedLock());
            return timedLock.tryLock();
        }

        /**
         * is not thread safe, may cause duplicate lock
         *
         * @param usedExpireMillis the expired time in millisecond
         * @return total numbers of TimedLock
         */
        public int cleanUnusedLock(final long usedExpireMillis) {
            Iterator<Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();

            int total = 0;
            while (iterator.hasNext()) {
                Entry<String, TimedLock> entry = iterator.next();

                if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                    iterator.remove();
                    POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                        entry.getValue().getLockTime(),
                        entry.getValue().isLock());
                }

                total++;
            }

            return total;
        }

        public void unLock(String topic, String consumerGroup, int queueId) {
            unLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public void unLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);
            if (timedLock != null) {
                timedLock.unLock();
            }
        }

        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
            }
            return QueueLockManager.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    this.waitForRunning(60000);
                    int count = cleanUnusedLock(60000);
                    POP_LOGGER.info("QueueLockSize={}", count);
                } catch (Exception e) {
                    PopMessageProcessor.POP_LOGGER.error("QueueLockManager run error", e);
                }
            }
        }
    }
}
