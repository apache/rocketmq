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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteMetadataUtil;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.broker.pop.PopConsumerService;
import org.apache.rocketmq.broker.pop.PopConsumerService;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;

/**
 * Processes consumer ack messages in Pop consumption mode.
 *
 * <p>Handles both single ({@link RequestCode#ACK_MESSAGE}) and batch
 * ({@link RequestCode#BATCH_ACK_MESSAGE}) acks. Each ack is processed
 * through one of two paths:
 * <ul>
 *   <li><b>KVStore path</b> ({@code popConsumerKVServiceEnable=true}) —
 *       delegates to {@link PopConsumerService#ackAsync}</li>
 *   <li><b>File-based path</b> — tries {@link PopBufferMergeService#addAk}
 *       first; if the buffer merge is not available, writes the ack as a
 *       message to the system revive topic</li>
 * </ul>
 *
 * <p>Orderly ack is handled separately by {@link #ackOrderly} /
 * {@link #ackOrderlyNew}, which update the consumer order info and advance
 * the consumer offset while notifying any long-polling waiters.
 *
 * <p>This class also owns and manages the {@link PopReviveService} instances
 * for the file-based revive path.
 */
public class AckMessageProcessor implements NettyRequestProcessor {

    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final String reviveTopic;
    private final PopReviveService[] popReviveServices;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(
            this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(brokerController, reviveTopic, i);
            this.popReviveServices[i].setShouldRunPopRevive(brokerController.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public PopReviveService[] getPopReviveServices() {
        return popReviveServices;
    }

    public void shutdown() throws Exception {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.shutdown();
        }
    }

    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.shutdown();
        }
    }

    public void setPopReviveServiceStatus(boolean shouldStart) {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isPopReviveServiceRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    /**
     * Process an ack request (single or batch).
     *
     * <p>Routes to one of two paths based on {@code popConsumerKVServiceEnable}:
     * <ul>
     *   <li>{@code true} — {@link #appendAckNew} (KVStore path, delegates to
     *       {@link PopConsumerService#ackAsync})</li>
     *   <li>{@code false} — {@link #appendAck} (file-based path, tries
     *       {@link PopBufferMergeService#addAk} first, then writes to revive topic)</li>
     * </ul>
     *
     * <p>Orderly acks ({@code rqId == POP_ORDER_REVIVE_QUEUE}) are handled by
     * {@link #ackOrderly} / {@link #ackOrderlyNew} instead.
     *
     * @param channel           the Netty channel of the requesting client
     * @param request           the incoming request
     * @param brokerAllowSuspend whether the broker may suspend the request
     * @return the response to send back to the client
     * @throws RemotingCommandException if the request cannot be decoded
     */
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {
        // init context params
        AckMessageRequestHeader requestHeader;
        BatchAckMessageRequestBody reqBody = null;
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());

        if (request.getCode() == RequestCode.ACK_MESSAGE) {
            // decode and validate request
            requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
            if (null == topicConfig) {
                POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
                return response;
            }

            if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
                String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                    requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
                return response;
            }

            RemotingCommand ackLiteResponse = ackLite(requestHeader, null, response, channel);
            if (ackLiteResponse != null) {
                return ackLiteResponse;
            }

            // get and validate offset
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            long maxOffset;
            try {
                maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to get max offset", e);
            }
            if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
                String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.NO_MESSAGE);
                response.setRemark(errorInfo);
                return response;
            }

            // append ack, default mode is queue based merge, call appendAck
            if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
                appendAckNew(requestHeader, null, response, channel, null);
            } else {
                appendAck(requestHeader, null, response, channel, null);
            }
        } else if (request.getCode() == RequestCode.BATCH_ACK_MESSAGE) {
            // decode and validate request
            if (request.getBody() != null) {
                reqBody = BatchAckMessageRequestBody.decode(request.getBody(), BatchAckMessageRequestBody.class);
            }
            if (reqBody == null || reqBody.getAcks() == null || reqBody.getAcks().isEmpty()) {
                response.setCode(ResponseCode.NO_MESSAGE);
                return response;
            }

            // process each ack
            for (BatchAck bAck : reqBody.getAcks()) {
                // default value of popConsumerKVServiceEnable is false
                if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
                    appendAckNew(null, bAck, response, channel, reqBody.getBrokerName());
                } else {
                    appendAck(null, bAck, response, channel, reqBody.getBrokerName());
                }
            }
        } else {
            // unsupported request, logging and return
            POP_LOGGER.error("AckMessageProcessor failed to process RequestCode: {}, consumer: {} ", request.getCode(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(String.format("AckMessageProcessor failed to process RequestCode: %d", request.getCode()));
            return response;
        }
        return response;
    }

    /**
     * Append an ack (single or batch) in the <b>file-based path</b>.
     *
     * <p>For <b>single ack</b>: parses the extra info from the request header,
     * routes orderly acks to {@link #ackOrderly}, or creates a single {@link AckMsg}.
     *
     * <p>For <b>batch ack</b>: expands the {@link BitSet} from the
     * {@link BatchAck} into individual offsets, routes orderly acks individually,
     * and packs the remaining offsets into a {@link BatchAckMsg}.
     *
     * <p>The ack is first offered to {@link PopBufferMergeService#addAk}.
     * If the buffer merge is not available, the ack is serialized as JSON and
     * written to the revive topic with tag {@link PopAckConstants#ACK_TAG}
     * or {@link PopAckConstants#BATCH_ACK_TAG}.
     *
     * @param requestHeader the single-ack request header (null for batch)
     * @param batchAck      the batch ack body (null for single)
     * @param response      the response to modify on error
     * @param channel       the Netty channel
     * @param brokerName    the broker name
     * @throws RemotingCommandException if offset validation fails
     */
    private void appendAck(final AckMessageRequestHeader requestHeader, final BatchAck batchAck,
        final RemotingCommand response, final Channel channel, String brokerName) throws RemotingCommandException {
        // init context params
        String[] extraInfo;
        String consumeGroup, topic;
        int qId, rqId;
        long startOffset, ackOffset;
        long popTime, invisibleTime;
        AckMsg ackMsg;
        int ackCount = 0;

        // ack orderly or set context params
        if (batchAck == null) {
            // single ack
            // set context params
            extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            brokerName = ExtraInfoUtil.getBrokerName(extraInfo);
            consumeGroup = requestHeader.getConsumerGroup();
            topic = requestHeader.getTopic();
            qId = requestHeader.getQueueId();
            rqId = ExtraInfoUtil.getReviveQid(extraInfo);
            startOffset = ExtraInfoUtil.getCkQueueOffset(extraInfo);
            ackOffset = requestHeader.getOffset();
            popTime = ExtraInfoUtil.getPopTime(extraInfo);
            invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

            // ack orderly if revive queue
            if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                ackOrderly(topic, consumeGroup, qId, ackOffset, popTime, invisibleTime, channel, response);
                return;
            }

            // set ackMsg and ackCount
            ackMsg = new AckMsg();
            ackCount = 1;
        } else {
            // batch ack
            // set context params
            consumeGroup = batchAck.getConsumerGroup();
            topic = ExtraInfoUtil.getRealTopic(batchAck.getTopic(), batchAck.getConsumerGroup(), batchAck.getRetry());
            qId = batchAck.getQueueId();
            rqId = batchAck.getReviveQueueId();
            startOffset = batchAck.getStartOffset();
            ackOffset = -1;
            popTime = batchAck.getPopTime();
            invisibleTime = batchAck.getInvisibleTime();

            // offset check
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, qId);
            long maxOffset;
            try {
                maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, qId);
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to get max offset in queue", e);
            }
            if (minOffset == -1 || maxOffset == -1) {
                POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
                return;
            }

            // ack orderly or add offset to batchAckMsg
            BatchAckMsg batchAckMsg = new BatchAckMsg();
            BitSet bitSet = batchAck.getBitSet();
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                if (i == Integer.MAX_VALUE) {
                    break;
                }
                long offset = startOffset + i;
                if (offset < minOffset || offset > maxOffset) {
                    continue;
                }
                if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                    ackOrderly(topic, consumeGroup, qId, offset, popTime, invisibleTime, channel, response);
                } else {
                    batchAckMsg.getAckOffsetList().add(offset);
                }
            }

            // skip if empty or is revive queue
            if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE || batchAckMsg.getAckOffsetList().isEmpty()) {
                return;
            }

            // set ackMsg and ackCount
            ackMsg = batchAckMsg;
            ackCount = batchAckMsg.getAckOffsetList().size();
        }

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(consumeGroup, topic, ackCount);

        // set ackMsg
        ackMsg.setConsumerGroup(consumeGroup);
        ackMsg.setTopic(topic);
        ackMsg.setQueueId(qId);
        ackMsg.setStartOffset(startOffset);
        ackMsg.setAckOffset(ackOffset);
        ackMsg.setPopTime(popTime);
        ackMsg.setBrokerName(brokerName);

        // add ackMsg
        if (this.brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
            return;
        }

        // create revive message by ackMsg, if add ackMsg failed
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(rqId);
        if (ackMsg instanceof BatchAckMsg) {
            msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genBatchAckUniqueId((BatchAckMsg) ackMsg));
        } else {
            msgInner.setTags(PopAckConstants.ACK_TAG);
            msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        }
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(popTime + invisibleTime);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // store revive message
        if (brokerController.getBrokerConfig().isAppendAckAsync()) { // default is false
            int finalAckCount = ackCount;
            this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handlePutMessageResult(putMessageResult, ackMsg, topic, consumeGroup, popTime, qId, finalAckCount);
            }).exceptionally(throwable -> {
                handlePutMessageResult(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null, false),
                    ackMsg, topic, consumeGroup, popTime, qId, finalAckCount);
                POP_LOGGER.error("put ack msg error ", throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handlePutMessageResult(putMessageResult, ackMsg, topic, consumeGroup, popTime, qId, ackCount);
        }
    }

    private void appendAckNew(final AckMessageRequestHeader requestHeader, final BatchAck batchAck,
        final RemotingCommand response, final Channel channel, String brokerName) throws RemotingCommandException {

        if (requestHeader != null && batchAck == null) {
            // init context params
            String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
            String groupId = requestHeader.getConsumerGroup();
            String topicId = requestHeader.getTopic();
            int queueId = requestHeader.getQueueId();
            long ackOffset = requestHeader.getOffset();
            long popTime = ExtraInfoUtil.getPopTime(extraInfo);
            long invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

            int reviveQueueId = ExtraInfoUtil.getReviveQid(extraInfo);

            if (reviveQueueId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                ackOrderlyNew(topicId, groupId, queueId, ackOffset, popTime, invisibleTime, channel, response);
            } else {
                this.brokerController.getPopConsumerService().ackAsync(
                    popTime, invisibleTime, groupId, topicId, queueId, ackOffset);
            }

            this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
            this.brokerController.getBrokerStatsManager().incGroupAckNums(groupId, topicId, 1);
        } else {
            // init context params
            String groupId = batchAck.getConsumerGroup();
            String topicId = ExtraInfoUtil.getRealTopic(
                batchAck.getTopic(), batchAck.getConsumerGroup(), batchAck.getRetry());
            int queueId = batchAck.getQueueId();
            int reviveQueueId = batchAck.getReviveQueueId();
            long startOffset = batchAck.getStartOffset();
            long popTime = batchAck.getPopTime();
            long invisibleTime = batchAck.getInvisibleTime();

            try {
                // get minOffset and maxOffset
                long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topicId, queueId);
                long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);
                if (minOffset == -1 || maxOffset == -1) {
                    POP_LOGGER.error("Illegal topic or queue found when batch ack {}", batchAck);
                    return;
                }

                int ackCount = 0;
                // Maintain consistency with the old implementation code style
                BitSet bitSet = batchAck.getBitSet();
                for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                    // validate offset
                    if (i == Integer.MAX_VALUE) {
                        break;
                    }
                    long offset = startOffset + i;
                    if (offset < minOffset || offset > maxOffset) {
                        continue;
                    }
                    if (reviveQueueId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
                        ackOrderlyNew(topicId, groupId, queueId, offset, popTime, invisibleTime, channel, response);
                    } else {
                        this.brokerController.getPopConsumerService().ackAsync(
                            popTime, invisibleTime, groupId, topicId, queueId, offset);
                    }
                    ackCount++;
                }

                this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
                this.brokerController.getBrokerStatsManager().incGroupAckNums(groupId, topicId, ackCount);
            } catch (ConsumeQueueException e) {
                throw new RemotingCommandException("Failed to ack message", e);
            }
        }
    }

    private void handlePutMessageResult(PutMessageResult putMessageResult, AckMsg ackMsg, String topic,
        String consumeGroup, long popTime, int qId, int ackCount) {
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }
        brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, ackCount);
    }

    protected void ackOrderly(String topic, String consumeGroup, int qId, long ackOffset, long popTime,
        long invisibleTime, Channel channel, RemotingCommand response) {
        String lockKey = topic + PopAckConstants.SPLIT + consumeGroup + PopAckConstants.SPLIT + qId;
        long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
        if (ackOffset < oldOffset) {
            return;
        }
        while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(lockKey)) {
        }
        try {
            oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumeGroup, topic, qId);
            if (ackOffset < oldOffset) {
                return;
            }
            long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(
                topic, consumeGroup, qId, ackOffset, popTime);
            if (nextOffset > -1) {
                if (!this.brokerController.getConsumerOffsetManager().hasOffsetReset(topic, consumeGroup, qId)) {
                    this.brokerController.getConsumerOffsetManager().commitOffset(
                        channel.remoteAddress().toString(), consumeGroup, topic, qId, nextOffset);
                }
                if (!this.brokerController.getConsumerOrderInfoManager().checkBlock(null, topic, consumeGroup, qId, invisibleTime)) {
                    this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, qId, consumeGroup);
                }
            } else if (nextOffset == -1) {
                String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s",
                    lockKey, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
                return;
            }
        } finally {
            this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(lockKey);
        }
        brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(topic, consumeGroup, popTime, qId, 1);
    }

    protected void ackOrderlyNew(String topic, String consumeGroup, int qId, long ackOffset, long popTime,
        long invisibleTime, Channel channel, RemotingCommand response) {

        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
        ConsumerOrderInfoManager consumerOrderInfoManager = brokerController.getConsumerOrderInfoManager();
        PopConsumerLockService consumerLockService = this.brokerController.getPopConsumerService().getConsumerLockService();

        long oldOffset = consumerOffsetManager.queryOffset(consumeGroup, topic, qId);
        if (ackOffset < oldOffset) {
            return;
        }

        while (!consumerLockService.tryLock(consumeGroup, topic)) {
        }

        try {
            // double check
            oldOffset = consumerOffsetManager.queryOffset(consumeGroup, topic, qId);
            if (ackOffset < oldOffset) {
                return;
            }

            long nextOffset = consumerOrderInfoManager.commitAndNext(topic, consumeGroup, qId, ackOffset, popTime);
            if (brokerController.getBrokerConfig().isPopConsumerKVServiceLog()) {
                POP_LOGGER.info("PopConsumerService ack orderly, time={}, topicId={}, groupId={}, queueId={}, " +
                    "offset={}, next={}", popTime, topic, consumeGroup, qId, ackOffset, nextOffset);
            }

            if (nextOffset > -1L) {
                if (!consumerOffsetManager.hasOffsetReset(topic, consumeGroup, qId)) {
                    String remoteAddress = RemotingHelper.parseSocketAddressAddr(channel.remoteAddress());
                    consumerOffsetManager.commitOffset(remoteAddress, consumeGroup, topic, qId, nextOffset);
                }
                if (!consumerOrderInfoManager.checkBlock(null, topic, consumeGroup, qId, invisibleTime)) {
                    this.brokerController.getPopMessageProcessor().notifyMessageArriving(topic, qId, consumeGroup);
                }
                return;
            }

            if (nextOffset == -1) {
                String errorInfo = String.format("offset is illegal, key:%s %s %s, old:%d, commit:%d, next:%d, %s",
                    consumeGroup, topic, qId, oldOffset, ackOffset, nextOffset, channel.remoteAddress());
                POP_LOGGER.warn(errorInfo);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(errorInfo);
            }
        } finally {
            consumerLockService.unlock(consumeGroup, topic);
        }
    }

    /**
     * Currently, batch ack for lite messages is not supported, so we should ensure that all acknowledgements are individual.
     */
    protected RemotingCommand ackLite(AckMessageRequestHeader requestHeader, BatchAckMessageRequestBody batchAckBody,
        final RemotingCommand response, final Channel channel) {
        if (batchAckBody != null) {
            POP_LOGGER.warn("bad request, batch ack lite, {}", batchAckBody);
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("batch ack lite is not supported.");
            return response;
        }
        if (StringUtils.isBlank(requestHeader.getLiteTopic())) {
            return null;
        }
        String group = requestHeader.getConsumerGroup();
        if (!requestHeader.getTopic().equals(LiteMetadataUtil.getLiteBindTopic(group, brokerController))) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("group type or bind topic not match.");
            return response;
        }

        String lmqName = LiteUtil.toLmqName(requestHeader.getTopic(), requestHeader.getLiteTopic());
        long ackOffset = requestHeader.getOffset();
        long maxOffset = this.brokerController.getLiteLifecycleManager().getMaxOffsetInQueue(lmqName);
        if (ackOffset > maxOffset) {
            POP_LOGGER.warn("ack lite offset illegal, {}, {}, {}", lmqName, ackOffset, maxOffset);
            response.setCode(ResponseCode.NO_MESSAGE);
            response.setRemark("ack offset illegal.");
            return response;
        }
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());
        if (requestHeader.getQueueId() != 0
            || ExtraInfoUtil.getReviveQid(extraInfo) != KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("ack queue illegal.");
            return response;
        }

        long popTime = ExtraInfoUtil.getPopTime(extraInfo);
        long invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfo);

        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
        ConsumerOrderInfoManager consumerOrderInfoManager =
            brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager();
        PopConsumerLockService consumerLockService = this.brokerController.getPopLiteMessageProcessor().getLockService();

        long oldOffset = consumerOffsetManager.queryOffset(group, lmqName, 0);
        if (ackOffset < oldOffset) {
            return response;
        }
        String lockKey = KeyBuilder.buildPopLiteLockKey(group, lmqName);
        while (!consumerLockService.tryLock(lockKey)) {
        }

        try {
            oldOffset = consumerOffsetManager.queryOffset(group, lmqName, 0);
            if (ackOffset < oldOffset) {
                return response;
            }
            long nextOffset = consumerOrderInfoManager.commitAndNext(lmqName, group, 0, ackOffset, popTime);
            if (nextOffset > -1L) {
                if (!consumerOffsetManager.hasOffsetReset(lmqName, group, 0)) {
                    consumerOffsetManager.commitOffset("AckLiteHost", group, lmqName, 0, nextOffset);
                }
                if (!consumerOrderInfoManager.checkBlock(null, lmqName, group, 0, invisibleTime)) {
                    this.brokerController.getLiteEventDispatcher().dispatch(group, lmqName, 0, nextOffset, -1);
                }
            }
            if (nextOffset == -1) {
                POP_LOGGER.warn("ack lite, nextOffset illegal. lmq:{}, old:{}, commit:{}", lmqName, oldOffset, ackOffset);
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark("ack offset illegal.");
                return response;
            }
        } finally {
            consumerLockService.unlock(lockKey);
        }

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(group, requestHeader.getTopic(), 1);
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }
}
