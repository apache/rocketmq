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

package org.apache.rocketmq.broker.grpc.handler;

import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import com.google.common.base.Stopwatch;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.grpc.adapter.InvocationContext;
import org.apache.rocketmq.broker.stat.Histogram;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.grpc.common.Converter;
import org.apache.rocketmq.grpc.common.ResponseBuilder;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiveMessageResponseHandler implements ResponseHandler<ReceiveMessageRequest, ReceiveMessageResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);
    private final Histogram messageLatency;
    private final String brokerName;

    public ReceiveMessageResponseHandler(String brokerName, Histogram messageLatency) {
        this.brokerName = brokerName;
        this.messageLatency = messageLatency;
    }

    @Override public void handle(RemotingCommand responseCommand, InvocationContext<ReceiveMessageRequest, ReceiveMessageResponse> context) {
        long currentTimeInMillis = System.currentTimeMillis();
        long popCosts = currentTimeInMillis - context.getTimestamp();
        try {
            Stopwatch stopWatch = Stopwatch.createStarted();
            ReceiveMessageResponse.Builder builder = ReceiveMessageResponse.newBuilder();
            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) responseCommand.readCustomHeader();
            builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            builder.setInvisibleDuration(Durations.fromMillis(responseHeader.getInvisibleTime()))
                .setDeliveryTimestamp(Timestamps.fromMillis(responseHeader.getPopTime()));

            ReceiveMessageResponse response;
            if (responseCommand.getCode() == RemotingSysResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(responseCommand.getBody());
                List<MessageExt> msgFoundList = MessageDecoder.decodes(byteBuffer);

                Map<String, Long> startOffsetInfo;
                Map<String, List<Long>> msgOffsetInfo;
                Map<String, Integer> orderCountInfo;
                startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
                msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
                orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
                Map<String/*topicMark@queueId*/, List<Long>/*msg queueOffset*/> sortMap = new HashMap<>(16);
                for (MessageExt messageExt : msgFoundList) {
                    String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                    if (!sortMap.containsKey(key)) {
                        sortMap.put(key, new ArrayList<>(4));
                    }
                    sortMap.get(key).add(messageExt.getQueueOffset());
                }
                Map<String, String> map = new HashMap<>(5);
                for (MessageExt messageExt : msgFoundList) {
                    if (startOffsetInfo == null) {
                        // we should set the check point info to extraInfo field , if the command is popMsg
                        // find pop ck offset
                        String key = messageExt.getTopic() + messageExt.getQueueId();
                        if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
                            String extraInfo = ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(),
                                responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                responseHeader.getReviveQid(), messageExt.getTopic(), brokerName,
                                messageExt.getQueueId());
                            map.put(key, extraInfo);
                        }
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                            map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
                    } else {
                        String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(),
                            messageExt.getQueueId());
                        int index = sortMap.get(key).indexOf(messageExt.getQueueOffset());
                        Long msgQueueOffset = msgOffsetInfo.get(key).get(index);
                        if (msgQueueOffset != messageExt.getQueueOffset()) {
                            LOGGER.warn("Queue offset[{}] of msg is strange, not equal to the stored in msg, {}",
                                msgQueueOffset, messageExt);
                        }
                        String extraInfo = ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(key),
                            responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                            responseHeader.getReviveQid(), messageExt.getTopic(),
                            brokerName, messageExt.getQueueId(), msgQueueOffset);
                        messageExt.setQueueOffset(msgQueueOffset);
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, extraInfo);
                        if (context.getRequest().getFifoFlag() && orderCountInfo != null) {
                            Integer count = orderCountInfo.get(key);
                            if (count != null && count > 0) {
                                messageExt.setReconsumeTimes(count);
                            }
                        }
                    }
                    messageExt.getProperties().computeIfAbsent(MessageConst.PROPERTY_FIRST_POP_TIME,
                        k -> String.valueOf(responseHeader.getPopTime()));
                }

                for (MessageExt messageExt : msgFoundList) {
                    long latency = currentTimeInMillis - messageExt.getStoreTimestamp();
                    messageLatency.countIn((int) (latency / 10));
                    builder.addMessages(Converter.buildMessage(messageExt));
                }
            }
            response = builder.build();
            long elapsed = stopWatch.stop().elapsed(TimeUnit.MILLISECONDS);
            LOGGER.debug("Translating remoting response to gRPC response costs {}ms. Duration request received: {}",
                elapsed, popCosts);
            ResponseWriter.write(context.getStreamObserver(), response);
        } catch (Exception e) {
            LOGGER.error("Unexpected exception raised when handle pop remoting command", e);
            ResponseWriter.writeException(context.getStreamObserver(), e);
        }
    }
}
