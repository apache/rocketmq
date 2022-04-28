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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseWriter;
import org.apache.rocketmq.proxy.grpc.v2.service.BaseService;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResponseStreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResultFilter;

public class DefaultReceiveMessageResponseStreamWriter extends ReceiveMessageResponseStreamWriter {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected static final long NACK_INVISIBLE_TIME = Duration.ofSeconds(1).toMillis();
    protected final ForwardWriteConsumer writeConsumer;
    protected final TopicRouteCache topicRouteCache;
    protected volatile ReceiveMessageResultFilter receiveMessageResultFilter;

    public DefaultReceiveMessageResponseStreamWriter(
        StreamObserver<ReceiveMessageResponse> observer,
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook,
        ForwardWriteConsumer writeConsumer,
        TopicRouteCache topicRouteCache,
        ReceiveMessageResultFilter receiveMessageResultFilter) {
        super(observer, hook);
        this.writeConsumer = writeConsumer;
        this.topicRouteCache = topicRouteCache;
        this.receiveMessageResultFilter = receiveMessageResultFilter;
    }

    @Override
    public void write(Context ctx, ReceiveMessageRequest request, PopStatus status, List<MessageExt> messageFoundList) {
        ReceiveMessageResponseStreamObserver responseStreamObserver = new ReceiveMessageResponseStreamObserver(
            ctx,
            request,
            receiveMessageHook,
            streamObserver);
        try {
            switch (status) {
                case FOUND:
                    List<Message> messageList = this.receiveMessageResultFilter.filterMessage(ctx, request, messageFoundList);
                    if (messageList.isEmpty()) {
                        responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.buildStatus(Code.OK, "no new message"))
                            .build());
                    } else {
                        responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                            .build());
                        Iterator<Message> messageIterator = messageList.iterator();
                        while (messageIterator.hasNext()) {
                            if (responseStreamObserver.isCancelled()) {
                                break;
                            }
                            responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                                .setMessage(messageIterator.next())
                                .build());
                        }
                        messageIterator.forEachRemaining(message -> this.nackFailToWriteMessage(ctx, request, message));
                    }
                    break;
                case POLLING_FULL:
                    responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.TOO_MANY_REQUESTS, "polling full"))
                        .build());
                    break;
                case NO_NEW_MSG:
                case POLLING_NOT_FOUND:
                default:
                    responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.OK, "no new message"))
                        .build());
                    break;
            }
        } catch (Throwable t) {
            write(ctx, request, t);
        } finally {
            responseStreamObserver.onCompleted();
        }
    }

    @Override
    public void write(Context ctx, ReceiveMessageRequest request, Throwable throwable) {
        ReceiveMessageResponseStreamObserver responseStreamObserver = new ReceiveMessageResponseStreamObserver(
            ctx,
            request,
            receiveMessageHook,
            streamObserver);
        ResponseWriter.write(
            responseStreamObserver,
            ReceiveMessageResponse.newBuilder().setStatus(ResponseBuilder.buildStatus(throwable)).build()
        );
    }

    protected void nackFailToWriteMessage(Context ctx, ReceiveMessageRequest request, Message message) {
        try {
            String receiptHandleStr = message.getSystemProperties().getReceiptHandle();
            ReceiptHandle handle = BaseService.resolveReceiptHandle(ctx, receiptHandleStr);
            String brokerAddr = BaseService.getBrokerAddr(ctx, this.topicRouteCache, handle.getBrokerName());

            String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
            String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
            changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
            changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
            changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
            changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
            changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
            changeInvisibleTimeRequestHeader.setInvisibleTime(NACK_INVISIBLE_TIME);

            this.writeConsumer.changeInvisibleTimeAsync(
                ctx,
                brokerAddr,
                handle.getBrokerName(),
                message.getSystemProperties().getMessageId(),
                changeInvisibleTimeRequestHeader
            ).whenComplete((ackResult, t) -> {
                if (t != null) {
                    log.warn("err when nack message. request:{}, message:{}", request, message, t);
                } else if (!AckStatus.OK.equals(ackResult.getStatus())) {
                    log.warn("nack failed. request:{}, message:{}, ackResult:{}", request, message, ackResult);
                }
            });
        } catch (Throwable t) {
            log.warn("err when nack message. request:{}, message:{}", request, message, t);
        }
    }

    public ReceiveMessageResultFilter getReceiveMessageResultFilter() {
        return receiveMessageResultFilter;
    }

    public void setReceiveMessageResultFilter(
        ReceiveMessageResultFilter receiveMessageResultFilter) {
        this.receiveMessageResultFilter = receiveMessageResultFilter;
    }
}
