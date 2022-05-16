///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.rocketmq.proxy.grpc.v2.service.cluster;
//
//import apache.rocketmq.v2.Message;
//import apache.rocketmq.v2.ReceiveMessageRequest;
//import apache.rocketmq.v2.ReceiveMessageResponse;
//import io.grpc.Context;
//import io.grpc.stub.StreamObserver;
//import java.time.Duration;
//import org.apache.rocketmq.client.consumer.AckStatus;
//import org.apache.rocketmq.common.constant.LoggerName;
//import org.apache.rocketmq.common.consumer.ReceiptHandle;
//import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
//import org.apache.rocketmq.logging.InternalLogger;
//import org.apache.rocketmq.logging.InternalLoggerFactory;
//import org.apache.rocketmq.proxy.service.ForwardWriteConsumer;
//import org.apache.rocketmq.proxy.service.route.TopicRouteService;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
//import org.apache.rocketmq.proxy.grpc.v2.service.BaseService;
//import org.apache.rocketmq.proxy.grpc.v2.service.BaseReceiveMessageResponseStreamWriter;
//import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResultFilter;
//
//public class DefaultReceiveMessageResponseStreamWriter extends BaseReceiveMessageResponseStreamWriter {
//    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
//
//    protected static final long NACK_INVISIBLE_TIME = Duration.ofSeconds(1).toMillis();
//    protected final ForwardWriteConsumer writeConsumer;
//    protected final TopicRouteService topicRouteService;
//
//    public DefaultReceiveMessageResponseStreamWriter(
//        StreamObserver<ReceiveMessageResponse> observer,
//        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook,
//        ForwardWriteConsumer writeConsumer,
//        TopicRouteService topicRouteService,
//        ReceiveMessageResultFilter receiveMessageResultFilter) {
//        super(observer, hook, receiveMessageResultFilter);
//        this.writeConsumer = writeConsumer;
//        this.topicRouteService = topicRouteService;
//    }
//
//    @Override
//    protected void processThrowableWhenWriteMessage(Throwable throwable, Context context, ReceiveMessageRequest request,
//        Message message) {
//        this.nackFailToWriteMessage(context, request, message);
//    }
//
//    protected void nackFailToWriteMessage(Context ctx, ReceiveMessageRequest request, Message message) {
//        try {
//            String receiptHandleStr = message.getSystemProperties().getReceiptHandle();
//            ReceiptHandle handle = BaseService.resolveReceiptHandle(ctx, receiptHandleStr);
//            String brokerAddr = BaseService.getBrokerAddr(ctx, this.topicRouteService, handle.getBrokerName());
//
//            String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
//            String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
//            ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
//            changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
//            changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
//            changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
//            changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
//            changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
//            changeInvisibleTimeRequestHeader.setInvisibleTime(NACK_INVISIBLE_TIME);
//
//            this.writeConsumer.changeInvisibleTimeAsync(
//                ctx,
//                brokerAddr,
//                handle.getBrokerName(),
//                message.getSystemProperties().getMessageId(),
//                changeInvisibleTimeRequestHeader
//            ).whenComplete((ackResult, t) -> {
//                if (t != null) {
//                    log.warn("err when nack message. request:{}, message:{}", request, message, t);
//                } else if (!AckStatus.OK.equals(ackResult.getStatus())) {
//                    log.warn("nack failed. request:{}, message:{}, ackResult:{}", request, message, ackResult);
//                }
//            });
//        } catch (Throwable t) {
//            log.warn("err when nack message. request:{}, message:{}", request, message, t);
//        }
//    }
//}
