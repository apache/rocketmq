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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class ReceiveMessageResponseStreamWriter {

    protected static final long NACK_INVISIBLE_TIME = Duration.ofSeconds(1).toMillis();

    protected final MessagingProcessor messagingProcessor;
    protected final StreamObserver<ReceiveMessageResponse> streamObserver;

    public ReceiveMessageResponseStreamWriter(
        MessagingProcessor messagingProcessor,
        StreamObserver<ReceiveMessageResponse> observer) {
        this.messagingProcessor = messagingProcessor;
        this.streamObserver = observer;
    }

    public void write(ProxyContext ctx, ReceiveMessageRequest request, PopResult popResult) {
        PopStatus status = popResult.getPopStatus();
        List<MessageExt> messageFoundList = popResult.getMsgFoundList();
        try {
            switch (status) {
                case FOUND:
                    if (messageFoundList.isEmpty()) {
                        streamObserver.onNext(ReceiveMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.buildStatus(Code.MESSAGE_NOT_FOUND, "no new message"))
                            .build());
                    } else {
                        streamObserver.onNext(ReceiveMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                            .build());
                        Iterator<MessageExt> messageIterator =  messageFoundList.iterator();
                        while (messageIterator.hasNext()) {
                            MessageExt curMessageExt = messageIterator.next();
                            Message curMessage = convertToMessage(curMessageExt);
                            try {
                                streamObserver.onNext(ReceiveMessageResponse.newBuilder()
                                    .setMessage(curMessage)
                                    .build());
                            } catch (Throwable t) {
                                this.processThrowableWhenWriteMessage(t, ctx, request, curMessageExt);
                                messageIterator.forEachRemaining(messageExt ->
                                    this.processThrowableWhenWriteMessage(t, ctx, request, messageExt));
                                return;
                            }
                        }
                    }
                    break;
                case POLLING_FULL:
                    streamObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.TOO_MANY_REQUESTS, "polling full"))
                        .build());
                    break;
                case NO_NEW_MSG:
                case POLLING_NOT_FOUND:
                default:
                    streamObserver.onNext(ReceiveMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.MESSAGE_NOT_FOUND, "no new message"))
                        .build());
                    break;
            }
        } catch (Throwable t) {
            write(ctx, request, t);
        } finally {
            streamObserver.onCompleted();
        }
    }

    protected Message convertToMessage(MessageExt messageExt) {
        return GrpcConverter.buildMessage(messageExt);
    }

    protected void processThrowableWhenWriteMessage(Throwable throwable,
        ProxyContext ctx, ReceiveMessageRequest request, MessageExt messageExt) {

        ReceiptHandle handle = ReceiptHandle.create(messageExt);
        if (handle == null) {
            return;
        }

        this.messagingProcessor.changeInvisibleTime(
            ctx,
            handle,
            messageExt.getMsgId(),
            GrpcConverter.wrapResourceWithNamespace(request.getGroup()),
            GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic()),
            NACK_INVISIBLE_TIME
        );
    }

    public void write(ProxyContext ctx, Code code, String message) {
        ResponseWriter.write(
            streamObserver,
            ReceiveMessageResponse.newBuilder().setStatus(ResponseBuilder.buildStatus(code, message)).build()
        );
    }

    public void write(ProxyContext ctx, ReceiveMessageRequest request, Throwable throwable) {
        ResponseWriter.write(
            streamObserver,
            ReceiveMessageResponse.newBuilder().setStatus(ResponseBuilder.buildStatus(throwable)).build()
        );
    }
}
