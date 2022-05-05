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
package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseWriter;

public abstract class ReceiveMessageResponseStreamWriter {

    protected final StreamObserver<ReceiveMessageResponse> streamObserver;
    protected final ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;
    protected final ReceiveMessageResultFilter receiveMessageResultFilter;

    public interface Builder {
        ReceiveMessageResponseStreamWriter build(
            StreamObserver<ReceiveMessageResponse> observer,
            ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook);
    }

    public ReceiveMessageResponseStreamWriter(
        StreamObserver<ReceiveMessageResponse> observer,
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook,
        ReceiveMessageResultFilter messageResultFilter) {
        streamObserver = observer;
        receiveMessageHook = hook;
        receiveMessageResultFilter = messageResultFilter;
    }

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
                            Message curMessage = messageIterator.next();
                            try {
                                responseStreamObserver.onNext(ReceiveMessageResponse.newBuilder()
                                    .setMessage(curMessage)
                                    .build());
                            } catch (Throwable t) {
                                this.processThrowableWhenWriteMessage(t, ctx, request, curMessage);
                                messageIterator.forEachRemaining(message ->
                                    this.processThrowableWhenWriteMessage(t, ctx, request, message));
                                return;
                            }
                        }
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

    protected abstract void processThrowableWhenWriteMessage(Throwable throwable,
        Context context, ReceiveMessageRequest request, Message message);

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
}
