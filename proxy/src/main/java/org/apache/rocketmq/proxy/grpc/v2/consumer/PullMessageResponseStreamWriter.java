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
import apache.rocketmq.v2.PullMessageResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;

public class PullMessageResponseStreamWriter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final StreamObserver<PullMessageResponse> streamObserver;

    public PullMessageResponseStreamWriter(StreamObserver<PullMessageResponse> streamObserver) {
        this.streamObserver = streamObserver;
    }

    public void writeAndComplete(ProxyContext ctx, PullResult pullResult) {
        List<MessageExt> messageFoundList = pullResult.getMsgFoundList();
        try {
            switch (pullResult.getPullStatus()) {
                case FOUND:
                    if (messageFoundList.isEmpty()) {
                        streamObserver.onNext(PullMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.MESSAGE_NOT_FOUND, "no match message"))
                            .build());
                    } else {
                        streamObserver.onNext(PullMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                            .build());
                        for (MessageExt messageExt : messageFoundList) {
                            streamObserver.onNext(PullMessageResponse.newBuilder()
                                .setMessage(GrpcConverter.getInstance().buildMessage(messageExt))
                                .build());
                        }
                    }
                    break;
                case OFFSET_ILLEGAL:
                    streamObserver.onNext(PullMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.ILLEGAL_OFFSET, Code.ILLEGAL_OFFSET.name()))
                        .build());
                    break;
                case NO_NEW_MSG:
                case NO_MATCHED_MSG:
                default:
                    streamObserver.onNext(PullMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.MESSAGE_NOT_FOUND, "no new message"))
                        .build());
                    break;
            }
            streamObserver.onNext(PullMessageResponse.newBuilder()
                .setNextOffset(pullResult.getNextBeginOffset())
                .build());
        } catch (Throwable t) {
            log.warn("err when write pull message response. pullResult:{}", pullResult, t);
            writeAndComplete(ctx, t);
        } finally {
            onCompleted();
        }
    }

    public void writeAndComplete(ProxyContext ctx, Throwable throwable) {
        try {
            streamObserver.onNext(PullMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(throwable))
                .build());
            streamObserver.onCompleted();
        } catch (Throwable t) {
            log.warn("err when write throwable when pull message. throwable:{}", throwable, t);
            onCompleted();
        }
    }

    protected void onCompleted() {
        try {
            streamObserver.onCompleted();
        } catch (Throwable ignored) {
        }
    }
}
