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

import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.broker.grpc.adapter.InvocationContext;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.grpc.common.Converter;
import org.apache.rocketmq.grpc.common.ResponseBuilder;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class PullMessageResponseHandler implements ResponseHandler<PullMessageRequest, PullMessageResponse> {
    @Override public void handle(RemotingCommand responseCommand,
        InvocationContext<PullMessageRequest, PullMessageResponse> context) {
        try {
            PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) responseCommand.readCustomHeader();
            PullMessageResponse.Builder builder = PullMessageResponse.newBuilder();
            if (responseCommand.getCode() == RemotingSysResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(responseCommand.getBody());
                List<MessageExt> msgFoundList = MessageDecoder.decodes(byteBuffer);
                for (MessageExt messageExt : msgFoundList) {
                    builder.addMessages(Converter.buildMessage(messageExt));
                }
            }
            PullMessageResponse response = builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()))
                .setMinOffset(responseHeader.getMinOffset())
                .setNextOffset(responseHeader.getNextBeginOffset())
                .setMaxOffset(responseHeader.getMaxOffset())
                .build();
            ResponseWriter.write(context.getStreamObserver(), response);
        } catch (Exception e) {
            ResponseWriter.writeException(context.getStreamObserver(), e);
        }
    }
}
