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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.ReceiveMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ParameterConverter;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;

public class DefaultPopResultResponseParameterConverter implements ParameterConverter<PopResult, ReceiveMessageResponse> {

    @Override
    public ReceiveMessageResponse convert(Context ctx, PopResult result) throws Throwable {
        PopStatus status = result.getPopStatus();
        switch (status) {
            case FOUND:
                break;
            case POLLING_FULL:
                return ReceiveMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.RESOURCE_EXHAUSTED, "polling full"))
                    .build();
            case NO_NEW_MSG:
            case POLLING_NOT_FOUND:
            default:
                return ReceiveMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.OK, "no new message"))
                    .build();
        }

        List<Message> messages = new ArrayList<>();
        for (MessageExt messageExt : result.getMsgFoundList()) {
            messages.add(Converter.buildMessage(messageExt));
        }

        return ReceiveMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .addAllMessages(messages)
            .build();
    }
}
