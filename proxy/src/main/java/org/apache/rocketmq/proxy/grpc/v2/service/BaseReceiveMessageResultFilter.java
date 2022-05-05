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

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Settings;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;

public abstract class BaseReceiveMessageResultFilter implements ReceiveMessageResultFilter {

    protected final GrpcClientManager grpcClientManager;

    public BaseReceiveMessageResultFilter(GrpcClientManager manager) {
        grpcClientManager = manager;
    }

    @Override
    public List<Message> filterMessage(Context ctx, ReceiveMessageRequest request, List<MessageExt> messageExtList) {
        if (messageExtList == null || messageExtList.isEmpty()) {
            return Collections.emptyList();
        }
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
        SubscriptionData subscriptionData = GrpcConverter.buildSubscriptionData(topicName, request.getFilterExpression());
        Settings settings = grpcClientManager.getClientSettings(ctx);
        int maxAttempts = settings.getBackoffPolicy().getMaxAttempts();
        List<Message> resMessageList = new ArrayList<>();
        for (MessageExt messageExt : messageExtList) {
            if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
                processNoMatchMessage(ctx, request, messageExt);
                continue;
            }
            if (messageExt.getReconsumeTimes() >= maxAttempts) {
                processExceedMaxAttemptsMessage(ctx, request, messageExt, maxAttempts);
                continue;
            }
            resMessageList.add(GrpcConverter.buildMessage(messageExt));
        }
        return resMessageList;
    }

    protected abstract void processNoMatchMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt);

    protected abstract void processExceedMaxAttemptsMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt, int maxAttempts);
}
