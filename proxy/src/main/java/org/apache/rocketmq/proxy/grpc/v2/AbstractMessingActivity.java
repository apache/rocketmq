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
package org.apache.rocketmq.proxy.grpc.v2;

import io.grpc.Context;
import io.netty.channel.Channel;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ChannelUtils;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public abstract class AbstractMessingActivity {

    protected final MessagingProcessor messagingProcessor;
    protected final GrpcClientSettingsManager grpcClientSettingsManager;
    protected final GrpcChannelManager grpcChannelManager;

    public AbstractMessingActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        this.messagingProcessor = messagingProcessor;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.grpcChannelManager = grpcChannelManager;
    }

    protected ProxyContext createContext(Context ctx) {
        return ProxyContext.create()
            .withVal(ContextVariable.LOCAL_ADDRESS, InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.LOCAL_ADDRESS))
            .withVal(ContextVariable.REMOTE_ADDRESS, InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.REMOTE_ADDRESS))
            .withVal(ContextVariable.CLIENT_ID, InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID))
            .withVal(ContextVariable.LANGUAGE, LanguageCode.valueOf(InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.LANGUAGE)));
    }

    protected void attachChannelId(Context ctx, ProxyContext context, String groupName) {
        String clientId = context.getVal(ContextVariable.CLIENT_ID);
        Channel channel = grpcChannelManager.createChannel(ctx, groupName, clientId);
        context.withVal(ContextVariable.CHANNEL_KEY, ChannelUtils.buildChannelKey(channel, groupName));
    }
}
