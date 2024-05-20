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

package org.apache.rocketmq.broker.ratelimit;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.ratelimit.config.RatelimitConfig;
import org.apache.rocketmq.ratelimit.context.RatelimitContext;
import org.apache.rocketmq.ratelimit.factory.RatelimitFactory;
import org.apache.rocketmq.ratelimit.provider.RatelimitProvider;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class RatelimitPipeline implements RequestPipeline {

    private final RatelimitConfig ratelimitConfig;
    private final RatelimitProvider ratelimitProvider;
    private final BrokerStatsManager brokerStatsManager;

    public RatelimitPipeline(RatelimitConfig ratelimitConfig, BrokerStatsManager brokerStatsManager) {
        this.ratelimitConfig = ratelimitConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.ratelimitProvider = RatelimitFactory.getProvider(ratelimitConfig);
        if (this.ratelimitProvider != null) {
            this.ratelimitProvider.initialize(ratelimitConfig);
        }
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RemotingCommand request) {
        if (!ratelimitConfig.isRatelimitEnabled()) {
            return;
        }
        RatelimitContext context;
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
            case RequestCode.SEND_BATCH_MESSAGE:
                if (request.getBody() == null || request.getBody().length == 0) {
                    return;
                }
                context = ratelimitProvider.newContext(ctx, request);
                if (isInnerTopic(context.getTopic())) {
                    return;
                }
                if (!ratelimitProvider.produceTryAcquire(context, 1)) {
                    String message = "[FLOW]client has exhausted the send quota for the current time slot, start flow control for a while.";
                    brokerStatsManager.incTopicPutRatelimitNums(context.getTopic());
                    throw new AbortProcessException(ResponseCode.FLOW_CONTROL, message);
                }
                break;
            case RequestCode.PULL_MESSAGE:
            case RequestCode.LITE_PULL_MESSAGE:
            case RequestCode.POP_MESSAGE:
                context = ratelimitProvider.newContext(ctx, request);
                if (!ratelimitProvider.consumeCanAcquire(context, 1)) {
                    String message = "[FLOW]client has exhausted the pull quota for the current time slot, start flow control for a while.";
                    brokerStatsManager.incTopicGetRatelimitNums(context.getTopic());
                    throw new AbortProcessException(ResponseCode.FLOW_CONTROL, message);
                }
                break;
            default:
                return;
        }
    }

    @Override
    public void executeResponse(ChannelHandlerContext ctx, RemotingCommand request,

                                RemotingCommand response) {
        if (!ratelimitConfig.isRatelimitEnabled()) {
            return;
        }
        switch (request.getCode()) {
            case RequestCode.PULL_MESSAGE:
            case RequestCode.LITE_PULL_MESSAGE:
            case RequestCode.POP_MESSAGE:
                Integer foundMsgs = RemotingHelper.getAttributeValue(AttributeKeys.FOUND_MSGS_KEY, ctx.channel());
                if (foundMsgs != null) {
                    RatelimitContext context = ratelimitProvider.newContext(ctx, request);
                    ratelimitProvider.consumeReserve(context, foundMsgs);
                }
                break;
        }
    }

    private static boolean isInnerTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        return NamespaceUtil.isRetryTopic(topic) || NamespaceUtil.isDLQTopic(topic) || TopicValidator.isSystemTopic(topic);
    }
}
