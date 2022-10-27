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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class PullMessageActivity extends AbstractRemotingActivity {
    public PullMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        if (request.getExtFields().get(BROKER_NAME_FIELD) == null) {
            return RemotingCommand.buildErrorResponse(ResponseCode.VERSION_NOT_SUPPORTED,
                "Request doesn't have field bname");
        }
        PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        if (!PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag())) {
            ConsumerGroupInfo consumerInfo = messagingProcessor.getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (consumerInfo == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.SUBSCRIPTION_NOT_LATEST,
                    "the consumer's subscription not latest");
            }
            SubscriptionData subscriptionData = consumerInfo.findSubscriptionData(requestHeader.getTopic());
            if (subscriptionData == null) {
                return RemotingCommand.buildErrorResponse(ResponseCode.SUBSCRIPTION_NOT_EXIST,
                    "the consumer's subscription not exist");
            }
            requestHeader.setSubscription(subscriptionData.getSubString());
            requestHeader.setExpressionType(subscriptionData.getExpressionType());
            request.makeCustomHeaderToNet();
        }
        String brokerName = requestHeader.getBname();
        long timeoutMillis = requestHeader.getSuspendTimeoutMillis() + Duration.ofSeconds(10).toMillis();
        CompletableFuture<RemotingCommand> future = messagingProcessor.request(context, brokerName, request, timeoutMillis);
        future.thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }
}
