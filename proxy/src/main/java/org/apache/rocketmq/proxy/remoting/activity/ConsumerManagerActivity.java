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
import java.util.ArrayList;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerManagerActivity extends AbstractRemotingActivity {
    public ConsumerManagerActivity(RequestPipeline requestPipeline, MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP: {
                return getConsumerListByGroup(ctx, request, context);
            }
            case RequestCode.LOCK_BATCH_MQ: {
                return lockBatchMQ(ctx, request, context);
            }
            case RequestCode.UNLOCK_BATCH_MQ: {
                return unlockBatchMQ(ctx, request, context);
            }
            case RequestCode.UPDATE_CONSUMER_OFFSET:
            case RequestCode.QUERY_CONSUMER_OFFSET:
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
            case RequestCode.GET_MIN_OFFSET:
            case RequestCode.GET_MAX_OFFSET:
            case RequestCode.GET_EARLIEST_MSG_STORETIME: {
                return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
            }
            default:
                break;
        }
        return null;
    }

    protected RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        // TODO after connection-related module
        return null;
    }

    protected RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            response.setBody(requestBody.encode());
            response.setRemark("MessageQueue set is empty");
            return response;
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }

    protected RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);
        Set<MessageQueue> mqSet = requestBody.getMqSet();
        if (mqSet.isEmpty()) {
            response.setBody(requestBody.encode());
            response.setRemark("MessageQueue set is empty");
            return response;
        }

        String brokerName = new ArrayList<>(mqSet).get(0).getBrokerName();
        messagingProcessor.request(context, brokerName, request, Duration.ofSeconds(3).toMillis())
            .thenAccept(r -> writeResponse(ctx, context, request, r))
            .exceptionally(t -> {
                writeErrResponse(ctx, context, request, t);
                return null;
            });
        return null;
    }
}
