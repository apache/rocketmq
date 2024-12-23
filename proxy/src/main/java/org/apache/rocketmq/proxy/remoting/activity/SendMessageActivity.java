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
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class SendMessageActivity extends AbstractRemotingActivity {
    TopicMessageTypeValidator topicMessageTypeValidator;

    public SendMessageActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
            case RequestCode.SEND_BATCH_MESSAGE: {
                return sendMessage(ctx, request, context);
            }
            case RequestCode.CONSUMER_SEND_MSG_BACK: {
                return consumerSendMessage(ctx, request, context);
            }
            default:
                break;
        }
        return null;
    }

    protected RemotingCommand sendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        SendMessageRequestHeader requestHeader = SendMessageRequestHeader.parseRequestHeader(request);
        String topic = requestHeader.getTopic();
        Map<String, String> property = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String syncFlag = property.get(MessageConst.PROPERTY_SYNC_FLAG);
        TopicMessageType messageType = TopicMessageType.parseFromMessageProperty(property);
        if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck() && !StringUtils.equals(syncFlag, "1")) {
            if (topicMessageTypeValidator != null) {
                // Do not check retry or dlq topic
                if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
                    TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(context, topic);
                    topicMessageTypeValidator.validate(topicMessageType, messageType);
                }
            }
        }
        if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
            if (TopicMessageType.TRANSACTION.equals(messageType)) {
                messagingProcessor.addTransactionSubscription(context, requestHeader.getProducerGroup(), requestHeader.getTopic());
            }
        }
        return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
    }

    protected RemotingCommand consumerSendMessage(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        return request(ctx, request, context, Duration.ofSeconds(3).toMillis());
    }
}
