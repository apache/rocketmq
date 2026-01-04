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
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;

import java.time.Duration;

public class RecallMessageActivity extends AbstractRemotingActivity {
    TopicMessageTypeValidator topicMessageTypeValidator;

    public RecallMessageActivity(RequestPipeline requestPipeline,
                                 MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    @Override
    public RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        RecallMessageRequestHeader requestHeader = request.decodeCommandCustomHeader(RecallMessageRequestHeader.class);
        String topic = requestHeader.getTopic();
        if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
            TopicMessageType messageType = messagingProcessor.getMetadataService().getTopicMessageType(context, topic);
            topicMessageTypeValidator.validate(messageType, TopicMessageType.DELAY);
        }
        return request(ctx, request, context, Duration.ofSeconds(2).toMillis());
    }
}
