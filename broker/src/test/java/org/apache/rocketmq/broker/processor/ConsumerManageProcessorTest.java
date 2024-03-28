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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManageProcessorTest {
    private ConsumerManageProcessor consumerManageProcessor;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;

    private String topic = "FooBar";
    private String group = "FooBarGroup";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        TopicConfigManager topicConfigManager = new TopicConfigManager(brokerController);
        topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager(brokerController);
        subscriptionGroupManager.getSubscriptionGroupTable().put(group, new SubscriptionGroupConfig());
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        consumerManageProcessor = new ConsumerManageProcessor(brokerController);
    }

    @Test
    public void testUpdateConsumerOffset_InvalidTopic() throws Exception {
        RemotingCommand request = buildUpdateConsumerOffsetRequest(group, "InvalidTopic", 0, 0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testUpdateConsumerOffset_GroupNotExist() throws Exception {
        RemotingCommand request = buildUpdateConsumerOffsetRequest("NotExistGroup", topic, 0, 0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    }

    private RemotingCommand buildUpdateConsumerOffsetRequest(String group, String topic, int queueId, long offset) {
        UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setCommitOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
