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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.AllocateMessageQueueRequestBody;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueStrategyConstants;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManageProcessorTest {
    private ConsumerManageProcessor consumerManageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private Channel channel;

    private ClientChannelInfo clientChannelInfo;
    private String clientId = UUID.randomUUID().toString();
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private List<MessageQueue> mqAll = new ArrayList<MessageQueue>();

    @Before
    public void init() {
        consumerManageProcessor = new ConsumerManageProcessor(brokerController);
        clientChannelInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 100);

        mqAll.add(new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), 0));
        mqAll.add(new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), 1));
        mqAll.add(new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), 3));
        mqAll.add(new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), 4));

        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientChannelInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
    }

    @Test
    public void testAllocateMessageQueue() throws RemotingCommandException {
        String emptyClientId = "";
        RemotingCommand request = buildAllocateMessageQueueRequest(emptyClientId, AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_AVERAGELY);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
        assertThat(response.getRemark()).isEqualTo("currentCID is empty");

        request = buildAllocateMessageQueueRequest(clientId, AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_AVERAGELY);
        response = consumerManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private RemotingCommand buildAllocateMessageQueueRequest(String clientId, String strategy) {
        AllocateMessageQueueRequestHeader requestHeader = new AllocateMessageQueueRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setClientID(clientId);
        requestHeader.setStrategyName(strategy);

        AllocateMessageQueueRequestBody requestBody = new AllocateMessageQueueRequestBody();
        requestBody.setMqAll(mqAll);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ALLOCATE_MESSAGE_QUEUE, requestHeader);
        request.setBody(requestBody.encode());
        request.makeCustomHeaderToNet();
        return request;
    }
}
