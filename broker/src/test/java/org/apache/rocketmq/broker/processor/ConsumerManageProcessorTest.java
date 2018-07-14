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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
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

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManageProcessorTest {
    private ConsumerManageProcessor consumerManageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;

    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() {
        consumerManageProcessor = new ConsumerManageProcessor(brokerController);
    }

    @Test
    public void processRequest_QueryOffsetWithExistingSubscription() throws Exception {
        long commitOffset = 1;
        brokerController.getConsumerOffsetManager().commitOffset(null, group, topic, 0, commitOffset);

        RemotingCommand request = createQueryConsumerOffsetCommand(0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.readCustomHeader();

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseHeader.getOffset()).isEqualTo(commitOffset);
    }

    @Test
    public void processRequest_QueryOffsetWithNewSubscription() throws Exception {
        brokerController.getConsumerOffsetManager().getOffsetTable().clear();

        RemotingCommand request = createQueryConsumerOffsetCommand(0);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);
        assertThat(response.getRemark()).contains("The subscription is probably new.");
    }

    @Test
    public void processRequest_QueryOffsetWithNewScaledQueue() throws Exception {
        int commitQueueId = 0;
        brokerController.getConsumerOffsetManager().commitOffset("", group, topic, commitQueueId, 0);

        RemotingCommand request = createQueryConsumerOffsetCommand(commitQueueId + 1);
        RemotingCommand response = consumerManageProcessor.processRequest(handlerContext, request);
        QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.readCustomHeader();

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseHeader.getOffset()).isEqualTo(0);
    }

    private RemotingCommand createQueryConsumerOffsetCommand(int queueId) {
        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setConsumerGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        request.setLanguage(LanguageCode.JAVA);
        request.setVersion(100);
        request.makeCustomHeaderToNet();
        return request;
    }
}
