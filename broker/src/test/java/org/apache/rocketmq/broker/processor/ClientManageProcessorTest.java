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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
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
public class ClientManageProcessorTest {
    private ClientManageProcessor clientManageProcessor;
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

    @Before
    public void init() {
        when(handlerContext.channel()).thenReturn(channel);
        clientManageProcessor = new ClientManageProcessor(brokerController);
        clientChannelInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 100);
        brokerController.getProducerManager().registerProducer(group, clientChannelInfo);

        ConsumerData consumerData = PullMessageProcessorTest.createConsumerData(group, topic);
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
    public void processRequest_UnRegisterProducer() throws Exception {
        brokerController.getProducerManager().registerProducer(group, clientChannelInfo);
        Map<Channel, ClientChannelInfo> channelMap = brokerController.getProducerManager().getGroupChannelTable().get(group);
        assertThat(channelMap).isNotNull();
        assertThat(channelMap.get(channel)).isEqualTo(clientChannelInfo);

        RemotingCommand request = createUnRegisterProducerCommand();
        RemotingCommand response = clientManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        channelMap = brokerController.getProducerManager().getGroupChannelTable().get(group);
        assertThat(channelMap).isNull();
    }

    @Test
    public void processRequest_UnRegisterConsumer() throws RemotingCommandException {
        ConsumerGroupInfo consumerGroupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(group);
        assertThat(consumerGroupInfo).isNotNull();

        RemotingCommand request = createUnRegisterConsumerCommand();
        RemotingCommand response = clientManageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        consumerGroupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(group);
        assertThat(consumerGroupInfo).isNull();
    }

    @Test
    public void processRequest_heartbeat() throws RemotingCommandException {
        RemotingCommand request = createHeartbeatCommand(false, "topicA");
        RemotingCommand response = clientManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUB_CHANGE))).isFalse();
        ConsumerGroupInfo consumerGroupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(group);

        RemotingCommand requestSimple = createHeartbeatCommand(true, "topicA");
        RemotingCommand responseSimple = clientManageProcessor.processRequest(handlerContext, requestSimple);
        assertThat(responseSimple.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(Boolean.parseBoolean(responseSimple.getExtFields().get(MixAll.IS_SUB_CHANGE))).isFalse();
        ConsumerGroupInfo consumerGroupInfoSimple = brokerController.getConsumerManager().getConsumerGroupInfo(group);
        assertThat(consumerGroupInfoSimple).isEqualTo(consumerGroupInfo);

        request = createHeartbeatCommand(false, "topicB");
        response = clientManageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUB_CHANGE))).isTrue();
        consumerGroupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(group);

        requestSimple = createHeartbeatCommand(true, "topicB");
        responseSimple = clientManageProcessor.processRequest(handlerContext, requestSimple);
        assertThat(responseSimple.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(Boolean.parseBoolean(responseSimple.getExtFields().get(MixAll.IS_SUB_CHANGE))).isFalse();
        consumerGroupInfoSimple = brokerController.getConsumerManager().getConsumerGroupInfo(group);
        assertThat(consumerGroupInfoSimple).isEqualTo(consumerGroupInfo);
    }

    @Test
    public void test_heartbeat_costTime() {
        String topic = "TOPIC_TEST";
        List<String> topicList = new ArrayList<>();
        for (int i = 0; i < 500; i ++) {
            topicList.add(topic + i);
        }
        HeartbeatData heartbeatData = prepareHeartbeatData(false, topicList);
        long time = System.currentTimeMillis();
        heartbeatData.computeHeartbeatFingerprint();
        System.out.print("computeHeartbeatFingerprint cost time : " + (System.currentTimeMillis() - time) + " ms \n");
    }

    private RemotingCommand createUnRegisterProducerCommand() {
        UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientId);
        requestHeader.setProducerGroup(group);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);
        request.setLanguage(LanguageCode.JAVA);
        request.setVersion(100);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createUnRegisterConsumerCommand() {
        UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientId);
        requestHeader.setConsumerGroup(group);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);
        request.setLanguage(LanguageCode.JAVA);
        request.setVersion(100);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createHeartbeatCommand(boolean isWithoutSub, String topic) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(LanguageCode.JAVA);
        HeartbeatData heartbeatDataWithSub = prepareHeartbeatData(false, topic);
        int heartbeatFingerprint = heartbeatDataWithSub.computeHeartbeatFingerprint();
        HeartbeatData heartbeatData = prepareHeartbeatData(isWithoutSub, topic);
        heartbeatData.setHeartbeatFingerprint(heartbeatFingerprint);
        request.setBody(heartbeatData.encode());
        return request;
    }

    private HeartbeatData prepareHeartbeatData(boolean isWithoutSub, String topic) {
        List<String> list = new ArrayList<>();
        list.add(topic);
        return prepareHeartbeatData(isWithoutSub, list);
    }

    private HeartbeatData prepareHeartbeatData(boolean isWithoutSub, List<String> topicList) {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(this.clientId);
        ConsumerData consumerData = createConsumerData(group);
        if (!isWithoutSub) {
            Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
            for (String topic : topicList) {
                SubscriptionData subscriptionData = new SubscriptionData();
                subscriptionData.setTopic(topic);
                subscriptionData.setSubString("*");
                subscriptionData.setSubVersion(100L);
                subscriptionDataSet.add(subscriptionData);
            }
            consumerData.getSubscriptionDataSet().addAll(subscriptionDataSet);
        }
        heartbeatData.getConsumerDataSet().add(consumerData);
        heartbeatData.setWithoutSub(isWithoutSub);
        return heartbeatData;
    }

    static ConsumerData createConsumerData(String group) {
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName(group);
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        return consumerData;
    }
}
