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

package org.apache.rocketmq.proxy.service.sysmessage;

import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIExt;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HeartbeatSyncerTest extends InitConfigTest {
    @Mock
    private TopicRouteService topicRouteService;
    @Mock
    private AdminService adminService;
    @Mock
    private ConsumerManager consumerManager;
    @Mock
    private MQClientAPIFactory mqClientAPIFactory;
    @Mock
    private MQClientAPIExt mqClientAPIExt;
    @Mock
    private ProxyRelayService proxyRelayService;

    private String clientId;
    private final String remoteAddress = "10.152.39.53:9768";
    private final String localAddress = "11.193.0.1:1210";
    private final String clusterName = "cluster";
    private final String brokerName = "broker-01";

    @Before
    public void before() throws Throwable {
        super.before();
        this.clientId = RandomStringUtils.randomAlphabetic(10);
        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);

        {
            TopicRouteData topicRouteData = new TopicRouteData();
            QueueData queueData = new QueueData();
            queueData.setReadQueueNums(8);
            queueData.setWriteQueueNums(8);
            queueData.setPerm(6);
            queueData.setBrokerName(brokerName);
            topicRouteData.getQueueDatas().add(queueData);
            BrokerData brokerData = new BrokerData();
            brokerData.setCluster(clusterName);
            brokerData.setBrokerName(brokerName);
            HashMap<Long, String> brokerAddr = new HashMap<>();
            brokerAddr.put(0L, "127.0.0.1:10911");
            brokerData.setBrokerAddrs(brokerAddr);
            topicRouteData.getBrokerDatas().add(brokerData);
            MessageQueueView messageQueueView = new MessageQueueView("foo", topicRouteData);
            when(this.topicRouteService.getAllMessageQueueView(anyString())).thenReturn(messageQueueView);
        }
    }

    @Test
    public void testSyncGrpcV2Channel() throws Exception {
        String consumerGroup = "consumerGroup";
        GrpcClientSettingsManager grpcClientSettingsManager = mock(GrpcClientSettingsManager.class);
        GrpcChannelManager grpcChannelManager = mock(GrpcChannelManager.class);
        GrpcClientChannel grpcClientChannel = new GrpcClientChannel(
            proxyRelayService, grpcClientSettingsManager, grpcChannelManager,
            ProxyContext.create().setRemoteAddress(remoteAddress).setLocalAddress(localAddress),
            clientId);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            grpcClientChannel,
            clientId,
            LanguageCode.JAVA,
            5
        );

        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        doReturn(CompletableFuture.completedFuture(sendResult)).when(this.mqClientAPIExt)
            .sendMessageAsync(anyString(), anyString(), messageArgumentCaptor.capture(), any(), anyLong());

        Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setTopic(Resource.newBuilder().setName("topic").build())
                    .setExpression(FilterExpression.newBuilder()
                        .setType(FilterType.TAG)
                        .setExpression("tag")
                        .build())
                    .build())
                .build())
            .build();
        when(grpcClientSettingsManager.getRawClientSettings(eq(clientId))).thenReturn(settings);

        HeartbeatSyncer heartbeatSyncer = new HeartbeatSyncer(topicRouteService, adminService, consumerManager, mqClientAPIFactory);
        heartbeatSyncer.onConsumerRegister(
            consumerGroup,
            clientChannelInfo,
            ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            Sets.newHashSet(FilterAPI.buildSubscriptionData("topic", "tag"))
        );

        await().atMost(Duration.ofSeconds(3)).until(() -> !messageArgumentCaptor.getAllValues().isEmpty());
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        verify(consumerManager, never()).registerConsumer(anyString(), any(), any(), any(), any(), any(), anyBoolean());

        String localServeAddr = ConfigurationManager.getProxyConfig().getLocalServeAddr();
        // change local serve addr, to simulate other proxy receive messages
        heartbeatSyncer.localProxyId = RandomStringUtils.randomAlphabetic(10);
        ArgumentCaptor<ClientChannelInfo> syncChannelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doReturn(true).when(consumerManager).registerConsumer(anyString(), syncChannelInfoArgumentCaptor.capture(), any(), any(), any(), any(), anyBoolean());

        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        assertEquals(2, syncChannelInfoArgumentCaptor.getAllValues().size());
        List<ClientChannelInfo> channelInfoList = syncChannelInfoArgumentCaptor.getAllValues();
        assertSame(channelInfoList.get(0).getChannel(), channelInfoList.get(1).getChannel());
        assertEquals(settings, GrpcClientChannel.parseChannelExtendAttribute(channelInfoList.get(0).getChannel()));
        assertEquals(settings, GrpcClientChannel.parseChannelExtendAttribute(channelInfoList.get(1).getChannel()));

        // start test sync client unregister
        // reset localServeAddr
        ConfigurationManager.getProxyConfig().setLocalServeAddr(localServeAddr);
        heartbeatSyncer.onConsumerUnRegister(consumerGroup, clientChannelInfo);
        await().atMost(Duration.ofSeconds(3)).until(() -> messageArgumentCaptor.getAllValues().size() == 2);

        ArgumentCaptor<ClientChannelInfo> syncUnRegisterChannelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(consumerManager).unregisterConsumer(anyString(), syncUnRegisterChannelInfoArgumentCaptor.capture(), anyBoolean());

        // change local serve addr, to simulate other proxy receive messages
        heartbeatSyncer.localProxyId = RandomStringUtils.randomAlphabetic(10);
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getAllValues().get(1))), null);
        assertSame(channelInfoList.get(0).getChannel(), syncUnRegisterChannelInfoArgumentCaptor.getValue().getChannel());
    }

    @Test
    public void testSyncRemotingChannel() throws Exception {
        String consumerGroup = "consumerGroup";
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        subscriptionDataSet.add(FilterAPI.buildSubscriptionData("topic", "tagSub"));
        RemotingProxyOutClient remotingProxyOutClient = mock(RemotingProxyOutClient.class);
        RemotingChannel remotingChannel = new RemotingChannel(remotingProxyOutClient, proxyRelayService, createMockChannel(), clientId, subscriptionDataSet);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            remotingChannel,
            clientId,
            LanguageCode.JAVA,
            4
        );

        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        doReturn(CompletableFuture.completedFuture(sendResult)).when(this.mqClientAPIExt)
            .sendMessageAsync(anyString(), anyString(), messageArgumentCaptor.capture(), any(), anyLong());

        HeartbeatSyncer heartbeatSyncer = new HeartbeatSyncer(topicRouteService, adminService, consumerManager, mqClientAPIFactory);
        heartbeatSyncer.onConsumerRegister(
            consumerGroup,
            clientChannelInfo,
            ConsumeType.CONSUME_PASSIVELY,
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            subscriptionDataSet
        );

        await().atMost(Duration.ofSeconds(3)).until(() -> !messageArgumentCaptor.getAllValues().isEmpty());
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        verify(consumerManager, never()).registerConsumer(anyString(), any(), any(), any(), any(), any(), anyBoolean());

        String localServeAddr = ConfigurationManager.getProxyConfig().getLocalServeAddr();
        // change local serve addr, to simulate other proxy receive messages
        heartbeatSyncer.localProxyId = RandomStringUtils.randomAlphabetic(10);
        ArgumentCaptor<ClientChannelInfo> syncChannelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doReturn(true).when(consumerManager).registerConsumer(anyString(), syncChannelInfoArgumentCaptor.capture(), any(), any(), any(), any(), anyBoolean());

        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getValue())), null);
        assertEquals(2, syncChannelInfoArgumentCaptor.getAllValues().size());
        List<ClientChannelInfo> channelInfoList = syncChannelInfoArgumentCaptor.getAllValues();
        assertSame(channelInfoList.get(0).getChannel(), channelInfoList.get(1).getChannel());
        assertEquals(subscriptionDataSet, RemotingChannel.parseChannelExtendAttribute(channelInfoList.get(0).getChannel()));
        assertEquals(subscriptionDataSet, RemotingChannel.parseChannelExtendAttribute(channelInfoList.get(1).getChannel()));

        // start test sync client unregister
        // reset localServeAddr
        ConfigurationManager.getProxyConfig().setLocalServeAddr(localServeAddr);
        heartbeatSyncer.onConsumerUnRegister(consumerGroup, clientChannelInfo);
        await().atMost(Duration.ofSeconds(3)).until(() -> messageArgumentCaptor.getAllValues().size() == 2);

        ArgumentCaptor<ClientChannelInfo> syncUnRegisterChannelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(consumerManager).unregisterConsumer(anyString(), syncUnRegisterChannelInfoArgumentCaptor.capture(), anyBoolean());

        // change local serve addr, to simulate other proxy receive messages
        heartbeatSyncer.localProxyId = RandomStringUtils.randomAlphabetic(10);
        heartbeatSyncer.consumeMessage(Lists.newArrayList(convertFromMessage(messageArgumentCaptor.getAllValues().get(1))), null);
        assertSame(channelInfoList.get(0).getChannel(), syncUnRegisterChannelInfoArgumentCaptor.getValue().getChannel());
    }

    private MessageExt convertFromMessage(Message message) {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(message.getTopic());
        messageExt.setBody(message.getBody());
        return messageExt;
    }

    private Channel createMockChannel() {
        return new MockChannel(RandomStringUtils.randomAlphabetic(10));
    }

    private class MockChannel extends SimpleChannel {

        public MockChannel(String channelId) {
            super(null, new MockChannelId(channelId), HeartbeatSyncerTest.this.remoteAddress, HeartbeatSyncerTest.this.localAddress);
        }
    }

    private static class MockChannelId implements ChannelId {

        private final String channelId;

        public MockChannelId(String channelId) {
            this.channelId = channelId;
        }

        @Override
        public String asShortText() {
            return channelId;
        }

        @Override
        public String asLongText() {
            return channelId;
        }

        @Override
        public int compareTo(@NotNull ChannelId o) {
            return this.channelId.compareTo(o.asLongText());
        }
    }
}