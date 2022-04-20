package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.ActivePublishingSettings;
import apache.rocketmq.v2.ActiveSubscriptionSettings;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.ReportActiveSettingsCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SubscriptionEntry;
import io.grpc.Context;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class ForwardClientServiceTest extends BaseServiceTest {

    private ChannelManager channelManager = new ChannelManager();
    private TelemetryCommandManager telemetryCommandManager = new TelemetryCommandManager();
    private ForwardClientService clientService;

    @Override
    public void beforeEach() throws Throwable {
        clientService = new ForwardClientService(
            this.connectorManager,
            Executors.newSingleThreadScheduledExecutor(),
            this.channelManager,
            this.grpcClientManager,
            this.telemetryCommandManager);
    }

    @Test
    public void testProducerHeartbeat() {
        GrpcClientManager.ActiveClientSettings clientSettings = new GrpcClientManager.ActiveClientSettings(ReportActiveSettingsCommand.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setActivePublishingSettings(ActivePublishingSettings.newBuilder()
                .addPublishingTopics(Resource.newBuilder()
                    .setName("topic1")
                    .build())
                .addPublishingTopics(Resource.newBuilder()
                    .setName("topic2")
                    .build())
                .build())
            .build());
        when(grpcClientManager.getClientSettings(anyString())).thenReturn(clientSettings);

        clientService.heartbeat(Context.current(), HeartbeatRequest.newBuilder().build());

        assertEquals(2, clientService.getProducerManager().getGroupChannelTable().size());
        Channel channel = clientService.getProducerManager().findChannel(CLIENT_ID);
        assertNotNull(channel);
        assertTrue(channel instanceof GrpcClientChannel);

        clientService.notifyClientTermination(Context.current(), NotifyClientTerminationRequest.newBuilder().build());
        assertTrue(clientService.getProducerManager().getGroupChannelTable().isEmpty());
    }

    @Test
    public void testConsumerHeartbeat() {
        List<SubscriptionEntry> subscriptionEntryList = new ArrayList<>();
        subscriptionEntryList.add(SubscriptionEntry.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setExpression(FilterExpression.newBuilder()
                .setExpression("*")
                .setType(FilterType.TAG)
                .build())
            .build());

        GrpcClientManager.ActiveClientSettings clientSettings = new GrpcClientManager.ActiveClientSettings(ReportActiveSettingsCommand.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setActiveSubscriptionSettings(ActiveSubscriptionSettings.newBuilder()
                .addAllSubscriptions(subscriptionEntryList)
                .build())
            .build());
        when(grpcClientManager.getClientSettings(anyString())).thenReturn(clientSettings);

        clientService.heartbeat(Context.current(), HeartbeatRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("consumerGroup")
                .build())
            .build());

        ClientChannelInfo clientChannelInfo = clientService.getConsumerManager().findChannel("consumerGroup", CLIENT_ID);
        assertNotNull(clientChannelInfo);
        assertEquals(LanguageCode.JAVA, clientChannelInfo.getLanguage());
        assertEquals(CLIENT_ID, clientChannelInfo.getClientId());
        assertTrue(clientChannelInfo.getChannel() instanceof GrpcClientChannel);
        ConsumerGroupInfo consumerGroupInfo = clientService.getConsumerManager().getConsumerGroupInfo("consumerGroup");
        assertEquals(MessageModel.CLUSTERING, consumerGroupInfo.getMessageModel());
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, consumerGroupInfo.getConsumeFromWhere());
        assertEquals(ConsumeType.CONSUME_PASSIVELY, consumerGroupInfo.getConsumeType());
        assertEquals("TAG", consumerGroupInfo.getSubscriptionTable().get("topic").getExpressionType());
        assertEquals("*", consumerGroupInfo.getSubscriptionTable().get("topic").getSubString());


        clientService.notifyClientTermination(Context.current(), NotifyClientTerminationRequest.newBuilder()
            .setGroup(Resource.newBuilder()
                .setName("consumerGroup")
                .build())
            .build());
        assertNull(clientService.getConsumerManager().getConsumerGroupInfo("consumerGroup"));
    }
}