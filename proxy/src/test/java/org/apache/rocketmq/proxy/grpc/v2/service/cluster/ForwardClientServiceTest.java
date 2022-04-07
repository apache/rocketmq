package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v1.ConsumeMessageType;
import apache.rocketmq.v1.ConsumeModel;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SubscriptionEntry;
import io.grpc.Context;
import io.grpc.Metadata;
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
import org.apache.rocketmq.proxy.common.PollResponseManager;
import org.apache.rocketmq.proxy.grpc.v1.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Test;

import static org.junit.Assert.*;

public class ForwardClientServiceTest extends BaseServiceTest {

    private ChannelManager channelManager = new ChannelManager();
    private PollResponseManager pollResponseManager = new PollResponseManager();

    @Override
    public void beforeEach() throws Throwable {

    }

    @Test
    public void testProducerHeartbeat() {
        ForwardClientService clientService = new ForwardClientService(
            this.connectorManager,
            Executors.newSingleThreadScheduledExecutor(),
            this.channelManager,
            this.pollResponseManager);

        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.LANGUAGE, "JAVA");
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "127.0.0.1:8080");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "127.0.0.1:8081");
        Context ctx = Context.current().withValue(InterceptorConstants.METADATA, metadata);
        clientService.heartbeat(ctx, HeartbeatRequest.newBuilder()
            .setClientId("clientId")
            .setProducerData(ProducerData.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName("producerGroup")
                    .build())
                .build())
            .build());

        assertEquals(1, clientService.getProducerManager().getGroupChannelTable().size());
        Channel channel = clientService.getProducerManager().findChannel("clientId");
        assertNotNull(channel);
        assertTrue(channel instanceof GrpcClientChannel);

        clientService.unregister(ctx, NotifyClientTerminationRequest.newBuilder()
            .setClientId("clientId")
            .setProducerGroup(Resource.newBuilder()
                .setName("producerGroup")
                .build())
            .build());
        assertTrue(clientService.getProducerManager().getGroupChannelTable().isEmpty());
    }

    @Test
    public void testConsumerHeartbeat() {
        ForwardClientService clientService = new ForwardClientService(
            this.connectorManager,
            Executors.newSingleThreadScheduledExecutor(),
            this.channelManager,
            this.pollResponseManager);

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
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.LANGUAGE, "JAVA");
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, "127.0.0.1:8080");
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, "127.0.0.1:8081");
        Context ctx = Context.current().withValue(InterceptorConstants.METADATA, metadata);

        clientService.heartbeat(ctx, HeartbeatRequest.newBuilder()
            .setClientId("clientId")
            .setConsumerData(ConsumerData.newBuilder()
                .setGroup(Resource.newBuilder()
                    .setName("consumerGroup")
                    .build())
                .setConsumeType(ConsumeMessageType.PASSIVE)
                .setConsumeModel(ConsumeModel.CLUSTERING)
                .setConsumePolicy(ConsumePolicy.RESUME)
                .addAllSubscriptions(subscriptionEntryList)
                .build())
            .build());

        ClientChannelInfo clientChannelInfo = clientService.getConsumerManager().findChannel("consumerGroup", "clientId");
        assertNotNull(clientChannelInfo);
        assertEquals(LanguageCode.JAVA, clientChannelInfo.getLanguage());
        assertEquals("clientId", clientChannelInfo.getClientId());
        assertTrue(clientChannelInfo.getChannel() instanceof GrpcClientChannel);
        ConsumerGroupInfo consumerGroupInfo = clientService.getConsumerManager().getConsumerGroupInfo("consumerGroup");
        assertEquals(MessageModel.CLUSTERING, consumerGroupInfo.getMessageModel());
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, consumerGroupInfo.getConsumeFromWhere());
        assertEquals(ConsumeType.CONSUME_PASSIVELY, consumerGroupInfo.getConsumeType());
        assertEquals("TAG", consumerGroupInfo.getSubscriptionTable().get("topic").getExpressionType());
        assertEquals("*", consumerGroupInfo.getSubscriptionTable().get("topic").getSubString());


        clientService.unregister(ctx, NotifyClientTerminationRequest.newBuilder()
            .setClientId("clientId")
            .setConsumerGroup(Resource.newBuilder()
                .setName("consumerGroup")
                .build())
            .build());
        assertNull(clientService.getConsumerManager().getConsumerGroupInfo("consumerGroup"));
    }
}