package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SystemProperties;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

public class DefaultWriteQueueSelectorTest extends BaseServiceTest {

    private static final String TOPIC_NAME = "namespace%topic";

    @Override
    public void beforeEach() throws Throwable {
        SelectableMessageQueue queue = new SelectableMessageQueue(
            new MessageQueue("topic", "selectOrderQueue", 0),
            "selectOrderQueueAddr");
        when(topicRouteCache.selectOneWriteQueueByKey(anyString(), anyString()))
            .thenReturn(queue);

        queue = new SelectableMessageQueue(
            new MessageQueue("topic", "selectTargetQueue", 0),
            "selectTargetQueueAddr");
        when(topicRouteCache.selectOneWriteQueue(anyString(), anyString(), anyInt()))
            .thenReturn(queue);

        queue = new SelectableMessageQueue(
            new MessageQueue("topic", "selectNormalQueue", 0),
            "selectNormalQueueAddr");
        when(topicRouteCache.selectOneWriteQueue(anyString(), isNull()))
            .thenReturn(queue);
    }

    @Test
    public void selectWithShardingKey() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .putUserProperties(MessageConst.PROPERTY_SHARDING_KEY, "key")
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("msgId")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .build();
        WriteQueueSelector queueSelector = new DefaultWriteQueueSelector(this.topicRouteCache);
        SelectableMessageQueue queue = queueSelector.selectQueue(Context.current(), request,
            GrpcConverter.buildSendMessageRequestHeader(request, TOPIC_NAME),
            GrpcConverter.buildMessage(request.getMessagesList(), TOPIC_NAME));

        assertEquals("selectOrderQueue", queue.getBrokerName());
        assertEquals("selectOrderQueueAddr", queue.getBrokerAddr());
    }

    @Test
    public void selectNormalQueue() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("msgId")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .build();
        WriteQueueSelector queueSelector = new DefaultWriteQueueSelector(this.topicRouteCache);
        SelectableMessageQueue queue = queueSelector.selectQueue(Context.current(), request,
            GrpcConverter.buildSendMessageRequestHeader(request, TOPIC_NAME),
            GrpcConverter.buildMessage(request.getMessagesList(), TOPIC_NAME));

        assertEquals("selectNormalQueue", queue.getBrokerName());
        assertEquals("selectNormalQueueAddr", queue.getBrokerAddr());
    }

    @Test
    public void selectTargetQueue() {
        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageId("msgId")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .setMessageQueue(apache.rocketmq.v2.MessageQueue.newBuilder()
                .setBroker(Broker.newBuilder()
                    .setName("brokerName")
                    .build())
                .build())
            .build();
        WriteQueueSelector queueSelector = new DefaultWriteQueueSelector(this.topicRouteCache);
        SelectableMessageQueue queue = queueSelector.selectQueue(Context.current(), request,
            GrpcConverter.buildSendMessageRequestHeader(request, TOPIC_NAME),
            GrpcConverter.buildMessage(request.getMessagesList(), TOPIC_NAME));

        assertEquals("selectTargetQueue", queue.getBrokerName());
        assertEquals("selectTargetQueueAddr", queue.getBrokerAddr());
    }
}