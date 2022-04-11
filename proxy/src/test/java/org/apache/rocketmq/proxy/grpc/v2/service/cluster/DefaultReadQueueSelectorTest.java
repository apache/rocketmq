package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.ReceiveMessageRequest;
import io.grpc.Context;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

public class DefaultReadQueueSelectorTest extends BaseServiceTest {

    private final String brokerAddress = "127.0.0.1:10911";

    @Override
    public void beforeEach() throws Throwable {
    }

    @Test
    public void test() throws Exception {
        SelectableMessageQueue messageQueue1 = new SelectableMessageQueue(
            new MessageQueue("readBrokerTopicByName", "brokerName", 0), "brokerAddr1");
        SelectableMessageQueue messageQueue2 = new SelectableMessageQueue(
            new MessageQueue("oneReadBroker", "brokerName", 0), "brokerAddr1");

        when(topicRouteCache.selectReadBrokerByName(eq("readBrokerTopicByName"), anyString())).thenReturn(messageQueue1);
        when(topicRouteCache.selectOneReadBroker(eq("oneReadBroker"), isNull())).thenReturn(messageQueue2);

        ReadQueueSelector readQueueSelector = new DefaultReadQueueSelector(topicRouteCache);

        {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setTopic("readBrokerTopicByName");
            SelectableMessageQueue messageQueue = readQueueSelector.select(Context.current(),
                ReceiveMessageRequest.newBuilder()
                    .setMessageQueue(apache.rocketmq.v2.MessageQueue.newBuilder()
                        .setBroker(Broker.newBuilder()
                            .setName("brokerName")
                            .build())
                        .build())
                    .build(),
                requestHeader);
            assertSame(messageQueue1, messageQueue);
        }

        {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setTopic("oneReadBroker");
            SelectableMessageQueue messageQueue = readQueueSelector.select(Context.current(),
                ReceiveMessageRequest.newBuilder()
                    .build(),
                requestHeader);
            assertSame(messageQueue2, messageQueue);
        }

        {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setTopic("topic");
            SelectableMessageQueue messageQueue = readQueueSelector.select(Context.current(),
                ReceiveMessageRequest.newBuilder()
                    .build(),
                requestHeader);
            assertNull(messageQueue);
        }
    }
}