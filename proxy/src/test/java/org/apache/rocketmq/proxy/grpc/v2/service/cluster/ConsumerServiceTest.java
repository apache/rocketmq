package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class ConsumerServiceTest extends BaseServiceTest {

    @Mock
    private ReadQueueSelector readQueueSelector;

    private ConsumerService consumerService;

    @Override
    public void beforeEach() throws Throwable {
        consumerService = new ConsumerService(this.connectorManager, this.grpcClientManager);
        consumerService.setReadQueueSelector(readQueueSelector);
    }

    @Test
    public void testReceiveMessage() throws Exception {
        SelectableMessageQueue selectableMessageQueue = new SelectableMessageQueue(
            new MessageQueue("namespace%topic", "brokerName", 0), "brokerAddr");
        when(readQueueSelector.select(any(), any(), any())).thenReturn(selectableMessageQueue);

        Settings clientSettings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .setFifo(false)
                .build())
            .build();
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        List<MessageExt> messageExtList = Lists.newArrayList(
            createMessageExt("msg1", "msg1"),
            createMessageExt("msg2", "msg2")
        );
        PopResult popResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(readConsumerClient.popMessage(anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
        when(writeConsumerClient.ackMessage(anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(new AckResult()));

        Context ctx = Context.current().withDeadlineAfter(3, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor());
        AtomicReference<String> ackHandler = new AtomicReference<>();
        consumerService.setAckNoMatchedMessageHook((ctx1, request, response, t) -> ackHandler.set(request.getExtraInfo()));
        ReceiveMessageResponse response = consumerService.receiveMessage(ctx,
            ReceiveMessageRequest.newBuilder()
                .setMessageQueue(apache.rocketmq.v2.MessageQueue.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setResourceNamespace("namespace")
                        .setName("topic")
                        .build())
                    .build())
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("msg1")
                    .build())
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getMessagesCount());
        assertEquals("msg1", response.getMessages(0).getSystemProperties().getMessageId());
        assertEquals(ReceiptHandle.create(messageExtList.get(1)).getReceiptHandle(), ackHandler.get());
    }

    @Test
    public void testAckMessage() throws Exception {
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        when(writeConsumerClient.ackMessage(anyString(), any())).thenReturn(CompletableFuture.completedFuture(ackResult));

        AckMessageResponse response = consumerService.ackMessage(Context.current(), AckMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .addEntries(AckMessageEntry.newBuilder()
                .setMessageId("msgId")
                .setReceiptHandle(createReceiptHandle().encode()))
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
    }

    @Test
    public void testNackMessageToDLQ() throws Exception {
        ReceiptHandle receiptHandle = createReceiptHandle();
        AtomicReference<ConsumerSendMsgBackRequestHeader> headerRef = new AtomicReference<>();
        doAnswer(mock -> {
            headerRef.set(mock.getArgument(1));
            return CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""));
        }).when(producerClient).sendMessageBack(anyString(), any());
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");

        Settings clientSettings = createClientSettings(3);
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        NackMessageResponse response = consumerService.nackMessage(Context.current(), NackMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setReceiptHandle(receiptHandle.encode())
            .setDeliveryAttempt(3)
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(receiptHandle.getCommitLogOffset(), headerRef.get().getOffset().longValue());
    }

    @Test
    public void testNackMessage() throws Exception {
        ReceiptHandle receiptHandle = createReceiptHandle();
        AtomicReference<ChangeInvisibleTimeRequestHeader> headerRef = new AtomicReference<>();
        doAnswer(mock -> {
            headerRef.set(mock.getArgument(2));
            AckResult ackResult = new AckResult();
            ackResult.setStatus(AckStatus.OK);
            return CompletableFuture.completedFuture(ackResult);
        }).when(writeConsumerClient).changeInvisibleTimeAsync(anyString(), anyString(), any());
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");

        Settings clientSettings = createClientSettings(3);
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        NackMessageResponse response = consumerService.nackMessage(Context.current(), NackMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setReceiptHandle(receiptHandle.encode())
            .setDeliveryAttempt(1)
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(receiptHandle.getOffset(), headerRef.get().getOffset().longValue());
        assertEquals(receiptHandle.encode(), headerRef.get().getExtraInfo());
    }

    private Settings createClientSettings(int maxDeliveryAttempts) {
        return Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .setBackoffPolicy(RetryPolicy.newBuilder()
                    .setMaxAttempts(maxDeliveryAttempts)
                    .build())
                .build())
            .build();
    }
}