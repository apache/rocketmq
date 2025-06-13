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

package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReceiveMessageActivityTest extends BaseActivityTest {

    protected static final String BROKER_NAME = "broker";
    protected static final String CLUSTER_NAME = "cluster";
    protected static final String BROKER_ADDR = "127.0.0.1:10911";
    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private ReceiveMessageActivity receiveMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setGrpcClientConsumerMinLongPollingTimeoutMillis(0);
        this.receiveMessageActivity = new ReceiveMessageActivity(messagingProcessor,
            grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testReceiveMessagePollingTime() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor.capture());

        ArgumentCaptor<Long> pollTimeCaptor = ArgumentCaptor.forClass(Long.class);
        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder()
            .setRequestTimeout(Durations.fromSeconds(3))
            .build());
        when(this.messagingProcessor.popMessage(any(), any(), anyString(), anyString(), anyInt(), anyLong(),
            pollTimeCaptor.capture(), anyInt(), any(), anyBoolean(), any(), isNull(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList())));

        ProxyContext context = createContext();
        context.setRemainingMs(1L);
        this.receiveMessageActivity.receiveMessage(
            context,
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setAutoRenew(true)
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("*")
                    .build())
                .build(),
            receiveStreamObserver
        );

        assertEquals(Code.MESSAGE_NOT_FOUND, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor.getAllValues()));
        assertEquals(0L, pollTimeCaptor.getValue().longValue());
    }

    @Test
    public void testReceiveMessageWithIllegalPollingTime() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor0 = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor0.capture());

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        final ProxyContext context = createContext();
        context.setClientVersion("5.0.2");
        context.setRemainingMs(-1L);
        final ReceiveMessageRequest request = ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
            .setAutoRenew(false)
            .setLongPollingTimeout(Duration.newBuilder().setSeconds(20).build())
            .setFilterExpression(FilterExpression.newBuilder()
                .setType(FilterType.TAG)
                .setExpression("*")
                .build())
            .build();
        this.receiveMessageActivity.receiveMessage(
            context,
            request,
            receiveStreamObserver
        );
        assertEquals(Code.BAD_REQUEST, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor0.getAllValues()));

        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor1 =
            ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor1.capture());
        context.setClientVersion("5.0.3");
        this.receiveMessageActivity.receiveMessage(
            context,
            request,
            receiveStreamObserver
        );
        assertEquals(Code.ILLEGAL_POLLING_TIME,
            getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor1.getAllValues()));
    }

    @Test
    public void testReceiveMessageIllegalFilter() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor.capture());

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        this.receiveMessageActivity.receiveMessage(
            createContext(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setAutoRenew(true)
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.SQL)
                    .setExpression("")
                    .build())
                .build(),
            receiveStreamObserver
        );

        assertEquals(Code.ILLEGAL_FILTER_EXPRESSION, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor.getAllValues()));
    }

    @Test
    public void testReceiveMessageIllegalInvisibleTimeTooSmall() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor.capture());

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        this.receiveMessageActivity.receiveMessage(
            createContext(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setAutoRenew(false)
                .setInvisibleDuration(Durations.fromSeconds(0))
                .build(),
            receiveStreamObserver
        );

        assertEquals(Code.ILLEGAL_INVISIBLE_TIME, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor.getAllValues()));
    }

    @Test
    public void testReceiveMessageIllegalInvisibleTimeTooLarge() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor.capture());

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        this.receiveMessageActivity.receiveMessage(
            createContext(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setAutoRenew(false)
                .setInvisibleDuration(Durations.fromDays(7))
                .build(),
            receiveStreamObserver
        );

        assertEquals(Code.ILLEGAL_INVISIBLE_TIME, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor.getAllValues()));
    }

    @Test
    public void testReceiveMessageAddReceiptHandle() {
        ConfigurationManager.getProxyConfig().setEnableProxyAutoRenew(true);
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        doNothing().when(receiveStreamObserver).onNext(any());
        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        MessageExt messageExt1 = new MessageExt();
        String msgId1 = "msgId1";
        String popCk1 = "0 0 60000 0 0 broker 0 0 0";
        messageExt1.setTopic(TOPIC);
        messageExt1.setMsgId(msgId1);
        MessageAccessor.putProperty(messageExt1, MessageConst.PROPERTY_POP_CK, popCk1);
        messageExt1.setBody("body1".getBytes());
        MessageExt messageExt2 = new MessageExt();
        String msgId2 = "msgId2";
        String popCk2 = "0 0 60000 0 0 broker 0 1 1000";
        messageExt2.setTopic(TOPIC);
        messageExt2.setMsgId(msgId2);
        MessageAccessor.putProperty(messageExt2, MessageConst.PROPERTY_POP_CK, popCk2);
        messageExt2.setBody("body2".getBytes());
        PopResult popResult = new PopResult(PopStatus.FOUND, Arrays.asList(messageExt1, messageExt2));
        when(this.messagingProcessor.popMessage(
            any(),
            any(),
            anyString(),
            anyString(),
            anyInt(),
            anyLong(),
            anyLong(),
            anyInt(),
            any(),
            anyBoolean(),
            any(),
            isNull(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(popResult));
        ArgumentCaptor<String> msgIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ReceiptHandle> receiptHandleCaptor = ArgumentCaptor.forClass(ReceiptHandle.class);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            receiptHandleCaptor.capture(),
            msgIdCaptor.capture(),
            anyString(),
            anyString(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(new AckResult()));

        // normal
        ProxyContext ctx = createContext();
        this.grpcChannelManager.createChannel(ctx, ctx.getClientID());
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
            .setAutoRenew(true)
            .setFilterExpression(FilterExpression.newBuilder()
                .setType(FilterType.TAG)
                .setExpression("*")
                .build())
            .build();
        this.receiveMessageActivity.receiveMessage(ctx, receiveMessageRequest, receiveStreamObserver);
        verify(this.messagingProcessor, times(0)).changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong());

        // abnormal
        this.grpcChannelManager.removeChannel(ctx.getClientID());
        this.receiveMessageActivity.receiveMessage(ctx, receiveMessageRequest, receiveStreamObserver);
        verify(this.messagingProcessor, times(2)).changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong());
        assertEquals(Arrays.asList(msgId1, msgId2), msgIdCaptor.getAllValues());
        assertEquals(Arrays.asList(popCk1, popCk2), receiptHandleCaptor.getAllValues().stream().map(ReceiptHandle::encode).collect(Collectors.toList()));
    }

    @Test
    public void testReceiveMessage() {
        StreamObserver<ReceiveMessageResponse> receiveStreamObserver = mock(ServerCallStreamObserver.class);
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(receiveStreamObserver).onNext(responseArgumentCaptor.capture());

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder().getDefaultInstanceForType());

        PopResult popResult = new PopResult(PopStatus.NO_NEW_MSG, new ArrayList<>());
        when(this.messagingProcessor.popMessage(
            any(),
            any(),
            anyString(),
            anyString(),
            anyInt(),
            anyLong(),
            anyLong(),
            anyInt(),
            any(),
            anyBoolean(),
            any(),
            isNull(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(popResult));

        this.receiveMessageActivity.receiveMessage(
            createContext(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setAutoRenew(true)
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("*")
                    .build())
                .build(),
            receiveStreamObserver
        );
        assertEquals(Code.MESSAGE_NOT_FOUND, getResponseCodeFromReceiveMessageResponseList(responseArgumentCaptor.getAllValues()));
    }

    private Code getResponseCodeFromReceiveMessageResponseList(List<ReceiveMessageResponse> responseList) {
        for (ReceiveMessageResponse response : responseList) {
            if (response.hasStatus()) {
                return response.getStatus().getCode();
            }
        }
        return null;
    }

    @Test
    public void testReceiveMessageQueueSelector() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueData> queueDatas = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            QueueData queueData = new QueueData();
            queueData.setBrokerName(BROKER_NAME + i);
            queueData.setReadQueueNums(1);
            queueData.setPerm(PermName.PERM_READ);
            queueDatas.add(queueData);
        }
        topicRouteData.setQueueDatas(queueDatas);

        List<BrokerData> brokerDatas = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            BrokerData brokerData = new BrokerData();
            brokerData.setCluster(CLUSTER_NAME);
            brokerData.setBrokerName(BROKER_NAME + i);
            HashMap<Long, String> brokerAddrs = new HashMap<>();
            brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
            brokerData.setBrokerAddrs(brokerAddrs);
            brokerDatas.add(brokerData);
        }
        topicRouteData.setBrokerDatas(brokerDatas);

        MessageQueueView messageQueueView = new MessageQueueView(TOPIC, topicRouteData, null);
        ReceiveMessageActivity.ReceiveMessageQueueSelector selector = new ReceiveMessageActivity.ReceiveMessageQueueSelector("");

        AddressableMessageQueue firstSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue secondSelect = selector.select(ProxyContext.create(), messageQueueView);
        AddressableMessageQueue thirdSelect = selector.select(ProxyContext.create(), messageQueueView);

        assertEquals(firstSelect, thirdSelect);
        assertNotEquals(firstSelect, secondSelect);

        for (int i = 0; i < 2; i++) {
            ReceiveMessageActivity.ReceiveMessageQueueSelector selectorBrokerName =
                new ReceiveMessageActivity.ReceiveMessageQueueSelector(BROKER_NAME + i);
            assertEquals(BROKER_NAME + i, selectorBrokerName.select(ProxyContext.create(), messageQueueView).getBrokerName());
        }
    }
}
