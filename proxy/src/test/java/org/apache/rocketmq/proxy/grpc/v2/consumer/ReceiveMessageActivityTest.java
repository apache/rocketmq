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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReceiveMessageActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private ReceiveMessageActivity receiveMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.receiveMessageActivity = new ReceiveMessageActivity(this.messagingProcessor, this.grpcClientSettingsManager);
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
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.SQL)
                    .setExpression("")
                    .build())
                .build(),
            receiveStreamObserver
        );

        assertEquals(Code.ILLEGAL_FILTER_EXPRESSION, responseArgumentCaptor.getValue().getStatus().getCode());
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
            anyLong()
        )).thenReturn(CompletableFuture.completedFuture(popResult));

        this.receiveMessageActivity.receiveMessage(
            createContext(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("*")
                    .build())
                .build(),
            receiveStreamObserver
        );
        assertEquals(Code.MESSAGE_NOT_FOUND, responseArgumentCaptor.getValue().getStatus().getCode());
    }
}