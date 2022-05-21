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

package org.apache.rocketmq.thinclient.impl.consumer;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.message.MessageView;
import org.apache.rocketmq.thinclient.impl.ClientManagerImpl;
import org.apache.rocketmq.thinclient.impl.ClientManagerRegistry;
import org.apache.rocketmq.thinclient.impl.TelemetrySession;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.apache.rocketmq.thinclient.misc.ThreadFactoryImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SimpleConsumerImplTest extends TestBase {
    @Mock
    private ClientManagerImpl clientManager;
    @Mock
    private StreamObserver<TelemetryCommand> telemetryRequestObserver;
    @InjectMocks
    private ClientManagerRegistry clientManagerRegistry = ClientManagerRegistry.getInstance();
    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint(FAKE_ACCESS_POINT).build();

    private final Duration awaitDuration = Duration.ofSeconds(3);
    private final Map<String, FilterExpression> subscriptionExpressions = createSubscriptionExpressions(FAKE_TOPIC_0);

    private SimpleConsumerImpl simpleConsumer;

    private void start(SimpleConsumerImpl simpleConsumer) throws ClientException {
        SettableFuture<QueryRouteResponse> future0 = SettableFuture.create();
        Status status = Status.newBuilder().setCode(Code.OK).build();
        List<MessageQueue> messageQueueList = new ArrayList<>();
        MessageQueue mq = MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(FAKE_TOPIC_0))
            .setPermission(Permission.READ_WRITE)
            .setBroker(Broker.newBuilder().setName(FAKE_BROKER_NAME_0).setEndpoints(fakePbEndpoints0())).setId(0).build();
        messageQueueList.add(mq);
        QueryRouteResponse response = QueryRouteResponse.newBuilder().setStatus(status).addAllMessageQueues(messageQueueList).build();
        future0.set(response);
        when(clientManager.queryRoute(any(Endpoints.class), any(Metadata.class), any(QueryRouteRequest.class), any(Duration.class)))
            .thenReturn(future0);
        when(clientManager.telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class), any(TelemetrySession.class)))
            .thenReturn(telemetryRequestObserver);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));
        when(clientManager.getScheduler()).thenReturn(scheduler);
        doNothing().when(telemetryRequestObserver).onNext(any(TelemetryCommand.class));

        Subscription subscription = Subscription.newBuilder().build();
        Settings settings = Settings.newBuilder().setSubscription(subscription).build();
        final Service service = simpleConsumer.startAsync();
        simpleConsumer.getSimpleConsumerSettings().applySettingsCommand(settings);
        service.awaitRunning();
    }

    private void shutdown(SimpleConsumerImpl simpleConsumer) {
        final Service clientManagerService = mock(Service.class);
        when(clientManager.stopAsync()).thenReturn(clientManagerService);
        doNothing().when(clientManagerService).awaitTerminated();
        simpleConsumer.stopAsync().awaitTerminated();
    }

    @Test(expected = IllegalStateException.class)
    public void testReceiveWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        simpleConsumer.receive(1, Duration.ofSeconds(1));
    }

    @Test(expected = IllegalStateException.class)
    public void testAckWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        simpleConsumer.ack(fakeMessageViewImpl());
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeWithoutStart() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        simpleConsumer.subscribe(FAKE_TOPIC_1, FilterExpression.SUB_ALL);
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribeWithoutStart() {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        simpleConsumer.unsubscribe(FAKE_TOPIC_0);
    }

    @Test
    public void testStartAndShutdown() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        shutdown(simpleConsumer);
    }

    @Test
    public void testSubscribeWithSubscriptionOverwriting() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        final FilterExpression filterExpression = new FilterExpression(FAKE_TAG_0);
        simpleConsumer.subscribe(FAKE_TOPIC_0, filterExpression);
        shutdown(simpleConsumer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReceiveWithAllTopicsAreUnsubscribed() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        simpleConsumer.unsubscribe(FAKE_TOPIC_0);
        try {
            simpleConsumer.receive(1, Duration.ofSeconds(1));
        } finally {
            shutdown(simpleConsumer);
        }
    }

    @Test
    public void testReceiveMessageSuccess() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        int receivedMessageCount = 16;
        final ListenableFuture<Iterator<ReceiveMessageResponse>> future = okReceiveMessageResponsesFuture(FAKE_TOPIC_0, receivedMessageCount);
        when(clientManager.receiveMessage(any(Endpoints.class), any(Metadata.class), any(ReceiveMessageRequest.class), any(Duration.class))).thenReturn(future);
        final List<MessageView> messageViews = simpleConsumer.receive(1, Duration.ofSeconds(1));
        verify(clientManager, times(1)).receiveMessage(any(Endpoints.class), any(Metadata.class), any(ReceiveMessageRequest.class), any(Duration.class));
        assertEquals(receivedMessageCount, messageViews.size());
        shutdown(simpleConsumer);
    }

    @Test
    public void testAckMessageSuccess() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        try {
            final MessageViewImpl messageView = fakeMessageViewImpl();
            final ListenableFuture<AckMessageResponse> future = okAckMessageResponseFuture();
            when(clientManager.ackMessage(any(Endpoints.class), any(Metadata.class), any(AckMessageRequest.class), any(Duration.class))).thenReturn(future);
            simpleConsumer.ack(messageView);
        } finally {
            shutdown(simpleConsumer);
        }
    }

    @Test
    public void testChangeInvisibleDurationSuccess() throws ClientException {
        simpleConsumer = new SimpleConsumerImpl(clientConfiguration, FAKE_GROUP_0, awaitDuration, subscriptionExpressions);
        start(simpleConsumer);
        try {
            final MessageViewImpl messageView = fakeMessageViewImpl();
            final ListenableFuture<ChangeInvisibleDurationResponse> future = okChangeInvisibleDurationResponseFuture(FAKE_RECEIPT_HANDLE_1);
            when(clientManager.changeInvisibleDuration(any(Endpoints.class), any(Metadata.class), any(ChangeInvisibleDurationRequest.class), any(Duration.class))).thenReturn(future);
            simpleConsumer.changeInvisibleDuration0(messageView, Duration.ofSeconds(3));
            assertEquals(FAKE_RECEIPT_HANDLE_1, messageView.getReceiptHandle());
        } finally {
            shutdown(simpleConsumer);
        }
    }
}