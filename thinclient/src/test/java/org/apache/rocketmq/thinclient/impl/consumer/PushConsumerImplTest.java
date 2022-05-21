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

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.Service;
import com.google.protobuf.util.Durations;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.thinclient.impl.ClientManagerImpl;
import org.apache.rocketmq.thinclient.impl.ClientManagerRegistry;
import org.apache.rocketmq.thinclient.impl.TelemetrySession;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.apache.rocketmq.thinclient.misc.ThreadFactoryImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PushConsumerImplTest extends TestBase {
    @Mock
    private ClientManagerImpl clientManager;

    @Mock
    private StreamObserver<TelemetryCommand> telemetryRequestObserver;

    @SuppressWarnings("unused")
    @InjectMocks
    private ClientManagerRegistry clientManagerRegistry = ClientManagerRegistry.getInstance();

    private final Map<String, FilterExpression> subscriptionExpressions = createSubscriptionExpressions(FAKE_TOPIC_0);

    private final MessageListener messageListener = messageView -> ConsumeResult.OK;

    private final int maxCacheMessageCount = 8;
    private final int maxCacheMessageSizeInBytes = 1024;
    private final int consumptionThreadCount = 4;

    private final ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setAccessPoint(FAKE_ACCESS_POINT).build();

    private PushConsumerImpl pushConsumer;

    private void start(PushConsumerImpl pushConsumer) throws ClientException {
        when(clientManager.queryRoute(any(Endpoints.class), any(Metadata.class), any(QueryRouteRequest.class), any(Duration.class)))
            .thenReturn(okQueryRouteResponseFuture());
        when(clientManager.telemetry(any(Endpoints.class), any(Metadata.class), any(Duration.class), any(TelemetrySession.class)))
            .thenReturn(telemetryRequestObserver);
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("TestScheduler"));
        when(clientManager.getScheduler()).thenReturn(scheduler);
        doNothing().when(telemetryRequestObserver).onNext(any(TelemetryCommand.class));

        CustomizedBackoff customizedBackoff = CustomizedBackoff.newBuilder().addNext(Durations.fromNanos(Duration.ofSeconds(3).toNanos())).build();
        RetryPolicy retryPolicy = RetryPolicy.newBuilder().setMaxAttempts(17).setCustomizedBackoff(customizedBackoff).build();
        Subscription subscription = Subscription.newBuilder().build();
        Settings settings = Settings.newBuilder().setSubscription(subscription).setBackoffPolicy(retryPolicy).build();
        final Service service = pushConsumer.startAsync();
        pushConsumer.getPushConsumerSettings().applySettingsCommand(settings);
        service.awaitRunning();
    }

    private void shutdown(PushConsumerImpl pushConsumer) {
        final Service clientManagerService = mock(Service.class);
        when(clientManager.stopAsync()).thenReturn(clientManagerService);
        doNothing().when(clientManagerService).awaitTerminated();
        pushConsumer.stopAsync().awaitTerminated();
    }

    @Test
    public void testScanAssignment() throws ExecutionException, InterruptedException, ClientException {
        pushConsumer = new PushConsumerImpl(clientConfiguration, FAKE_GROUP_0, subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        start(pushConsumer);
        when(clientManager.queryAssignment(any(Endpoints.class), any(Metadata.class), any(QueryAssignmentRequest.class), any(Duration.class))).thenReturn(okQueryAssignmentResponseFuture());
        pushConsumer.scanAssignments();
        verify(clientManager, atLeast(1)).queryRoute(any(Endpoints.class), any(Metadata.class), any(QueryRouteRequest.class), any(Duration.class));
        verify(clientManager, atLeast(1)).queryAssignment(any(Endpoints.class), any(Metadata.class), any(QueryAssignmentRequest.class), any(Duration.class));
        Assert.assertEquals(okQueryAssignmentResponseFuture().get().getAssignmentsCount(), pushConsumer.getQueueSize());
        when(clientManager.queryAssignment(any(Endpoints.class), any(Metadata.class), any(QueryAssignmentRequest.class), any(Duration.class))).thenReturn(okEmptyQueryAssignmentResponseFuture());
        pushConsumer.scanAssignments();
        Assert.assertEquals(0, pushConsumer.getQueueSize());
        shutdown(pushConsumer);
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscribeWithoutStart() throws ClientException {
        pushConsumer = new PushConsumerImpl(clientConfiguration, FAKE_GROUP_0, subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        pushConsumer.subscribe(FAKE_TOPIC_0, new FilterExpression(FAKE_TOPIC_0));
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribeWithoutStart() {
        pushConsumer = new PushConsumerImpl(clientConfiguration, FAKE_GROUP_0, subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        pushConsumer.unsubscribe(FAKE_TOPIC_0);
    }

    @Test
    public void testSubscribeWithSubscriptionOverwriting() throws ClientException {
        pushConsumer = new PushConsumerImpl(clientConfiguration, FAKE_GROUP_0, subscriptionExpressions, messageListener, maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        start(pushConsumer);
        final FilterExpression filterExpression = new FilterExpression(FAKE_TAG_0);
        pushConsumer.subscribe(FAKE_TOPIC_0, filterExpression);
        shutdown(pushConsumer);
    }
}