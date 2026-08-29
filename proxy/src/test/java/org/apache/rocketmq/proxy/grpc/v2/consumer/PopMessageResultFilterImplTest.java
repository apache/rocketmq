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

import java.util.Collections;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.PopMessageResultFilter.FilterResult;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PopMessageResultFilterImplTest extends BaseActivityTest {

    private static final String CONSUMER_GROUP = "consumerGroup";

    private ProxyContext ctx;
    private SubscriptionData subscriptionData;

    @Before
    public void before() throws Throwable {
        super.before();
        // BaseActivityTest installs a mock; the default-consumer-settings contract lives on the real instance,
        // so replace it with a spy (matching GrpcClientSettingsManagerTest).
        grpcClientSettingsManager = spy(new GrpcClientSettingsManager(messagingProcessor));
        ctx = createContext();
        // empty tagsSet subscribes to all tags (SUB_ALL), so the DLQ decision depends only on reconsumeTimes
        subscriptionData = mock(SubscriptionData.class);
        when(subscriptionData.getTagsSet()).thenReturn(Collections.emptySet());
    }

    private static MessageExt message(int reconsumeTimes) {
        MessageExt messageExt = new MessageExt();
        messageExt.setReconsumeTimes(reconsumeTimes);
        return messageExt;
    }

    @Test
    public void testFreshMessageNotRoutedToDlqWithDefaultConsumerMaxAttempts() {
        // Regression for apache/rocketmq#8714: when client settings are missing, ReceiveMessageActivity must
        // fall back to the real consumer default (maxAttempts = retryMaxTimes + 1), not the protobuf empty
        // default whose maxAttempts == 0. With maxAttempts == 0 every message, including a fresh one with
        // reconsumeTimes == 0, would be routed straight to the DLQ.
        int maxAttempts = grpcClientSettingsManager.getDefaultConsumerSettings().getBackoffPolicy().getMaxAttempts();
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(maxAttempts);

        assertEquals("fresh message (reconsumeTimes == 0) must not be DLQ'd under the real consumer default",
            FilterResult.MATCH, filter.filterMessage(ctx, CONSUMER_GROUP, subscriptionData, message(0)));
    }

    @Test
    public void testZeroMaxAttemptsRoutesFreshMessageToDlq() {
        // Documents the bug the PR fixes: the protobuf empty default yields maxAttempts == 0, which DLQs
        // a fresh message (reconsumeTimes == 0) because 0 >= 0. Kept as a guard so the fallback never
        // regresses back to an empty default.
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(0);
        assertEquals(FilterResult.TO_DLQ, filter.filterMessage(ctx, CONSUMER_GROUP, subscriptionData, message(0)));
    }

    @Test
    public void testExhaustedMessageRoutedToDlq() {
        // A message that has been retried up to maxAttempts is correctly DLQ'd, confirming the filter still
        // enforces the retry ceiling once a real default is in place.
        int maxAttempts = grpcClientSettingsManager.getDefaultConsumerSettings().getBackoffPolicy().getMaxAttempts();
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(maxAttempts);
        assertEquals(FilterResult.TO_DLQ,
            filter.filterMessage(ctx, CONSUMER_GROUP, subscriptionData, message(maxAttempts)));
    }
}
