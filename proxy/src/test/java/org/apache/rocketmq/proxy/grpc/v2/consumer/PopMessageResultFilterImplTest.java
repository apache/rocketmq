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

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.PopMessageResultFilter;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PopMessageResultFilterImplTest {

    private final ProxyContext ctx = ProxyContext.create();
    private SubscriptionData subscriptionData;

    @Before
    public void before() throws Exception {
        subscriptionData = FilterAPI.buildSubscriptionData("topic", "*");
    }

    @Test
    public void testZeroMaxAttemptsStillDeliversFirstAttempt() {
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(0);

        MessageExt messageExt = new MessageExt();
        messageExt.setReconsumeTimes(0);

        assertEquals(PopMessageResultFilter.FilterResult.MATCH,
            filter.filterMessage(ctx, "group", subscriptionData, messageExt));
    }

    @Test
    public void testZeroMaxAttemptsGoesToDlqAfterRetry() {
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(0);

        MessageExt messageExt = new MessageExt();
        messageExt.setReconsumeTimes(1);

        assertEquals(PopMessageResultFilter.FilterResult.TO_DLQ,
            filter.filterMessage(ctx, "group", subscriptionData, messageExt));
    }

    @Test
    public void testPositiveMaxAttempts() {
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(2);

        MessageExt firstAttempt = new MessageExt();
        firstAttempt.setReconsumeTimes(0);
        MessageExt secondAttempt = new MessageExt();
        secondAttempt.setReconsumeTimes(1);
        MessageExt thirdAttempt = new MessageExt();
        thirdAttempt.setReconsumeTimes(2);

        assertEquals(PopMessageResultFilter.FilterResult.MATCH,
            filter.filterMessage(ctx, "group", subscriptionData, firstAttempt));
        assertEquals(PopMessageResultFilter.FilterResult.MATCH,
            filter.filterMessage(ctx, "group", subscriptionData, secondAttempt));
        assertEquals(PopMessageResultFilter.FilterResult.TO_DLQ,
            filter.filterMessage(ctx, "group", subscriptionData, thirdAttempt));
    }

    @Test
    public void testTagMismatchReturnsNoMatch() {
        PopMessageResultFilterImpl filter = new PopMessageResultFilterImpl(16);

        SubscriptionData tagSubscription;
        try {
            tagSubscription = FilterAPI.buildSubscriptionData("topic", "tagA");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        MessageExt messageExt = new MessageExt();
        messageExt.setTags("tagB");

        assertEquals(PopMessageResultFilter.FilterResult.NO_MATCH,
            filter.filterMessage(ctx, "group", tagSubscription, messageExt));
    }
}
