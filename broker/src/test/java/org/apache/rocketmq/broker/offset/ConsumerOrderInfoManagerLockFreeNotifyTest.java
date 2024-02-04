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

package org.apache.rocketmq.broker.offset;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerOrderInfoManagerLockFreeNotifyTest {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final int QUEUE_ID_0 = 0;

    private long popTime;
    private ConsumerOrderInfoManager consumerOrderInfoManager;
    private AtomicBoolean notified;

    private final BrokerConfig brokerConfig = new BrokerConfig();
    private final PopMessageProcessor popMessageProcessor = mock(PopMessageProcessor.class);
    private final BrokerController brokerController = mock(BrokerController.class);

    @Before
    public void before() {
        notified = new AtomicBoolean(false);
        brokerConfig.setEnableNotifyAfterPopOrderLockRelease(true);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        doAnswer((Answer<Void>) mock -> {
            notified.set(true);
            return null;
        }).when(popMessageProcessor).notifyLongPollingRequestIfNeed(anyString(), anyString(), anyInt());

        consumerOrderInfoManager = new ConsumerOrderInfoManager(brokerController);
        popTime = System.currentTimeMillis();
    }

    @Test
    public void testConsumeMessageThenNoAck() {
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        await().atLeast(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).until(notified::get);
        assertTrue(consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
    }

    @Test
    public void testConsumeMessageThenAck() {
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        consumerOrderInfoManager.commitAndNext(
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            1,
            popTime
        );
        await().atMost(Duration.ofSeconds(1)).until(notified::get);
        assertTrue(consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
    }

    @Test
    public void testConsumeTheChangeInvisibleLonger() {
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        consumerOrderInfoManager.updateNextVisibleTime(
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            1,
            popTime,
            popTime + 5000
        );
        await().atLeast(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(6)).until(notified::get);
        assertTrue(consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
    }

    @Test
    public void testConsumeTheChangeInvisibleShorter() {
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        consumerOrderInfoManager.updateNextVisibleTime(
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            1,
            popTime,
            popTime + 1000
        );
        await().atLeast(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(notified::get);
        assertTrue(consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
    }

    @Test
    public void testRecover() {
        ConsumerOrderInfoManager savedConsumerOrderInfoManager = new ConsumerOrderInfoManager();
        savedConsumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        String encodedData = savedConsumerOrderInfoManager.encode();

        consumerOrderInfoManager.decode(encodedData);
        await().atLeast(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).until(notified::get);
        assertTrue(consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
    }
}