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

package org.apache.rocketmq.broker.pop.orderly;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.assertj.core.util.Lists;
import org.junit.After;
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
    private QueueLevelConsumerManager consumerOrderInfoManager;
    private AtomicLong notifiedAt;

    private final BrokerConfig brokerConfig = new BrokerConfig();
    private final PopMessageProcessor popMessageProcessor = mock(PopMessageProcessor.class);
    private final BrokerController brokerController = mock(BrokerController.class);

    @Before
    public void before() throws ConsumeQueueException {
        notifiedAt = new AtomicLong(-1);
        brokerConfig.setEnableNotifyAfterPopOrderLockRelease(true);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        doAnswer((Answer<Void>) mock -> {
            notifiedAt.compareAndSet(-1, System.currentTimeMillis());
            return null;
        }).when(popMessageProcessor).notifyLongPollingRequestIfNeed(anyString(), anyString(), anyInt());

        consumerOrderInfoManager = new QueueLevelConsumerManager(brokerController);
        popTime = System.currentTimeMillis();
    }

    @After
    public void after() {
        consumerOrderInfoManager.shutdown();
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
        awaitNotification(popTime + 3000, Duration.ofSeconds(4));
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
        awaitNotification(popTime, Duration.ofSeconds(1));
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
        awaitNotification(popTime + 5000, Duration.ofSeconds(6));
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
        awaitNotification(popTime + 1000, Duration.ofSeconds(2));
    }

    @Test
    public void testRecover() {
        long recoverPopTime = System.currentTimeMillis();
        QueueLevelConsumerManager savedConsumerOrderInfoManager = new QueueLevelConsumerManager();
        savedConsumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            recoverPopTime,
            3000,
            Lists.newArrayList(1L),
            new StringBuilder()
        );
        String encodedData = savedConsumerOrderInfoManager.encode();

        consumerOrderInfoManager.decode(encodedData);
        awaitNotification(recoverPopTime + 3000, Duration.ofSeconds(4));
    }

    private void awaitNotification(long visibleAt, Duration timeout) {
        // The visibility deadline starts at POP time, before setup/serialization and the await call.
        await().atMost(timeout).until(() -> notifiedAt.get() >= 0
            && consumerOrderInfoManager.getConsumerOrderInfoLockManager().getTimeoutMap().isEmpty());
        assertTrue("Notification must not precede the visibility deadline", notifiedAt.get() >= visibleAt);
    }
}
