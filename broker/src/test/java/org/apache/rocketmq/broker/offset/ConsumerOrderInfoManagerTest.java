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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerOrderInfoManagerTest {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final int QUEUE_ID_0 = 0;
    private static final int QUEUE_ID_1 = 1;

    private long popTime;
    private ConsumerOrderInfoManager consumerOrderInfoManager;

    @Before
    public void before() {
        consumerOrderInfoManager = new ConsumerOrderInfoManager();
        popTime = System.currentTimeMillis();
    }

    @Test
    public void testCommitAndNext() {
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
        assertEncodeAndDecode();
        assertEquals(-2, consumerOrderInfoManager.commitAndNext(
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            1L,
            popTime - 10
        ));
        assertEncodeAndDecode();
        assertTrue(consumerOrderInfoManager.checkBlock(
            null,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            TimeUnit.SECONDS.toMillis(3)
        ));

        assertEquals(2, consumerOrderInfoManager.commitAndNext(
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            1L,
            popTime
        ));
        assertEncodeAndDecode();
        assertFalse(consumerOrderInfoManager.checkBlock(
            null,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            TimeUnit.SECONDS.toMillis(3)
        ));
    }

    @Test
    public void testConsumedCount() {
        {
            // consume three new messages
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(1L, 2L, 3L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(1, orderInfoMap.size());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
        }

        {
            // reconsume same messages
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(1L, 2L, 3L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(4, orderInfoMap.size());
            assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            for (int i = 1; i <= 3; i++) {
                assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, i)).intValue());
            }
        }

        {
            // reconsume last two message
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(2L, 3L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(3, orderInfoMap.size());
            assertEquals(2, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            for (int i = 2; i <= 3; i++) {
                assertEquals(2, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, i)).intValue());
            }
        }

        {
            // consume a new message and reconsume last message
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(3L, 4L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(2, orderInfoMap.size());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            assertEquals(3, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 3)).intValue());
        }

        {
            // consume two new messages
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(5L, 6L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(1, orderInfoMap.size());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
        }
    }

    @Test
    public void testConsumedCountForMultiQueue() {
        {
            // consume two new messages
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(0L),
                orderInfoBuilder
            );
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_1,
                popTime,
                3000,
                Lists.newArrayList(0L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(2, orderInfoMap.size());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_1)).intValue());
        }
        {
            // reconsume two message
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(0L),
                orderInfoBuilder
            );
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_1,
                popTime,
                3000,
                Lists.newArrayList(0L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(4, orderInfoMap.size());
            assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_1)).intValue());
            assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 0L)).intValue());
            assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_1, 0L)).intValue());
        }
        {
            // reconsume with a new message
            StringBuilder orderInfoBuilder = new StringBuilder();
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                3000,
                Lists.newArrayList(0L, 1L),
                orderInfoBuilder
            );
            consumerOrderInfoManager.update(
                null,
                false,
                TOPIC,
                GROUP,
                QUEUE_ID_1,
                popTime,
                3000,
                Lists.newArrayList(0L),
                orderInfoBuilder
            );
            assertEncodeAndDecode();
            Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
            assertEquals(4, orderInfoMap.size());
            assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
            assertEquals(2, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_1)).intValue());
            assertEquals(2, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 0L)).intValue());
            assertNull(orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 1L)));
            assertEquals(2, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_1, 0L)).intValue());
        }
    }

    @Test
    public void testUpdateNextVisibleTime() {
        long invisibleTime = 3000;

        StringBuilder orderInfoBuilder = new StringBuilder();
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            1,
            Lists.newArrayList(1L, 2L, 3L),
            orderInfoBuilder
        );

        consumerOrderInfoManager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID_0, 2L, popTime, System.currentTimeMillis() + invisibleTime);
        assertEncodeAndDecode();

        assertEquals(2, consumerOrderInfoManager.commitAndNext(TOPIC, GROUP, QUEUE_ID_0, 1L, popTime));
        assertEncodeAndDecode();
        assertEquals(2, consumerOrderInfoManager.commitAndNext(TOPIC, GROUP, QUEUE_ID_0, 3L, popTime));
        assertEncodeAndDecode();

        await().atMost(Duration.ofSeconds(invisibleTime + 1)).until(() -> !consumerOrderInfoManager.checkBlock(null, TOPIC, GROUP, QUEUE_ID_0, invisibleTime));

        orderInfoBuilder = new StringBuilder();
        consumerOrderInfoManager.update(
            null,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            1,
            Lists.newArrayList(2L, 3L, 4L),
            orderInfoBuilder
        );

        consumerOrderInfoManager.updateNextVisibleTime(TOPIC, GROUP, QUEUE_ID_0, 2L, popTime, System.currentTimeMillis() + invisibleTime);
        assertEncodeAndDecode();

        assertEquals(2, consumerOrderInfoManager.commitAndNext(TOPIC, GROUP, QUEUE_ID_0, 3L, popTime));
        assertEncodeAndDecode();
        assertEquals(2, consumerOrderInfoManager.commitAndNext(TOPIC, GROUP, QUEUE_ID_0, 4L, popTime));
        assertEncodeAndDecode();
        assertTrue(consumerOrderInfoManager.checkBlock(null, TOPIC, GROUP, QUEUE_ID_0, invisibleTime));

        assertEquals(5L, consumerOrderInfoManager.commitAndNext(TOPIC, GROUP, QUEUE_ID_0, 2L, popTime));
        assertEncodeAndDecode();
        assertFalse(consumerOrderInfoManager.checkBlock(null, TOPIC, GROUP, QUEUE_ID_0, invisibleTime));
    }

    @Test
    public void testAutoCleanAndEncode() {
        BrokerConfig brokerConfig = new BrokerConfig();
        BrokerController brokerController = mock(BrokerController.class);
        TopicConfigManager topicConfigManager = mock(TopicConfigManager.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);

        SubscriptionGroupManager subscriptionGroupManager = mock(SubscriptionGroupManager.class);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(subscriptionGroupManager.containsSubscriptionGroup(GROUP)).thenReturn(true);

        TopicConfig topicConfig = new TopicConfig(TOPIC);
        when(topicConfigManager.selectTopicConfig(eq(TOPIC))).thenReturn(topicConfig);

        ConsumerOrderInfoManager consumerOrderInfoManager = new ConsumerOrderInfoManager(brokerController);

        {
            consumerOrderInfoManager.update(null, false,
                "errTopic",
                "errGroup",
                QUEUE_ID_0,
                popTime,
                1,
                Lists.newArrayList(2L, 3L, 4L),
                new StringBuilder());

            consumerOrderInfoManager.autoClean();
            assertEquals(0, consumerOrderInfoManager.getTable().size());
        }
        {
            consumerOrderInfoManager.update(null, false,
                TOPIC,
                "errGroup",
                QUEUE_ID_0,
                popTime,
                1,
                Lists.newArrayList(2L, 3L, 4L),
                new StringBuilder());

            consumerOrderInfoManager.autoClean();
            assertEquals(0, consumerOrderInfoManager.getTable().size());
        }
        {
            topicConfig.setReadQueueNums(0);
            consumerOrderInfoManager.update(null, false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                1,
                Lists.newArrayList(2L, 3L, 4L),
                new StringBuilder());

            await().atMost(Duration.ofSeconds(1)).until(() -> {
                consumerOrderInfoManager.autoClean();
                return consumerOrderInfoManager.getTable().size() == 0;
            });
        }
        {
            topicConfig.setReadQueueNums(8);
            consumerOrderInfoManager.update(null, false,
                TOPIC,
                GROUP,
                QUEUE_ID_0,
                popTime,
                1,
                Lists.newArrayList(2L, 3L, 4L),
                new StringBuilder());

            consumerOrderInfoManager.autoClean();
            assertEquals(1, consumerOrderInfoManager.getTable().size());
            for (ConcurrentHashMap<Integer, ConsumerOrderInfoManager.OrderInfo> orderInfoMap : consumerOrderInfoManager.getTable().values()) {
                assertEquals(1, orderInfoMap.size());
                assertNotNull(orderInfoMap.get(QUEUE_ID_0));
                break;
            }
        }
    }

    private void assertEncodeAndDecode() {
        ConsumerOrderInfoManager.OrderInfo prevOrderInfo = consumerOrderInfoManager.getTable().values().stream().findFirst()
            .get().get(QUEUE_ID_0);

        String dataEncoded = consumerOrderInfoManager.encode();

        consumerOrderInfoManager.decode(dataEncoded);
        ConsumerOrderInfoManager.OrderInfo newOrderInfo = consumerOrderInfoManager.getTable().values().stream().findFirst()
            .get().get(QUEUE_ID_0);

        assertNotSame(prevOrderInfo, newOrderInfo);
        assertEquals(prevOrderInfo.getPopTime(), newOrderInfo.getPopTime());
        assertEquals(prevOrderInfo.getInvisibleTime(), newOrderInfo.getInvisibleTime());
        assertEquals(prevOrderInfo.getOffsetList(), newOrderInfo.getOffsetList());
        assertEquals(prevOrderInfo.getOffsetConsumedCount(), newOrderInfo.getOffsetConsumedCount());
        assertEquals(prevOrderInfo.getOffsetNextVisibleTime(), newOrderInfo.getOffsetNextVisibleTime());
        assertEquals(prevOrderInfo.getLastConsumeTimestamp(), newOrderInfo.getLastConsumeTimestamp());
        assertEquals(prevOrderInfo.getCommitOffsetBit(), newOrderInfo.getCommitOffsetBit());
    }

    @Test
    public void testLoadFromOldVersionOrderInfoData() {
        consumerOrderInfoManager.update(null, false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            1,
            Lists.newArrayList(2L, 3L, 4L),
            new StringBuilder());
        ConsumerOrderInfoManager.OrderInfo orderInfo = consumerOrderInfoManager.getTable().values().stream().findFirst()
            .get().get(QUEUE_ID_0);

        orderInfo.setInvisibleTime(null);
        orderInfo.setOffsetConsumedCount(null);
        orderInfo.setOffsetNextVisibleTime(null);

        String dataEncoded = consumerOrderInfoManager.encode();

        consumerOrderInfoManager.decode(dataEncoded);
        assertTrue(consumerOrderInfoManager.checkBlock(null, TOPIC, GROUP, QUEUE_ID_0, 3000));

        StringBuilder orderInfoBuilder = new StringBuilder();
        consumerOrderInfoManager.update(null, false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            1,
            Lists.newArrayList(3L, 4L, 5L),
            orderInfoBuilder);
        assertEncodeAndDecode();
        Map<String, Integer> orderInfoMap = ExtraInfoUtil.parseOrderCountInfo(orderInfoBuilder.toString());
        assertEquals(3, orderInfoMap.size());
        assertEquals(0, orderInfoMap.get(ExtraInfoUtil.getStartOffsetInfoMapKey(TOPIC, QUEUE_ID_0)).intValue());
        assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 3)).intValue());
        assertEquals(1, orderInfoMap.get(ExtraInfoUtil.getQueueOffsetMapKey(TOPIC, QUEUE_ID_0, 4)).intValue());
    }

    @Test
    public void testReentrant() {
        StringBuilder orderInfoBuilder = new StringBuilder();
        String attemptId = UUID.randomUUID().toString();
        consumerOrderInfoManager.update(
            attemptId,
            false,
            TOPIC,
            GROUP,
            QUEUE_ID_0,
            popTime,
            3000,
            Lists.newArrayList(1L, 2L, 3L),
            orderInfoBuilder
        );

        assertTrue(consumerOrderInfoManager.checkBlock(null, TOPIC, GROUP, QUEUE_ID_0, 3000));
        assertFalse(consumerOrderInfoManager.checkBlock(attemptId, TOPIC, GROUP, QUEUE_ID_0, 3000));
    }
}
