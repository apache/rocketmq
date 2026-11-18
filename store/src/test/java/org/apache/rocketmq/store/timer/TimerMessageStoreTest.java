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

package org.apache.rocketmq.store.timer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TimerMessageStoreTest {
    private final byte[] msgBody = new byte[1024];
    private static MessageStore messageStore;
    private MessageStore mockMessageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private final int precisionMs = 500;

    private final Set<String> baseDirs = new HashSet<>();
    private final List<TimerMessageStore> timerStores = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger(0);

    public static MessageStoreConfig storeConfig;

    @Before
    public void init() throws Exception {
        String baseDir = StoreTestUtils.createBaseDir();
        baseDirs.add(baseDir);

        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        storeConfig = new MessageStoreConfig();
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024 * 1024);
        storeConfig.setMappedFileSizeTimerLog(1024 * 1024 * 1024);
        storeConfig.setMappedFileSizeConsumeQueue(10240);
        storeConfig.setMaxHashSlotNum(10000);
        storeConfig.setMaxIndexNum(100 * 1000);
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        storeConfig.setTimerInterceptDelayLevel(true);
        storeConfig.setTimerPrecisionMs(precisionMs);
        storeConfig.setAppendTopicForTimerDeleteKey(false); // reset default value

        mockMessageStore = Mockito.mock(MessageStore.class);
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest",false), new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    public TimerMessageStore createTimerMessageStore(String rootDir , boolean needMock) throws IOException {
        if (null == rootDir) {
            rootDir = StoreTestUtils.createBaseDir();
        }

        TimerCheckpoint timerCheckpoint = new TimerCheckpoint(rootDir + File.separator + "config" + File.separator + "timercheck");
        TimerMetrics timerMetrics = new TimerMetrics(rootDir + File.separator + "config" + File.separator + "timermetrics");
        MessageStore ms = needMock ? mockMessageStore : messageStore;
        TimerMessageStore timerMessageStore = new TimerMessageStore(ms, storeConfig, timerCheckpoint, timerMetrics, null);
        ms.setTimerMessageStore(timerMessageStore);

        baseDirs.add(rootDir);
        timerStores.add(timerMessageStore);

        return timerMessageStore;
    }

    private static PutMessageResult transformTimerMessage(TimerMessageStore timerMessageStore, MessageExtBrokerInner msg) {
        //do transform
        int delayLevel = msg.getDelayTimeLevel();
        long deliverMs;

        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis() + Integer.parseInt(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                deliverMs = System.currentTimeMillis() + Integer.parseInt(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
            } else {
                deliverMs = Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
            }
        } catch (Exception e) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }
        if (deliverMs > System.currentTimeMillis()) {
            if (delayLevel <= 0 && deliverMs - System.currentTimeMillis() > storeConfig.getTimerMaxDelaySec() * 1000L) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
            }

            int timerPrecisionMs = storeConfig.getTimerPrecisionMs();
            if (deliverMs % timerPrecisionMs == 0) {
                deliverMs -= timerPrecisionMs;
            } else {
                deliverMs = deliverMs / timerPrecisionMs * timerPrecisionMs;
            }

            if (timerMessageStore.isReject(deliverMs)) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL, null);
            }
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, deliverMs + "");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
            msg.setTopic(TimerMessageStore.TIMER_TOPIC);
            msg.setQueueId(0);
        } else if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }
        return null;
    }

    @Test
    public void testConvertMessagePropertiesStringMatchesProperties() throws Exception {
        final TimerMessageStore timerMessageStore = createTimerMessageStore(null, true);

        MessageExtBrokerInner msgExt = buildMessage(3000, "TimerTest_testConvertMessage", false);
        MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_REAL_TOPIC, msgExt.getTopic());
        MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        msgExt.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgExt.setTopic(TimerMessageStore.TIMER_TOPIC);

        // delivered message: internal properties are cleared from both the map and the wire data
        MessageExtBrokerInner delivered = timerMessageStore.convertMessage(msgExt, false);
        assertEquals("TimerTest_testConvertMessage", delivered.getTopic());
        assertFalse(delivered.getPropertiesString().contains(MessageConst.PROPERTY_REAL_TOPIC));
        assertFalse(delivered.getPropertiesString().contains(MessageConst.PROPERTY_REAL_QUEUE_ID));
        assertEquals(MessageDecoder.messageProperties2String(delivered.getProperties()), delivered.getPropertiesString());

        // rolled message: keeps REAL_TOPIC and stays consistent between the map and the wire data
        MessageExtBrokerInner rolled = timerMessageStore.convertMessage(msgExt, true);
        assertEquals(TimerMessageStore.TIMER_TOPIC, rolled.getTopic());
        assertTrue(rolled.getPropertiesString().contains(MessageConst.PROPERTY_REAL_TOPIC));
        assertTrue(rolled.getPropertiesString().contains(MessageConst.PROPERTY_REAL_QUEUE_ID));
        assertEquals(MessageDecoder.messageProperties2String(rolled.getProperties()), rolled.getPropertiesString());
    }

    @Test
    public void testPutTimerMessage() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());
        String topic = "TimerTest_testPutTimerMessage";

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null , false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 3000;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                MessageExtBrokerInner inner = buildMessage((i % 2 == 0) ? 3000 : delayMs, topic + i, i % 2 == 0);
                transformTimerMessage(timerMessageStore,inner);
                PutMessageResult putMessageResult = messageStore.putMessage(inner);
                assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            }
        }

        // Wait until messages have been wrote to TimerLog but the slot (delayMs) hasn't expired.
        await().atMost(2000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() == 10 * 5;
            }
        });

        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(5, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                ByteBuffer msgBuff = getOneMessage(topic + i, 0, j, 4000);
                assertNotNull(msgBuff);
                MessageExt msgExt = MessageDecoder.decode(msgBuff);
                assertNotNull(msgExt);
                assertEquals(topic + i, msgExt.getTopic());
                // assertThat(System.currentTimeMillis()).isLessThan(delayMs + precisionMs * 2);
            }
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(0, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }
    }

    @Test
    public void testRetryUntilSuccess() throws Exception {
        storeConfig.setTimerEnableRetryUntilSuccess(true);
        TimerMessageStore timerMessageStore = createTimerMessageStore(null , true);
        timerMessageStore.load();
        timerMessageStore.setShouldRunningDequeue(true);
        Field stateField = TimerMessageStore.class.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(timerMessageStore, TimerMessageStore.RUNNING);

        MessageExtBrokerInner msg = buildMessage(3000L, "TestRetry", true);
        transformTimerMessage(timerMessageStore, msg);
        TimerRequest timerRequest = new TimerRequest(100, 200, 3000, System.currentTimeMillis(), 0, msg);
        boolean offered = timerMessageStore.dequeuePutQueue.offer(timerRequest);
        assertTrue(offered);
        assertFalse(timerMessageStore.dequeuePutQueue.isEmpty());

        // If enableRetryUntilSuccess is set and putMessage return NEED_RETRY type, the message should be retried until success.
        when(mockMessageStore.putMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, null))
                .thenReturn(new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, null))
                .thenReturn(new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY, null))
                .thenReturn(new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY, null))
                .thenReturn(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null))
                .thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                timerMessageStore.getDequeuePutMessageServices()[0].run();
            } finally {
                latch.countDown();
            }
        }).start();
        latch.await(5, TimeUnit.SECONDS);
        assertTrue(timerMessageStore.dequeuePutQueue.isEmpty());
        verify(mockMessageStore, times(6)).putMessage(any(MessageExtBrokerInner.class));
    }

    @Test
    public void testTimerFlowControl() throws Exception {
        String topic = "TimerTest_testTimerFlowControl";

        storeConfig.setTimerCongestNumEachSlot(100);
        TimerMessageStore timerMessageStore = createTimerMessageStore(null , false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        // Make sure delayMs won't be over.
        long delayMs = curr + 100000;

        int passFlowControlNum = 0;
        for (int i = 0; i < 500; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);

            PutMessageResult putMessageResult = transformTimerMessage(timerMessageStore,inner);
            if (putMessageResult == null || !putMessageResult.getPutMessageStatus().equals(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL)) {
                putMessageResult = messageStore.putMessage(inner);
            }
            else {
                putMessageResult = new PutMessageResult(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL,null);
            }

            // Message with delayMs in getSlotIndex(delayMs - precisionMs).
            long congestNum = timerMessageStore.getCongestNum(delayMs - precisionMs);
            assertTrue(congestNum <= 220);
            if (congestNum < 100) {
                assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            } else {
                Assert.assertTrue(PutMessageStatus.PUT_OK == putMessageResult.getPutMessageStatus()
                        || PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL == putMessageResult.getPutMessageStatus());
                if (PutMessageStatus.PUT_OK == putMessageResult.getPutMessageStatus()) {
                    passFlowControlNum++;
                }
            }
            //wait reput
            Thread.sleep(5);
        }
        assertThat(passFlowControlNum).isGreaterThan(0).isLessThan(120);
    }


    @Test
    public void testPutExpiredTimerMessage() throws Exception {
        // Skip on Mac to make CI pass
        Assume.assumeFalse(MixAll.isMac());
        Assume.assumeFalse(MixAll.isWindows());

        String topic = "TimerTest_testPutExpiredTimerMessage";

        TimerMessageStore timerMessageStore = createTimerMessageStore(null ,false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long delayMs = System.currentTimeMillis() - 2 * precisionMs;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore,inner);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
            assertTrue(System.currentTimeMillis() - curr < 200);
        }
    }

    @Test
    public void testDeleteTimerMessage() throws Exception {
        String topic = "TimerTest_testDeleteTimerMessage";

        TimerMessageStore timerMessageStore = createTimerMessageStore(null ,false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 1000;
        String uniqKey = null;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore,inner);
            if (null == uniqKey) {
                uniqKey = MessageClientIDSetter.getUniqID(inner);
            }
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore,delMsg);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, TimerMessageStore.buildDeleteKey(topic, uniqKey, false));
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());

        // The first one should have been deleted.
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 3000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertNotEquals(uniqKey, MessageClientIDSetter.getUniqID(msgExt));

        // The last one should be null.
        assertNull(getOneMessage(topic, 0, 4, 500));
    }

    @Test
    public void testDeleteTimerMessage_ukCollision() throws Exception {
        storeConfig.setAppendTopicForTimerDeleteKey(true); // append topic as namespace
        String topic = "TimerTest_testDeleteTimerMessage";
        String collisionTopic = "TimerTest_testDeleteTimerMessage_collision";

        TimerMessageStore timerMessageStore = createTimerMessageStore(null , false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 1000;

        MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore, inner);
        String firstUniqKey = MessageClientIDSetter.getUniqID(inner);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());

        inner = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore, inner);
        String secondUniqKey = MessageClientIDSetter.getUniqID(inner);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());

        MessageExtBrokerInner delMsg = buildMessage(delayMs, "whatever", false);
        transformTimerMessage(timerMessageStore, delMsg);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, TimerMessageStore.buildDeleteKey(topic, firstUniqKey, true));
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());

        delMsg = buildMessage(delayMs, "whatever", false);
        transformTimerMessage(timerMessageStore, delMsg);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, TimerMessageStore.buildDeleteKey(collisionTopic, secondUniqKey, true));
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());

        // The first one should have been deleted, the second one should not be deleted.
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 3000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertNotEquals(firstUniqKey, MessageClientIDSetter.getUniqID(msgExt));
        assertEquals(secondUniqKey, MessageClientIDSetter.getUniqID(msgExt));
    }

    @Test
    public void testPutDeleteTimerMessage() throws Exception {
        String topic = "TimerTest_testPutDeleteTimerMessage";

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null , false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        final long delayMs = curr + 1000;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore,inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore,delMsg);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, "XXX");
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());

        // Wait until currReadTimeMs catches up current time and delayMs is over.
        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                long curr = System.currentTimeMillis() / precisionMs * precisionMs;
                return curr >= delayMs
                        && (timerMessageStore.getCurrReadTimeMs() == curr || timerMessageStore.getCurrReadTimeMs() == curr + precisionMs);
            }
        });

        for (int i = 0; i < 5; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
            // assertThat(System.currentTimeMillis()).isLessThan(delayMs + precisionMs);
        }
        assertNull(getOneMessage(topic, 0, 5, 1000));

        // Test put expired delete msg.
        MessageExtBrokerInner expiredInner = buildMessage(System.currentTimeMillis() - 100, topic, false);
        MessageAccessor.putProperty(expiredInner, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, "XXX");
        PutMessageResult putMessageResult = transformTimerMessage(timerMessageStore,expiredInner);
        assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, putMessageResult.getPutMessageStatus());
    }

    @Test
    public void testStateAndRecover() throws Exception {
        // damage the wheel via the non-snapshot flush path to trigger checkPhyPos on recovery.
        storeConfig.setTimerWheelSnapshotFlush(false);
        final String topic = "TimerTest_testStateAndRecover";

        String base = StoreTestUtils.createBaseDir();
        final TimerMessageStore first = createTimerMessageStore(base , false);
        first.load();
        first.start(true);

        final int msgNum = 250;
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        final long delayMs = curr + 5000;
        for (int i = 0; i < msgNum; i++) {
            MessageExtBrokerInner inner = buildMessage((i % 2 == 0) ? 5000 : delayMs, topic, i % 2 == 0);
            transformTimerMessage(first,inner);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            long cqOffset = first.getCommitQueueOffset();
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        // Wait until messages have written to TimerLog and currReadTimeMs catches up current time.
        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                long curr = System.currentTimeMillis() / precisionMs * precisionMs;
                long cqOffset = first.getCommitQueueOffset();
                return first.getCommitQueueOffset() == msgNum
                        && (first.getCurrReadTimeMs() == curr || first.getCurrReadTimeMs() == curr + precisionMs);
            }
        });
        assertThat(first.getTimerLog().getMappedFileQueue().getMappedFiles().size())
                .isGreaterThanOrEqualTo(msgNum / (storeConfig.getMappedFileSizeTimerLog() / TimerLog.UNIT_SIZE));
        assertThat(first.getQueueOffset()).isEqualTo(msgNum);
        assertThat(first.getCommitQueueOffset()).isEqualTo(first.getQueueOffset());
        assertThat(first.getCommitReadTimeMs()).isEqualTo(first.getCurrReadTimeMs());
        curr = System.currentTimeMillis() / precisionMs * precisionMs;
        assertThat(first.getCurrReadTimeMs()).isLessThanOrEqualTo(curr + precisionMs);

        for (int i = 0; i <= first.getTimerLog().getMappedFileQueue().getMappedFiles().size() + 10; i++) {
            first.getTimerLog().getMappedFileQueue().flush(0);
            Thread.sleep(10);
        }

        // Damage the timer wheel, trigger the check physical pos.
        Slot slot = first.getTimerWheel().getSlot(delayMs - precisionMs);
        assertNotEquals(-1, slot.timeMs);
        first.getTimerWheel().putSlot(slot.timeMs, -1, Long.MAX_VALUE, slot.num, slot.magic);
        first.getTimerWheel().flush();
        first.shutdown();

        final TimerMessageStore second = createTimerMessageStore(base , false);
        second.debug = true;
        assertTrue(second.load());
        assertEquals(msgNum, second.getQueueOffset());
        assertEquals(second.getCommitQueueOffset(), second.getQueueOffset());
        assertEquals(second.getCurrReadTimeMs(), second.getCommitReadTimeMs());
        assertEquals(first.getCommitReadTimeMs(), second.getCommitReadTimeMs());
        second.start(true);

        // Wait until all messages have been written back to commitLog and consumeQueue.
        await().atMost(30000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                ConsumeQueue cq = (ConsumeQueue) messageStore.getConsumeQueue(topic, 0);
                return cq != null && cq.getMaxOffsetInQueue() >= msgNum - 1;
            }
        });

        for (int i = 0; i < msgNum; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 2000);
            assertThat(msgBuff).isNotNull();
        }
        second.shutdown();
    }

    @Test
    public void testMaxDelaySec() throws Exception {
        String topic = "TimerTest_testMaxDelaySec";

        TimerMessageStore first = createTimerMessageStore(null , false);
        first.load();
        first.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delaySec = storeConfig.getTimerMaxDelaySec() + 20;

        MessageExtBrokerInner absolute = buildMessage(curr + delaySec * 1000, topic, false);
        PutMessageResult putMessageResult = transformTimerMessage(first,absolute);
        assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, putMessageResult.getPutMessageStatus());

        MessageExtBrokerInner relative = buildMessage(delaySec * 1000, topic, true);
        putMessageResult = transformTimerMessage(first,relative);
        assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, putMessageResult.getPutMessageStatus());
    }


    @Test
    public void testRollMessage() throws Exception {
        storeConfig.setTimerRollWindowSlot(2);
        String topic = "TimerTest_testRollMessage";

        TimerMessageStore timerMessageStore = createTimerMessageStore(null , false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 4 * precisionMs;
        MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore,inner);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());

        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 5000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertEquals(1, Integer.valueOf(msgExt.getProperty(MessageConst.PROPERTY_TIMER_ROLL_TIMES)).intValue());
        storeConfig.setTimerRollWindowSlot(Integer.MAX_VALUE);
    }

    @Test
    public void defaultSnapshotEnabledTest() {
        assertTrue(new MessageStoreConfig().isTimerWheelSnapshotFlush());
    }

    @Test
    public void snapshotFlushAndRecoverTest() throws Exception {
        storeConfig.setTimerWheelSnapshotFlush(true);
        storeConfig.setTimerWheelSnapshotIntervalMs(0);
        storeConfig.setTimerFlushIntervalMs(100);
        final String topic = "TimerTest_snapshotFlushAndRecover";

        String base = StoreTestUtils.createBaseDir();
        final TimerMessageStore first = createTimerMessageStore(base, false);
        first.load();
        first.start(true);

        final int shortNum = 300;
        final int longNum = 10;
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        final long longDeliverMs = curr + 60000 - precisionMs;
        for (int i = 0; i < shortNum + longNum; i++) {
            MessageExtBrokerInner inner = buildMessage(curr + (i < shortNum ? 5000 : 60000), topic, false);
            transformTimerMessage(first, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> first.getCommitQueueOffset() == shortNum + longNum);

        // wait for a promote covering every enqueued record: base latest, no numeric snapshot, CP paired.
        long target = first.getTimerLog().getMappedFileQueue().getMaxWrotePosition();
        String wheelPath = TimerMessageStore.getTimerWheelPath(storeConfig.getStorePathRootDir());
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            first.getTimerCheckpoint().getLastTimerLogFlushPos() >= target
                && TimerWheel.getMaxSnapshotFlag(wheelPath) == -1);
        assertThat(first.getTimerCheckpoint().getLastTimerLogFlushPos())
            .isLessThanOrEqualTo(first.getTimerLog().getMappedFileQueue().getMaxWrotePosition());
        // base is the promoted wheel; recovery rewinds one timerlog file and replays the gap (verified below).
        assertEquals(604800L * 2 * Slot.SIZE, new File(wheelPath).length());
        first.shutdown();

        final TimerMessageStore second = createTimerMessageStore(base, false);
        assertTrue(second.load());
        assertEquals(shortNum + longNum, second.getQueueOffset());
        assertEquals(second.getCommitQueueOffset(), second.getQueueOffset());
        assertEquals(first.getCommitReadTimeMs(), second.getCommitReadTimeMs());
        assertEquals(longNum, second.getTimerWheel().getNum(longDeliverMs));
        second.start(true);

        await().atMost(30000, TimeUnit.MILLISECONDS).until(() -> {
            ConsumeQueue cq = (ConsumeQueue) messageStore.getConsumeQueue(topic, 0);
            return cq != null && cq.getMaxOffsetInQueue() >= shortNum;
        });
        for (int i = 0; i < shortNum; i++) {
            assertThat(getOneMessage(topic, 0, i, 2000)).isNotNull();
        }
        assertEquals(longNum, second.getTimerWheel().getNum(longDeliverMs));
        second.shutdown();
    }

    @Test
    public void snapshotRollMessageTest() throws Exception {
        storeConfig.setTimerWheelSnapshotFlush(true);
        storeConfig.setTimerWheelSnapshotIntervalMs(0);
        storeConfig.setTimerFlushIntervalMs(100);
        storeConfig.setTimerRollWindowSlot(2);
        String topic = "TimerTest_snapshotRollMessage";

        TimerMessageStore timerMessageStore = createTimerMessageStore(null, false);
        timerMessageStore.load();
        timerMessageStore.start(true);

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 4 * precisionMs;
        MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore, inner);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());

        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 5000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertEquals(1, Integer.valueOf(msgExt.getProperty(MessageConst.PROPERTY_TIMER_ROLL_TIMES)).intValue());
        storeConfig.setTimerRollWindowSlot(Integer.MAX_VALUE);
    }

    @Test
    public void checkpointPinnedBetweenSnapshotsTest() throws Exception {
        storeConfig.setTimerWheelSnapshotFlush(true);
        storeConfig.setTimerWheelSnapshotIntervalMs(600000); // no auto snapshot during the test
        storeConfig.setTimerFlushIntervalMs(100);
        final String topic = "TimerTest_checkpointPinned";

        String base = StoreTestUtils.createBaseDir();
        final TimerMessageStore first = createTimerMessageStore(base, false);
        first.load();
        first.start(true);

        final int msgNum = 50;
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        for (int i = 0; i < msgNum; i++) {
            MessageExtBrokerInner inner = buildMessage(curr + 60000, topic, false);
            transformTimerMessage(first, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> first.getCommitQueueOffset() == msgNum);
        // let timerlog flush ahead while no snapshot is taken
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            first.getTimerLog().getMappedFileQueue().getFlushedWhere() > 0);
        // CP must stay pinned to the base end (0, no promote yet), not advance with flushedWhere
        assertEquals(0, first.getTimerCheckpoint().getLastTimerLogFlushPos());

        // now promote a snapshot: CP advances to the flag and base becomes latest
        storeConfig.setTimerWheelSnapshotIntervalMs(0);
        long target = first.getTimerLog().getMappedFileQueue().getMaxWrotePosition();
        String wheelPath = TimerMessageStore.getTimerWheelPath(storeConfig.getStorePathRootDir());
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            first.getTimerCheckpoint().getLastTimerLogFlushPos() >= target
                && TimerWheel.getMaxSnapshotFlag(wheelPath) == -1);
        first.shutdown();

        // recovery replays [base end, crash) from timerlog: zero loss
        final TimerMessageStore second = createTimerMessageStore(base, false);
        assertTrue(second.load());
        assertEquals(msgNum, second.getQueueOffset());
        second.shutdown();
    }

    @Test
    public void recoverFromPendingSnapshotTest() throws Exception {
        storeConfig.setTimerWheelSnapshotFlush(true);
        storeConfig.setTimerWheelSnapshotIntervalMs(0);
        storeConfig.setTimerFlushIntervalMs(100);
        final String topic = "TimerTest_pendingSnapshot";

        String base = StoreTestUtils.createBaseDir();
        final TimerMessageStore first = createTimerMessageStore(base, false);
        first.load();
        first.start(true);

        final int longNum = 8;
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        final long longDeliverMs = curr + 60000 - precisionMs;
        for (int i = 0; i < longNum; i++) {
            MessageExtBrokerInner inner = buildMessage(curr + 60000, topic, false);
            transformTimerMessage(first, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> first.getCommitQueueOffset() == longNum);
        String wheelPath = TimerMessageStore.getTimerWheelPath(storeConfig.getStorePathRootDir());
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            first.getTimerCheckpoint().getLastTimerLogFlushPos() > 0
                && TimerWheel.getMaxSnapshotFlag(wheelPath) == -1);
        long promotedFlag = first.getTimerCheckpoint().getLastTimerLogFlushPos();

        // freeze a pending snapshot WITHOUT promoting: the "CP old, pending x present" crash window.
        storeConfig.setTimerWheelSnapshotIntervalMs(600000);
        final int moreNum = 6;
        for (int i = 0; i < moreNum; i++) {
            MessageExtBrokerInner inner = buildMessage(curr + 60000, topic, false);
            transformTimerMessage(first, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> first.getCommitQueueOffset() == longNum + moreNum);
        first.getTimerWheel().copyForSnapshot();
        long pendingFlag = first.getTimerLog().getMappedFileQueue().getMaxWrotePosition();
        first.getTimerLog().getMappedFileQueue().flush(0);
        first.getTimerWheel().backup(pendingFlag);
        assertThat(pendingFlag).isGreaterThan(promotedFlag);
        assertEquals(pendingFlag, TimerWheel.getMaxSnapshotFlag(wheelPath));
        first.shutdown();
        // after shutdown CP is pinned to the old base end while the pending snapshot remains on disk
        assertEquals(promotedFlag, first.getTimerCheckpoint().getLastTimerLogFlushPos());
        assertEquals(pendingFlag, TimerWheel.getMaxSnapshotFlag(wheelPath));

        // recovery must prefer the pending snapshot (flag) over base + old CP: zero loss
        final TimerMessageStore second = createTimerMessageStore(base, false);
        assertTrue(second.load());
        assertEquals(longNum + moreNum, second.getQueueOffset());
        assertEquals(longNum + moreNum, second.getTimerWheel().getNum(longDeliverMs));
        second.shutdown();
    }

    public ByteBuffer getOneMessage(String topic, int queue, long offset, int timeout) throws Exception {
        int retry = timeout / 100;
        while (retry-- > 0) {
            GetMessageResult getMessageResult = messageStore.getMessage("TimerGroup", topic, queue, offset, 1, null);
            if (null != getMessageResult && GetMessageStatus.FOUND == getMessageResult.getStatus()) {
                return getMessageResult.getMessageBufferList().get(0);
            }
            Thread.sleep(100);
        }
        return null;
    }

    public MessageExtBrokerInner buildMessage(long delayedMs, String topic, boolean relative) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(0);
        msg.setTags(counter.incrementAndGet() + "");
        msg.setKeys("timer");
        if (relative) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC, delayedMs / 1000 + "");
        } else {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS, delayedMs + "");
        }
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(bornHost);
        msg.setStoreHost(storeHost);
        MessageClientIDSetter.setUniqID(msg);
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msg.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msg.getTags());
        msg.setTagsCode(tagsCodeValue);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    @Test
    public void oldNewOldNewStalePendingBoundaryTest() throws Exception {
        // old->new->old->new: stale pending survives the old period and wins over the fresh base;
        // the timerlog gap expired with commitlog is unreplayable -> entries lost.
        // Known boundary: if recovery ever compares maxFlag against CP, update this test.
        storeConfig.setMappedFileSizeTimerLog(TimerLog.UNIT_SIZE * 40);
        storeConfig.setTimerWheelSnapshotFlush(true);
        storeConfig.setTimerWheelSnapshotIntervalMs(0);
        storeConfig.setTimerFlushIntervalMs(100);
        final String topic = "TimerTest_oldNewOldNew";
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long t1 = curr + 60000, t2 = curr + 61000;

        String base = StoreTestUtils.createBaseDir();
        final TimerMessageStore new1 = createTimerMessageStore(base, false);
        new1.load();
        new1.start(true);
        for (int i = 0; i < 50; i++) {
            MessageExtBrokerInner inner = buildMessage(t1 + 100, topic, false);
            transformTimerMessage(new1, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> new1.getCommitQueueOffset() == 50);
        long target1 = new1.getTimerLog().getMappedFileQueue().getMaxWrotePosition();
        String wheelPath = TimerMessageStore.getTimerWheelPath(storeConfig.getStorePathRootDir());
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            new1.getTimerCheckpoint().getLastTimerLogFlushPos() >= target1
                && TimerWheel.getMaxSnapshotFlag(wheelPath) == -1);
        new1.shutdown();
        long staleFlag = new1.getTimerCheckpoint().getLastTimerLogFlushPos();
        // crash-leftover shape: a complete wheel image under a numeric flag beside the promoted base
        Files.copy(new File(wheelPath).toPath(), new File(wheelPath + "." + staleFlag).toPath());

        // old single-file period: base stays fresh, the stale snapshot is ignored and never cleaned
        storeConfig.setTimerWheelSnapshotFlush(false);
        final TimerMessageStore old = createTimerMessageStore(base, false);
        old.load();
        old.start(true);
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner inner = buildMessage(t2 + 100, topic, false);
            transformTimerMessage(old, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> old.getCommitQueueOffset() == 150);
        long target2 = old.getTimerLog().getMappedFileQueue().getMaxWrotePosition();
        await().atMost(5000, TimeUnit.MILLISECONDS).until(() ->
            old.getTimerCheckpoint().getLastTimerLogFlushPos() >= target2);
        // long interval: commitlog rolls and timerlog files expire, same call as the 30s scheduler
        SelectMappedBufferResult sbr = old.getTimerLog().getWholeBuffer(100 * TimerLog.UNIT_SIZE);
        sbr.getByteBuffer().position((100 * TimerLog.UNIT_SIZE) % storeConfig.getMappedFileSizeTimerLog()
            + TimerLog.UNIT_PRE_SIZE_FOR_MSG);
        long minPySim = sbr.getByteBuffer().getLong();
        sbr.release();
        old.getTimerLog().getMappedFileQueue().deleteExpiredFileByOffsetForTimerLog(
            minPySim, old.getTimerLog().getOffsetForLastUnit(), TimerLog.UNIT_SIZE);
        long t2InBase = old.getTimerWheel().getNum(t2);
        assertThat(t2InBase).isGreaterThanOrEqualTo(100);
        old.shutdown();

        // second new upgrade: stale pending wins over the fresh base, the expired gap is lost
        storeConfig.setTimerWheelSnapshotFlush(true);
        final TimerMessageStore new2 = createTimerMessageStore(base, false);
        assertTrue(new2.load());
        assertThat(new2.getTimerWheel().getNum(t1)).isGreaterThanOrEqualTo(50);
        assertThat(new2.getTimerWheel().getNum(t2)).isLessThan(t2InBase);
        new2.shutdown();

        // workaround: delete the stale numeric snapshot before the upgrade, base + CP recovers all
        assertTrue(new File(wheelPath + "." + staleFlag).delete());
        final TimerMessageStore new3 = createTimerMessageStore(base, false);
        assertTrue(new3.load());
        assertEquals(t2InBase, new3.getTimerWheel().getNum(t2));
        new3.shutdown();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @After
    public void clear() {
        for (TimerMessageStore store : timerStores) {
            store.shutdown();
        }
        for (String baseDir : baseDirs) {
            StoreTestUtils.deleteFile(baseDir);
        }
        if (null != messageStore) {
            messageStore.shutdown();
            messageStore.destroy();
        }
    }
}
