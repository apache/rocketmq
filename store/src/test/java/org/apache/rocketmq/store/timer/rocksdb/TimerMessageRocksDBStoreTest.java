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
package org.apache.rocketmq.store.timer.rocksdb;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.StoreTestUtils;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.junit.Assert;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.store.timer.rocksdb.TimerMessageRocksDBStore.TIMER_TOPIC;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TimerMessageRocksDBStoreTest {
    MessageStore messageStore;
    MessageStoreConfig storeConfig;
    int precisionMs = 500;
    AtomicInteger counter = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    Set<String> baseDirs = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        String baseDir = StoreTestUtils.createBaseDir();
        this.baseDirs.add(baseDir);
        storeConfig = new MessageStoreConfig();
        storeConfig.setEnableTimerMessageOnRocksDB(true);
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest",
            false), new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());

        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        assertTrue(messageStore.load());
        messageStore.start();
    }

    private TimerMessageRocksDBStore createTimerMessageRocksDBStore(String rootDir) {
        if (null == rootDir) {
            rootDir = StoreTestUtils.createBaseDir();
            baseDirs.add(rootDir);
        }
        TimerMetrics timerMetrics = new TimerMetrics(rootDir + File.separator + "config" + File.separator + "timermetrics");
        return new TimerMessageRocksDBStore(messageStore, storeConfig, timerMetrics, null);
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
        msg.setBody(new byte[100]);
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

    private PutMessageResult transformTimerMessage(TimerMessageRocksDBStore timerMessageStore, MessageExtBrokerInner msg) {
        //do transform
        int delayLevel = msg.getDelayTimeLevel();
        long deliverMs;

        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis() + Integer.parseInt(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000L;
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

            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, deliverMs + "");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
            msg.setTopic(TIMER_TOPIC);
            msg.setQueueId(0);
        } else if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }
        return null;
    }

    @Test
    public void testDoNormalTimer() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());
        String topic = "TimerRocksdbTest_testPutTimerMessage";

        final TimerMessageRocksDBStore timerMessageStore = createTimerMessageRocksDBStore(null);
        timerMessageStore.createTimer(RocksDB.DEFAULT_COLUMN_FAMILY);
        timerMessageStore.load();
        timerMessageStore.start();
        long commitOffset = timerMessageStore.getCommitOffset();
        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        long delayMs = curr + 3000;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                MessageExtBrokerInner inner = buildMessage((i % 2 == 0) ? 3000 : delayMs, topic + i, i % 2 == 0);
                transformTimerMessage(timerMessageStore, inner);
                PutMessageResult putMessageResult = messageStore.putMessage(inner);
                assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            }
        }

        // Wait until messages have been wrote to TimerLog but the slot (delayMs) hasn't expired.
        await().atMost(2000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitOffset() - commitOffset == 10 * 5;
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
            }
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(0, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }
        timerMessageStore.shutdown();
    }

    @Test
    public void testPutExpiredTimerMessage() throws Exception {
        // Skip on Mac to make CI pass
        Assume.assumeFalse(MixAll.isMac());
        Assume.assumeFalse(MixAll.isWindows());

        String topic = "TimerRocksdbTest_testPutExpiredTimerMessage";

        TimerMessageRocksDBStore timerMessageStore = createTimerMessageRocksDBStore(null);
        timerMessageStore.createTimer(RocksDB.DEFAULT_COLUMN_FAMILY);
        timerMessageStore.load();
        timerMessageStore.start();

        long delayMs = System.currentTimeMillis() - 2L * precisionMs;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore, inner);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }

        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
            assertTrue(System.currentTimeMillis() - curr < 200);
        }
        timerMessageStore.shutdown();
    }

    @Test
    public void testDeleteTimerMessage() throws Exception {
        String topic = "TimerRocksdbTest_testDeleteTimerMessage";

        TimerMessageRocksDBStore timerMessageStore = createTimerMessageRocksDBStore(null);
        timerMessageStore.createTimer(RocksDB.DEFAULT_COLUMN_FAMILY);
        timerMessageStore.load();
        timerMessageStore.start();

        long delayMs = System.currentTimeMillis() + 4000;
        String uniqKey = null;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore, inner);
            if (null == uniqKey) {
                uniqKey = MessageClientIDSetter.getUniqID(inner);
            }
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore, delMsg);
        MessageAccessor.putProperty(delMsg, MessageConst.PROPERTY_TIMER_DEL_UNIQKEY, TimerMessageStore.buildDeleteKey(topic, uniqKey));
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());
        Thread.sleep(1000);

        // The first one should have been deleted.
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 3000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertNotEquals(uniqKey, MessageClientIDSetter.getUniqID(msgExt));

        // The last one should be null.
        assertNull(getOneMessage(topic, 0, 4, 500));
        timerMessageStore.shutdown();
    }

    @Test
    public void testPutDeleteTimerMessage() throws Exception {
        String topic = "TimerRocksdbTest_testPutDeleteTimerMessage";

        final TimerMessageRocksDBStore timerMessageStore = createTimerMessageRocksDBStore(null);
        timerMessageStore.createTimer(RocksDB.DEFAULT_COLUMN_FAMILY);
        timerMessageStore.load();
        timerMessageStore.start();

        long curr = System.currentTimeMillis() / precisionMs * precisionMs;
        final long delayMs = curr + 2000;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore, inner);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        transformTimerMessage(timerMessageStore, delMsg);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, "XXX+1");
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());

        // Wait until currReadTimeMs catches up current time and delayMs is over.
        Thread.sleep(2000);

        for (int i = 0; i < 5; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
        }

        // Test put expired delete msg.
        MessageExtBrokerInner expiredInner = buildMessage(System.currentTimeMillis() - 100, topic, false);
        MessageAccessor.putProperty(expiredInner, TimerMessageStore.TIMER_DELETE_UNIQUE_KEY, "XXX");
        PutMessageResult putMessageResult = transformTimerMessage(timerMessageStore, expiredInner);
        assertEquals(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, putMessageResult.getPutMessageStatus());
        timerMessageStore.shutdown();
    }

    @Test
    public void testExtractUniqueKey() {
        String deleteKey = TimerMessageStore.buildDeleteKey("topic", "123456");
        assertEquals("123456", TimerMessageStore.extractUniqueKey(deleteKey));
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @After
    public void destroy() {
        if (null != messageStore) {
            messageStore.shutdown();
            messageStore.destroy();
        }
        for (String baseDir : baseDirs) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }
}
