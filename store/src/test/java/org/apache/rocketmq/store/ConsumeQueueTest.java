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

package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ConsumeQueueTest {

    private static final String MSG = "Once, there was a chance for me!";
    private static final byte[] MSG_BODY = MSG.getBytes();

    private static final String TOPIC = "abc";
    private static final int QUEUE_ID = 0;
    private static final String STORE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "unit_test_store";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 8;
    private static final int CQ_FILE_SIZE = 10 * 20;
    private static final int CQ_EXT_FILE_SIZE = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress bornHost;

    private static SocketAddress storeHost;

    static {
        try {
            storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TOPIC);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MSG_BODY);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(QUEUE_ID);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageExtBrokerInner buildIPv6HostMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TOPIC);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MSG_BODY);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(QUEUE_ID);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(new InetSocketAddress("1050:0000:0000:0000:0005:0600:300c:326b", 123));
        msg.setStoreHost(new InetSocketAddress("::1", 124));
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageStoreConfig buildStoreConfig(int commitLogFileSize, int cqFileSize,
        boolean enableCqExt, int cqExtFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(enableCqExt);
        messageStoreConfig.setHaListenPort(0);
        messageStoreConfig.setStorePathRootDir(STORE_PATH);
        messageStoreConfig.setStorePathCommitLog(STORE_PATH + File.separator + "commitlog");

        return messageStoreConfig;
    }

    protected DefaultMessageStore gen() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
                COMMIT_LOG_FILE_SIZE, CQ_FILE_SIZE, true, CQ_EXT_FILE_SIZE
        );

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat()),
            new MessageArrivingListener() {
                @Override
                public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                    long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                }
            }
            , brokerConfig, new ConcurrentHashMap<>());

        assertThat(master.load()).isTrue();

        master.start();

        return master;
    }

    protected DefaultMessageStore genForMultiQueue() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
                COMMIT_LOG_FILE_SIZE, CQ_FILE_SIZE, true, CQ_EXT_FILE_SIZE
        );

        messageStoreConfig.setEnableLmq(true);
        messageStoreConfig.setEnableMultiDispatch(true);

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat()),
            new MessageArrivingListener() {
                @Override
                public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                    long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                }
            }
            , brokerConfig, new ConcurrentHashMap<>());

        assertThat(master.load()).isTrue();

        master.start();

        return master;
    }

    protected void putMsg(DefaultMessageStore master) {
        long totalMsgs = 200;

        for (long i = 0; i < totalMsgs; i++) {
            if (i < totalMsgs / 2) {
                master.putMessage(buildMessage());
            } else {
                master.putMessage(buildIPv6HostMessage());
            }
        }
    }

    protected void putMsgMultiQueue(DefaultMessageStore master) {
        for (long i = 0; i < 1; i++) {
            master.putMessage(buildMessageMultiQueue());
        }
    }

    private MessageExtBrokerInner buildMessageMultiQueue() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TOPIC);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MSG_BODY);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(QUEUE_ID);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        for (int i = 0; i < 1; i++) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%123,%LMQ%456");
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    protected void deleteDirectory(String rootPath) {
        File file = new File(rootPath);
        deleteFile(file);
    }

    protected void deleteFile(File file) {
        File[] subFiles = file.listFiles();
        if (subFiles != null) {
            for (File sub : subFiles) {
                deleteFile(sub);
            }
        }

        file.delete();
    }

    @Test
    public void testPutMessagePositionInfo_buildCQRepeatedly() throws Exception {
        DefaultMessageStore messageStore = null;
        try {

            messageStore = gen();

            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsg(messageStore);
            }


            // Wait consume queue build finish.
            final MessageStore store = messageStore;
            Awaitility.with().pollInterval(100, TimeUnit.MILLISECONDS).await().timeout(1, TimeUnit.MINUTES).until(() -> {
                return store.dispatchBehindBytes() == 0;
            });

            ConsumeQueueInterface cq = messageStore.getConsumeQueueTable().get(TOPIC).get(QUEUE_ID);
            Method method = cq.getClass().getDeclaredMethod("putMessagePositionInfo", long.class, int.class, long.class, long.class);

            assertThat(method).isNotNull();

            method.setAccessible(true);

            SelectMappedBufferResult result = messageStore.getCommitLog().getData(0);
            assertThat(result != null).isTrue();

            DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);

            assertThat(cq).isNotNull();

            Object dispatchResult = method.invoke(cq, dispatchRequest.getCommitLogOffset(),
                dispatchRequest.getMsgSize(), dispatchRequest.getTagsCode(), dispatchRequest.getConsumeQueueOffset());

            assertThat(Boolean.parseBoolean(dispatchResult.toString())).isTrue();

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(STORE_PATH);
        }

    }

    @Test
    public void testPutMessagePositionInfoWrapper_MultiQueue() throws Exception {
        Assume.assumeTrue(!MixAll.isWindows() && !MixAll.isMac());
        DefaultMessageStore messageStore = null;
        try {
            messageStore = genForMultiQueue();

            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsgMultiQueue(messageStore);
            }

            // Wait consume queue build finish.
            final MessageStore store = messageStore;
            Awaitility.with().pollInterval(100, TimeUnit.MILLISECONDS).await().timeout(1, TimeUnit.MINUTES).until(() -> {
                return store.dispatchBehindBytes() == 0;
            });

            ConsumeQueueInterface cq = messageStore.getConsumeQueueTable().get(TOPIC).get(QUEUE_ID);
            Method method = ((ConsumeQueue) cq).getClass().getDeclaredMethod("putMessagePositionInfoWrapper", DispatchRequest.class);

            assertThat(method).isNotNull();

            method.setAccessible(true);

            SelectMappedBufferResult result = messageStore.getCommitLog().getData(0);
            assertThat(result != null).isTrue();

            DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);

            assertThat(cq).isNotNull();

            Object dispatchResult = method.invoke(cq, dispatchRequest);

            ConsumeQueueInterface lmqCq1 = messageStore.getConsumeQueueTable().get("%LMQ%123").get(0);

            ConsumeQueueInterface lmqCq2 = messageStore.getConsumeQueueTable().get("%LMQ%456").get(0);

            assertThat(lmqCq1).isNotNull();

            assertThat(lmqCq2).isNotNull();

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(STORE_PATH);
        }

    }

    @Test
    public void testPutMessagePositionInfoMultiQueue() throws Exception {
        DefaultMessageStore messageStore = null;
        try {

            messageStore = genForMultiQueue();

            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsgMultiQueue(messageStore);
            }

            // Wait consume queue build finish.
            final MessageStore store = messageStore;
            Awaitility.with().pollInterval(100, TimeUnit.MILLISECONDS).await().timeout(1, TimeUnit.MINUTES).until(() -> {
                return store.dispatchBehindBytes() == 0;
            });

            ConsumeQueueInterface cq = messageStore.getConsumeQueueTable().get(TOPIC).get(QUEUE_ID);

            ConsumeQueueInterface lmqCq1 = messageStore.getConsumeQueueTable().get("%LMQ%123").get(0);

            ConsumeQueueInterface lmqCq2 = messageStore.getConsumeQueueTable().get("%LMQ%456").get(0);

            assertThat(cq).isNotNull();

            assertThat(lmqCq1).isNotNull();

            assertThat(lmqCq2).isNotNull();

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(STORE_PATH);
        }
    }

    @Test
    public void testConsumeQueueWithExtendData() {
        DefaultMessageStore master = null;
        try {
            master = gen();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        master.getDispatcherList().addFirst(new CommitLogDispatcher() {

            @Override
            public void dispatch(DispatchRequest request) {
                runCount++;
            }

            private int runCount = 0;
        });

        try {

            putMsg(master);
            final DefaultMessageStore master1 = master;
            ConsumeQueueInterface cq = await().atMost(3, SECONDS).until(() -> {
                ConcurrentMap<Integer, ConsumeQueueInterface> map = master1.getConsumeQueueTable().get(TOPIC);
                if (map == null) {
                    return null;
                }
                ConsumeQueueInterface anInterface = map.get(QUEUE_ID);
                return anInterface;
            }, item -> null != item);

            assertThat(cq).isNotNull();

            ReferredIterator<CqUnit> bufferResult = cq.iterateFrom(0);

            assertThat(bufferResult).isNotNull();

            Assert.assertTrue(bufferResult.hasNext());

            try {
                while (bufferResult.hasNext()) {
                    CqUnit cqUnit = bufferResult.next();
                    Assert.assertNotNull(cqUnit);
                    long phyOffset = cqUnit.getPos();
                    int size = cqUnit.getSize();
                    long tagsCode = cqUnit.getTagsCode();

                    assertThat(phyOffset).isGreaterThanOrEqualTo(0);
                    assertThat(size).isGreaterThan(0);
                    assertThat(tagsCode).isGreaterThan(0);

                    ConsumeQueueExt.CqExtUnit cqExtUnit = cqUnit.getCqExtUnit();
                    assertThat(cqExtUnit).isNotNull();
                    assertThat(tagsCode).isEqualTo(cqExtUnit.getTagsCode());
                    assertThat(cqExtUnit.getSize()).isGreaterThan((short) 0);
                    assertThat(cqExtUnit.getMsgStoreTime()).isGreaterThan(0);
                    assertThat(cqExtUnit.getTagsCode()).isGreaterThan(0);
                }

            } finally {
                bufferResult.release();
            }

        } finally {
            master.shutdown();
            master.destroy();
            UtilAll.deleteFile(new File(STORE_PATH));
        }
    }

    @Test
    public void testCorrectMinOffset() {
        String topic = "T1";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "test_correct_min_offset");
        tmpDir.deleteOnExit();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);

        int max = 10000;
        int messageSize = 100;
        for (int i = 0; i < max; ++i) {
            DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, messageSize * i, messageSize, 0, 0, i, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(dispatchRequest);
        }

        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(0L);
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());

        consumeQueue.setMinLogicOffset(100);
        consumeQueue.correctMinOffset(2000);
        Assert.assertEquals(20, consumeQueue.getMinOffsetInQueue());

        consumeQueue.setMinLogicOffset((max - 1) * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        consumeQueue.correctMinOffset(max * messageSize);
        Assert.assertEquals(max * ConsumeQueue.CQ_STORE_UNIT_SIZE, consumeQueue.getMinLogicOffset());

        consumeQueue.setMinLogicOffset(max * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        consumeQueue.correctMinOffset(max * messageSize);
        Assert.assertEquals(max * ConsumeQueue.CQ_STORE_UNIT_SIZE, consumeQueue.getMinLogicOffset());
        consumeQueue.destroy();
    }

    @Test
    public void testFillBankThenCorrectMinOffset() throws IOException {
        String topic = "T1";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "testFillBankThenCorrectMinOffset");
        FileUtils.deleteDirectory(tmpDir);
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        {
            ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(consumeQueue.load());
            consumeQueue.recover();
            consumeQueue.initializeWithOffset(100, 100);
            Assert.assertEquals(100, consumeQueue.getMinOffsetInQueue());
            Assert.assertEquals(100, consumeQueue.getMaxOffsetInQueue());
        }

        {
            ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(consumeQueue.load());
            consumeQueue.recover();
            consumeQueue.correctMinOffset(1L);
            Assert.assertEquals(100, consumeQueue.getMinOffsetInQueue());
            Assert.assertEquals(100, consumeQueue.getMaxOffsetInQueue());
        }

//        {
//            ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
//                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
//            Assert.assertTrue(consumeQueue.load());
//            consumeQueue.recover();
//            consumeQueue.correctMinOffset(0L);
//            Assert.assertEquals(100, consumeQueue.getMinOffsetInQueue());
//            Assert.assertEquals(100, consumeQueue.getMaxOffsetInQueue());
//        }

        ConsumeQueue consumeQueue0 = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        consumeQueue0.destroy();
    }

    @Test
    public void correctMinOffsetWithReadAheadOptimizationTest() throws IOException {
        Assume.assumeTrue(!MixAll.isWindows() && !MixAll.isMac());
        String topic = "ReadAheadOptimizationTestTopic";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "correctMinOffsetWithReadAheadOptimizationTest");
        FileUtils.deleteDirectory(tmpDir);
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        storeConfig.setCorrectMinOffsetMadviseEnable(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);

        // Write 5000 messages to create a substantial CQ file for binary search
        int messageCount = 5000;
        int messageSize = 100;
        for (int i = 0; i < messageCount; i++) {
            DispatchRequest dispatchRequest = new DispatchRequest(
                topic, queueId, messageSize * i, messageSize, 0, 0, i, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(dispatchRequest);
        }

        // Test 1: correctMinOffset should work correctly with madvise optimization
        // Set min offset to 0 and correct with minCommitLogOffset = 0
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(0L);
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());

        // Test 2: Correct with a mid-range offset to trigger binary search
        // This will exercise the madvise(MADV_RANDOM) -> binary search -> madvise(MADV_NORMAL) path
        int targetOffset = 2500;
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(targetOffset * messageSize);
        Assert.assertEquals(targetOffset, consumeQueue.getMinOffsetInQueue());

        // Test 3: Correct with a high offset near the end
        int highOffset = 4500;
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(highOffset * messageSize);
        Assert.assertEquals(highOffset, consumeQueue.getMinOffsetInQueue());

        // Test 4: Correct with exact match offset
        int exactOffset = 1234;
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(exactOffset * messageSize);
        Assert.assertEquals(exactOffset, consumeQueue.getMinOffsetInQueue());

        // Test 5: Correct with offset beyond all messages (should point to end)
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(messageCount * messageSize);
        Assert.assertEquals(messageCount * ConsumeQueue.CQ_STORE_UNIT_SIZE, consumeQueue.getMinLogicOffset());

        // Test 6: Multiple sequential corrections to verify madvise restore works properly
        for (int i = 0; i < 5; i++) {
            int testOffset = 1000 * (i + 1);
            consumeQueue.setMinLogicOffset(0L);
            consumeQueue.correctMinOffset(testOffset * messageSize);
            Assert.assertEquals(testOffset, consumeQueue.getMinOffsetInQueue());
        }

        consumeQueue.destroy();
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void correctMinOffsetWithSmallDatasetReadAheadOptimizationTest() throws IOException {
        Assume.assumeTrue(!MixAll.isWindows() && !MixAll.isMac());
        String topic = "SmallDatasetTopic";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "correctMinOffsetWithSmallDatasetReadAheadOptimizationTest");
        FileUtils.deleteDirectory(tmpDir);
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        storeConfig.setCorrectMinOffsetMadviseEnable(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);

        // Test with very small dataset (edge case for binary search)
        int messageCount = 10;
        int messageSize = 100;
        for (int i = 0; i < messageCount; i++) {
            DispatchRequest dispatchRequest = new DispatchRequest(
                topic, queueId, messageSize * i, messageSize, 0, 0, i, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(dispatchRequest);
        }

        // Correct with various offsets
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(5 * messageSize);
        Assert.assertEquals(5, consumeQueue.getMinOffsetInQueue());

        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(0L);
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());

        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(9 * messageSize);
        Assert.assertEquals(9, consumeQueue.getMinOffsetInQueue());

        consumeQueue.destroy();
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void correctMinOffsetWithEmptyQueueReadAheadOptimizationTest() throws IOException {
        Assume.assumeTrue(!MixAll.isWindows() && !MixAll.isMac());
        String topic = "EmptyQueueTopic";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "correctMinOffsetWithEmptyQueueReadAheadOptimizationTest");
        FileUtils.deleteDirectory(tmpDir);
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        storeConfig.setCorrectMinOffsetMadviseEnable(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);

        // Test with empty queue - should handle gracefully without madvise errors
        consumeQueue.setMinLogicOffset(0L);
        consumeQueue.correctMinOffset(0L);
        Assert.assertEquals(0, consumeQueue.getMinOffsetInQueue());

        consumeQueue.destroy();
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void testCorrectMinOffsetAfterAllFilesDeleted() throws IOException {
        String topic = "T1";
        int queueId = 0;
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        File tmpDir = Files.createTempDirectory("testCorrectMinOffsetAfterAllFilesDeleted").toFile();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setEnableConsumeQueueExt(false);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);

        RunningFlags runningFlags = new RunningFlags();
        Mockito.when(messageStore.getRunningFlags()).thenReturn(runningFlags);

        StoreCheckpoint storeCheckpoint = Mockito.mock(StoreCheckpoint.class);
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(storeCheckpoint);

        ConsumeQueue consumeQueue = null;
        ConsumeQueue reloadedConsumeQueue = null;
        try {
            consumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);

            int max = 10;
            int messageSize = 100;
            for (int i = 0; i < max; ++i) {
                DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, messageSize * i, messageSize, 0, 0, i,
                    null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(dispatchRequest);
            }

            File consumeQueueDir = new File(storeConfig.getStorePathRootDir(), topic + File.separator + queueId);
            Assert.assertTrue(consumeQueueDir.exists());

            consumeQueue.setMinLogicOffset(ConsumeQueue.CQ_STORE_UNIT_SIZE);
            consumeQueue.setMaxPhysicOffset(max * messageSize);
            consumeQueue.destroy();
            consumeQueue = null;
            FileUtils.deleteQuietly(consumeQueueDir);
            Assert.assertFalse(consumeQueueDir.exists());

            reloadedConsumeQueue = new ConsumeQueue(topic, queueId, storeConfig.getStorePathRootDir(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();
            reloadedConsumeQueue.setMinLogicOffset(ConsumeQueue.CQ_STORE_UNIT_SIZE);
            reloadedConsumeQueue.setMaxPhysicOffset(max * messageSize);
            reloadedConsumeQueue.correctMinOffset(0L);
            Assert.assertEquals(0L, reloadedConsumeQueue.getMinLogicOffset());
            Assert.assertEquals(-1L, reloadedConsumeQueue.getMaxPhysicOffset());
        } finally {
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            if (consumeQueue != null) {
                consumeQueue.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateRetainsPreviousFullFileAfterDeletingDirtyTailFile() throws IOException {
        File tmpDir = Files.createTempDirectory("truncate-cq-tail-files").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(2 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(false);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        try {
            long[] physicalOffsets = {0, 10, 1000, 1010};
            for (int i = 0; i < physicalOffsets.length; i++) {
                DispatchRequest request = new DispatchRequest("truncateTopic", 0, physicalOffsets[i], 10,
                    0, 0, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            Assert.assertEquals(4, consumeQueue.getMaxOffsetInQueue());

            consumeQueue.truncateDirtyLogicFiles(500);

            Assert.assertEquals(2, consumeQueue.getMaxOffsetInQueue());
        } finally {
            consumeQueue.destroy();
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateDirtyLogicFilesTruncatesConsumeQueueExtAndSurvivesReload() throws IOException {
        File tmpDir = Files.createTempDirectory("truncate-cq-ext").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(4 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateExtTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        try {
            for (int i = 0; i < 4; i++) {
                DispatchRequest request = new DispatchRequest("truncateExtTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long truncatedExtAddress = getRawTagsCode(consumeQueue, 2);

            consumeQueue.truncateDirtyLogicFiles(200);

            for (int i = 2; i < 4; i++) {
                DispatchRequest replacement = new DispatchRequest("truncateExtTopic", 0, 100L * i, 10,
                    200 + i, 2000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(replacement);
            }
            long replacementExtAddress = getRawTagsCode(consumeQueue, 2);
            consumeQueue.flush(0);

            reloadedConsumeQueue = new ConsumeQueue("truncateExtTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();

            Assert.assertEquals(202, reloadedConsumeQueue.getExt(truncatedExtAddress).getTagsCode());
            Assert.assertEquals(truncatedExtAddress, replacementExtAddress);
        } finally {
            consumeQueue.destroy();
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateAllConsumeQueueExtSurvivesReloadAndReusesFirstAddress() throws IOException {
        File tmpDir = Files.createTempDirectory("truncate-all-cq-ext").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(4 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateAllExtTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue emptyReloadedConsumeQueue = null;
        ConsumeQueue replacementReloadedConsumeQueue = null;
        try {
            for (int i = 0; i < 2; i++) {
                DispatchRequest request = new DispatchRequest("truncateAllExtTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long firstExtAddress = getRawTagsCode(consumeQueue, 0);
            consumeQueue.flush(0);

            consumeQueue.truncateDirtyLogicFiles(0);
            consumeQueue.flush(0);

            emptyReloadedConsumeQueue = new ConsumeQueue("truncateAllExtTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(emptyReloadedConsumeQueue.load());
            emptyReloadedConsumeQueue.recover();
            Assert.assertEquals(0, emptyReloadedConsumeQueue.getMaxOffsetInQueue());
            Assert.assertNull(emptyReloadedConsumeQueue.getExt(firstExtAddress));

            DispatchRequest replacement = new DispatchRequest("truncateAllExtTopic", 0, 0, 10,
                200, 2000, 0, null, null, 0, 0, null);
            emptyReloadedConsumeQueue.putMessagePositionInfoWrapper(replacement);
            long replacementExtAddress = getRawTagsCode(emptyReloadedConsumeQueue, 0);
            Assert.assertEquals(firstExtAddress, replacementExtAddress);
            emptyReloadedConsumeQueue.flush(0);

            replacementReloadedConsumeQueue = new ConsumeQueue("truncateAllExtTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(replacementReloadedConsumeQueue.load());
            replacementReloadedConsumeQueue.recover();
            Assert.assertEquals(200, replacementReloadedConsumeQueue.getExt(firstExtAddress).getTagsCode());
        } finally {
            consumeQueue.destroy();
            if (emptyReloadedConsumeQueue != null) {
                emptyReloadedConsumeQueue.destroy();
            }
            if (replacementReloadedConsumeQueue != null) {
                replacementReloadedConsumeQueue.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateDeletesEmptyTailAndRetainsPreviousFileExt() throws Exception {
        File tmpDir = Files.createTempDirectory("truncate-empty-cq-tail").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(2 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateEmptyTailTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        try {
            for (int i = 0; i < 2; i++) {
                DispatchRequest request = new DispatchRequest("truncateEmptyTailTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long lastRetainedExtAddress = getRawTagsCode(consumeQueue, 1);
            MappedFileQueue mappedFileQueue = getMappedFileQueue(consumeQueue);
            Assert.assertNotNull(mappedFileQueue.getLastMappedFile(0));
            Assert.assertEquals(2, mappedFileQueue.getMappedFiles().size());
            Assert.assertEquals(0, mappedFileQueue.getLastMappedFile().getWrotePosition());

            consumeQueue.truncateDirtyLogicFiles(500);

            Assert.assertEquals(1, mappedFileQueue.getMappedFiles().size());
            Assert.assertEquals(2, consumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(101, consumeQueue.getExt(lastRetainedExtAddress).getTagsCode());
            consumeQueue.flush(0);

            reloadedConsumeQueue = new ConsumeQueue("truncateEmptyTailTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();
            Assert.assertEquals(2, reloadedConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(101, reloadedConsumeQueue.getExt(lastRetainedExtAddress).getTagsCode());
        } finally {
            consumeQueue.destroy();
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateWithoutDeletingFilesRetainsExtForReload() throws Exception {
        File tmpDir = Files.createTempDirectory("truncate-cq-without-delete").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(2 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateWithoutDeleteTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        MappedFile removedTailFile = null;
        try {
            for (int i = 0; i < 4; i++) {
                DispatchRequest request = new DispatchRequest("truncateWithoutDeleteTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long dirtyExtAddress = getRawTagsCode(consumeQueue, 2);
            consumeQueue.flush(0);
            MappedFileQueue mappedFileQueue = getMappedFileQueue(consumeQueue);
            removedTailFile = mappedFileQueue.getLastMappedFile();

            consumeQueue.truncateDirtyLogicFiles(200, false);

            Assert.assertEquals(1, mappedFileQueue.getMappedFiles().size());
            Assert.assertEquals(2, consumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(102, consumeQueue.getExt(dirtyExtAddress).getTagsCode());
            consumeQueue.flush(0);

            reloadedConsumeQueue = new ConsumeQueue("truncateWithoutDeleteTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();
            Assert.assertEquals(4, reloadedConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(102, reloadedConsumeQueue.getExt(dirtyExtAddress).getTagsCode());
        } finally {
            consumeQueue.destroy();
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            if (removedTailFile != null) {
                removedTailFile.destroy(1000);
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateIgnoresExpiredExtBeforeMinLogicOffset() throws IOException {
        File tmpDir = Files.createTempDirectory("truncate-expired-cq-ext").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(4 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateExpiredExtTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        try {
            DispatchRequest expiredRequest = new DispatchRequest("truncateExpiredExtTopic", 0, 0, 10,
                100, 1000, 0, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(expiredRequest);
            long expiredExtAddress = getRawTagsCode(consumeQueue, 0);

            storeConfig.setEnableConsumeQueueExt(false);
            for (int i = 1; i < 3; i++) {
                DispatchRequest request = new DispatchRequest("truncateExpiredExtTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            consumeQueue.setMinLogicOffset(ConsumeQueue.CQ_STORE_UNIT_SIZE);

            consumeQueue.truncateDirtyLogicFiles(200);

            Assert.assertEquals(2, consumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(101, getRawTagsCode(consumeQueue, 1));
            Assert.assertNull(consumeQueue.getExt(expiredExtAddress));
        } finally {
            consumeQueue.destroy();
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testRecoverClearsExtWhenConsumeQueueFilesAreMissing() throws Exception {
        File tmpDir = Files.createTempDirectory("recover-orphan-cq-ext").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(4 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("recoverOrphanExtTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        try {
            for (int i = 0; i < 2; i++) {
                DispatchRequest request = new DispatchRequest("recoverOrphanExtTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long orphanedExtAddress = getRawTagsCode(consumeQueue, 0);
            consumeQueue.flush(0);

            MappedFileQueue mappedFileQueue = getMappedFileQueue(consumeQueue);
            mappedFileQueue.destroy();
            Assert.assertTrue(mappedFileQueue.getMappedFiles().isEmpty());

            reloadedConsumeQueue = new ConsumeQueue("recoverOrphanExtTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();

            Assert.assertEquals(0, reloadedConsumeQueue.getMaxOffsetInQueue());
            Assert.assertNull(reloadedConsumeQueue.getExt(orphanedExtAddress));
        } finally {
            consumeQueue.destroy();
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testRecoverCompletesPendingExtCleanupAfterFallbackDispatch() throws Exception {
        File tmpDir = Files.createTempDirectory("recover-pending-cq-ext").toFile();
        String topic = "recoverPendingExtTopic";
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(4 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(2 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        SelectMappedBufferResult heldBuffer = null;
        try {
            for (int i = 0; i < 2; i++) {
                DispatchRequest request = new DispatchRequest(topic, 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long firstExtAddress = getRawTagsCode(consumeQueue, 0);
            long retainedExtAddress = getRawTagsCode(consumeQueue, 1);
            consumeQueue.flush(0);

            ConsumeQueueExt consumeQueueExt = getConsumeQueueExt(consumeQueue);
            MappedFileQueue extMappedFileQueue = getMappedFileQueue(consumeQueueExt);
            Assert.assertEquals(2, extMappedFileQueue.getMappedFiles().size());
            MappedFile retainedMappedFile = extMappedFileQueue.getLastMappedFile();
            File retainedMappedFilePath = new File(retainedMappedFile.getFileName());
            heldBuffer = retainedMappedFile.selectMappedBuffer(
                0, ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
            Assert.assertNotNull(heldBuffer);

            consumeQueue.truncateDirtyLogicFiles(0);

            Assert.assertEquals(0, consumeQueue.getMaxOffsetInQueue());
            Assert.assertTrue(retainedMappedFilePath.exists());

            DispatchRequest fallback = new DispatchRequest(topic, 0, 0, 10,
                200, 2000, 0, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(fallback);
            Assert.assertEquals(200, getRawTagsCode(consumeQueue, 0));
            consumeQueue.flush(0);

            heldBuffer.release();
            heldBuffer = null;
            Assert.assertTrue(retainedMappedFilePath.exists());

            reloadedConsumeQueue = new ConsumeQueue(topic, 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();

            Assert.assertFalse(retainedMappedFilePath.exists());
            Assert.assertNull(reloadedConsumeQueue.getExt(retainedExtAddress));

            DispatchRequest replacement = new DispatchRequest(topic, 0, 100, 10,
                300, 3000, 1, null, null, 0, 0, null);
            reloadedConsumeQueue.putMessagePositionInfoWrapper(replacement);
            long replacementExtAddress = getRawTagsCode(reloadedConsumeQueue, 1);
            Assert.assertEquals(firstExtAddress, replacementExtAddress);
            Assert.assertEquals(300, reloadedConsumeQueue.getExt(replacementExtAddress).getTagsCode());
        } finally {
            if (heldBuffer != null) {
                heldBuffer.release();
            }
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            consumeQueue.destroy();
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testRecoverRetainsExtAddressBeforeRecoveryWindow() throws Exception {
        File tmpDir = Files.createTempDirectory("recover-old-cq-ext").toFile();
        String topic = "recoverOldExtTopic";
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue(topic, 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        try {
            DispatchRequest first = new DispatchRequest(topic, 0, 0, 10,
                100, 1000, 0, null, null, 0, 0, null);
            consumeQueue.putMessagePositionInfoWrapper(first);
            long firstExtAddress = getRawTagsCode(consumeQueue, 0);

            storeConfig.setEnableConsumeQueueExt(false);
            for (int i = 1; i < 4; i++) {
                DispatchRequest request = new DispatchRequest(topic, 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            consumeQueue.flush(0);
            storeConfig.setEnableConsumeQueueExt(true);

            reloadedConsumeQueue = new ConsumeQueue(topic, 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();

            ConsumeQueueExt.CqExtUnit retainedUnit = reloadedConsumeQueue.getExt(firstExtAddress);
            Assert.assertNotNull(retainedUnit);
            Assert.assertEquals(100, retainedUnit.getTagsCode());
        } finally {
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            consumeQueue.destroy();
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateKeepsExtWhenConsumeQueueTailDeletionFails() throws Exception {
        File tmpDir = Files.createTempDirectory("truncate-cq-tail-delete-failure").toFile();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(tmpDir.getAbsolutePath());
        storeConfig.setMappedFileSizeConsumeQueue(2 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
        storeConfig.setMappedFileSizeConsumeQueueExt(10 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
        storeConfig.setEnableConsumeQueueExt(true);
        DefaultMessageStore messageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getRunningFlags()).thenReturn(new RunningFlags());
        Mockito.when(messageStore.getStoreCheckpoint()).thenReturn(Mockito.mock(StoreCheckpoint.class));

        ConsumeQueue consumeQueue = new ConsumeQueue("truncateDeleteFailureTopic", 0, tmpDir.getAbsolutePath(),
            storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
        ConsumeQueue reloadedConsumeQueue = null;
        MappedFile retainedTailFile = null;
        SelectMappedBufferResult heldBuffer = null;
        try {
            for (int i = 0; i < 4; i++) {
                DispatchRequest request = new DispatchRequest("truncateDeleteFailureTopic", 0, 100L * i, 10,
                    100 + i, 1000 + i, i, null, null, 0, 0, null);
                consumeQueue.putMessagePositionInfoWrapper(request);
            }
            long dirtyExtAddress = getRawTagsCode(consumeQueue, 2);
            consumeQueue.flush(0);

            MappedFileQueue mappedFileQueue = getMappedFileQueue(consumeQueue);
            retainedTailFile = mappedFileQueue.getLastMappedFile();
            File retainedTailPath = new File(retainedTailFile.getFileName());
            heldBuffer = retainedTailFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
            Assert.assertNotNull(heldBuffer);

            consumeQueue.truncateDirtyLogicFiles(200);

            Assert.assertTrue(retainedTailPath.exists());
            Assert.assertEquals(1, mappedFileQueue.getMappedFiles().size());
            Assert.assertEquals(2, consumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(102, consumeQueue.getExt(dirtyExtAddress).getTagsCode());

            heldBuffer.release();
            heldBuffer = null;

            reloadedConsumeQueue = new ConsumeQueue("truncateDeleteFailureTopic", 0, tmpDir.getAbsolutePath(),
                storeConfig.getMappedFileSizeConsumeQueue(), messageStore);
            Assert.assertTrue(reloadedConsumeQueue.load());
            reloadedConsumeQueue.recover();

            Assert.assertEquals(4, reloadedConsumeQueue.getMaxOffsetInQueue());
            Assert.assertEquals(102, reloadedConsumeQueue.getExt(dirtyExtAddress).getTagsCode());
        } finally {
            if (heldBuffer != null) {
                heldBuffer.release();
            }
            consumeQueue.destroy();
            if (reloadedConsumeQueue != null) {
                reloadedConsumeQueue.destroy();
            }
            if (retainedTailFile != null) {
                retainedTailFile.destroy(1000);
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    @Test
    public void testTruncateAllConsumeQueueExtRetriesAfterMappedFileRelease() throws Exception {
        File tmpDir = Files.createTempDirectory("truncate-all-cq-ext-retry").toFile();
        String topic = "truncateAllExtRetryTopic";
        String extStorePath = StorePathConfigHelper.getStorePathConsumeQueueExt(tmpDir.getAbsolutePath());
        int mappedFileSize = 2 * ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE;
        ConsumeQueueExt consumeQueueExt = new ConsumeQueueExt(topic, 0, extStorePath, mappedFileSize, 0);
        ConsumeQueueExt reloadedConsumeQueueExt = null;
        SelectMappedBufferResult heldBuffer = null;
        try {
            long firstExtAddress = consumeQueueExt.put(
                new ConsumeQueueExt.CqExtUnit(100L, 1000L, null));
            long secondExtAddress = consumeQueueExt.put(
                new ConsumeQueueExt.CqExtUnit(101L, 1001L, null));
            Assert.assertTrue(ConsumeQueueExt.isExtAddr(firstExtAddress));
            Assert.assertTrue(ConsumeQueueExt.isExtAddr(secondExtAddress));
            Assert.assertNotEquals(firstExtAddress, secondExtAddress);
            consumeQueueExt.flush(0);

            MappedFileQueue mappedFileQueue = getMappedFileQueue(consumeQueueExt);
            Assert.assertEquals(2, mappedFileQueue.getMappedFiles().size());
            MappedFile firstMappedFile = mappedFileQueue.getMappedFiles().get(0);
            MappedFile retainedMappedFile = mappedFileQueue.getMappedFiles().get(1);
            File firstMappedFilePath = new File(firstMappedFile.getFileName());
            File retainedMappedFilePath = new File(retainedMappedFile.getFileName());
            long flushedWhereBeforeTruncate = mappedFileQueue.getFlushedWhere();
            Assert.assertTrue(flushedWhereBeforeTruncate > 0);
            heldBuffer = retainedMappedFile.selectMappedBuffer(0, ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE);
            Assert.assertNotNull(heldBuffer);

            Assert.assertFalse(consumeQueueExt.truncateAll());
            Assert.assertFalse(firstMappedFilePath.exists());
            Assert.assertTrue(retainedMappedFilePath.exists());
            Assert.assertEquals(1, mappedFileQueue.getMappedFiles().size());
            Assert.assertSame(retainedMappedFile, mappedFileQueue.getMappedFiles().get(0));
            Assert.assertEquals(flushedWhereBeforeTruncate, mappedFileQueue.getFlushedWhere());
            Assert.assertEquals(1, consumeQueueExt.put(
                new ConsumeQueueExt.CqExtUnit(200L, 2000L, null)));
            Assert.assertEquals(flushedWhereBeforeTruncate, mappedFileQueue.getFlushedWhere());
            Assert.assertEquals(1, mappedFileQueue.getMappedFiles().size());

            heldBuffer.release();
            heldBuffer = null;
            Assert.assertTrue(retainedMappedFilePath.exists());

            AtomicLong replacementExtAddress = new AtomicLong(1);
            Awaitility.await().atMost(5, SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
                replacementExtAddress.set(consumeQueueExt.put(
                    new ConsumeQueueExt.CqExtUnit(200L, 2000L, null)));
                return ConsumeQueueExt.isExtAddr(replacementExtAddress.get());
            });
            Assert.assertFalse(retainedMappedFilePath.exists());
            Assert.assertEquals(0, mappedFileQueue.getFlushedWhere());
            Assert.assertEquals(0, mappedFileQueue.getCommittedWhere());
            Assert.assertEquals(firstExtAddress, replacementExtAddress.get());
            consumeQueueExt.flush(0);

            reloadedConsumeQueueExt = new ConsumeQueueExt(topic, 0, extStorePath, mappedFileSize, 0);
            Assert.assertTrue(reloadedConsumeQueueExt.load());
            reloadedConsumeQueueExt.recover();
            ConsumeQueueExt.CqExtUnit replacement = reloadedConsumeQueueExt.get(replacementExtAddress.get());
            Assert.assertNotNull(replacement);
            Assert.assertEquals(200, replacement.getTagsCode());
        } finally {
            if (heldBuffer != null) {
                heldBuffer.release();
            }
            consumeQueueExt.destroy();
            if (reloadedConsumeQueueExt != null) {
                reloadedConsumeQueueExt.destroy();
            }
            FileUtils.deleteQuietly(tmpDir);
        }
    }

    private MappedFileQueue getMappedFileQueue(ConsumeQueue consumeQueue) throws ReflectiveOperationException {
        Field mappedFileQueueField = ConsumeQueue.class.getDeclaredField("mappedFileQueue");
        mappedFileQueueField.setAccessible(true);
        return (MappedFileQueue) mappedFileQueueField.get(consumeQueue);
    }

    private MappedFileQueue getMappedFileQueue(ConsumeQueueExt consumeQueueExt) throws ReflectiveOperationException {
        Field mappedFileQueueField = ConsumeQueueExt.class.getDeclaredField("mappedFileQueue");
        mappedFileQueueField.setAccessible(true);
        return (MappedFileQueue) mappedFileQueueField.get(consumeQueueExt);
    }

    private ConsumeQueueExt getConsumeQueueExt(ConsumeQueue consumeQueue) throws ReflectiveOperationException {
        Field consumeQueueExtField = ConsumeQueue.class.getDeclaredField("consumeQueueExt");
        consumeQueueExtField.setAccessible(true);
        return (ConsumeQueueExt) consumeQueueExtField.get(consumeQueue);
    }

    private long getRawTagsCode(ConsumeQueue consumeQueue, long queueOffset) {
        SelectMappedBufferResult result = consumeQueue.getIndexBuffer(queueOffset);
        Assert.assertNotNull(result);
        try {
            return result.getByteBuffer().getLong(12);
        } finally {
            result.release();
        }
    }
}
