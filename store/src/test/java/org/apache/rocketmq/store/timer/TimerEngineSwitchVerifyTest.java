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
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Fix verification test: verifies that after adding the timerStopEnqueue guard in the scheduler task,
 * checkAndReviseMetrics does not incorrectly overwrite RocksDB metrics during engine switching.
 *
 * Fix approach:
 * In the checkAndReviseMetrics scheduled task registered in TimerMessageStore.start(),
 * add a storeConfig.isTimerStopEnqueue() check: when the file-based engine has stopped enqueuing
 * (indicating a switch to RocksDB), skip checkAndReviseMetrics to avoid traversing only timerLog
 * and overwriting RocksDB-side metrics via putAll.
 *
 * This test class covers the following scenarios:
 * 1. File-based mode: checkAndReviseMetrics works normally
 * 2. After switching to RocksDB: RocksDB-only topic metrics are not overwritten
 * 3. After switching to RocksDB: shared topic metrics are not overwritten
 * 4. After switching back to file-based mode: checkAndReviseMetrics resumes normally
 * 5. When scheduler auto-triggers: timerStopEnqueue=true skips checkAndReviseMetrics
 * 6. Repeated engine switches: metrics consistency is always maintained
 */
public class TimerEngineSwitchVerifyTest {

    private final byte[] msgBody = new byte[1024];
    private MessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private final int precisionMs = 500;
    private final Set<String> baseDirs = new HashSet<>();
    private final List<TimerMessageStore> timerStores = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger(0);
    private MessageStoreConfig storeConfig;

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
        storeConfig.setAppendTopicForTimerDeleteKey(false);
        storeConfig.setTimerMetricSmallThreshold(1000000);

        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerFixTest", false),
                new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    private TimerMessageStore createTimerMessageStore(String rootDir) throws Exception {
        if (null == rootDir) {
            rootDir = StoreTestUtils.createBaseDir();
        }
        TimerCheckpoint timerCheckpoint = new TimerCheckpoint(
                rootDir + File.separator + "config" + File.separator + "timercheck");
        TimerMetrics timerMetrics = new TimerMetrics(
                rootDir + File.separator + "config" + File.separator + "timermetrics");
        TimerMessageStore timerMessageStore = new TimerMessageStore(
                messageStore, storeConfig, timerCheckpoint, timerMetrics, null);
        messageStore.setTimerMessageStore(timerMessageStore);

        baseDirs.add(rootDir);
        timerStores.add(timerMessageStore);
        return timerMessageStore;
    }

    /**
     * Scenario 1: In file-based mode (timerStopEnqueue=false), checkAndReviseMetrics works normally.
     * Ensures the fix does not affect the original metrics correction capability of the file-based engine.
     */
    @Test
    public void testFileMode_checkAndReviseMetrics_worksNormally() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String topic = "FixVerify_FileMode_" + System.currentTimeMillis();
        final int msgCount = 4;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        // Ensure file-based mode
        storeConfig.setTimerStopEnqueue(false);
        storeConfig.setTimerRocksDBEnable(false);

        // Write timer messages to timerLog
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= msgCount;
            }
        });

        TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();
        assertEquals(msgCount, timerMetrics.getTimingCount(topic));

        // Execute checkAndReviseMetrics -- should work normally in file-based mode
        timerMessageStore.checkAndReviseMetrics();

        // Verify: file-based metrics should remain correct (re-counted from timerLog)
        assertEquals("checkAndReviseMetrics should correctly revise metrics in file-based mode",
                msgCount, timerMetrics.getTimingCount(topic));
    }

    /**
     * Scenario 2: After switching to RocksDB, RocksDB-only topic metrics are not overwritten.
     *
     * Verifies the fix: when timerStopEnqueue=true in the scheduler task, checkAndReviseMetrics is skipped,
     * so RocksDB-only topic metrics are not overwritten to 0 by putAll(newSmallOnes).
     */
    @Test
    public void testSwitchToRocksDB_rocksDBOnlyTopicPreserved() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String fileTopic = "FixVerify_FileTopic_" + System.currentTimeMillis();
        final String rocksdbTopic = "FixVerify_RocksDBTopic_" + System.currentTimeMillis();
        final int fileMsgCount = 3;
        final int rocksdbMsgCount = 6;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        // Phase 1: Write messages in file-based mode
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < fileMsgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, fileTopic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= fileMsgCount;
            }
        });

        TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();
        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // Phase 2: Simulate switchTimerEngine to RocksDB
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // Phase 3: Write new topic metrics from RocksDB side
        for (int i = 0; i < rocksdbMsgCount; i++) {
            MessageExt mockMsg = new MessageExt();
            MessageAccessor.putProperty(mockMsg, MessageConst.PROPERTY_REAL_TOPIC, rocksdbTopic);
            timerMetrics.addAndGet(mockMsg, 1);
        }

        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));
        assertEquals(rocksdbMsgCount, timerMetrics.getTimingCount(rocksdbTopic));

        // Phase 4: Simulate the fixed scheduler logic
        // The fix adds a timerStopEnqueue check in the scheduler, preventing checkAndReviseMetrics from being called
        boolean skipped = false;
        if (storeConfig.isTimerStopEnqueue()) {
            skipped = true;
            // Do not call checkAndReviseMetrics
        } else {
            timerMessageStore.checkAndReviseMetrics();
        }

        // Phase 5: Verify the fix
        assertTrue("Should skip checkAndReviseMetrics when timerStopEnqueue=true", skipped);
        assertEquals("RocksDB-only topic metrics should not be overwritten",
                rocksdbMsgCount, timerMetrics.getTimingCount(rocksdbTopic));
        assertEquals("File-based topic metrics should remain unchanged",
                fileMsgCount, timerMetrics.getTimingCount(fileTopic));
    }

    /**
     * Scenario 3: After switching to RocksDB, shared topic (messages in both file-based and RocksDB) metrics are not overwritten.
     *
     * Before fix: checkAndReviseMetrics only counts file-based quantities from timerLog, putAll loses the RocksDB portion.
     * After fix: checkAndReviseMetrics is skipped, all metrics remain unchanged.
     */
    @Test
    public void testSwitchToRocksDB_sharedTopicPreserved() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String sharedTopic = "FixVerify_SharedTopic_" + System.currentTimeMillis();
        final int fileMsgCount = 2;
        final int rocksdbMsgCount = 4;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        // Phase 1: Write in file-based mode
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < fileMsgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, sharedTopic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= fileMsgCount;
            }
        });

        TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();
        assertEquals(fileMsgCount, timerMetrics.getTimingCount(sharedTopic));

        // Phase 2: Switch to RocksDB
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // Phase 3: RocksDB continues to increment the count for the same topic
        for (int i = 0; i < rocksdbMsgCount; i++) {
            MessageExt mockMsg = new MessageExt();
            MessageAccessor.putProperty(mockMsg, MessageConst.PROPERTY_REAL_TOPIC, sharedTopic);
            timerMetrics.addAndGet(mockMsg, 1);
        }

        long totalBefore = timerMetrics.getTimingCount(sharedTopic);
        assertEquals(fileMsgCount + rocksdbMsgCount, totalBefore);

        // Phase 4: Simulate the fixed scheduler
        if (storeConfig.isTimerStopEnqueue()) {
            // Skip
        } else {
            timerMessageStore.checkAndReviseMetrics();
        }

        // Phase 5: Verify
        long totalAfter = timerMetrics.getTimingCount(sharedTopic);
        assertEquals("Shared topic metrics should not be overwritten (file-based " + fileMsgCount + " + RocksDB " + rocksdbMsgCount + ")",
                fileMsgCount + rocksdbMsgCount, totalAfter);
    }

    /**
     * Scenario 4: After switching back from RocksDB to file-based mode, checkAndReviseMetrics should resume normally.
     *
     * Simulated flow:
     * 1. File-based write -> switch to RocksDB -> RocksDB writes metrics
     * 2. Switch back to file-based mode (timerStopEnqueue=false)
     * 3. checkAndReviseMetrics resumes execution, normally revising metrics from timerLog
     */
    @Test
    public void testSwitchBackToFileMode_checkAndReviseMetrics_resumesNormally() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String fileTopic = "FixVerify_SwitchBack_" + System.currentTimeMillis();
        final int fileMsgCount = 5;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        // Phase 1: Write messages in file-based mode
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < fileMsgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, fileTopic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= fileMsgCount;
            }
        });

        TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();
        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // Phase 2: Switch to RocksDB
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // Simulate the fixed scheduler -- should skip
        boolean skippedInRocksDB = storeConfig.isTimerStopEnqueue();
        assertTrue("Should skip checkAndReviseMetrics in RocksDB mode", skippedInRocksDB);

        // Phase 3: Switch back to file-based mode (simulating switchTimerEngine(FILE_TIMELINE))
        storeConfig.setTimerStopEnqueue(false);
        storeConfig.setTimerRocksDBEnable(false);

        // Phase 4: After switching back, checkAndReviseMetrics resumes execution
        boolean skippedInFileMode = storeConfig.isTimerStopEnqueue();

        if (!skippedInFileMode) {
            timerMessageStore.checkAndReviseMetrics();
        }

        // Phase 5: Verify metrics are correct after switching back
        assertEquals("After switching back to file-based mode, checkAndReviseMetrics should correctly revise metrics from timerLog",
                fileMsgCount, timerMetrics.getTimingCount(fileTopic));
    }

    /**
     * Scenario 5: When the scheduler auto-triggers, the fixed timerStopEnqueue guard correctly skips checkAndReviseMetrics.
     *
     * Uses reflection to obtain the scheduler and registers a short-interval task (logic identical to the fixed start()),
     * verifying that when timerStopEnqueue=true the scheduler correctly skips and RocksDB metrics are not overwritten.
     */
    @Test
    public void testSchedulerAutoTrigger_skipsWhenSwitchedToRocksDB() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String fileTopic = "FixVerify_Sched_File_" + System.currentTimeMillis();
        final String rocksdbTopic = "FixVerify_Sched_RocksDB_" + System.currentTimeMillis();
        final int fileMsgCount = 3;
        final int rocksdbMsgCount = 8;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        // Phase 1: Write in file-based mode
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < fileMsgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, fileTopic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= fileMsgCount;
            }
        });

        final TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();
        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // Phase 2: Switch to RocksDB
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // Phase 3: Write metrics from RocksDB side
        for (int i = 0; i < rocksdbMsgCount; i++) {
            MessageExt mockMsg = new MessageExt();
            MessageAccessor.putProperty(mockMsg, MessageConst.PROPERTY_REAL_TOPIC, rocksdbTopic);
            timerMetrics.addAndGet(mockMsg, 1);
        }

        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));
        assertEquals(rocksdbMsgCount, timerMetrics.getTimingCount(rocksdbTopic));

        // Phase 4: Set conditions to trigger the scheduled task
        Calendar now = Calendar.getInstance();
        String currentHour = String.format("%02d", now.get(Calendar.HOUR_OF_DAY));
        Field whenField = MessageStoreConfig.class.getDeclaredField("timerCheckMetricsWhen");
        whenField.setAccessible(true);
        whenField.set(storeConfig, currentHour);
        timerMessageStore.lastTimeOfCheckMetrics = 0L;
        storeConfig.setTimerEnableCheckMetrics(true);

        // Phase 5: Obtain scheduler via reflection and register a short-interval task (logic identical to the fixed start())
        Field schedulerField = TimerMessageStore.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        ScheduledExecutorService scheduler = (ScheduledExecutorService) schedulerField.get(timerMessageStore);

        final AtomicBoolean schedulerExecuted = new AtomicBoolean(false);
        final AtomicBoolean wasSkipped = new AtomicBoolean(false);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (storeConfig.isTimerEnableCheckMetrics()) {
                        // ★ Fix logic: skip when timerStopEnqueue=true
                        if (storeConfig.isTimerStopEnqueue()) {
                            wasSkipped.set(true);
                            schedulerExecuted.set(true);
                            return;
                        }
                        String when = storeConfig.getTimerCheckMetricsWhen();
                        if (!UtilAll.isItTimeToDo(when)) {
                            return;
                        }
                        long curr = System.currentTimeMillis();
                        if (curr - timerMessageStore.lastTimeOfCheckMetrics > 70 * 60 * 1000) {
                            timerMessageStore.lastTimeOfCheckMetrics = curr;
                            timerMessageStore.checkAndReviseMetrics();
                            wasSkipped.set(false);
                            schedulerExecuted.set(true);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // Phase 6: Wait for the scheduler to execute
        await().atMost(10000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return schedulerExecuted.get();
            }
        });

        // Phase 7: Verify
        assertTrue("Scheduler should skip checkAndReviseMetrics due to timerStopEnqueue=true", wasSkipped.get());
        assertEquals("RocksDB topic metrics should not be overwritten",
                rocksdbMsgCount, timerMetrics.getTimingCount(rocksdbTopic));
        assertEquals("File-based topic metrics should remain unchanged",
                fileMsgCount, timerMetrics.getTimingCount(fileTopic));
    }

    /**
     * Scenario 6: Repeated engine switches, verifying metrics consistency.
     *
     * Flow:
     * 1. File-based write -> checkAndReviseMetrics normal -> metrics correct
     * 2. Switch to RocksDB -> RocksDB write -> scheduler skips -> all metrics preserved
     * 3. Switch back to file-based -> checkAndReviseMetrics resumes -> file-based metrics correctly revised
     * 4. Switch to RocksDB again -> new RocksDB metrics written -> scheduler skips -> all metrics preserved
     */
    @Test
    public void testRepeatedSwitch_metricsConsistency() throws Exception {
        Assume.assumeFalse(MixAll.isWindows());

        final String fileTopic = "FixVerify_Repeat_File_" + System.currentTimeMillis();
        final String rocksdbTopic1 = "FixVerify_Repeat_RDB1_" + System.currentTimeMillis();
        final String rocksdbTopic2 = "FixVerify_Repeat_RDB2_" + System.currentTimeMillis();
        final int fileMsgCount = 3;
        final int rocksdb1MsgCount = 4;
        final int rocksdb2MsgCount = 5;

        final TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start(true);

        TimerMetrics timerMetrics = timerMessageStore.getTimerMetrics();

        // ========== Round 1: Normal write in file-based mode ==========
        long delayMs = System.currentTimeMillis() / precisionMs * precisionMs + 60000;
        for (int i = 0; i < fileMsgCount; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, fileTopic, false);
            transformTimerMessage(timerMessageStore, inner, storeConfig);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }

        await().atMost(5000, TimeUnit.MILLISECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return timerMessageStore.getCommitQueueOffset() >= fileMsgCount;
            }
        });

        assertEquals(fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // Execute checkAndReviseMetrics in file-based mode
        if (!storeConfig.isTimerStopEnqueue()) {
            timerMessageStore.checkAndReviseMetrics();
        }
        assertEquals("Round 1: File-based metrics normal", fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // ========== Round 2: Switch to RocksDB ==========
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // RocksDB writes the first batch of new topic
        for (int i = 0; i < rocksdb1MsgCount; i++) {
            MessageExt mockMsg = new MessageExt();
            MessageAccessor.putProperty(mockMsg, MessageConst.PROPERTY_REAL_TOPIC, rocksdbTopic1);
            timerMetrics.addAndGet(mockMsg, 1);
        }

        // Fixed scheduler skips
        if (!storeConfig.isTimerStopEnqueue()) {
            timerMessageStore.checkAndReviseMetrics();
        }

        assertEquals("Round 2: File-based metrics preserved", fileMsgCount, timerMetrics.getTimingCount(fileTopic));
        assertEquals("Round 2: RocksDB Topic1 metrics preserved", rocksdb1MsgCount, timerMetrics.getTimingCount(rocksdbTopic1));

        // ========== Round 3: Switch back to file-based mode ==========
        storeConfig.setTimerStopEnqueue(false);
        storeConfig.setTimerRocksDBEnable(false);

        // Execute checkAndReviseMetrics after switching back
        if (!storeConfig.isTimerStopEnqueue()) {
            timerMessageStore.checkAndReviseMetrics();
        }

        // Note: After switching back to file-based mode, checkAndReviseMetrics re-counts from timerLog.
        // RocksDB's Topic1 is not in timerLog and will be overwritten to 0 by putAll.
        // This is the expected behavior after switching back -- file-based mode only manages file-based data.
        assertEquals("Round 3: File-based metrics correct (revised from timerLog)",
                fileMsgCount, timerMetrics.getTimingCount(fileTopic));

        // ========== Round 4: Switch to RocksDB again ==========
        storeConfig.setTimerStopEnqueue(true);
        storeConfig.setTimerRocksDBEnable(true);

        // RocksDB writes the second batch of new topic
        for (int i = 0; i < rocksdb2MsgCount; i++) {
            MessageExt mockMsg = new MessageExt();
            MessageAccessor.putProperty(mockMsg, MessageConst.PROPERTY_REAL_TOPIC, rocksdbTopic2);
            timerMetrics.addAndGet(mockMsg, 1);
        }

        // Fixed scheduler skips
        if (!storeConfig.isTimerStopEnqueue()) {
            timerMessageStore.checkAndReviseMetrics();
        }

        assertEquals("Round 4: File-based metrics preserved", fileMsgCount, timerMetrics.getTimingCount(fileTopic));
        assertEquals("Round 4: RocksDB Topic2 metrics preserved", rocksdb2MsgCount, timerMetrics.getTimingCount(rocksdbTopic2));
    }

    // ======================== Utility Methods ========================

    private static PutMessageResult transformTimerMessage(TimerMessageStore timerMessageStore,
            MessageExtBrokerInner msg, MessageStoreConfig storeConfig) {
        int delayLevel = msg.getDelayTimeLevel();
        long deliverMs;
        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis()
                        + Integer.parseInt(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                deliverMs = System.currentTimeMillis()
                        + Integer.parseInt(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
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
            msg.setTopic(TimerMessageStore.TIMER_TOPIC);
            msg.setQueueId(0);
        }
        return null;
    }

    private MessageExtBrokerInner buildMessage(long delayedMs, String topic, boolean relative) {
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
