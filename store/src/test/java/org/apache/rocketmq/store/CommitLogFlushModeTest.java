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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.PerfCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommitLogFlushModeTest {
    private final MessageStoreConfig config = new MessageStoreConfig();
    private final MappedFileQueue queue = mock(MappedFileQueue.class);
    private final AtomicLong flushedOffset = new AtomicLong();
    private final AtomicLong appendedOffset = new AtomicLong(128);
    private FlushManager manager;
    private MessageExt message;

    @Before
    public void setUp() throws Exception {
        config.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        config.setFlushCommitLogTimed(true);
        config.setFlushIntervalCommitLog(10000);
        DefaultMessageStore store = mock(DefaultMessageStore.class);
        when(store.getMessageStoreConfig()).thenReturn(config);
        when(store.getBrokerConfig()).thenReturn(new BrokerConfig());
        when(store.getStoreCheckpoint()).thenReturn(mock(StoreCheckpoint.class));
        when(store.getPerfCounter()).thenReturn(mock(PerfCounter.Ticks.class));
        CommitLog commitLog = new CommitLog(store);
        FieldUtils.writeDeclaredField(commitLog, "mappedFileQueue", queue, true);
        when(queue.getFlushedWhere()).thenAnswer(invocation -> flushedOffset.get());
        when(queue.flush(anyInt())).thenAnswer(invocation -> {
            flushedOffset.set(appendedOffset.get());
            return true;
        });
        manager = commitLog.getFlushManager();
        message = new MessageExt();
        message.setTopic("flush-mode-test");
        message.setWaitStoreMsgOK(true);
    }

    @After
    public void tearDown() {
        manager.shutdown();
    }

    @Test
    public void testSwitchToSyncWaitsForFlush() throws Exception {
        CountDownLatch enteredFlush = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        doAnswer(invocation -> {
            enteredFlush.countDown();
            assertThat(releaseFlush.await(5, TimeUnit.SECONDS)).isTrue();
            flushedOffset.set(appendedOffset.get());
            return true;
        }).when(queue).flush(anyInt());
        manager.start();
        config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        try {
            CompletableFuture<PutMessageStatus> result = manager.handleDiskFlush(appendResult(), message);
            assertThat(enteredFlush.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(result.isDone()).isFalse();
            appendedOffset.addAndGet(128);
            CompletableFuture<PutMessageStatus> nextResult = manager.handleDiskFlush(appendResult(), message);
            config.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
            releaseFlush.countDown();
            assertThat(result.get(3, TimeUnit.SECONDS)).isEqualTo(PutMessageStatus.PUT_OK);
            assertThat(nextResult.get(3, TimeUnit.SECONDS)).isEqualTo(PutMessageStatus.PUT_OK);
            assertThat(flushedOffset.get()).isEqualTo(appendedOffset.get());
        } finally {
            releaseFlush.countDown();
        }
    }

    @Test
    public void testRepeatedModeChanges() throws Exception {
        manager.start();
        for (int i = 0; i < 10; i++) {
            config.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
            appendedOffset.addAndGet(128);
            assertThat(manager.handleDiskFlush(appendResult(), message).isDone()).isTrue();
            config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
            appendedOffset.addAndGet(128);
            assertThat(manager.handleDiskFlush(appendResult(), message).get(3, TimeUnit.SECONDS))
                .isEqualTo(PutMessageStatus.PUT_OK);
            assertThat(flushedOffset.get()).isEqualTo(appendedOffset.get());
        }
    }

    @Test
    public void testBlockingFlushAfterModeChange() throws Exception {
        manager.start();
        config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PutMessageResult result = new PutMessageResult(PutMessageStatus.PUT_OK, appendResult());
            Future<?> completion = executor.submit(() -> manager.handleDiskFlush(appendResult(), result, message));
            completion.get(3, TimeUnit.SECONDS);
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
            assertThat(flushedOffset.get()).isEqualTo(appendedOffset.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTimedAsyncFlushIgnoresWakeup() throws Exception {
        CountDownLatch flushed = new CountDownLatch(1);
        doAnswer(invocation -> {
            flushed.countDown();
            return true;
        }).when(queue).flush(anyInt());
        manager.start();
        manager.wakeUpFlush();
        assertThat(flushed.await(100, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    public void testUntimedAsyncFlushWakesUp() throws Exception {
        config.setFlushCommitLogTimed(false);
        CountDownLatch flushed = new CountDownLatch(1);
        doAnswer(invocation -> {
            flushed.countDown();
            return true;
        }).when(queue).flush(anyInt());
        manager.start();
        manager.wakeUpFlush();
        assertThat(flushed.await(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testUnflushedRequestDoesNotReportSuccess() throws Exception {
        doReturn(true).when(queue).flush(anyInt());
        manager.start();
        config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        assertThat(manager.handleDiskFlush(appendResult(), message).get(5, TimeUnit.SECONDS))
            .isEqualTo(PutMessageStatus.FLUSH_DISK_TIMEOUT);
        assertThat(flushedOffset.get()).isZero();
    }

    private AppendMessageResult appendResult() {
        return new AppendMessageResult(AppendMessageStatus.PUT_OK, 0, (int) appendedOffset.get(), 0);
    }
}
