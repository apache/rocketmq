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

import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedFileQueueTest {

    private String storePath = System.getProperty("java.io.tmpdir") + File.separator + "unit_test_store";

    @Test
    public void testGetLastMappedFile() {
        final String fixedMsg = "0123456789abcdef";

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "a/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testFindMappedFileByOffset() {
        // four-byte string.
        final String fixedMsg = "abcd";

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "b/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }

        assertThat(mappedFileQueue.getMappedMemorySize()).isEqualTo(fixedMsg.getBytes().length * 1024);

        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(0);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(0);

        mappedFile = mappedFileQueue.findMappedFileByOffset(100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(0);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(1024);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 + 100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(1024);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(1024 * 2);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2 + 100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getFileFromOffset()).isEqualTo(1024 * 2);

        // over mapped memory size.
        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4);
        assertThat(mappedFile).isNull();

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4 + 100);
        assertThat(mappedFile).isNull();

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testFindMappedFileByOffset_StartOffsetIsNonZero() {
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "b/", 1024, null);

        //Start from a non-zero offset
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile(1024);
        assertThat(mappedFile).isNotNull();

        assertThat(mappedFileQueue.findMappedFileByOffset(1025)).isEqualTo(mappedFile);

        assertThat(mappedFileQueue.findMappedFileByOffset(0)).isNull();
        assertThat(mappedFileQueue.findMappedFileByOffset(123, false)).isNull();
        assertThat(mappedFileQueue.findMappedFileByOffset(123, true)).isEqualTo(mappedFile);

        assertThat(mappedFileQueue.findMappedFileByOffset(0, false)).isNull();
        assertThat(mappedFileQueue.findMappedFileByOffset(0, true)).isEqualTo(mappedFile);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testAppendMessage() {
        final String fixedMsg = "0123456789abcdef";

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "c/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024);

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024 * 2);

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024 * 3);

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024 * 4);

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024 * 5);

        assertThat(mappedFileQueue.flush(0)).isFalse();
        assertThat(mappedFileQueue.getFlushedWhere()).isEqualTo(1024 * 6);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testGetMappedMemorySize() {
        final String fixedMsg = "abcd";

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "d/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }

        assertThat(mappedFileQueue.getMappedMemorySize()).isEqualTo(fixedMsg.length() * 1024);
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testDeleteExpiredFileByOffset() {
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "e/", 5120, null);

        for (int i = 0; i < 2048; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            ByteBuffer byteBuffer = ByteBuffer.allocate(ConsumeQueue.CQ_STORE_UNIT_SIZE);
            byteBuffer.putLong(i);
            byte[] padding = new byte[12];
            Arrays.fill(padding, (byte) '0');
            byteBuffer.put(padding);
            byteBuffer.flip();

            assertThat(mappedFile.appendMessage(byteBuffer.array())).isTrue();
        }

        MappedFile first = mappedFileQueue.getFirstMappedFile();
        first.hold();

        assertThat(mappedFileQueue.deleteExpiredFileByOffset(20480, ConsumeQueue.CQ_STORE_UNIT_SIZE)).isEqualTo(0);
        first.release();

        assertThat(mappedFileQueue.deleteExpiredFileByOffset(20480, ConsumeQueue.CQ_STORE_UNIT_SIZE)).isGreaterThan(0);
        first = mappedFileQueue.getFirstMappedFile();
        assertThat(first.getFileFromOffset()).isGreaterThan(0);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testDeleteExpiredFileByTime() throws Exception {
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "f/", 1024, null);

        for (int i = 0; i < 100; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            byte[] bytes = new byte[512];
            assertThat(mappedFile.appendMessage(bytes)).isTrue();
        }

        assertThat(mappedFileQueue.getMappedFiles().size()).isEqualTo(50);
        long expiredTime = 100 * 1000;
        for (int i = 0; i < mappedFileQueue.getMappedFiles().size(); i++) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mappedFileQueue.getMappedFiles().get(i);
            if (i < 5) {
                mappedFile.getFile().setLastModified(System.currentTimeMillis() - expiredTime * 2);
            }
            if (i > 20) {
                mappedFile.getFile().setLastModified(System.currentTimeMillis() - expiredTime * 2);
            }
        }
        int maxBatchDeleteFilesNum = 50;
        mappedFileQueue.deleteExpiredFileByTime(expiredTime, 0, 0, false, maxBatchDeleteFilesNum);
        assertThat(mappedFileQueue.getMappedFiles().size()).isEqualTo(45);
    }

    @Test
    public void testFindMappedFile_ByIteration() {
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "g/", 1024, null);
        for (int i = 0; i < 3; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(1024 * i);
            mappedFile.setWrotePosition(1024);
        }

        assertThat(mappedFileQueue.findMappedFileByOffset(1028).getFileFromOffset()).isEqualTo(1024);

        // Switch two MappedFiles and verify findMappedFileByOffset method
        MappedFile tmpFile = mappedFileQueue.getMappedFiles().get(1);
        mappedFileQueue.getMappedFiles().set(1, mappedFileQueue.getMappedFiles().get(2));
        mappedFileQueue.getMappedFiles().set(2, tmpFile);
        assertThat(mappedFileQueue.findMappedFileByOffset(1028).getFileFromOffset()).isEqualTo(1024);
    }

    @Test
    public void testMappedFile_SwapMap() {
        // four-byte string.
        final String fixedMsg = "abcdefgh";
        final int mappedFileSize = 102400;

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "b/", mappedFileSize, null);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3, 1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("testThreadPool"));

        for (int i = 0; i < mappedFileSize; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }
        assertThat(mappedFileQueue.getMappedMemorySize()).isEqualTo(fixedMsg.getBytes().length * mappedFileSize);

        AtomicBoolean readOver = new AtomicBoolean(false);
        AtomicBoolean hasException = new AtomicBoolean(false);

        executor.submit(() -> {
                try {
                    while (!readOver.get()) {
                        for (MappedFile mappedFile : mappedFileQueue.getMappedFiles()) {
                            mappedFile.swapMap();
                            Thread.sleep(10);
                            mappedFile.cleanSwapedMap(true);
                        }
                    }
                } catch (Throwable t) {
                    hasException.set(true);
                }
            }
        );
        long start = System.currentTimeMillis();
        long maxReadTimeMs = 60 * 1000;
        try {
            while (System.currentTimeMillis() - start <= maxReadTimeMs) {
                for (int i = 0; i < mappedFileSize && !readOver.get(); i++) {
                    MappedFile mappedFile = null;
                    int retryTime = 0;
                    while (mappedFile == null && retryTime < 10000) {
                        mappedFile = mappedFileQueue.findMappedFileByOffset(i * fixedMsg.getBytes().length);
                        retryTime++;
                        if (mappedFile == null) {
                            Thread.sleep(1);
                        }
                    }
                    assertThat(mappedFile != null).isTrue();
                    retryTime = 0;
                    int pos = (i * fixedMsg.getBytes().length) % mappedFileSize;
                    while ((pos + fixedMsg.getBytes().length) > mappedFile.getReadPosition() && retryTime < 10000) {
                        retryTime++;
                        if ((pos + fixedMsg.getBytes().length) > mappedFile.getReadPosition()) {
                            Thread.sleep(1);
                        }
                    }
                    assertThat((pos + fixedMsg.getBytes().length) <= mappedFile.getReadPosition()).isTrue();
                    SelectMappedBufferResult ret = mappedFile.selectMappedBuffer(pos, fixedMsg.getBytes().length);
                    byte[] readRes = new byte[fixedMsg.getBytes().length];
                    ret.getByteBuffer().get(readRes);
                    String readStr = new String(readRes, StandardCharsets.UTF_8);
                    assertThat(readStr.equals(fixedMsg)).isTrue();
                }
            }
            readOver.set(true);
        } catch (Throwable e) {
            hasException.set(true);
            readOver.set(true);
        }
        assertThat(readOver.get()).isTrue();
        assertThat(hasException.get()).isFalse();

    }

    @Test
    public void testMappedFile_CleanSwapedMap() throws InterruptedException {
        // four-byte string.
        final String fixedMsg = "abcd";
        final int mappedFileSize = 1024000;

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue(storePath + File.separator + "b/", mappedFileSize, null);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("testThreadPool"));
        for (int i = 0; i < mappedFileSize; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg.getBytes())).isTrue();
        }

        for (MappedFile mappedFile : mappedFileQueue.getMappedFiles()) {
            mappedFile.swapMap();
        }
        AtomicBoolean hasException = new AtomicBoolean(false);
        CountDownLatch downLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    for (MappedFile mappedFile : mappedFileQueue.getMappedFiles()) {
                        mappedFile.cleanSwapedMap(true);
                        mappedFile.cleanSwapedMap(true);
                    }
                } catch (Exception e) {
                    hasException.set(true);
                } finally {
                    downLatch.countDown();
                }
            });
        }

        downLatch.await(10, TimeUnit.SECONDS);
        assertThat(hasException.get()).isFalse();
    }

    @Test
    public void testMappedFile_Rename() throws IOException, InterruptedException {
        Assume.assumeFalse(MixAll.isWindows());
        final String fixedMsg = RandomStringUtils.randomAlphanumeric(128);
        final byte[] msgByteArr = fixedMsg.getBytes(StandardCharsets.UTF_8);
        final int mappedFileSize = 5 * 1024 * 1024;

        MappedFileQueue mappedFileQueue =
            new MappedFileQueue("target/unit_test_store", mappedFileSize, null);

        int currentSize = 0;
        while (currentSize <= 2 * mappedFileSize) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            mappedFile.appendMessage(msgByteArr);
            currentSize += fixedMsg.length();
        }

        assertThat(mappedFileQueue.getMappedFiles().size()).isEqualTo(3);

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleWithFixedDelay(() -> {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            mappedFile.appendMessage(msgByteArr);
        }, 1,1, TimeUnit.MILLISECONDS);

        List<MappedFile> mappedFileList = Lists.newArrayList(mappedFileQueue.getMappedFiles());
        mappedFileList.remove(mappedFileList.size() - 1);

        MappedFileQueue compactingMappedFileQueue =
            new MappedFileQueue("target/unit_test_store/compacting", mappedFileSize, null);

        currentSize = 0;
        while (currentSize < (2 * mappedFileSize - mappedFileSize / 2)) {
            MappedFile mappedFile = compactingMappedFileQueue.getLastMappedFile(0);
            mappedFile.appendMessage(msgByteArr);
            currentSize += fixedMsg.length();
        }


        mappedFileList.forEach(MappedFile::renameToDelete);
        assertThat(mappedFileQueue.getFirstMappedFile().getFileName()).endsWith(".delete");
        assertThat(mappedFileQueue.findMappedFileByOffset(mappedFileSize + fixedMsg.length()).getFileName()).endsWith(".delete");

        SelectMappedBufferResult sbr = mappedFileList.get(mappedFileList.size() - 1).selectMappedBuffer(0, msgByteArr.length);
        assertThat(sbr).isNotNull();
        try {
            assertThat(sbr.getMappedFile().getFileName().endsWith(".delete")).isTrue();
            if (sbr.getByteBuffer().hasArray()) {
                assertThat(sbr.getByteBuffer().array()).isEqualTo(msgByteArr);
            } else {
                for (int i = 0; i < msgByteArr.length; i++) {
                    assertThat(sbr.getByteBuffer().get(i)).isEqualTo(msgByteArr[i]);
                }
            }
        } finally {
            sbr.release();
        }


        compactingMappedFileQueue.getMappedFiles().forEach(mappedFile -> {
            try {
                mappedFile.moveToParent();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        mappedFileQueue.getMappedFiles().stream()
            .filter(m -> !mappedFileList.contains(m))
            .forEach(m -> compactingMappedFileQueue.getMappedFiles().add(m));

        int wrotePosition = mappedFileQueue.getLastMappedFile().getWrotePosition();

        mappedFileList.forEach(mappedFile -> {
            mappedFile.destroy(1000);
        });

        TimeUnit.SECONDS.sleep(3);
        ses.shutdown();

        mappedFileQueue.getMappedFiles().clear();
        mappedFileQueue.getMappedFiles().addAll(compactingMappedFileQueue.getMappedFiles());

        TimeUnit.SECONDS.sleep(3);
    }

    @After
    public void destroy() {
        File file = new File(storePath);
        UtilAll.deleteFile(file);
    }
}
