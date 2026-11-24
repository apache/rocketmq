/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Covers the per-thread reusable scratch buffer used by
 * {@link CommitLog#checkMessageAndReturnSize} (via {@code borrowCheckMessageBuffer}):
 * reuse across messages, grow-only behaviour, the transient fallback for messages larger than
 * {@code maxCheckMessageReuseBufferSize}, and rejection of corrupt totalSize values.
 */
public class CheckMessageBufferReuseTest {

    private static final int REUSE_CAP = 64 * 1024;
    private static final int SMALL = 1024;
    private static final int LARGER = 8 * 1024;
    private static final int OVERSIZED = REUSE_CAP + 1;

    private static final String STORE_PATH =
        System.getProperty("java.io.tmpdir") + File.separator + "checkbufferreusetest-store";

    private CommitLog commitLog;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setMaxCheckMessageReuseBufferSize(REUSE_CAP);
        messageStoreConfig.setStorePathRootDir(STORE_PATH);
        messageStoreConfig.setStorePathCommitLog(STORE_PATH + File.separator + "commitlog");
        DefaultMessageStore messageStore =
            new DefaultMessageStore(messageStoreConfig, null, null, new BrokerConfig(), new ConcurrentHashMap<>());
        commitLog = new CommitLog(messageStore);
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(STORE_PATH));
    }

    @Test
    public void testBufferIsReusedForSameThread() {
        byte[] first = commitLog.borrowCheckMessageBuffer(SMALL);
        byte[] second = commitLog.borrowCheckMessageBuffer(SMALL);
        assertSame("the same thread must reuse the same scratch buffer", first, second);
        assertTrue(first.length >= SMALL);
    }

    @Test
    public void testBufferGrowsForLargerMessage() {
        byte[] small = commitLog.borrowCheckMessageBuffer(SMALL);
        byte[] grown = commitLog.borrowCheckMessageBuffer(LARGER);
        assertNotSame("a larger message must trigger a grown buffer", small, grown);
        assertTrue("grown buffer must fit the larger message", grown.length >= LARGER);
        // The grown buffer is retained and reused for subsequent smaller messages.
        byte[] again = commitLog.borrowCheckMessageBuffer(SMALL);
        assertSame("grown buffer must be reused for smaller messages", grown, again);
    }

    @Test
    public void testOversizedMessageUsesTransientBufferAndIsNotRetained() {
        byte[] reusable = commitLog.borrowCheckMessageBuffer(SMALL);

        byte[] oversized = commitLog.borrowCheckMessageBuffer(OVERSIZED);
        assertNotSame("oversized message must not use the reusable buffer", reusable, oversized);
        assertEquals("oversized transient buffer must fit exactly the requested size", OVERSIZED, oversized.length);

        // The oversized buffer must not be pinned in the ThreadLocal: a later small request still
        // returns the previously cached small buffer, not the oversized one.
        byte[] afterOversized = commitLog.borrowCheckMessageBuffer(SMALL);
        assertSame("oversized buffer must not be retained", reusable, afterOversized);
    }

    @Test
    public void testCorruptTotalSizeIsRejectedWithoutAllocating() {
        // Negative totalSize (corrupt length): must fail before any buffer allocation, no exception.
        ByteBuffer negative = ByteBuffer.allocate(16);
        negative.putInt(-1);
        negative.putInt(0);
        negative.putLong(0L);
        negative.flip();
        DispatchRequest negativeRequest = commitLog.checkMessageAndReturnSize(negative, false, false);
        assertFalse("negative totalSize must be rejected", negativeRequest.isSuccess());
        assertEquals(-1, negativeRequest.getMsgSize());

        // totalSize larger than the remaining bytes (truncated/corrupt): must also fail safely.
        ByteBuffer tooLarge = ByteBuffer.allocate(16);
        tooLarge.putInt(1_000_000);
        tooLarge.putInt(0);
        tooLarge.putLong(0L);
        tooLarge.flip();
        DispatchRequest tooLargeRequest = commitLog.checkMessageAndReturnSize(tooLarge, false, false);
        assertFalse("totalSize exceeding remaining bytes must be rejected", tooLargeRequest.isSuccess());
        assertEquals(-1, tooLargeRequest.getMsgSize());
    }
}
