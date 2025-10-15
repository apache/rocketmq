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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueTable.CQ_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBConsumeQueueTest extends QueueTestBase {

    @Test
    public void testIterator() throws Exception {
        if (MixAll.isMac()) {
            return;
        }
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        RocksDBConsumeQueueStore rocksDBConsumeQueueStore = mock(RocksDBConsumeQueueStore.class);
        when(messageStore.getQueueStore()).thenReturn(rocksDBConsumeQueueStore);
        when(rocksDBConsumeQueueStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(10000L);
        when(rocksDBConsumeQueueStore.get(anyString(), anyInt(), anyLong())).then(new Answer<ByteBuffer>() {
            @Override
            public ByteBuffer answer(InvocationOnMock mock) throws Throwable {
                long startIndex = mock.getArgument(2);
                final ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_UNIT_SIZE);
                long phyOffset = startIndex * 10;
                byteBuffer.putLong(phyOffset);
                byteBuffer.putInt(1);
                byteBuffer.putLong(0);
                byteBuffer.putLong(0);
                byteBuffer.flip();
                return byteBuffer;
            }
        });

        RocksDBConsumeQueue consumeQueue = new RocksDBConsumeQueue(messageStore.getMessageStoreConfig(), rocksDBConsumeQueueStore, "topic", 0);
        ReferredIterator<CqUnit> it = consumeQueue.iterateFrom(9000);
        for (int i = 0; i < 1000; i++) {
            assertTrue(it.hasNext());
            CqUnit next = it.next();
            assertEquals(9000 + i, next.getQueueOffset());
            assertEquals(10 * (9000 + i), next.getPos());
        }
        assertFalse(it.hasNext());
    }
}