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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.LmqDispatch;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueTable.CQ_UNIT_SIZE;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBConsumeQueueTest extends QueueTestBase {

    private MessageStore messageStore;
    private ConcurrentMap<String, TopicConfig> topicConfigTableMap;

    @Before
    public void init() throws Exception {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
        storeConfig.setEnableCompaction(false);
        this.topicConfigTableMap = new ConcurrentHashMap<>();

        messageStore = createMessageStore(null, true, topicConfigTableMap, storeConfig);
        messageStore.load();
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

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

    @Test
    public void testLmqCounter_running() throws ConsumeQueueException {
        messageStore.getMessageStoreConfig().setEnableMultiDispatch(true);
        messageStore.getMessageStoreConfig().setEnableLmq(true);
        int num = 5;
        String topic = "topic";
        List<String> lmqNameList = IntStream.range(0, num)
            .mapToObj(i -> MixAll.LMQ_PREFIX + UUID.randomUUID())
            .collect(java.util.stream.Collectors.toList());
        assertEquals(0, messageStore.getQueueStore().getLmqNum());

        lmqNameList.forEach(lmqName -> assertNotNull(messageStore.getConsumeQueue(lmqName, 0))); // create if not exist
        assertEquals(0, messageStore.getQueueStore().getLmqNum());

        for (String lmqName : lmqNameList) {
            MessageExtBrokerInner message = buildMessage(topic, -1);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
            LmqDispatch.wrapLmqDispatch(messageStore, message);
            PutMessageResult putMessageResult = messageStore.putMessage(message);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));

        lmqNameList.forEach(lmqName -> assertNotNull(messageStore.getConsumeQueue(lmqName, 0)));
        assertEquals(num, messageStore.getQueueStore().getLmqNum());

        lmqNameList.forEach(lmqName -> messageStore.deleteTopics(Sets.newHashSet(lmqName)));
        assertEquals(0, messageStore.getQueueStore().getLmqNum());
    }

    @Test
    public void testLmqCounter_reload() throws Exception {
        messageStore.getMessageStoreConfig().setEnableMultiDispatch(true);
        messageStore.getMessageStoreConfig().setEnableLmq(true);
        int num = 5;
        String topic = "topic";
        List<String> lmqNameList = IntStream.range(0, num)
            .mapToObj(i -> MixAll.LMQ_PREFIX + UUID.randomUUID())
            .collect(java.util.stream.Collectors.toList());
        assertEquals(0, messageStore.getQueueStore().getLmqNum());

        for (String lmqName : lmqNameList) {
            MessageExtBrokerInner message = buildMessage(topic, -1);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
            LmqDispatch.wrapLmqDispatch(messageStore, message);
            PutMessageResult putMessageResult = messageStore.putMessage(message);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        await().atMost(5, SECONDS).until(fullyDispatched(messageStore));
        assertEquals(num, messageStore.getQueueStore().getLmqNum());
        messageStore.shutdown();

        // create new one based on current store
        MessageStore newStore = createMessageStore(messageStore.getMessageStoreConfig().getStorePathRootDir(),
            true, topicConfigTableMap, messageStore.getMessageStoreConfig());
        newStore.load();
        newStore.start();

        assertEquals(num, newStore.getQueueStore().getLmqNum());
        lmqNameList.forEach(lmqName -> assertNull(newStore.getQueueStore().getConsumeQueueTable().get(lmqName))); // not in consumeQueueTable
        newStore.shutdown();
    }
}