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
package org.apache.rocketmq.broker.transaction.rocksdb;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.transaction.TransMessageRocksDBStore;
import org.apache.rocketmq.store.transaction.TransRocksDBRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalMessageRocksDBServiceNullReadRegressionTest {

    @Mock
    private MessageStore messageStore;
    @Mock
    private TransMessageRocksDBStore transMessageRocksDBStore;
    @Mock
    private MessageRocksDBStorage messageRocksDBStorage;
    @Mock
    private MessageStoreConfig messageStoreConfig;
    @Mock
    private BrokerController brokerController;
    @Mock
    private BrokerConfig brokerConfig;

    private TransactionalMessageRocksDBService service;

    @Before
    public void setUp() {
        when(messageStore.getTransMessageRocksDBStore()).thenReturn(transMessageRocksDBStore);
        when(transMessageRocksDBStore.getMessageRocksDBStorage()).thenReturn(messageRocksDBStorage);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStoreConfig.getMaxMessageSize()).thenReturn(4 * 1024 * 1024);
        when(messageStore.getMinPhyOffset()).thenReturn(100L);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerConfig.getTransactionCheckMax()).thenReturn(15);
        service = new TransactionalMessageRocksDBService(messageStore, brokerController);
    }

    @Test
    public void shouldKeepRecordWhenMessageReadFails() throws Exception {
        TransRocksDBRecord record = new TransRocksDBRecord(123L, "topicA", "uniqKeyA", 100, 0);
        when(transMessageRocksDBStore.getMessage(123L, 100)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        System.out.printf("[KEEP] offsetPy=%d sizePy=%d checkTimes=%d delete=%s%n",
            record.getOffsetPy(), record.getSizePy(), record.getCheckTimes(), record.isDelete());
        Assert.assertFalse("a transient read failure must not delete the half record", record.isDelete());
        Assert.assertEquals("a read failure must not consume check-back attempts", 0, record.getCheckTimes());
        verify(messageRocksDBStorage, never()).updateRecordsForTrans(any(), anyList());
    }

    @Test
    public void shouldKeepRecordWhenOffsetEqualsMinPhyOffset() throws Exception {
        TransRocksDBRecord record = new TransRocksDBRecord(100L, "topicA", "uniqKeyBoundary", 100, 0);
        when(transMessageRocksDBStore.getMessage(100L, 100)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        Assert.assertFalse("offset == minPhyOffset is still within the retained commitLog", record.isDelete());
    }

    @Test
    public void shouldDeleteExpiredRecordWhenMessageReadFails() throws Exception {
        TransRocksDBRecord record = new TransRocksDBRecord(50L, "topicA", "uniqKeyB", 100, 0);
        when(transMessageRocksDBStore.getMessage(50L, 100)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        ArgumentCaptor<List<TransRocksDBRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageRocksDBStorage).updateRecordsForTrans(eq(MessageRocksDBStorage.TRANS_COLUMN_FAMILY),
            captor.capture());
        Assert.assertEquals(1, captor.getValue().size());
        Assert.assertTrue(captor.getValue().get(0).isDelete());
    }

    @Test
    public void shouldDeleteInvalidRecordWhenMessageReadFails() throws Exception {
        TransRocksDBRecord record = new TransRocksDBRecord(123L, "topicA", "uniqKeyC", 0, 0);
        when(transMessageRocksDBStore.getMessage(123L, 0)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        ArgumentCaptor<List<TransRocksDBRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageRocksDBStorage).updateRecordsForTrans(eq(MessageRocksDBStorage.TRANS_COLUMN_FAMILY),
            captor.capture());
        Assert.assertEquals(1, captor.getValue().size());
        Assert.assertTrue(captor.getValue().get(0).isDelete());
    }

    @Test
    public void shouldDeleteRecordWithNegativeOffsetWhenMessageReadFails() throws Exception {
        TransRocksDBRecord record = new TransRocksDBRecord(-1L, "topicA", "uniqKeyNeg", 100, 0);
        when(transMessageRocksDBStore.getMessage(-1L, 100)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        ArgumentCaptor<List<TransRocksDBRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageRocksDBStorage).updateRecordsForTrans(eq(MessageRocksDBStorage.TRANS_COLUMN_FAMILY),
            captor.capture());
        Assert.assertEquals(1, captor.getValue().size());
        Assert.assertTrue(captor.getValue().get(0).isDelete());
    }

    @Test
    public void shouldDeleteRecordWithOversizedSizeWhenMessageReadFails() throws Exception {
        int oversized = 4 * 1024 * 1024 + 1;
        TransRocksDBRecord record = new TransRocksDBRecord(123L, "topicA", "uniqKeyBig", oversized, 0);
        when(transMessageRocksDBStore.getMessage(123L, oversized)).thenReturn(null);

        invokeCheckTransRecordsStatus(record);

        ArgumentCaptor<List<TransRocksDBRecord>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageRocksDBStorage).updateRecordsForTrans(eq(MessageRocksDBStorage.TRANS_COLUMN_FAMILY),
            captor.capture());
        Assert.assertEquals(1, captor.getValue().size());
        Assert.assertTrue(captor.getValue().get(0).isDelete());
    }

    private void invokeCheckTransRecordsStatus(TransRocksDBRecord record) throws Exception {
        Method method = TransactionalMessageRocksDBService.class
            .getDeclaredMethod("checkTransRecordsStatus", List.class);
        method.setAccessible(true);
        method.invoke(service, Collections.singletonList(record));
    }
}