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
package org.apache.rocketmq.broker.client.rebalance;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RebalanceLockManagerTest {
    
    @Mock
    private RebalanceLockManager.LockEntry lockEntry;
    
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    
    private final String defaultTopic = "defaultTopic";
    
    private final String defaultBroker = "defaultBroker";
    
    private final String defaultGroup = "defaultGroup";
    
    private final String defaultClientId = "defaultClientId";
    
    @Test
    public void testIsLockAllExpiredGroupNotExist() {
        assertTrue(rebalanceLockManager.isLockAllExpired(defaultGroup));
    }
    
    @Test
    public void testIsLockAllExpiredGroupExist() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        when(lockEntry.isExpired()).thenReturn(false);
        assertFalse(rebalanceLockManager.isLockAllExpired(defaultGroup));
    }
    
    @Test
    public void testIsLockAllExpiredGroupExistSomeExpired() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        when(lockEntry.isExpired()).thenReturn(true).thenReturn(false);
        assertFalse(rebalanceLockManager.isLockAllExpired(defaultGroup));
    }
    
    @Test
    public void testTryLockNotLocked() {
        assertTrue(rebalanceLockManager.tryLock(defaultGroup, createDefaultMessageQueue(), defaultClientId));
    }
    
    @Test
    public void testTryLockSameClient() throws IllegalAccessException {
        when(lockEntry.isLocked(defaultClientId)).thenReturn(true);
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        assertTrue(rebalanceLockManager.tryLock(defaultGroup, createDefaultMessageQueue(), defaultClientId));
    }
    
    @Test
    public void testTryLockDifferentClient() throws Exception {
        when(lockEntry.isLocked(defaultClientId)).thenReturn(false);
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        assertFalse(rebalanceLockManager.tryLock(defaultGroup, createDefaultMessageQueue(), defaultClientId));
    }
    
    @Test
    public void testTryLockButExpired() throws IllegalAccessException {
        when(lockEntry.isExpired()).thenReturn(true);
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        assertTrue(rebalanceLockManager.tryLock(defaultGroup, createDefaultMessageQueue(), defaultClientId));
    }
    
    @Test
    public void testTryLockBatchAllLocked() {
        Set<MessageQueue> mqs = createMessageQueue(2);
        Set<MessageQueue> actual = rebalanceLockManager.tryLockBatch(defaultGroup, mqs, defaultClientId);
        assertEquals(mqs, actual);
    }
    
    @Test
    public void testTryLockBatchNoneLocked() throws IllegalAccessException {
        when(lockEntry.isLocked(defaultClientId)).thenReturn(false);
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        Set<MessageQueue> actual = rebalanceLockManager.tryLockBatch(defaultGroup, createMessageQueue(2), defaultClientId);
        assertTrue(actual.isEmpty());
    }
    
    @Test
    public void testTryLockBatchSomeLocked() throws IllegalAccessException {
        Set<MessageQueue> mqs = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(defaultTopic, defaultBroker, 0);
        MessageQueue mq2 = new MessageQueue(defaultTopic, defaultBroker, 1);
        mqs.add(mq1);
        mqs.add(mq2);
        when(lockEntry.isLocked(defaultClientId)).thenReturn(true).thenReturn(false);
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", createMQLockTable(), true);
        Set<MessageQueue> actual = rebalanceLockManager.tryLockBatch(defaultGroup, mqs, defaultClientId);
        Set<MessageQueue> expected = new HashSet<>();
        expected.add(mq2);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testUnlockBatch() throws IllegalAccessException {
        when(lockEntry.getClientId()).thenReturn(defaultClientId);
        ConcurrentMap<String, ConcurrentHashMap<MessageQueue, RebalanceLockManager.LockEntry>> mqLockTable = createMQLockTable();
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", mqLockTable, true);
        rebalanceLockManager.unlockBatch(defaultGroup, createMessageQueue(1), defaultClientId);
        assertEquals(1, mqLockTable.get(defaultGroup).values().size());
    }
    
    @Test
    public void testUnlockBatchByOtherClient() throws IllegalAccessException {
        when(lockEntry.getClientId()).thenReturn("otherClientId");
        ConcurrentMap<String, ConcurrentHashMap<MessageQueue, RebalanceLockManager.LockEntry>> mqLockTable = createMQLockTable();
        FieldUtils.writeDeclaredField(rebalanceLockManager, "mqLockTable", mqLockTable, true);
        rebalanceLockManager.unlockBatch(defaultGroup, createMessageQueue(1), defaultClientId);
        assertEquals(2, mqLockTable.get(defaultGroup).values().size());
    }
    
    private MessageQueue createDefaultMessageQueue() {
        return createMessageQueue(1).iterator().next();
    }
    
    private Set<MessageQueue> createMessageQueue(final int count) {
        Set<MessageQueue> result = new HashSet<>();
        for (int i = 0; i < count; i++) {
            result.add(new MessageQueue(defaultTopic, defaultBroker, i));
        }
        return result;
    }
    
    private ConcurrentMap<String, ConcurrentHashMap<MessageQueue, RebalanceLockManager.LockEntry>> createMQLockTable() {
        MessageQueue messageQueue1 = new MessageQueue(defaultTopic, defaultBroker, 0);
        MessageQueue messageQueue2 = new MessageQueue(defaultTopic, defaultBroker, 1);
        ConcurrentHashMap<MessageQueue, RebalanceLockManager.LockEntry> lockEntryMap = new ConcurrentHashMap<>();
        lockEntryMap.put(messageQueue1, lockEntry);
        lockEntryMap.put(messageQueue2, lockEntry);
        ConcurrentMap<String, ConcurrentHashMap<MessageQueue, RebalanceLockManager.LockEntry>> result = new ConcurrentHashMap<>();
        result.put(defaultGroup, lockEntryMap);
        return result;
    }
}
