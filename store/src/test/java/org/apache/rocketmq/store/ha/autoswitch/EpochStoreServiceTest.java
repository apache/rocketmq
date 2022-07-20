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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EpochStoreServiceTest {

    private EpochStore epochStore;
    private EpochStore epochStore2;
    private String path;
    private String path2;

    @Before
    public void init() {
        this.path = Paths.get(System.getProperty("user.home"), "test", "epoch.ckpt").toString();
        this.path2 = Paths.get(System.getProperty("user.home"), "test", "epoch2.ckpt").toString();
        this.epochStore = new EpochStoreService(path);
        this.epochStore2 = new EpochStoreService(path2);
        assertTrue(this.epochStore.tryAppendEpochEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochStore.tryAppendEpochEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochStore.tryAppendEpochEntry(new EpochEntry(3, 500)));
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(path));
        UtilAll.deleteFile(new File(path2));
    }

    @Test
    public void testFilePersist() {
        this.epochStore = new EpochStoreService(path);
        assertTrue(this.epochStore.initStateFromFile());

        EpochEntry entry = this.epochStore.findEpochEntryByEpoch(2);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
    }

    @Test
    public void testInitFromEntry() {
        List<EpochEntry> entryList = this.epochStore.getAllEntries();
        assertEquals(3, entryList.size());

        this.epochStore2.initStateFromEntries(entryList);
        EpochEntry entry = this.epochStore.findEpochEntryByEpoch(2);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
        assertEquals(3, this.epochStore2.getAllEntries().size());
    }

    @Test
    public void testTruncate() {
        this.epochStore.truncateSuffixByOffset(150);
        assertNotNull(this.epochStore.findEpochEntryByEpoch(1));
        assertNull(this.epochStore.findEpochEntryByEpoch(2));
    }

    @Test
    public void testTryAppendEpochEntry() {
        this.epochStore.tryAppendEpochEntry(new EpochEntry(3, 600));
        assertEquals(3, this.epochStore.getAllEntries().size());
        this.epochStore.tryAppendEpochEntry(new EpochEntry(3, 700));
        assertEquals(3, this.epochStore.getAllEntries().size());
        this.epochStore.tryAppendEpochEntry(new EpochEntry(4, 800));
        assertEquals(4, this.epochStore.getAllEntries().size());
    }

    @Test
    public void testFindEpochEntryByOffset() {
        final EpochEntry entry = this.epochStore.findEpochEntryByOffset(350);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
    }

    @Test
    public void testFindConsistentPointSample1() {
        this.epochStore2 = new EpochStoreService(path2);
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(3, 450)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 450>
         *  The consistent point should be 450
         */
        final long consistentPoint = this.epochStore.findLastConsistentPoint(this.epochStore2);
        assertEquals(consistentPoint, 450);
    }

    private EpochStore setLastEpochEndOffset(EpochStore epochStore, long lastOffset) {
        List<EpochEntry> entries = epochStore.getAllEntries();
        entries.get(entries.size() - 1).setEndOffset(lastOffset);
        epochStore = new EpochStoreService();
        epochStore.initStateFromEntries(entries);
        return epochStore;
    }

    @Test
    public void testFindConsistentPointSample2() {
        this.epochStore2 = new EpochStoreService(path2);
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(3, 500)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 600>
         *  The consistent point should be 600
         */
        this.epochStore = setLastEpochEndOffset(epochStore, 700);
        this.epochStore2 = setLastEpochEndOffset(epochStore2, 600);
        final long consistentPoint = this.epochStore.findLastConsistentPoint(this.epochStore2);
        assertEquals(consistentPoint, 600);
    }

    @Test
    public void testFindConsistentPointSample3() {
        this.epochStore2 = new EpochStoreService(path2);
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(1, 200)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(2, 500)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 200>, <Epoch2, 500>
         *  The consistent point should be -1
         */
        final long consistentPoint = this.epochStore.findLastConsistentPoint(this.epochStore2);
        assertEquals(consistentPoint, -1);
    }

    @Test
    public void testFindConsistentPointSample4() {
        this.epochStore2 = new EpochStoreService(path2);
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(3, 500)));
        assertTrue(this.epochStore2.tryAppendEpochEntry(new EpochEntry(4, 800)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500>, <Epoch4, 800>
         *  The consistent point should be 700
         */
        this.epochStore = setLastEpochEndOffset(epochStore, 700);
        final long consistentPoint = this.epochStore2.findLastConsistentPoint(this.epochStore);
        assertEquals(consistentPoint, 700);
    }
}