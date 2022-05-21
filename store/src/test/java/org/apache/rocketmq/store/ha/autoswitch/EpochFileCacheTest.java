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
import org.apache.rocketmq.common.EpochEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EpochFileCacheTest {
    private EpochFileCache epochCache;
    private EpochFileCache epochCache2;
    private String path;
    private String path2;

    @Before
    public void setup() {
        this.path = Paths.get(File.separator + "tmp", "EpochCheckpoint").toString();
        this.epochCache = new EpochFileCache(path);
        assertTrue(this.epochCache.appendEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochCache.appendEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochCache.appendEntry(new EpochEntry(3, 500)));
        final EpochEntry entry = this.epochCache.getEntry(2);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
    }

    @After
    public void shutdown() {
        new File(this.path).delete();
        if (this.path2 != null) {
            new File(this.path2).delete();
        }
    }

    @Test
    public void testInitFromFile() {
        // Remove entries, init from file
        assertTrue(this.epochCache.initCacheFromFile());
        final EpochEntry entry = this.epochCache.getEntry(2);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
    }

    @Test
    public void testTruncate() {
        this.epochCache.truncateSuffixByOffset(150);
        assertNotNull(this.epochCache.getEntry(1));
        assertNull(this.epochCache.getEntry(2));
    }

    @Test
    public void testFindEpochEntryByOffset() {
        final EpochEntry entry = this.epochCache.findEpochEntryByOffset(350);
        assertEquals(entry.getEpoch(), 2);
        assertEquals(entry.getStartOffset(), 300);
        assertEquals(entry.getEndOffset(), 500);
    }

    @Test
    public void testFindConsistentPointSample1() {
        this.path2 = Paths.get(File.separator + "tmp", "EpochCheckpoint2").toString();
        this.epochCache2 = new EpochFileCache(path2);
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(3, 450)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 450>
         *  The consistent point should be 450
         */
        final long consistentPoint = this.epochCache.findConsistentPoint(this.epochCache2);
        assertEquals(consistentPoint, 450);
    }

    @Test
    public void testFindConsistentPointSample2() {
        this.path2 = Paths.get(File.separator + "tmp", "EpochCheckpoint2").toString();
        this.epochCache2 = new EpochFileCache(path2);
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(3, 500)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 600>
         *  The consistent point should be 600
         */
        this.epochCache.setLastEpochEntryEndOffset(700);
        this.epochCache2.setLastEpochEntryEndOffset(600);
        final long consistentPoint = this.epochCache.findConsistentPoint(this.epochCache2);
        assertEquals(consistentPoint, 600);
    }

    @Test
    public void testFindConsistentPointSample3() {
        this.path2 = Paths.get(File.separator + "tmp", "EpochCheckpoint2").toString();
        this.epochCache2 = new EpochFileCache(path2);
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(1, 200)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(2, 500)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 200>, <Epoch2, 500>
         *  The consistent point should be -1
         */
        final long consistentPoint = this.epochCache.findConsistentPoint(this.epochCache2);
        assertEquals(consistentPoint, -1);
    }

    @Test
    public void testFindConsistentPointSample4() {
        this.path2 = Paths.get(File.separator + "tmp", "EpochCheckpoint2").toString();
        this.epochCache2 = new EpochFileCache(path2);
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(1, 100)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(2, 300)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(3, 500)));
        assertTrue(this.epochCache2.appendEntry(new EpochEntry(4, 800)));
        /**
         *  cache1: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500, 700>
         *  cache2: <Epoch1, 100>, <Epoch2, 300>, <Epoch3, 500>, <Epoch4, 800>
         *  The consistent point should be 700
         */
        this.epochCache.setLastEpochEntryEndOffset(700);
        final long consistentPoint = this.epochCache2.findConsistentPoint(this.epochCache);
        assertEquals(consistentPoint, 700);
    }
}