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
package org.apache.rocketmq.store.kv;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CompactionPositionMgrTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    File file;

    @Before
    public void setUp() throws IOException  {
        file = tmpFolder.newFolder("compaction");
    }

    @Test
    public void testGetAndSet() {
        CompactionPositionMgr mgr = new CompactionPositionMgr(file.getAbsolutePath());
        mgr.setOffset("topic1", 1, 1);
        assertEquals(1, mgr.getOffset("topic1", 1));
        mgr.setOffset("topic1", 1, 2);
        assertEquals(2, mgr.getOffset("topic1", 1));
        mgr.setOffset("topic1", 2, 1);
        assertEquals(1, mgr.getOffset("topic1", 2));
    }

    @Test
    public void testLoadAndPersist() throws IOException {
        CompactionPositionMgr mgr = new CompactionPositionMgr(file.getAbsolutePath());
        mgr.setOffset("topic1", 1, 2);
        mgr.setOffset("topic1", 2, 1);
        mgr.persist();
        mgr = null;

        CompactionPositionMgr mgr2 = new CompactionPositionMgr(file.getAbsolutePath());
        mgr2.load();
        assertEquals(2, mgr2.getOffset("topic1", 1));
        assertEquals(1, mgr2.getOffset("topic1", 2));
    }
}