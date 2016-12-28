/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * $Id: MappedFileQueueTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import org.junit.*;

import static org.junit.Assert.*;


public class MappedFileQueueTest {
    private MappedFileQueue mappedFileQueue;

    // four-byte string.
    private static final String FIXED_MSG = "abcd";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        mappedFileQueue =
                new MappedFileQueue("target/unit_test_store/queue", 1024, null);
    }

    @After
    public void tearDown() throws Exception {
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    private void populateQueue() {
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertTrue(mappedFile != null);

            boolean result = mappedFile.appendMessage(FIXED_MSG.getBytes());
            assertTrue("appendMessage " + i, result);
        }

        assertEquals(FIXED_MSG.getBytes().length * 1024, mappedFileQueue.getMappedMemorySize());
    }

    @Test
    public void test_findMapedFileByOffset() {
        populateQueue();

        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(0);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);

        mappedFile = mappedFileQueue.findMappedFileByOffset(100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 2 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);

        // over mapped memory size.
        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4);
        assertTrue(mappedFile == null);

        mappedFile = mappedFileQueue.findMappedFileByOffset(1024 * 4 + 100);
        assertTrue(mappedFile == null);
    }

    @Test
    public void test_commit() {
        populateQueue();

        boolean result = mappedFileQueue.flush(0);
        assertTrue(result);
        assertEquals(1024 * 1, mappedFileQueue.getFlushOffset());

        result = mappedFileQueue.flush(0);
        assertTrue(result);
        assertEquals(1024 * 2, mappedFileQueue.getFlushOffset());

        result = mappedFileQueue.flush(0);
        assertTrue(result);
        assertEquals(1024 * 3, mappedFileQueue.getFlushOffset());

        result = mappedFileQueue.flush(0);
        assertTrue(result);
        assertEquals(1024 * 4, mappedFileQueue.getFlushOffset());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 4, mappedFileQueue.getFlushOffset());
    }
}
