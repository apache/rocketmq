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

/**
 * $Id: MappedFileQueueTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MappedFileQueueTest {
    private static final Logger logger = LoggerFactory.getLogger(MappedFileQueueTest.class);

    // private static final String StoreMessage =
    // "Once, there was a chance for me! but I did not treasure it. if";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test_getLastMapedFile() {
        final String fixedMsg = "0123456789abcdef";

        logger.debug("================================================================");
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue("target/unit_test_store/a/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertTrue(mappedFile != null);

            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            if (!result) {
                logger.debug("appendMessage " + i);
            }
            assertTrue(result);
        }

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        logger.debug("MappedFileQueue.getLastMappedFile() OK");
    }

    @Test
    public void test_findMapedFileByOffset() {
        // four-byte string.
        final String fixedMsg = "abcd";

        logger.debug("================================================================");
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue("target/unit_test_store/b/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertTrue(mappedFile != null);

            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        assertEquals(fixedMsg.getBytes().length * 1024, mappedFileQueue.getMappedMemorySize());

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

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        logger.debug("MappedFileQueue.findMappedFileByOffset() OK");
    }

    @Test
    public void test_commit() {
        final String fixedMsg = "0123456789abcdef";

        logger.debug("================================================================");
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue("target/unit_test_store/c/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertTrue(mappedFile != null);

            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        boolean result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 1, mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 2, mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 3, mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 4, mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 5, mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 6, mappedFileQueue.getFlushedWhere());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        logger.debug("MappedFileQueue.flush() OK");
    }

    @Test
    public void test_getMapedMemorySize() {
        final String fixedMsg = "abcd";

        logger.debug("================================================================");
        MappedFileQueue mappedFileQueue =
            new MappedFileQueue("target/unit_test_store/d/", 1024, null);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            assertTrue(mappedFile != null);

            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        assertEquals(fixedMsg.length() * 1024, mappedFileQueue.getMappedMemorySize());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        logger.debug("MappedFileQueue.getMappedMemorySize() OK");
    }
}
