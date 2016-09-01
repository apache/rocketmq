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
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/a/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMapedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            if (!result) {
                System.out.println("appendMessage " + i);
            }
            assertTrue(result);
        }

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.getLastMapedFile() OK");
    }


    @Test
    public void test_findMapedFileByOffset() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/b/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMapedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            // System.out.println("appendMessage " + bytes);
            assertTrue(result);
        }

        MappedFile mappedFile = mappedFileQueue.findMapedFileByOffset(0);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 0);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024 * 2);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024 * 2 + 100);
        assertTrue(mappedFile != null);
        assertEquals(mappedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mappedFile.getFileFromOffset());

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024 * 4);
        assertTrue(mappedFile == null);

        mappedFile = mappedFileQueue.findMapedFileByOffset(1024 * 4 + 100);
        assertTrue(mappedFile == null);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.findMapedFileByOffset() OK");
    }


    @Test
    public void test_commit() {
        final String fixedMsg = "0123456789abcdef";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/c/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMapedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }


        boolean result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 1, mappedFileQueue.getFlushedWhere());
        System.out.println("1 " + result + " " + mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 2, mappedFileQueue.getFlushedWhere());
        System.out.println("2 " + result + " " + mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 3, mappedFileQueue.getFlushedWhere());
        System.out.println("3 " + result + " " + mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 4, mappedFileQueue.getFlushedWhere());
        System.out.println("4 " + result + " " + mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 5, mappedFileQueue.getFlushedWhere());
        System.out.println("5 " + result + " " + mappedFileQueue.getFlushedWhere());

        result = mappedFileQueue.flush(0);
        assertFalse(result);
        assertEquals(1024 * 6, mappedFileQueue.getFlushedWhere());
        System.out.println("6 " + result + " " + mappedFileQueue.getFlushedWhere());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.flush() OK");
    }


    @Test
    public void test_getMapedMemorySize() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMappedFileService allocateMappedFileService = new AllocateMappedFileService(null);
        allocateMappedFileService.start();
        MappedFileQueue mappedFileQueue =
                new MappedFileQueue("./unit_test_store/d/", 1024, allocateMappedFileService);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMapedFile();
            assertTrue(mappedFile != null);
            boolean result = mappedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        assertEquals(fixedMsg.length() * 1024, mappedFileQueue.getMapedMemorySize());

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
        allocateMappedFileService.shutdown();
        System.out.println("MappedFileQueue.getMapedMemorySize() OK");
    }

}
