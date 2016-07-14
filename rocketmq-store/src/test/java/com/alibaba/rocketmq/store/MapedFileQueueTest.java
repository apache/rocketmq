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
 * $Id: MapedFileQueueTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import org.junit.*;

import static org.junit.Assert.*;


public class MapedFileQueueTest {

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
        AllocateMapedFileService allocateMapedFileService = new AllocateMapedFileService(null);
        allocateMapedFileService.start();
        MapedFileQueue mapedFileQueue =
                new MapedFileQueue("./unit_test_store/a/", 1024, allocateMapedFileService);

        for (int i = 0; i < 1024; i++) {
            MapedFile mapedFile = mapedFileQueue.getLastMapedFile();
            assertTrue(mapedFile != null);
            boolean result = mapedFile.appendMessage(fixedMsg.getBytes());
            if (!result) {
                System.out.println("appendMessage " + i);
            }
            assertTrue(result);
        }

        mapedFileQueue.shutdown(1000);
        mapedFileQueue.destroy();
        allocateMapedFileService.shutdown();
        System.out.println("MapedFileQueue.getLastMapedFile() OK");
    }


    @Test
    public void test_findMapedFileByOffset() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMapedFileService allocateMapedFileService = new AllocateMapedFileService(null);
        allocateMapedFileService.start();
        MapedFileQueue mapedFileQueue =
                new MapedFileQueue("./unit_test_store/b/", 1024, allocateMapedFileService);

        for (int i = 0; i < 1024; i++) {
            MapedFile mapedFile = mapedFileQueue.getLastMapedFile();
            assertTrue(mapedFile != null);
            boolean result = mapedFile.appendMessage(fixedMsg.getBytes());
            // System.out.println("appendMessage " + bytes);
            assertTrue(result);
        }

        MapedFile mapedFile = mapedFileQueue.findMapedFileByOffset(0);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 0);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(100);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 0);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 1024);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024 + 100);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 1024);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024 * 2);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024 * 2 + 100);
        assertTrue(mapedFile != null);
        assertEquals(mapedFile.getFileFromOffset(), 1024 * 2);
        System.out.println(mapedFile.getFileFromOffset());

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024 * 4);
        assertTrue(mapedFile == null);

        mapedFile = mapedFileQueue.findMapedFileByOffset(1024 * 4 + 100);
        assertTrue(mapedFile == null);

        mapedFileQueue.shutdown(1000);
        mapedFileQueue.destroy();
        allocateMapedFileService.shutdown();
        System.out.println("MapedFileQueue.findMapedFileByOffset() OK");
    }


    @Test
    public void test_commit() {
        final String fixedMsg = "0123456789abcdef";
        System.out.println("================================================================");
        AllocateMapedFileService allocateMapedFileService = new AllocateMapedFileService(null);
        allocateMapedFileService.start();
        MapedFileQueue mapedFileQueue =
                new MapedFileQueue("./unit_test_store/c/", 1024, allocateMapedFileService);

        for (int i = 0; i < 1024; i++) {
            MapedFile mapedFile = mapedFileQueue.getLastMapedFile();
            assertTrue(mapedFile != null);
            boolean result = mapedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }


        boolean result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 1, mapedFileQueue.getCommittedWhere());
        System.out.println("1 " + result + " " + mapedFileQueue.getCommittedWhere());

        result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 2, mapedFileQueue.getCommittedWhere());
        System.out.println("2 " + result + " " + mapedFileQueue.getCommittedWhere());

        result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 3, mapedFileQueue.getCommittedWhere());
        System.out.println("3 " + result + " " + mapedFileQueue.getCommittedWhere());

        result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 4, mapedFileQueue.getCommittedWhere());
        System.out.println("4 " + result + " " + mapedFileQueue.getCommittedWhere());

        result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 5, mapedFileQueue.getCommittedWhere());
        System.out.println("5 " + result + " " + mapedFileQueue.getCommittedWhere());

        result = mapedFileQueue.commit(0);
        assertFalse(result);
        assertEquals(1024 * 6, mapedFileQueue.getCommittedWhere());
        System.out.println("6 " + result + " " + mapedFileQueue.getCommittedWhere());

        mapedFileQueue.shutdown(1000);
        mapedFileQueue.destroy();
        allocateMapedFileService.shutdown();
        System.out.println("MapedFileQueue.commit() OK");
    }


    @Test
    public void test_getMapedMemorySize() {
        final String fixedMsg = "abcd";
        System.out.println("================================================================");
        AllocateMapedFileService allocateMapedFileService = new AllocateMapedFileService(null);
        allocateMapedFileService.start();
        MapedFileQueue mapedFileQueue =
                new MapedFileQueue("./unit_test_store/d/", 1024, allocateMapedFileService);

        for (int i = 0; i < 1024; i++) {
            MapedFile mapedFile = mapedFileQueue.getLastMapedFile();
            assertTrue(mapedFile != null);
            boolean result = mapedFile.appendMessage(fixedMsg.getBytes());
            assertTrue(result);
        }

        assertEquals(fixedMsg.length() * 1024, mapedFileQueue.getMapedMemorySize());

        mapedFileQueue.shutdown(1000);
        mapedFileQueue.destroy();
        allocateMapedFileService.shutdown();
        System.out.println("MapedFileQueue.getMapedMemorySize() OK");
    }

}
