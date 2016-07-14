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
 * $Id: MapedFileTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;


public class MapedFileTest {

    private static final String StoreMessage = "Once, there was a chance for me!";


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }


    @Test
    public void test_write_read() {
        try {
            MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/000", 1024 * 64);
            boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
            assertTrue(result);
            System.out.println("write OK");

            SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
            byte[] data = new byte[StoreMessage.length()];
            selectMapedBufferResult.getByteBuffer().get(data);
            String readString = new String(data);

            System.out.println("Read: " + readString);
            assertTrue(readString.equals(StoreMessage));


            mapedFile.shutdown(1000);


            assertTrue(!mapedFile.isAvailable());

            selectMapedBufferResult.release();


            assertTrue(mapedFile.isCleanupOver());


            assertTrue(mapedFile.destroy(1000));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Ignore
    public void test_jvm_crashed() {
        try {
            MapedFile mapedFile = new MapedFile("./unit_test_store/MapedFileTest/10086", 1024 * 64);
            boolean result = mapedFile.appendMessage(StoreMessage.getBytes());
            assertTrue(result);
            System.out.println("write OK");

            SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
            selectMapedBufferResult.release();
            mapedFile.shutdown(1000);

            byte[] data = new byte[StoreMessage.length()];
            selectMapedBufferResult.getByteBuffer().get(data);
            String readString = new String(data);
            System.out.println(readString);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
