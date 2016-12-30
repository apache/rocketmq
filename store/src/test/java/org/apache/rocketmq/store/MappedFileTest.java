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
 * $Id: MappedFileTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store;

import java.io.IOException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class MappedFileTest {

    private static final Logger logger = LoggerFactory.getLogger(MappedFileTest.class);

    private static final String StoreMessage = "Once, there was a chance for me!";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void test_write_read() throws IOException {
        MappedFile mappedFile = new MappedFile("target/unit_test_store/MappedFileTest/000", 1024 * 64);
        boolean result = mappedFile.appendMessage(StoreMessage.getBytes());
        assertTrue(result);
        logger.debug("write OK");

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[StoreMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        logger.debug("Read: " + readString);
        assertTrue(readString.equals(StoreMessage));

        mappedFile.shutdown(1000);
        assertTrue(!mappedFile.isAvailable());
        selectMappedBufferResult.release();
        assertTrue(mappedFile.isCleanupOver());
        assertTrue(mappedFile.destroy(1000));
    }

    @Ignore
    public void test_jvm_crashed() throws IOException {
        MappedFile mappedFile = new MappedFile("target/unit_test_store/MappedFileTest/10086", 1024 * 64);
        boolean result = mappedFile.appendMessage(StoreMessage.getBytes());
        assertTrue(result);
        logger.debug("write OK");

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        selectMappedBufferResult.release();
        mappedFile.shutdown(1000);

        byte[] data = new byte[StoreMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);
        logger.debug(readString);
    }
}
