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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.Buffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.AbstractMappedFile;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedFileTest {
    private final String storeMessage = "Once, there was a chance for me!";

    @Test
    public void testSelectMappedBuffer() throws IOException {
        DefaultMappedFile mappedFile = new DefaultMappedFile("target/unit_test_store/MappedFileTest/000", 1024 * 64);
        boolean result = mappedFile.appendMessage(storeMessage.getBytes());
        assertThat(result).isTrue();

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        assertThat(readString).isEqualTo(storeMessage);

        mappedFile.shutdown(1000);
        assertThat(mappedFile.isAvailable()).isFalse();
        selectMappedBufferResult.release();
        assertThat(mappedFile.isCleanupOver()).isTrue();
        assertThat(mappedFile.destroy(1000)).isTrue();
    }

    @Test
    public void testMappedReopen() throws IOException {
        AbstractMappedFile mappedFile = new DefaultMappedFile("target/unit_test_store/MappedFileTest/001", 1024 * 64);
        boolean result = mappedFile.appendMessage(storeMessage.getBytes());
        assertThat(result).isTrue();
        for (int i = 0; i < 10000; i++) {
            if (mappedFile.isFull()) {
                break;
            }
            result = mappedFile.appendMessage(storeMessage.getBytes());
            assertThat(result).isTrue();
        }

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        assertThat(readString).isEqualTo(storeMessage);

        final int inactiveMs = 1000 * 10;
        boolean reopened = mappedFile.checkInactiveAndRefresh(false, inactiveMs);
        assertThat(reopened).isFalse();

        // some one reading should not reopen
        reopened = mappedFile.checkInactiveAndRefresh(true, inactiveMs);
        assertThat(reopened).isFalse();
        assertThat(mappedFile.isAvailable()).isTrue();
        selectMappedBufferResult.release();

        reopened = mappedFile.checkInactiveAndRefresh(true, inactiveMs);
        assertThat(reopened).isTrue();

        // reading should ok after reopen and during reopen some read may failed 
        selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        readString = new String(data);
        assertThat(readString).isEqualTo(storeMessage);
        selectMappedBufferResult.release();

        ByteBuffer tmp = ByteBuffer.allocate(storeMessage.length());
        ((Buffer)tmp).position(0);
        tmp.limit(storeMessage.length());
        mappedFile.getData(0, storeMessage.length(), tmp);
        tmp.flip();
        data = new byte[storeMessage.length()];
        tmp.get(data);
        readString = new String(data);
        assertThat(readString).isEqualTo(storeMessage);
        AtomicInteger reopendCnt = new AtomicInteger(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    boolean reopened = mappedFile.checkInactiveAndRefresh(true, inactiveMs);
                    if (reopened) {
                        reopendCnt.incrementAndGet();
                    }
                    try {
                        Thread.sleep(1);
                    } catch (Exception e) {
                    }
                }
            }
        });
        t.start();
        int count = 0;
        for (int i = 0; i < 5000; i++) {
            selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
            if (selectMappedBufferResult == null) {
                count++;
                continue;
            }
            data = new byte[storeMessage.length()];
            selectMappedBufferResult.getByteBuffer().get(data);
            readString = new String(data);
            assertThat(readString).isEqualTo(storeMessage);
            selectMappedBufferResult.release();
            try {
                Thread.sleep(1);
            } catch (Exception e) {
            }
        }
        try {
            t.join();
        } catch (Exception e) {
        }
        assertThat(reopendCnt.get() > 0).isTrue();
        assertThat(reopendCnt.get() < 49999).isTrue();
        assertThat(count == 0).isTrue();
        // shutdown or destroy should not reopen
        mappedFile.shutdown(1000);

        reopened = mappedFile.checkInactiveAndRefresh(true, inactiveMs);
        assertThat(reopened).isFalse();

        assertThat(mappedFile.isCleanupOver()).isTrue();
        assertThat(mappedFile.destroy(1000)).isTrue();

        reopened = mappedFile.checkInactiveAndRefresh(true, inactiveMs);
        assertThat(reopened).isFalse();
    }

    @After
    public void destroy() {
        File file = new File("target/unit_test_store");
        UtilAll.deleteFile(file);
    }
}
