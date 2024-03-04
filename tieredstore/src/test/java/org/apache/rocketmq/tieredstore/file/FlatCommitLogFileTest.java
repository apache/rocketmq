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
package org.apache.rocketmq.tieredstore.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatCommitLogFileTest {

    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageQueue queue;
    private MetadataStore metadataStore;
    private MessageStoreConfig storeConfig;
    private FlatFileFactory flatFileFactory;

    @Before
    public void init() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredStoreFilePath(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        storeConfig.setTieredStoreCommitLogMaxSize(2000L);
        storeConfig.setTieredStoreConsumeQueueMaxSize(2000L);
        queue = new MessageQueue("TieredFlatFileTest", storeConfig.getBrokerName(), 0);
        metadataStore = new DefaultMetadataStore(storeConfig);
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);
    }

    @After
    public void shutdown() throws IOException {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void constructTest() {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatAppendFile flatFile = flatFileFactory.createFlatFileForCommitLog(filePath);
        Assert.assertEquals(1L, flatFile.fileSegmentTable.size());
    }

    @Test
    public void tryRollingFileTest() throws InterruptedException {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatCommitLogFile flatFile = flatFileFactory.createFlatFileForCommitLog(filePath);
        for (int i = 0; i < 3; i++) {
            ByteBuffer byteBuffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            byteBuffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, i);
            Assert.assertEquals(AppendResult.SUCCESS, flatFile.append(byteBuffer, i));
            TimeUnit.MILLISECONDS.sleep(2);
            Assert.assertTrue(flatFile.tryRollingFile(1));
        }
        Assert.assertEquals(4, flatFile.fileSegmentTable.size());
        Assert.assertFalse(flatFile.tryRollingFile(1000));
        flatFile.destroy();
    }

    @Test
    public void getMinOffsetFromFileAsyncTest() {
        String filePath = MessageStoreUtil.toFilePath(queue);
        FlatCommitLogFile flatFile = flatFileFactory.createFlatFileForCommitLog(filePath);

        // append some messages
        for (int i = 6; i < 9; i++) {
            ByteBuffer byteBuffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            byteBuffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, i);
            Assert.assertEquals(AppendResult.SUCCESS, flatFile.append(byteBuffer, 1L));
        }
        Assert.assertEquals(-1L, flatFile.getMinOffsetFromFileAsync().join().longValue());

        // append some messages
        for (int i = 9; i < 30; i++) {
            ByteBuffer byteBuffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            byteBuffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, i);
            Assert.assertEquals(AppendResult.SUCCESS, flatFile.append(byteBuffer, 1L));
        }

        flatFile.commitAsync().join();
        Assert.assertEquals(6L, flatFile.getMinOffsetFromFile());
        Assert.assertEquals(6L, flatFile.getMinOffsetFromFileAsync().join().longValue());
    }
}