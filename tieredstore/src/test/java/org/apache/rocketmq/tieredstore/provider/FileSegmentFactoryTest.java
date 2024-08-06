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
package org.apache.rocketmq.tieredstore.provider;

import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.Assert;
import org.junit.Test;

public class FileSegmentFactoryTest {

    @Test
    public void fileSegmentInstanceTest() throws ClassNotFoundException, NoSuchMethodException {
        int baseOffset = 1000;
        String filePath = "FileSegmentFactoryPath";
        String storePath = MessageStoreUtilTest.getRandomStorePath();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setTieredStoreCommitLogMaxSize(1024);
        storeConfig.setTieredStoreFilePath(storePath);
        MessageStoreExecutor executor = new MessageStoreExecutor();

        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FileSegmentFactory factory = new FileSegmentFactory(metadataStore, storeConfig, executor);

        Assert.assertEquals(metadataStore, factory.getMetadataStore());
        Assert.assertEquals(storeConfig, factory.getStoreConfig());

        FileSegment fileSegment = factory.createCommitLogFileSegment(filePath, baseOffset);
        Assert.assertEquals(1000, fileSegment.getBaseOffset());
        Assert.assertEquals(FileSegmentType.COMMIT_LOG, fileSegment.getFileType());
        fileSegment.destroyFile();

        fileSegment = factory.createConsumeQueueFileSegment(filePath, baseOffset);
        Assert.assertEquals(1000, fileSegment.getBaseOffset());
        Assert.assertEquals(FileSegmentType.CONSUME_QUEUE, fileSegment.getFileType());
        fileSegment.destroyFile();

        fileSegment = factory.createIndexServiceFileSegment(filePath, baseOffset);
        Assert.assertEquals(1000, fileSegment.getBaseOffset());
        Assert.assertEquals(FileSegmentType.INDEX, fileSegment.getFileType());
        fileSegment.destroyFile();

        Assert.assertThrows(RuntimeException.class,
            () -> factory.createSegment(null, null, 0L));
        storeConfig.setTieredBackendServiceProvider(null);
        Assert.assertThrows(RuntimeException.class,
            () -> new FileSegmentFactory(metadataStore, storeConfig, executor));

        executor.shutdown();
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }
}
