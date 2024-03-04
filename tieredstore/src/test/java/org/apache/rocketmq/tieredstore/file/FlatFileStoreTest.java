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
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.exception.TieredStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

public class FlatFileStoreTest {

    private final String storePath = MessageStoreUtilTest.getRandomStorePath();
    private MessageStoreConfig storeConfig;
    private MessageQueue mq;
    private MetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        mq = new MessageQueue("flatFileStoreTest", storeConfig.getBrokerName(), 0);
        metadataStore = new DefaultMetadataStore(storeConfig);
    }

    @After
    public void tearDown() throws IOException {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void flatFileStoreTest() {
        // Empty recover
        MessageStoreExecutor executor = new MessageStoreExecutor();
        FlatFileStore fileStore = new FlatFileStore(storeConfig, metadataStore, executor);
        Assert.assertTrue(fileStore.load());

        Assert.assertEquals(storeConfig, fileStore.getStoreConfig());
        Assert.assertEquals(metadataStore, fileStore.getMetadataStore());
        Assert.assertNotNull(fileStore.getFlatFileFactory());

        FlatMessageFile flatFile = fileStore.computeIfAbsent(mq);
        FlatMessageFile flatFileGet = fileStore.getFlatFile(mq);
        Assert.assertEquals(flatFile, flatFileGet);

        mq.setQueueId(1);
        fileStore.computeIfAbsent(mq);
        Assert.assertEquals(2, fileStore.deepCopyFlatFileToList().size());
        fileStore.shutdown();

        fileStore = new FlatFileStore(storeConfig, metadataStore, executor);
        Assert.assertTrue(fileStore.load());
        Assert.assertEquals(2, fileStore.deepCopyFlatFileToList().size());
        fileStore.shutdown();

        fileStore.destroyFile(mq);
        Assert.assertEquals(1, fileStore.deepCopyFlatFileToList().size());

        FlatFileStore fileStoreSpy = Mockito.spy(fileStore);
        Mockito.when(fileStoreSpy.recoverAsync(any())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "Test");
        }));
        Assert.assertFalse(fileStoreSpy.load());

        fileStore.destroy();
        Assert.assertEquals(0, fileStore.deepCopyFlatFileToList().size());
    }
}
