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
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
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
    private MetadataStore metadataStore;
    private TieredMessageStore messageStore;

    @Before
    public void init() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        storeConfig.setBrokerName("brokerName");
        metadataStore = new DefaultMetadataStore(storeConfig);
        messageStore = Mockito.mock(TieredMessageStore.class);
        DefaultMessageStore defaultMessageStore = Mockito.mock(DefaultMessageStore.class);
        Mockito.when(defaultMessageStore.getMessageStoreConfig()).thenReturn(new org.apache.rocketmq.store.config.MessageStoreConfig());
        Mockito.when(messageStore.getDefaultStore()).thenReturn(defaultMessageStore);
    }

    @After
    public void shutdown() throws IOException {
        MessageStoreUtilTest.deleteStoreDirectory(storePath);
    }

    @Test
    public void flatFileStoreTest() {
        // Empty recover
        MessageStoreExecutor executor = new MessageStoreExecutor();
        FlatFileStore fileStore = new FlatFileStore(messageStore, storeConfig, metadataStore, executor);
        Assert.assertTrue(fileStore.load());

        Assert.assertEquals(storeConfig, fileStore.getStoreConfig());
        Assert.assertEquals(metadataStore, fileStore.getMetadataStore());
        Assert.assertNotNull(fileStore.getFlatFileFactory());

        for (int i = 0; i < 4; i++) {
            MessageQueue mq = new MessageQueue("flatFileStoreTest", storeConfig.getBrokerName(), i);
            FlatMessageFile flatFile = fileStore.computeIfAbsent(mq);
            FlatMessageFile flatFileGet = fileStore.getFlatFile(mq);
            Assert.assertEquals(flatFile, flatFileGet);
        }
        Assert.assertEquals(4, fileStore.deepCopyFlatFileToList().size());
        fileStore.shutdown();

        fileStore = new FlatFileStore(messageStore, storeConfig, metadataStore, executor);
        Assert.assertTrue(fileStore.load());
        Assert.assertEquals(4, fileStore.deepCopyFlatFileToList().size());

        for (int i = 1; i < 3; i++) {
            MessageQueue mq = new MessageQueue("flatFileStoreTest", storeConfig.getBrokerName(), i);
            fileStore.destroyFile(mq);
        }
        Assert.assertEquals(2, fileStore.deepCopyFlatFileToList().size());
        fileStore.shutdown();

        FlatFileStore fileStoreSpy = Mockito.spy(fileStore);
        Mockito.when(fileStoreSpy.recoverAsync(any())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new TieredStoreException(TieredStoreErrorCode.ILLEGAL_PARAM, "Test");
        }));
        Assert.assertFalse(fileStoreSpy.load());

        Mockito.reset(fileStoreSpy);
        fileStore.load();
        Assert.assertEquals(2, fileStore.deepCopyFlatFileToList().size());
        fileStore.destroy();
        Assert.assertEquals(0, fileStore.deepCopyFlatFileToList().size());
    }
}
