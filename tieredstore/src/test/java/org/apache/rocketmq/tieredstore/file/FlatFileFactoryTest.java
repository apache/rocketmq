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

import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.junit.Assert;
import org.junit.Test;

public class FlatFileFactoryTest {

    @Test
    public void factoryTest() {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setTieredStoreFilePath(MessageStoreUtilTest.getRandomStorePath());
        MetadataStore metadataStore = new DefaultMetadataStore(storeConfig);
        FlatFileFactory factory = new FlatFileFactory(metadataStore, storeConfig);
        Assert.assertEquals(storeConfig, factory.getStoreConfig());
        Assert.assertEquals(metadataStore, factory.getMetadataStore());

        FlatAppendFile flatFile1 = factory.createFlatFileForCommitLog("CommitLog");
        FlatAppendFile flatFile2 = factory.createFlatFileForConsumeQueue("ConsumeQueue");
        FlatAppendFile flatFile3 = factory.createFlatFileForIndexFile("IndexFile");

        Assert.assertNotNull(flatFile1);
        Assert.assertNotNull(flatFile2);
        Assert.assertNotNull(flatFile3);

        flatFile1.destroy();
        flatFile2.destroy();
        flatFile3.destroy();
    }
}