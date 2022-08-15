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

package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import static org.assertj.core.api.Assertions.assertThat;

public class IndexServiceTest {
    private DefaultMessageStore messageStore;
    private IndexService indexService;

    private final int HASH_SLOT_NUM = 100;
    private final int INDEX_NUM = 400;


    @Test
    public void testMultiPathGetLastFile() throws IOException {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMaxIndexNum(INDEX_NUM);
        messageStoreConfig.setMaxHashSlotNum(HASH_SLOT_NUM);
        messageStoreConfig.setStorePathRootDir(
                "target/unit_test_store/a/store" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/store" + MixAll.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/store");
        messageStoreConfig.setEnableMultiDispatch(true);
        BrokerConfig brokerConfig = new BrokerConfig();
        messageStore = new DefaultMessageStore(messageStoreConfig, null, null, brokerConfig);
        indexService = new IndexService(messageStore);
        messageStore.load();
        String[] storePathDir = messageStoreConfig.getStorePathRootDir().split(MixAll.MULTI_PATH_SPLITTER);

        for (int i = 0; i < 100; i++) {
            IndexFile indexFile = indexService.getAndCreateLastIndexFile();
            Assert.assertTrue(indexFile.getFileName().startsWith(storePathDir[i % storePathDir.length]));
            for (int j = 0; j < (INDEX_NUM - 1); j++) {
                boolean putResult = indexFile.putKey(Long.toString(j), j, System.currentTimeMillis());
                assertThat(putResult).isTrue();
            }
        }

        indexService.destroy();
        messageStore.destroy();
    }
}
