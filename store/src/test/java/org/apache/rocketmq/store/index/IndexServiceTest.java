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
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import static org.junit.Assert.*;

public class IndexServiceTest {

    private DefaultMessageStore initDefaultMessageStore() throws Exception {
        DefaultMessageStore store = buildMessageStore();

        return store;
    }

    private DefaultMessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), null, new BrokerConfig());
    }

    @Test
    public void getAndCreateLastIndexFile() throws Exception {
    }

    @Test
    public void retryGetAndCreateIndexFile() throws Exception {

        DefaultMessageStore store = initDefaultMessageStore();
        IndexService indexService = new IndexService(store);
        IndexFile indexFile = indexService.retryGetAndCreateIndexFile();
        assertNotNull(indexFile);
    }

}