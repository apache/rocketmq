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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexServiceTest {

    private MessageStore messageStore;

    private IndexService indexService;

    private String keys = "test_keys";
    private String topic = "fooBar";

    private int offset = 1000;

    @Before
    public void init() throws Exception {
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        indexService = new IndexService((DefaultMessageStore) messageStore);
    }


    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setStorePathRootDir("test");
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new IndexServiceTest.MyMessageArrivingListener(), new BrokerConfig());
    }

    @Test
    public void testQueryOffset() throws Exception {

        DispatchRequest dispatchRequest = buildDispatchRequest(offset);

        indexService.buildIndex(dispatchRequest);
        QueryOffsetResult queryOffsetResult = indexService.queryOffset(topic, keys, 100, System.currentTimeMillis() - 3 * 1000, System.currentTimeMillis());
        assertThat(queryOffsetResult).isNotNull();
        assertThat(queryOffsetResult.getPhyOffsets().size()).isGreaterThan(0);
        assertThat(queryOffsetResult.getPhyOffsets().get(0)).isEqualTo(offset);

    }

    public class MyMessageArrivingListener implements MessageArrivingListener {
        @Override public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {

        }
    }

    private DispatchRequest buildDispatchRequest(int offsetVal) {

        Map<String, String> properties = new HashMap<String, String>(4);

        DispatchRequest dispatchRequest = new DispatchRequest(
            topic,
            0,
            offsetVal,
            100,
            (long) ("tags" + offsetVal).hashCode(),
            System.currentTimeMillis(),
            offsetVal,
            keys,
            null,
            0,
            0,
            properties
        );

        return dispatchRequest;
    }

}
