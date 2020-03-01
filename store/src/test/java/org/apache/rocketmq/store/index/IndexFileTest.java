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
 * $Id: IndexFileTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store.index;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.StoreTestUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;
import static org.apache.rocketmq.store.StoreTestBase.deleteFile;
import static org.assertj.core.api.Assertions.assertThat;

public class IndexFileTest {
    private final int HASH_SLOT_NUM = 100;
    private final int INDEX_NUM = 400;

    @Test
    public void testPutKey() throws Exception {
        IndexFile indexFile = new IndexFile("100", HASH_SLOT_NUM, INDEX_NUM, 0, 0);
        for (long i = 0; i < (INDEX_NUM - 1); i++) {
            boolean putResult = indexFile.putKey(Long.toString(i), i, System.currentTimeMillis());
            assertThat(putResult).isTrue();
        }

        // put over index file capacity.
        boolean putResult = indexFile.putKey(Long.toString(400), 400, System.currentTimeMillis());
        assertThat(putResult).isFalse();
        indexFile.destroy(0);
        File file = new File("100");
        UtilAll.deleteFile(file);
    }

    @Test
    public void testSelectPhyOffset() throws Exception {
        IndexFile indexFile = new IndexFile("200", HASH_SLOT_NUM, INDEX_NUM, 0, 0);

        for (long i = 0; i < (INDEX_NUM - 1); i++) {
            boolean putResult = indexFile.putKey(Long.toString(i), i, System.currentTimeMillis());
            assertThat(putResult).isTrue();
        }

        // put over index file capacity.
        boolean putResult = indexFile.putKey(Long.toString(400), 400, System.currentTimeMillis());
        assertThat(putResult).isFalse();

        final List<Long> phyOffsets = new ArrayList<Long>();
        indexFile.selectPhyOffset(phyOffsets, "60", 10, 0, Long.MAX_VALUE, true);
        assertThat(phyOffsets).isNotEmpty();
        assertThat(phyOffsets.size()).isEqualTo(1);
        indexFile.destroy(0);
        File file = new File("200");
        UtilAll.deleteFile(file);
    }

    @Test
    public void testPutMessageAndBuildIndex() throws Exception {
        final int maxNum = 10;
        final String storePath = "." + File.separator + "unit_test_store";
        DefaultMessageStore messageStore = null;
        try {
            // Start MessageStore.
            MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            messageStoreConfig.setStorePathRootDir(storePath);
            messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");
            messageStoreConfig.setMappedFileSizeCommitLog(8 * 1024);
            messageStoreConfig.setMappedFileSizeConsumeQueue(100 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
            messageStoreConfig.setMaxHashSlotNum(HASH_SLOT_NUM);
            messageStoreConfig.setMaxIndexNum(INDEX_NUM);
            messageStore = new DefaultMessageStore(messageStoreConfig,
                    new BrokerStatsManager("simpleTest"),
                    (String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) -> {},
                    new BrokerConfig());
            messageStore.load();
            messageStore.start();

            // Put message and wait reput.
            MessageExtBrokerInner msg = buildMessage();
            List<Long> offsetList = new ArrayList<>(maxNum);
            for (int i = 0; i < maxNum; i++) {
                PutMessageResult putMessageResult = messageStore.putMessage(msg);
                offsetList.add(putMessageResult.getAppendMessageResult().getWroteOffset());
            }
            Collections.reverse(offsetList);
            StoreTestUtil.waitCommitLogReput(messageStore);

            // Query pbyoffset by index.
            Field indexServiceField = messageStore.getClass().getDeclaredField("indexService");
            indexServiceField.setAccessible(true);
            IndexService indexService = (IndexService)indexServiceField.get(messageStore);

            IndexFile indexFile = indexService.getAndCreateLastIndexFile();
            List<Long> phyOffsets = new ArrayList<>(maxNum);
            Method buildKeyMethod = indexService.getClass().getDeclaredMethod("buildKey", String.class, String.class);
            buildKeyMethod.setAccessible(true);
            String key = (String) buildKeyMethod.invoke(indexService, msg.getTopic(), msg.getKeys());
            indexFile.selectPhyOffset(phyOffsets, key, maxNum, 0, System.currentTimeMillis(), false);

            assertThat(offsetList).isEqualTo(phyOffsets);
        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteFile(storePath);
        }
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("StoreTest");
        msg.setTags("TAG1");
        msg.setBody(new byte[100]);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        InetSocketAddress bornHost = new InetSocketAddress("127.0.0.1", 8123);
        msg.setStoreHost(bornHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

}
