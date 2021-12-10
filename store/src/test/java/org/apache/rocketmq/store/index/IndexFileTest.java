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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void testQueryMessageByIndexFile() throws Exception {
        MessageStore messageStore = buildMessageStore();
        clean(messageStore);
        try {
            boolean load = messageStore.load();
            assertTrue(load);
            messageStore.start();
            messageStore.putMessage(buildMessage("AaTopic", "Aa"));
            messageStore.putMessage(buildMessage("BBTopic", "BB"));

            while (!isIndexFileBuild(messageStore)) {
                Thread.sleep(1);
            }

            int timeDiff = 60 * 60 * 1000;
            long begin = System.currentTimeMillis() - timeDiff;
            long end = System.currentTimeMillis() + timeDiff;
            String queryTopicName = "AaTopic";
            String queryMsgKey = "Aa";
            QueryMessageResult result = messageStore.queryMessage(queryTopicName, queryMsgKey, 10, begin, end);
            List<ByteBuffer> messageBufferList = result.getMessageBufferList();
            for (ByteBuffer byteBuffer : messageBufferList) {
                Pair<String, String> topicAndKey = fetchTopicAndKey(byteBuffer);
                String topicName = topicAndKey.getObject1();
                String msgKey = topicAndKey.getObject2();
                assertEquals(queryTopicName, topicName);
                assertEquals(queryMsgKey, msgKey);
            }
        } finally {
            clean(messageStore);
        }
    }

    /**
     * fetch target topic name and message key
     *
     * @param byteBuffer    message
     * @return left: topic   right: message key
     */
    private Pair<String, String> fetchTopicAndKey(ByteBuffer byteBuffer) {
        int headLen = 28 * 3;
        int bodyLen = byteBuffer.getInt(headLen);
        byte topicLen = byteBuffer.get(headLen + 4 + bodyLen);
        byte[] topicName = new byte[topicLen];
        byteBuffer.position(headLen + 4 + bodyLen + 1);
        byteBuffer.get(topicName);

        byteBuffer.position(headLen + 4 + bodyLen + 1 + topicLen);
        short propLen = byteBuffer.getShort();
        byteBuffer.position(headLen + 4 + bodyLen + 1 + topicLen + 2);
        byte[] propArr = new byte[propLen];
        byteBuffer.get(propArr);

        String properties = new String(propArr, 0, propLen, MessageDecoder.CHARSET_UTF8);
        Map<String, String> propMap = MessageDecoder.string2messageProperties(properties);
        return new Pair<>(new String(topicName), propMap.get(MessageConst.PROPERTY_KEYS));
    }

    /**
     * is index file build over
     */
    private boolean isIndexFileBuild(MessageStore messageStore) throws Exception {
        Field indexServiceField = messageStore.getClass().getDeclaredField("indexService");
        indexServiceField.setAccessible(true);
        IndexService indexService = (IndexService) indexServiceField.get(messageStore);
        IndexFile indexFile = indexService.retryGetAndCreateIndexFile();
        Field indexNumField = indexFile.getClass().getDeclaredField("indexHeader");
        indexNumField.setAccessible(true);
        IndexHeader indexHeader = (IndexHeader) indexNumField.get(indexFile);
        // index count increment from 1, not 0
        return indexHeader.getIndexCount() == 3;
    }

    private String byteBufferToString(ByteBuffer byteBuffer) {
        if (byteBuffer instanceof DirectBuffer) {
            byte[] bytes = new byte[byteBuffer.limit()];
            System.out.println(bytes.length);
            byteBuffer.get(bytes);
            return new String(bytes);
        } else {
            return new String(byteBuffer.array());
        }
    }

    /**
     * clean file after test over
     */
    private void clean(MessageStore messageStore) {
        messageStore.shutdown();
        messageStore.destroy();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    /**
     * mock build message
     */
    private MessageExtBrokerInner buildMessage(String topic, String msgKey) throws Exception {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setKeys(msgKey);
        msg.setBody("index file hash conflict test".getBytes());
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 8123));
        msg.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_KEYS, msgKey);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(map));
        return msg;
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new EmptyMessageArrivingListener(), new BrokerConfig());
    }

    private static class EmptyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {

        }
    }
}
