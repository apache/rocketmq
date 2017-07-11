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

package org.apache.rocketmq.store;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long totalMsgs = 100;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig());

        boolean load = master.load();
        assertTrue(load);

        master.start();
        try {
            for (long i = 0; i < totalMsgs; i++) {
                master.putMessage(buildMessage());
            }

            for (long i = 0; i < totalMsgs; i++) {
                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
                assertThat(result).isNotNull();
                result.release();
            }
        } finally {
            master.shutdown();
            master.destroy();
        }
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(4);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    @Test
    public void testGroupCommit() throws Exception {
        long totalMsgs = 100;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = master.load();
        assertTrue(load);

        master.start();
        try {
            for (long i = 0; i < totalMsgs; i++) {
                master.putMessage(buildMessage());
            }

            for (long i = 0; i < totalMsgs; i++) {
                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
                assertThat(result).isNotNull();
                result.release();

            }
        } finally {
            master.shutdown();
            master.destroy();
        }
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @Test
    public void testQueryByTime() throws Exception {
        int totalMsgs = 100;
        int randomIndex = new Random().nextInt(10) + 40;
        QUEUE_TOTAL = 8;
        String topic = "TimeTopic";
        String keys = "testQueryByTime";
        long now = System.currentTimeMillis();
        MessageBody = StoreMessage.getBytes();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 16);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("test"), new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = master.load();
        assertTrue(load);

        master.start();
        try {
            for (int i = 0; i < totalMsgs; i++) {
                MessageExtBrokerInner messageExtBrokerInner = new MessageExtBrokerInner();
                messageExtBrokerInner.setBody(("time:" + System.currentTimeMillis() + " index:" + i).getBytes());
                messageExtBrokerInner.setTopic(topic);
                messageExtBrokerInner.setKeys(keys);
                messageExtBrokerInner.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
                messageExtBrokerInner.setBornTimestamp(System.currentTimeMillis());
                messageExtBrokerInner.setStoreHost(StoreHost);
                messageExtBrokerInner.setBornHost(BornHost);
                messageExtBrokerInner.setPropertiesString(MessageDecoder.messageProperties2String(messageExtBrokerInner.getProperties()));
                PutMessageResult putMessageResult = master.putMessage(messageExtBrokerInner);
                if (i == randomIndex) {
                    now = putMessageResult.getAppendMessageResult().getStoreTimestamp();
                }
            }
            Thread.sleep(2000L);
            long end = System.currentTimeMillis();
            QueryMessageResult result = master.queryMessage(topic, keys, totalMsgs, now, end);
            for (ByteBuffer byteBuffer : result.getMessageBufferList()) {
                MessageExt messageExt = MessageDecoder.decode(byteBuffer);
            }
            int bufferTotalSize = result.getMessageBufferList().size();
            result.release();
            Assert.assertTrue(totalMsgs - randomIndex - 1 <= bufferTotalSize);
        } finally {
            master.shutdown();
            master.destroy();
        }
    }

    @Test
    public void testQueueOffsetByTime() throws Exception {
        long totalMsgs = 200;
        QUEUE_TOTAL = 1;
        String topic = "TimeTopic";
        String keys = "testQueryByTime";
        String consumerGroup = "testQueryByTime";
        MessageBody = StoreMessage.getBytes();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 16);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("test"), new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = master.load();
        assertTrue(load);

        TreeMap<Long, AtomicInteger> sameTimeCountCache = new TreeMap<>();
        TreeMap<Long, AtomicInteger> sameTimeResultCache = new TreeMap<>();
        long hlTime = System.currentTimeMillis();
        long hlCount = 0;
        long start = 0;
        master.start();
        try {
            for (long i = 0; i < totalMsgs; i++) {
                if (i == totalMsgs - 20) {
                    Thread.sleep(1000);
                    hlTime = System.currentTimeMillis();
                    Thread.sleep(500);
                }
                MessageExtBrokerInner messageExtBrokerInner = new MessageExtBrokerInner();
                messageExtBrokerInner.setBody(("time:" + System.currentTimeMillis() + " index:" + i).getBytes());
                messageExtBrokerInner.setTopic(topic);
                messageExtBrokerInner.setKeys(keys);
                messageExtBrokerInner.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
                messageExtBrokerInner.setBornTimestamp(System.currentTimeMillis());
                messageExtBrokerInner.setStoreHost(StoreHost);
                messageExtBrokerInner.setBornHost(BornHost);
                messageExtBrokerInner.setPropertiesString(MessageDecoder.messageProperties2String(messageExtBrokerInner.getProperties()));
                PutMessageResult putMessageResult = master.putMessage(messageExtBrokerInner);
                long storeTimestamp = putMessageResult.getAppendMessageResult().getStoreTimestamp();
                AtomicInteger count = sameTimeCountCache.get(storeTimestamp);
                if (count == null) {
                    count = new AtomicInteger(0);
                    sameTimeCountCache.put(storeTimestamp, count);
                }
                count.incrementAndGet();
            }
            Thread.sleep(2000L);

            Map.Entry<Long, AtomicInteger> timeCount = sameTimeCountCache.lastEntry();
            start = timeCount.getKey();
            long offsetInQueueByTime = master.getOffsetInQueueByTime(topic, 0, start);
            GetMessageResult testQueryByTime = master.getMessage(consumerGroup, topic, 0, offsetInQueueByTime, 20, null);

            List<ByteBuffer> messageBufferList = testQueryByTime.getMessageBufferList();
            for (ByteBuffer byteBuffer : messageBufferList) {
                MessageExt messageExt = MessageDecoder.decode(byteBuffer);
                AtomicInteger cc = sameTimeResultCache.get(messageExt.getStoreTimestamp());
                if (cc == null) {
                    cc = new AtomicInteger(0);
                    sameTimeResultCache.put(messageExt.getStoreTimestamp(), cc);
                }
                cc.incrementAndGet();
            }
            testQueryByTime.release();

            long hlOffset = master.getOffsetInQueueByTime(topic, 0, hlTime);
            GetMessageResult hlResult = master.getMessage(consumerGroup, topic, 0, hlOffset, 20, null);
            hlCount = hlResult.getMessageCount();
            hlResult.release();
        } finally {
            master.shutdown();
            master.destroy();
        }

        Assert.assertTrue(start > 0);
        AtomicInteger cc = sameTimeCountCache.get(start);
        AtomicInteger result = sameTimeResultCache.get(start);
        Assert.assertEquals(cc.get(), result.get());
        Assert.assertTrue(19 == hlCount || hlCount == 20);
    }
}
