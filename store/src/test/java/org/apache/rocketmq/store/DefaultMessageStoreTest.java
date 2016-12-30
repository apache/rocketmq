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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class DefaultMessageStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageStoreTest.class);

    private static final String StoreMessage = "Once, there was a chance for me!";

    private static int QUEUE_TOTAL = 100;

    private static AtomicInteger QueueId = new AtomicInteger(0);

    private static SocketAddress BornHost;

    private static SocketAddress StoreHost;

    private static byte[] MessageBody;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void test_write_read() throws Exception {
        logger.debug("================================================================");
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
                PutMessageResult result = master.putMessage(buildMessage());
                logger.debug(i + "\t" + result.getAppendMessageResult().getMsgId());
            }

            for (long i = 0; i < totalMsgs; i++) {
                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
                if (result == null) {
                    logger.debug("result == null " + i);
                }
                assertTrue(result != null);
                result.release();
                logger.debug("read " + i + " OK");
            }
        } finally {
            master.shutdown();
            master.destroy();
        }
        logger.debug("================================================================");
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("AAA");
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
    public void test_group_commit() throws Exception {
        logger.debug("================================================================");
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
                PutMessageResult result = master.putMessage(buildMessage());
                logger.debug(i + "\t" + result.getAppendMessageResult().getMsgId());
            }

            for (long i = 0; i < totalMsgs; i++) {
                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
                if (result == null) {
                    logger.debug("result == null " + i);
                }
                assertTrue(result != null);
                result.release();
                logger.debug("read " + i + " OK");

            }
        } finally {
            master.shutdown();
            master.destroy();
        }
        logger.debug("================================================================");
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {

        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode) {
            // Do nothing here
        }
    }
}
