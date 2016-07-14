/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * $Id: ScheduleMessageTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store.schedule;

import com.alibaba.rocketmq.store.*;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;


public class ScheduleMessageTest {
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
    public void test_delay_message() throws Exception {
        System.out.println("================================================================");
        long totalMsgs = 10000;
        QUEUE_TOTAL = 32;


        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 32);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 16);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(1000 * 10);

        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, null, null);

        boolean load = master.load();
        assertTrue(load);


        master.start();
        for (int i = 0; i < totalMsgs; i++) {
            MessageExtBrokerInner msg = buildMessage();
            msg.setDelayTimeLevel(i % 4);

            PutMessageResult result = master.putMessage(msg);
            System.out.println(i + "\t" + result.getAppendMessageResult().getMsgId());
        }

        System.out.println("write message over, wait time up");
        Thread.sleep(1000 * 20);


        for (long i = 0; i < totalMsgs; i++) {
            try {
                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
                if (result == null) {
                    System.out.println("result == null " + i);
                }
                assertTrue(result != null);
                result.release();
                System.out.println("read " + i + " OK");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        Thread.sleep(1000 * 15);


        master.shutdown();


        master.destroy();
        System.out.println("================================================================");
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
}
