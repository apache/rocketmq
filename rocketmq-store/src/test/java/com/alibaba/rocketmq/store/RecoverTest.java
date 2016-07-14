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
 * $Id: RecoverTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;


public class RecoverTest {
    private static final String StoreMessage = "Once, there was a chance for me!aaaaaaaaaaaaaaaaaaaaaaaa";

    private static int QUEUE_TOTAL = 10;

    private static AtomicInteger QueueId = new AtomicInteger(0);

    private static SocketAddress BornHost;

    private static SocketAddress StoreHost;

    private static byte[] MessageBody;
    private MessageStore storeWrite1;
    private MessageStore storeWrite2;
    private MessageStore storeRead;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void test_recover_normally() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }

    public void writeMessage(boolean normal, boolean first) throws Exception {
        System.out.println("================================================================");
        long totalMsgs = 1000;
        QUEUE_TOTAL = 3;


        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 32);

        messageStoreConfig.setMapedFileSizeConsumeQueue(100 * 20);
        messageStoreConfig.setMessageIndexEnable(false);

        MessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null, null, null);
        if (first) {
            this.storeWrite1 = messageStore;
        } else {
            this.storeWrite2 = messageStore;
        }


        boolean loadResult = messageStore.load();
        assertTrue(loadResult);


        messageStore.start();


        for (long i = 0; i < totalMsgs; i++) {

            PutMessageResult result = messageStore.putMessage(buildMessage());

            System.out.println(i + "\t" + result.getAppendMessageResult().getMsgId());
        }

        if (normal) {

            messageStore.shutdown();
        }

        System.out.println("========================writeMessage OK========================================");
    }

    public void readMessage(final long msgCnt) throws Exception {
        System.out.println("================================================================");
        QUEUE_TOTAL = 3;


        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 32);

        messageStoreConfig.setMapedFileSizeConsumeQueue(100 * 20);
        messageStoreConfig.setMessageIndexEnable(false);

        storeRead = new DefaultMessageStore(messageStoreConfig, null, null, null);

        boolean loadResult = storeRead.load();
        assertTrue(loadResult);


        storeRead.start();


        long readCnt = 0;
        for (int queueId = 0; queueId < QUEUE_TOTAL; queueId++) {
            for (long offset = 0; ; ) {
                GetMessageResult result = storeRead.getMessage("GROUP_A", "TOPIC_A", queueId, offset, 1024 * 1024, null);
                if (result.getStatus() == GetMessageStatus.FOUND) {
                    System.out.println(queueId + "\t" + result.getMessageCount());
                    this.veryReadMessage(queueId, offset, result.getMessageBufferList());
                    offset += result.getMessageCount();
                    readCnt += result.getMessageCount();
                    result.release();
                } else {
                    break;
                }
            }
        }

        System.out.println("readCnt = " + readCnt);
        assertTrue(readCnt == msgCnt);

        System.out.println("========================readMessage OK========================================");
    }

    private void destroy() {
        if (storeWrite1 != null) {

            storeWrite1.shutdown();

            storeWrite1.destroy();
        }

        if (storeWrite2 != null) {

            storeWrite2.shutdown();

            storeWrite2.destroy();
        }

        if (storeRead != null) {

            storeRead.shutdown();

            storeRead.destroy();
        }
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("TOPIC_A");
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

    private void veryReadMessage(int queueId, long queueOffset, List<ByteBuffer> byteBuffers) {
        for (ByteBuffer byteBuffer : byteBuffers) {
            MessageExt msg = MessageDecoder.decode(byteBuffer);
            System.out.println("request queueId " + queueId + ", request queueOffset " + queueOffset + " msg queue offset "
                    + msg.getQueueOffset());

            assertTrue(msg.getQueueOffset() == queueOffset);

            queueOffset++;
        }
    }

    /**

     */
    @Test
    public void test_recover_normally_write() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(true, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }


    /**

     */
    @Test
    public void test_recover_abnormally() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }


    /**

     */
    @Test
    public void test_recover_abnormally_write() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(false, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }
}
