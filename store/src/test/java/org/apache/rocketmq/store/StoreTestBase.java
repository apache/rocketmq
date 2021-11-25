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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.junit.After;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StoreTestBase {

    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost = new InetSocketAddress("127.0.0.1", 8123);
    private SocketAddress StoreHost = BornHost;
    private byte[] MessageBody = new byte[1024];

    protected Set<String> baseDirs = new HashSet<>();

    private static AtomicInteger port = new AtomicInteger(30000);

    public static synchronized int nextPort() {
        return port.addAndGet(5);
    }

    protected MessageExtBatch buildBatchMessage(int size) {
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic("StoreTest");
        messageExtBatch.setTags("TAG1");
        messageExtBatch.setKeys("Hello");
        messageExtBatch.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        messageExtBatch.setSysFlag(0);

        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setBornHost(BornHost);
        messageExtBatch.setStoreHost(StoreHost);

        List<Message> messageList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messageList.add(buildMessage());
        }

        messageExtBatch.setBody(MessageDecoder.encodeMessages(messageList));

        return messageExtBatch;
    }

    protected MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("StoreTest");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    protected MessageExtBatch buildIPv6HostBatchMessage(int size) {
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic("StoreTest");
        messageExtBatch.setTags("TAG1");
        messageExtBatch.setKeys("Hello");
        messageExtBatch.setBody(MessageBody);
        messageExtBatch.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExtBatch.setKeys(String.valueOf(System.currentTimeMillis()));
        messageExtBatch.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        messageExtBatch.setSysFlag(0);
        messageExtBatch.setBornHostV6Flag();
        messageExtBatch.setStoreHostAddressV6Flag();
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        try {
            messageExtBatch.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 8123));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            messageExtBatch.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 8123));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        List<Message> messageList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messageList.add(buildIPv6HostMessage());
        }

        messageExtBatch.setBody(MessageDecoder.encodeMessages(messageList));
        return messageExtBatch;
    }

    protected MessageExtBrokerInner buildIPv6HostMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("StoreTest");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        try {
            msg.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 8123));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            msg.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 8123));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public static String createBaseDir() {
        String baseDir = System.getProperty("user.home") + File.separator + "unitteststore" + File.separator + UUID.randomUUID();
        final File file = new File(baseDir);
        if (file.exists()) {
            System.exit(1);
        }
        return baseDir;
    }

    public static boolean makeSureFileExists(String fileName) throws Exception {
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        return file.createNewFile();
    }

    public static void deleteFile(String fileName) {
        deleteFile(new File(fileName));
    }

    public static void deleteFile(File file) {
        UtilAll.deleteFile(file);
    }

    @After
    public void clear() {
        for (String baseDir : baseDirs) {
            deleteFile(baseDir);
        }
    }

}
