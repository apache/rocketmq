/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AppendPropCRCTest {

    AppendMessageCallback callback;

    MessageExtEncoder encoder;

    CommitLog commitLog;

    @Before
    public void init() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore");
        messageStoreConfig.setStorePathCommitLog(System.getProperty("java.io.tmpdir") + File.separator + "unitteststore" + File.separator + "commitlog");
        messageStoreConfig.setForceVerifyPropCRC(true);
        messageStoreConfig.setEnabledAppendPropCRC(true);
        //too much reference
        DefaultMessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null, null, new BrokerConfig(), new ConcurrentHashMap<>());
        commitLog = new CommitLog(messageStore);
        encoder = new MessageExtEncoder(10 * 1024 * 1024, true);
        callback = commitLog.new DefaultAppendMessageCallback();
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(System.getProperty("user.home") + File.separator + "unitteststore"));
    }

    @Test
    public void testAppendMessageSucc() throws Exception {
        String topic = "test-topic";
        int queue = 0;
        int msgNum = 10;
        int propertiesLen = 0;
        Message msg = new Message();
        msg.setBody("body".getBytes());
        msg.setTopic(topic);
        msg.setTags("abc");
        msg.putUserProperty("a", "aaaaaaaa");
        msg.putUserProperty("b", "bbbbbbbb");
        msg.putUserProperty("c", "cccccccc");
        msg.putUserProperty("d", "dddddddd");
        msg.putUserProperty("e", "eeeeeeee");
        msg.putUserProperty("f", "ffffffff");

        MessageExtBrokerInner messageExtBrokerInner = new MessageExtBrokerInner();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(queue);
        messageExtBrokerInner.setBornTimestamp(System.currentTimeMillis());
        messageExtBrokerInner.setBornHost(new InetSocketAddress("127.0.0.1", 123));
        messageExtBrokerInner.setStoreHost(new InetSocketAddress("127.0.0.1", 124));
        messageExtBrokerInner.setBody(msg.getBody());
        messageExtBrokerInner.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        propertiesLen = messageExtBrokerInner.getPropertiesString().length();

        ByteBuffer buff = ByteBuffer.allocate(1024 * 10);
        for (int i = 0; i < msgNum; i++) {
            encoder.encode(messageExtBrokerInner);
            messageExtBrokerInner.setEncodedBuff(encoder.getEncoderBuffer());
            AppendMessageResult allresult = callback.doAppend(0, buff, 1024 * 10, messageExtBrokerInner, null);
            assertEquals(AppendMessageStatus.PUT_OK, allresult.getStatus());
        }
        // Expected to pass when message is not modified
        buff.flip();
        for (int i = 0; i < msgNum - 1; i++) {
            DispatchRequest request = commitLog.checkMessageAndReturnSize(buff, true, false);
            assertTrue(request.isSuccess());
        }
        // Modify the properties of the last message and expect the verification to fail.
        int idx = buff.limit() - (propertiesLen / 2);
        buff.put(idx, (byte) (buff.get(idx) + 1));
        DispatchRequest request = commitLog.checkMessageAndReturnSize(buff, true, false);
        assertFalse(request.isSuccess());
    }

    @Test
    public void testAppendMessageBatchSucc() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "test-topic";
        int queue = 0;
        int propertiesLen = 0;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody("body".getBytes());
            msg.setTopic(topic);
            msg.setTags("abc");
            msg.putUserProperty("a", "aaaaaaaa");
            msg.putUserProperty("b", "bbbbbbbb");
            msg.putUserProperty("c", "cccccccc");
            msg.putUserProperty("d", "dddddddd");
            msg.putUserProperty("e", "eeeeeeee");
            msg.putUserProperty("f", "ffffffff");
            String propertiesString = MessageDecoder.messageProperties2String(msg.getProperties());
            propertiesLen = propertiesString.length();
            messages.add(msg);
        }
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 123));
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 124));
        messageExtBatch.setBody(MessageDecoder.encodeMessages(messages));

        PutMessageContext putMessageContext = new PutMessageContext(topic + "-" + queue);
        messageExtBatch.setEncodedBuff(encoder.encode(messageExtBatch, putMessageContext));
        ByteBuffer buff = ByteBuffer.allocate(1024 * 10);
        //encounter end of file when append half of the data
        AppendMessageResult allresult =
            callback.doAppend(0, buff, 1024 * 10, messageExtBatch, putMessageContext);

        assertEquals(AppendMessageStatus.PUT_OK, allresult.getStatus());
        assertEquals(0, allresult.getWroteOffset());
        assertEquals(0, allresult.getLogicsOffset());
        assertEquals(buff.position(), allresult.getWroteBytes());

        assertEquals(messages.size(), allresult.getMsgNum());

        Set<String> msgIds = new HashSet<>();
        for (String msgId : allresult.getMsgId().split(",")) {
            assertEquals(32, msgId.length());
            msgIds.add(msgId);
        }
        assertEquals(messages.size(), msgIds.size());

        List<MessageExt> decodeMsgs = MessageDecoder.decodes((ByteBuffer) buff.flip());
        assertEquals(decodeMsgs.size(), decodeMsgs.size());
        long queueOffset = decodeMsgs.get(0).getQueueOffset();
        long storeTimeStamp = decodeMsgs.get(0).getStoreTimestamp();
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i).getTopic(), decodeMsgs.get(i).getTopic());
            assertEquals(new String(messages.get(i).getBody()), new String(decodeMsgs.get(i).getBody()));
            assertEquals(messages.get(i).getTags(), decodeMsgs.get(i).getTags());

            assertEquals(messageExtBatch.getBornHostNameString(), decodeMsgs.get(i).getBornHostNameString());

            assertEquals(messageExtBatch.getBornTimestamp(), decodeMsgs.get(i).getBornTimestamp());
            assertEquals(storeTimeStamp, decodeMsgs.get(i).getStoreTimestamp());
            assertEquals(queueOffset++, decodeMsgs.get(i).getQueueOffset());
        }

        // Expected to pass when message is not modified
        buff.flip();
        for (int i = 0; i < messages.size() - 1; i++) {
            DispatchRequest request = commitLog.checkMessageAndReturnSize(buff, true, false);
            assertTrue(request.isSuccess());
        }
        // Modify the properties of the last message and expect the verification to fail.
        int idx = buff.limit() - (propertiesLen / 2);
        buff.put(idx, (byte) (buff.get(idx) + 1));
        DispatchRequest request = commitLog.checkMessageAndReturnSize(buff, true, false);
        assertFalse(request.isSuccess());
    }
}
