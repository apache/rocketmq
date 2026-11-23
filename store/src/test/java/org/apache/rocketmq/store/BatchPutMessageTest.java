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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.common.message.MessageDecoder.messageProperties2String;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchPutMessageTest {

    private MessageStore messageStore;

    public final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    @Before
    public void init() throws Exception {
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        UtilAll.deleteFile(new File(System.getProperty("java.io.tmpdir") + File.separator + "putmessagesteststore"));
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "putmessagesteststore");
        messageStoreConfig.setStorePathCommitLog(System.getProperty("java.io.tmpdir") + File.separator
            + "putmessagesteststore" + File.separator + "commitlog");
        messageStoreConfig.setHaListenPort(0);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest", true), new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());
    }

    @Test
    public void testPutMessages() throws Exception {
        String batchPropK = "extraKey";
        String batchPropV = "extraValue";
        Map<String, String> batchProp = new HashMap<>(1);
        batchProp.put(batchPropK, batchPropV);
        short batchPropLen = (short) messageProperties2String(batchProp).getBytes(MessageDecoder.CHARSET_UTF8).length;

        List<Message> messages = new ArrayList<>();
        String topic = "batch-write-topic";
        int queue = 0;
        int[] msgLengthArr = new int[11];
        msgLengthArr[0] = 0;
        int j = 1;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody(("body" + i).getBytes());
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
            String properties = messageProperties2String(msg.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short) propertiesBytes.length;
            final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            msgLengthArr[j] = calMsgLength(msg.getBody().length, topicLength, propertiesLength) + msgLengthArr[j - 1];
            j++;
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.putUserProperty(batchPropK, batchPropV);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        assertThat(putMessageResult.isOk()).isTrue();

        for (long i = 0; i < 10; i++) {
            final long index = i;
            Boolean exist = await().atMost(3, SECONDS).until(() -> {
                MessageExt messageExt = messageStore.lookMessageByOffset(msgLengthArr[(int) index]);
                if (messageExt == null) {
                    return false;
                }
                GetMessageResult result = messageStore.getMessage("batch_write_group", topic, queue, index, 1024 * 1024, null);
                if (result == null) {
                    return false;
                }
                boolean equals = GetMessageStatus.FOUND.equals(result.getStatus());
                result.release();
                return equals;
            }, item -> item);
            assertTrue(exist);
        }

    }

    @Test
    public void testPutIPv6HostMessages() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "batch-write-topic";
        int queue = 0;
        int[] msgLengthArr = new int[11];
        msgLengthArr[0] = 0;
        int j = 1;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody(("body" + i).getBytes());
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
            String properties = messageProperties2String(msg.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short) propertiesBytes.length;
            final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            msgLengthArr[j] = calIPv6HostMsgLength(msg.getBody().length, topicLength, propertiesLength) + msgLengthArr[j - 1];
            j++;
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setSysFlag(0);
        messageExtBatch.setBornHostV6Flag();
        messageExtBatch.setStoreHostAddressV6Flag();
        messageExtBatch.setStoreHost(new InetSocketAddress("1050:0000:0000:0000:0005:0600:300c:326b", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("::1", 126));

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        assertThat(putMessageResult.isOk()).isTrue();

        for (long i = 0; i < 10; i++) {
            final long index = i;
            Boolean exist = await().atMost(3, SECONDS).until(() -> {
                MessageExt messageExt = messageStore.lookMessageByOffset(msgLengthArr[(int) index]);
                if (messageExt == null) {
                    return false;
                }
                GetMessageResult result = messageStore.getMessage("batch_write_group", topic, queue, index, 1024 * 1024, null);
                if (result == null) {
                    return false;
                }
                boolean equals = GetMessageStatus.FOUND.equals(result.getStatus());
                result.release();
                return equals;
            }, item -> item);
            assertTrue(exist);
        }

    }

    @Test
    public void testPutMessagesLargerThanCommitLogFileRejected() throws Exception {
        String topic = "batch-oversized-topic";
        int queue = 0;
        List<Message> oversizedMessages = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Message msg = new Message();
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setBody(new byte[4 * 1024]);
            oversizedMessages.add(msg);
        }

        MessageExtBatch oversizedBatch = new MessageExtBatch();
        oversizedBatch.setTopic(topic);
        oversizedBatch.setQueueId(queue);
        oversizedBatch.setBody(MessageDecoder.encodeMessages(oversizedMessages));
        oversizedBatch.setBornTimestamp(System.currentTimeMillis());
        oversizedBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        oversizedBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

        PutMessageResult oversizedResult = messageStore.putMessages(oversizedBatch);
        assertThat(oversizedResult.isOk()).isFalse();
        assertThat(oversizedResult.getPutMessageStatus()).isEqualTo(PutMessageStatus.MESSAGE_ILLEGAL);
        assertThat(oversizedResult.getAppendMessageResult().getStatus()).isEqualTo(AppendMessageStatus.END_OF_FILE);
        assertThat(messageStore.getMaxOffsetInQueue(topic, queue)).isEqualTo(0L);

        List<Message> validMessages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Message msg = new Message();
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setBody(("ok-" + i).getBytes(CHARSET_UTF8));
            validMessages.add(msg);
        }

        MessageExtBatch validBatch = new MessageExtBatch();
        validBatch.setTopic(topic);
        validBatch.setQueueId(queue);
        validBatch.setBody(MessageDecoder.encodeMessages(validMessages));
        validBatch.setBornTimestamp(System.currentTimeMillis());
        validBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        validBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

        PutMessageResult validResult = messageStore.putMessages(validBatch);
        assertThat(validResult.isOk()).isTrue();
        assertThat(validResult.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);

        Boolean exist = await().atMost(3, SECONDS).until(() -> {
            GetMessageResult result = messageStore.getMessage("batch_write_group", topic, queue, 0, 32, null);
            if (result == null) {
                return false;
            }
            boolean matched = GetMessageStatus.FOUND.equals(result.getStatus()) && result.getMessageCount() == 2;
            result.release();
            return matched;
        }, item -> item);
        assertTrue(exist);
    }

    @Test
    public void testPutMessagesRetryAppendUnknownErrorPropagated() throws Exception {
        String topic = "batch-retry-unknown-error-topic";
        int queue = 0;
        DefaultMessageStore store = (DefaultMessageStore) messageStore;
        MappedFile mockFile = mock(MappedFile.class);
        when(mockFile.isFull()).thenReturn(false);
        when(mockFile.appendMessages(any(MessageExtBatch.class), any(AppendMessageCallback.class), any(PutMessageContext.class)))
            .thenReturn(
                new AppendMessageResult(AppendMessageStatus.END_OF_FILE),
                new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR));
        store.getCommitLog().getMappedFileQueue().getMappedFiles().add(mockFile);

        try {
            List<Message> messages = new ArrayList<>();
            Message msg = new Message();
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setBody("retry-batch-error".getBytes(CHARSET_UTF8));
            messages.add(msg);

            MessageExtBatch batch = new MessageExtBatch();
            batch.setTopic(topic);
            batch.setQueueId(queue);
            batch.setBody(MessageDecoder.encodeMessages(messages));
            batch.setBornTimestamp(System.currentTimeMillis());
            batch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
            batch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

            PutMessageResult putResult = messageStore.putMessages(batch);
            assertThat(putResult.isOk()).isFalse();
            assertThat(putResult.getPutMessageStatus()).isEqualTo(PutMessageStatus.UNKNOWN_ERROR);
            assertThat(putResult.getAppendMessageResult().getStatus()).isEqualTo(AppendMessageStatus.UNKNOWN_ERROR);
            assertThat(messageStore.getMaxOffsetInQueue(topic, queue)).isEqualTo(0L);
            assertThat(store.getStoreStatsService().getSinglePutMessageTopicTimesTotal(topic).longValue()).isEqualTo(0L);
            assertThat(store.getStoreStatsService().getSinglePutMessageTopicSizeTotal(topic).longValue()).isEqualTo(0L);
        } finally {
            store.getCommitLog().getMappedFileQueue().getMappedFiles().clear();
        }
    }

    private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    private int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID
            + 4 //FLAG
            + 8 //QUEUEOFFSET
            + 8 //PHYSICALOFFSET
            + 4 //SYSFLAG
            + 8 //BORNTIMESTAMP
            + 8 //BORNHOST
            + 8 //STORETIMESTAMP
            + 8 //STOREHOSTADDRESS
            + 4 //RECONSUMETIMES
            + 8 //Prepared Transaction Offset
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
            + 1 + topicLength //TOPIC
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
            + 0;
        return msgLen;
    }

    private int calIPv6HostMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID
            + 4 //FLAG
            + 8 //QUEUEOFFSET
            + 8 //PHYSICALOFFSET
            + 4 //SYSFLAG
            + 8 //BORNTIMESTAMP
            + 20 //BORNHOST
            + 8 //STORETIMESTAMP
            + 20 //STOREHOSTADDRESS
            + 4 //RECONSUMETIMES
            + 8 //Prepared Transaction Offset
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
            + 1 + topicLength //TOPIC
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
            + 0;
        return msgLen;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
