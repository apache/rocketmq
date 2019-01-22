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

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
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
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @Test(expected = OverlappingFileLockException.class)
    public void test_repeat_restart() throws Exception {
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

        try {
            master.start();
            master.start();
        } finally {
            master.shutdown();
            master.destroy();
        }
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
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MyMessageArrivingListener(), new BrokerConfig());
    }

    @Test
    public void testWriteAndRead() {
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMsgs, messageStore);
    }

    @Test
    public void should_look_message_successfully_when_offset_is_first() {
        final int totalCount = 10;
        int[] messageLengthArray = new int[totalCount];
        int firstOffset = 0;
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) this.messageStore;
        for (int i = firstOffset; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            messageStore.putMessage(msgInner);
            messageLengthArray[i] = calculateMessageLength(msgInner);
        }

        MessageExt messageExt = messageStore.lookMessageByOffset(firstOffset);
        MessageExt messageExt1 = defaultMessageStore.lookMessageByOffset(firstOffset, messageLengthArray[firstOffset]);

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
        assertThat(new String(messageExt1.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
    }

    @Test
    public void should_look_message_successfully_when_offset_is_last() {
        final int totalCount = 10;
        int[] messageLengthArray = new int[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) this.messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            messageStore.putMessage(msgInner);
            messageLengthArray[i] = calculateMessageLength(msgInner);
        }

        int lastIndex = totalCount - 1;
        int lastOffset = getOffsetByIndex(messageLengthArray, lastIndex);
        MessageExt messageExt = defaultMessageStore.lookMessageByOffset(lastOffset, messageLengthArray[lastIndex]);

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, lastIndex));
    }

    @Test
    public void should_look_message_failed_and_return_null_when_offset_is_out_of_bound() {
        final int totalCount = 10;
        int[] messageLengthArray = new int[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            messageStore.putMessage(msgInner);
            messageLengthArray[i] = calculateMessageLength(msgInner);
        }

        int lastOffset = getOffsetByIndex(messageLengthArray, totalCount);
        MessageExt messageExt = defaultMessageStore.lookMessageByOffset(lastOffset);

        assertThat(messageExt).isNull();
    }

    @Test
    public void should_return_empty_map_when_consume_queue_can_not_found() {
        final int totalCount = 10;
        int queueId = 0;
        int targetQueueId = 1;
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
        }
        String topic = "FooBar";
        long minOffset = appendMessageResultArray[0].getLogicsOffset();

        Map<String, Long> messageIds = defaultMessageStore.getMessageIds(topic, targetQueueId, minOffset, 0, StoreHost);

        assertThat(messageIds).isEmpty();
    }

    @Test
    public void should_return_empty_map_when_max_offset_is_zero() {
        final int totalCount = 10;
        int queueId = new Random().nextInt();
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
        }
        String topic = "FooBar";
        long minOffset = appendMessageResultArray[0].getLogicsOffset();

        Map<String, Long> messageIds = defaultMessageStore.getMessageIds(topic, queueId, minOffset, 0, StoreHost);

        assertThat(messageIds).isEmpty();
    }

    @Test
    public void should_return_empty_map_when_min_offset_is_large_than_max_offset() {
        final int totalCount = 10;
        int queueId = new Random().nextInt();
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
        }
        String topic = "FooBar";
        long maxOffset = appendMessageResultArray[totalCount - 1].getLogicsOffset() + 1;

        Map<String, Long> messageIds = defaultMessageStore.getMessageIds(topic, queueId, maxOffset, 1, StoreHost);

        assertThat(messageIds).isEmpty();
    }


    @Test
    public void should_return_consume_queue_index_map_by_msgId_successfully_when_getMessageIds_by_topic_and_queue_id() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        System.out.println("queueId" + queueId);
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes());
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
        }
        String topic = "FooBar";
        long minOffset = appendMessageResultArray[0].getLogicsOffset();
        long maxOffset = getMaxOffset(appendMessageResultArray);

        Map<String, Long> messageIds = defaultMessageStore.getMessageIds(topic, queueId, minOffset, maxOffset, StoreHost);

        assertThat(messageIds).isNotEmpty();
        assertThat(messageIds.size()).isEqualTo(10);
        for (int i = 0; i < totalCount; i++) {
            assertThat(messageIds.get(appendMessageResultArray[i].getMsgId())).isEqualTo(i);
        }
    }

    private long getMaxOffset(AppendMessageResult[] array) {
        long maxOffset = 0;
        for (AppendMessageResult appendMessageResult : array) {
            long offset = appendMessageResult.getLogicsOffset();
            if (offset > maxOffset) {
                maxOffset = offset;
            }
        }
        return maxOffset;
    }

    private String buildMessageBodyByOffset(String message, long i) {
        return String.format("%s offset %d", message, i);
    }

    private MessageExtBrokerInner buildMessage(byte[] messageBody) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    private MessageExtBrokerInner buildMessage() {
        return buildMessage(MessageBody);
    }

    private int getOffsetByIndex(int[] messageLengthArray, int index) {
        int result = 0;
        for (int i = 0; i < index; i++) {
            result += messageLengthArray[i];
        }
        return result;
    }

    /**
     * calculate message length
     *
     * @param msgInner MessageExtBrokerInner
     * @return message length
     */
    private static int calculateMessageLength(MessageExtBrokerInner msgInner) {
        int topicLength = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8).length;
        int propertiesLength = msgInner.getPropertiesString() == null ? 0 : msgInner.getPropertiesString().getBytes().length;
        int bodyLength = msgInner.getBody().length;

        // magic code 91 please reference CommitLog#calMsgLength
        return topicLength + propertiesLength + bodyLength + 91;
    }

    private void verifyThatMasterIsFunctional(long totalMsgs, MessageStore master) {
        for (long i = 0; i < totalMsgs; i++) {
            master.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();

        }
    }

    @Test
    public void testPullSize() throws Exception {
        String topic = "pullSizeTopic";

        for (int i = 0; i < 32; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        // wait for consume queue build
        // the sleep time should be great than consume queue flush interval
        Thread.sleep(100);
        String group = "simple";
        GetMessageResult getMessageResult32 = messageStore.getMessage(group, topic, 0, 0, 32, null);
        assertThat(getMessageResult32.getMessageBufferList().size()).isEqualTo(32);

        GetMessageResult getMessageResult20 = messageStore.getMessage(group, topic, 0, 0, 20, null);
        assertThat(getMessageResult20.getMessageBufferList().size()).isEqualTo(20);

        GetMessageResult getMessageResult45 = messageStore.getMessage(group, topic, 0, 0, 10, null);
        assertThat(getMessageResult45.getMessageBufferList().size()).isEqualTo(10);
    }

    @Test
    public void testRecover() throws Exception {
        String topic = "recoverTopic";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        Thread.sleep(100);//wait for build consumer queue
        long maxPhyOffset = messageStore.getMaxPhyOffset();
        long maxCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        //1.just reboot
        messageStore.shutdown();
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(maxPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(maxCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //2.damage commitlog and reboot normal
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        Thread.sleep(100);
        long secondLastPhyOffset = messageStore.getMaxPhyOffset();
        long secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);

        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);

        //reboot
        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //3.damage commitlog and reboot abnormal
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        Thread.sleep(100);
        secondLastPhyOffset = messageStore.getMaxPhyOffset();
        secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);
        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        file.createNewFile();

        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //message write again
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
    }

    private void damageCommitlog(long offset) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathCommitLog() + File.separator + "00000000000000000000");

        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 10);

        int bodyLen = mappedByteBuffer.getInt((int) offset + 84);
        int topicLenIndex = (int) offset + 84 + bodyLen + 4;
        mappedByteBuffer.position(topicLenIndex);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);

        mappedByteBuffer.force();
        fileChannel.force(true);
        fileChannel.close();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
