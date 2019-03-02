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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
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

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
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
    public void test_lookMessageByOffset_firstOffset() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        int firstOffset = 0;
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        AppendMessageResult firstResult = appendMessageResultArray[0];

        MessageExt messageExt = messageStore.lookMessageByOffset(firstResult.getWroteOffset());
        MessageExt messageExt1 = getDefaultMessageStore().lookMessageByOffset(firstResult.getWroteOffset(), firstResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
        assertThat(new String(messageExt1.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
    }

    @Test
    public void test_lookMessageByOffset_lastOffset() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        int lastIndex = totalCount - 1;
        AppendMessageResult lastResult = appendMessageResultArray[lastIndex];

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastResult.getWroteOffset(), lastResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, lastIndex));
    }

    @Test
    public void test_lookMessageByOffset_offsetOutOfBound() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        long lastOffset = getMaxOffset(appendMessageResultArray);

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastOffset);

        assertThat(messageExt).isNull();
    }

    @Test
    public void test_getOffsetInQueueByTime_success() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp());
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            indexBuffer.release();
        }
    }

    @Test
    public void test_getOffsetInQueueByTime_timestampIsSkewing() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, 10);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());
        int skewing = 1;

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() + skewing);
            long offset2 = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            SelectMappedBufferResult indexBuffer2 = consumeQueue.getIndexBuffer(offset2);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            assertThat(indexBuffer2.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer2.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            indexBuffer.release();
            indexBuffer2.release();
        }
    }

    @Test
    public void test_getOffsetInQueueByTime_timestampIsLargeDeviation() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());
        int skewing = 20000;

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() + skewing);
            long offset2 = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            SelectMappedBufferResult indexBuffer2 = consumeQueue.getIndexBuffer(offset2);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResults[totalCount - 1].getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResults[totalCount - 1].getWroteBytes());
            assertThat(indexBuffer2.getByteBuffer().getLong()).isEqualTo(appendMessageResults[0].getWroteOffset());
            assertThat(indexBuffer2.getByteBuffer().getInt()).isEqualTo(appendMessageResults[0].getWroteBytes());
        }
    }

    @Test
    public void test_getOffsetInQueueByTime_queueIdIsWrong() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        long offset = messageStore.getOffsetInQueueByTime(topic, wrongQueueId, appendMessageResults[0].getStoreTimestamp());

        assertThat(offset).isEqualTo(0);
    }

    @Test
    public void test_getMessageStoreTimeStamp_queueIdIsWrong() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, 0);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void test_getMessageStoreTimeStamp_consumeQueueOffsetNotExist() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, -1);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }


    @Test
    public void test_getMessageStoreTimeStamp_success() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        int minOffsetInQueue = (int)consumeQueue.getMinOffsetInQueue();
        for (int i = minOffsetInQueue; i < consumeQueue.getMaxOffsetInQueue(); i++) {
            long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, queueId, i);
            assertThat(messageStoreTimeStamp).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void test_getStoreTime_paramIsnull() {
        long storeTime = getStoreTime(null);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void test_getStoreTime_success() throws Exception {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());
        ConsumeQueue consumeQueue = messageStore.getConsumeQueue(topic, queueId);

        for (int i = 0; i < totalCount; i++) {
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(i);
            long storeTime = getStoreTime(indexBuffer);
            assertThat(storeTime).isEqualTo(appendMessageResults[i].getStoreTimestamp());
            indexBuffer.release();
        }
    }

    @Test
    public void test_getStoreTime_phyOffsetIsLessThanCommitLogMinOffset() {
        long phyOffset = -10;
        int size = 138;
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        byteBuffer.putLong(phyOffset);
        byteBuffer.putInt(size);
        byteBuffer.flip();
        MappedFile mappedFile = mock(MappedFile.class);
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, size, mappedFile);

        long storeTime = getStoreTime(result);
        result.release();

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void test_getMessage_success() throws Exception {
        String topic = "pullSizeTopic";
        int queueId = 0;
        String group = "simple";
        putMessages(32, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        GetMessageResult getMessageResult32 = messageStore.getMessage(group, topic, 0, 0, 32, null);
        GetMessageResult getMessageResult20 = messageStore.getMessage(group, topic, 0, 0, 20, null);
        GetMessageResult getMessageResult45 = messageStore.getMessage(group, topic, 0, 0, 10, null);

        assertThat(getMessageResult32.getMessageBufferList().size()).isEqualTo(32);
        assertThat(getMessageResult20.getMessageBufferList().size()).isEqualTo(20);
        assertThat(getMessageResult45.getMessageBufferList().size()).isEqualTo(10);
    }

    @Test
    public void test_recover_justRestart() throws Exception {
        String topic = "recoverTopic";
        int queueId = 0;
        int messageAmount = 100;
        AppendMessageResult[] appendMessageResults = putMessages(messageAmount, topic, queueId);
        long maxOffset = getMaxOffset(appendMessageResults);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());

        long lastPhyOffset = messageStore.getMaxPhyOffset();
        long lastCqOffset = messageStore.getMaxOffsetInQueue(topic, queueId);

        assertThat(lastPhyOffset).isEqualTo(maxOffset);
        assertThat(lastCqOffset).isEqualTo(messageAmount);

        //1.just reboot
        messageStore.shutdown();
        messageStore = buildMessageStore();
        assertTrue(messageStore.load());
        messageStore.start();

        assertThat(messageStore.getMaxPhyOffset()).isEqualTo(maxOffset);
        assertThat(messageStore.getMaxOffsetInQueue(topic, queueId)).isEqualTo(messageAmount);
    }

    @Test
    public void test_recover_damageLastMessage() throws Exception {
        String topic = "recoverTopic";
        int queueId = 0;
        int messageAmount = 100;
        AppendMessageResult[] appendMessageResults = putMessages(messageAmount + 1, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());
        long lastPhyOffset = getMessageOffset(appendMessageResults, messageAmount - 1);

        messageStore.shutdown();
        damageCommitLog(lastPhyOffset);
        messageStore = buildMessageStore();
        assertTrue(messageStore.load());
        messageStore.start();

        assertThat(messageStore.getMaxPhyOffset()).isEqualTo(lastPhyOffset);
        assertThat(messageStore.getMaxOffsetInQueue(topic, queueId)).isEqualTo(messageAmount);
    }

    @Test
    public void test_recover_abort() throws Exception {
        String topic = "recoverTopic";
        int queueId = 0;
        int messageAmount = 100;

        AppendMessageResult[] appendMessageResults = putMessages(messageAmount + 1, topic, queueId);
        StoreTestUtil.flushConsumeQueue(getDefaultMessageStore());
        long lastPhyOffset = getMessageOffset(appendMessageResults, messageAmount - 1);

        messageStore.shutdown();

        //damage last message
        damageCommitLog(lastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        file.createNewFile();

        messageStore = buildMessageStore();
        assertTrue(messageStore.load());
        messageStore.start();
        assertThat(messageStore.getMaxPhyOffset()).isEqualTo(lastPhyOffset);
        assertThat(messageStore.getMaxOffsetInQueue(topic, queueId)).isEqualTo(messageAmount);

        AppendMessageResult[] results = putMessages(1, topic, queueId);
        assertThat(results[0].getStatus()).isEqualTo(AppendMessageStatus.PUT_OK);
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

    private DefaultMessageStore getDefaultMessageStore() {
        return (DefaultMessageStore)this.messageStore;
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId) {
        return putMessages(totalCount, topic, queueId, 1);
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId, int millis) {
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);
            MessageExtBrokerInner msgInner = buildMessage(messageBody.getBytes(), topic);
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return appendMessageResultArray;
    }

    private long getMaxOffset(AppendMessageResult[] appendMessageResultArray) {
        int lastIndex = appendMessageResultArray.length - 1;
        return getMessageOffset(appendMessageResultArray, lastIndex);
    }

    private long getMessageOffset(AppendMessageResult[] appendMessageResultArray, int index) {
        if (appendMessageResultArray == null) {
            return 0;
        }
        AppendMessageResult last = appendMessageResultArray[index];
        return last.getWroteOffset() + last.getWroteBytes();
    }

    private String buildMessageBodyByOffset(String message, long i) {
        return String.format("%s offset %d", message, i);
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        try {
            Method getStoreTime = getDefaultMessageStore().getClass().getDeclaredMethod("getStoreTime", SelectMappedBufferResult.class);
            getStoreTime.setAccessible(true);
            return (long)getStoreTime.invoke(getDefaultMessageStore(), result);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageExtBrokerInner buildMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
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
        return buildMessage(MessageBody, "FooBar");
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

    private void damageCommitLog(long offset) throws Exception {
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
