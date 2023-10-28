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

import com.google.common.collect.Sets;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.assertj.core.util.Strings;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageStoreTest {
    private final String storeMessage = "Once, there was a chance for me!";
    private final String messageTopic = "FooBar";
    private int queueTotal = 100;
    private AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @Test(expected = OverlappingFileLockException.class)
    public void test_repeat_restart() throws Exception {
        queueTotal = 1;
        messageBody = storeMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "store");
        messageStoreConfig.setHaListenPort(0);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());

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
        return buildMessageStore(null);
    }

    private MessageStore buildMessageStore(String storePathRootDir) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setHaListenPort(0);
        if (Strings.isNullOrEmpty(storePathRootDir)) {
            UUID uuid = UUID.randomUUID();
            storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid.toString();
        }
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        return new DefaultMessageStore(messageStoreConfig,
            new BrokerStatsManager("simpleTest", true),
            new MyMessageArrivingListener(),
            new BrokerConfig(), new ConcurrentHashMap<>());
    }

    @Test
    public void testWriteAndRead() {
        long ipv4HostMsgs = 10;
        long ipv6HostMsgs = 10;
        long totalMsgs = ipv4HostMsgs + ipv6HostMsgs;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < ipv4HostMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < ipv6HostMsgs; i++) {
            messageStore.putMessage(buildIPv6HostMessage());
        }

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMsgs, messageStore);
    }

    @Test
    public void testLookMessageByOffset_OffsetIsFirst() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        int firstOffset = 0;
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        AppendMessageResult firstResult = appendMessageResultArray[0];

        MessageExt messageExt = messageStore.lookMessageByOffset(firstResult.getWroteOffset());
        MessageExt messageExt1 = getDefaultMessageStore().lookMessageByOffset(firstResult.getWroteOffset(), firstResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(storeMessage, firstOffset));
        assertThat(new String(messageExt1.getBody())).isEqualTo(buildMessageBodyByOffset(storeMessage, firstOffset));
    }

    @Test
    public void testLookMessageByOffset_OffsetIsLast() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        int lastIndex = totalCount - 1;
        AppendMessageResult lastResult = appendMessageResultArray[lastIndex];

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastResult.getWroteOffset(), lastResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(storeMessage, lastIndex));
    }

    @Test
    public void testLookMessageByOffset_OffsetIsOutOfBound() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        long lastOffset = getMaxOffset(appendMessageResultArray);

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastOffset);

        assertThat(messageExt).isNull();
    }

    @Test
    public void testGetOffsetInQueueByTime() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        ConsumeQueueInterface consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp());
            CqUnit cqUnit = consumeQueue.get(offset);
            assertThat(cqUnit.getPos()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(cqUnit.getSize()).isEqualTo(appendMessageResult.getWroteBytes());
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_TimestampIsSkewing() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        int skewing = 2;

        ConsumeQueueInterface consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            CqUnit cqUnit = consumeQueue.get(offset);
            assertThat(cqUnit.getPos()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(cqUnit.getSize()).isEqualTo(appendMessageResult.getWroteBytes());
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_TimestampSkewingIsLarge() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        int skewing = 20000;

        ConsumeQueueInterface consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            CqUnit cqUnit = consumeQueue.get(offset);
            assertThat(cqUnit.getPos()).isEqualTo(appendMessageResults[0].getWroteOffset());
            assertThat(cqUnit.getSize()).isEqualTo(appendMessageResults[0].getWroteBytes());
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueNotFound1() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long offset = messageStore.getOffsetInQueueByTime(topic, wrongQueueId, appendMessageResults[0].getStoreTimestamp());

        assertThat(offset).isEqualTo(0);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueNotFound2() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, 0);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueOffsetNotExist() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, -1);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetMessageStoreTimeStamp() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        ConsumeQueueInterface consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        int minOffsetInQueue = (int) consumeQueue.getMinOffsetInQueue();
        for (int i = minOffsetInQueue; i < consumeQueue.getMaxOffsetInQueue(); i++) {
            long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, queueId, i);
            assertThat(messageStoreTimeStamp).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void testGetStoreTime_ParamIsNull() {
        long storeTime = getStoreTime(null);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void testGetStoreTime_EverythingIsOk() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, queueId);

        for (int i = 0; i < totalCount; i++) {
            CqUnit cqUnit = consumeQueue.get(i);
            long storeTime = getStoreTime(cqUnit);
            assertThat(storeTime).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void testGetStoreTime_PhyOffsetIsLessThanCommitLogMinOffset() {
        long phyOffset = -10;
        int size = 138;
        CqUnit cqUnit = new CqUnit(0, phyOffset, size, 0);
        long storeTime = getStoreTime(cqUnit);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void testPutMessage_whenMessagePropertyIsTooLong() {
        String topicName = "messagePropertyIsTooLongTest";
        MessageExtBrokerInner illegalMessage = buildSpecifyLengthPropertyMessage("123".getBytes(StandardCharsets.UTF_8), topicName, Short.MAX_VALUE + 1);
        assertEquals(messageStore.putMessage(illegalMessage).getPutMessageStatus(), PutMessageStatus.PROPERTIES_SIZE_EXCEEDED);
        assertEquals(0L, messageStore.getQueueStore().getMaxOffset(topicName, 0).longValue());
        MessageExtBrokerInner normalMessage = buildSpecifyLengthPropertyMessage("123".getBytes(StandardCharsets.UTF_8), topicName, 100);
        assertEquals(messageStore.putMessage(normalMessage).getPutMessageStatus(), PutMessageStatus.PUT_OK);
        assertEquals(1L, messageStore.getQueueStore().getMaxOffset(topicName, 0).longValue());
    }

    private DefaultMessageStore getDefaultMessageStore() {
        return (DefaultMessageStore) this.messageStore;
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId) {
        return putMessages(totalCount, topic, queueId, false);
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId, boolean interval) {
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(storeMessage, i);

            MessageExtBrokerInner msgInner =
                i < totalCount / 2 ? buildMessage(messageBody.getBytes(), topic) : buildIPv6HostMessage(messageBody.getBytes(), topic);
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
            if (interval) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread sleep ERROR");
                }
            }
        }
        return appendMessageResultArray;
    }

    private long getMaxOffset(AppendMessageResult[] appendMessageResultArray) {
        if (appendMessageResultArray == null) {
            return 0;
        }
        AppendMessageResult last = appendMessageResultArray[appendMessageResultArray.length - 1];
        return last.getWroteOffset() + last.getWroteBytes();
    }

    private String buildMessageBodyByOffset(String message, long i) {
        return String.format("%s offset %d", message, i);
    }

    private long getStoreTime(CqUnit cqUnit) {
        try {
            Class abstractConsumeQueueStore = getDefaultMessageStore().getQueueStore().getClass().getSuperclass();
            Method getStoreTime = abstractConsumeQueueStore.getDeclaredMethod("getStoreTime", CqUnit.class);
            getStoreTime.setAccessible(true);
            return (long) getStoreTime.invoke(getDefaultMessageStore().getQueueStore(), cqUnit);
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
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private MessageExtBrokerInner buildSpecifyLengthPropertyMessage(byte[] messageBody, String topic, int length) {
        StringBuilder stringBuilder = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            stringBuilder.append(random.nextInt(10));
        }
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.putUserProperty("test", stringBuilder.toString());
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private MessageExtBrokerInner buildIPv6HostMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        try {
            msg.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        try {
            msg.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        return msg;
    }

    private MessageExtBrokerInner buildMessage() {
        return buildMessage(messageBody, messageTopic);
    }

    public MessageExtBatch buildMessageBatch(MessageBatch msgBatch) {
        MessageExtBatch msgExtBatch = new MessageExtBatch();
        msgExtBatch.setTopic(messageTopic);
        msgExtBatch.setTags("TAG1");
        msgExtBatch.setKeys("Hello");
        msgExtBatch.setBody(msgBatch.getBody());
        msgExtBatch.setKeys(String.valueOf(System.currentTimeMillis()));
        msgExtBatch.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msgExtBatch.setSysFlag(0);
        msgExtBatch.setBornTimestamp(System.currentTimeMillis());
        msgExtBatch.setStoreHost(storeHost);
        msgExtBatch.setBornHost(bornHost);
        return msgExtBatch;
    }

    @Test
    public void testGroupCommit() throws Exception {
        long totalMsgs = 10;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
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
    public void testMaxOffset() throws InterruptedException {
        int firstBatchMessages = 3;
        int queueId = 0;
        messageBody = storeMessage.getBytes();

        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId)).isEqualTo(0);

        for (int i = 0; i < firstBatchMessages; i++) {
            final MessageExtBrokerInner msg = buildMessage();
            msg.setQueueId(queueId);
            messageStore.putMessage(msg);
        }

        while (messageStore.dispatchBehindBytes() != 0) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId)).isEqualTo(firstBatchMessages);

        // Disable the dispatcher
        messageStore.getDispatcherList().clear();

        int secondBatchMessages = 2;

        for (int i = 0; i < secondBatchMessages; i++) {
            final MessageExtBrokerInner msg = buildMessage();
            msg.setQueueId(queueId);
            messageStore.putMessage(msg);
        }

        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId)).isEqualTo(firstBatchMessages);
        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId, true)).isEqualTo(firstBatchMessages);
        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId, false)).isEqualTo(firstBatchMessages + secondBatchMessages);
    }

    private MessageExtBrokerInner buildIPv6HostMessage() {
        return buildIPv6HostMessage(messageBody, "FooBar");
    }

    private void verifyThatMasterIsFunctional(long totalMsgs, MessageStore master) {
        for (long i = 0; i < totalMsgs; i++) {
            master.putMessage(buildMessage());
        }

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
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
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        String group = "simple";
        GetMessageResult getMessageResult32 = messageStore.getMessage(group, topic, 0, 0, 32, null);
        assertThat(getMessageResult32.getMessageBufferList().size()).isEqualTo(32);
        getMessageResult32.release();

        GetMessageResult getMessageResult20 = messageStore.getMessage(group, topic, 0, 0, 20, null);
        assertThat(getMessageResult20.getMessageBufferList().size()).isEqualTo(20);

        getMessageResult20.release();
        GetMessageResult getMessageResult45 = messageStore.getMessage(group, topic, 0, 0, 10, null);
        assertThat(getMessageResult45.getMessageBufferList().size()).isEqualTo(10);
        getMessageResult45.release();

    }

    @Test
    public void testRecover() throws Exception {
        String topic = "recoverTopic";
        messageBody = storeMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        // Thread.sleep(100);//wait for build consumer queue
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long maxPhyOffset = messageStore.getMaxPhyOffset();
        long maxCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        //1.just reboot
        messageStore.shutdown();
        String storeRootDir = ((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir();
        messageStore = buildMessageStore(storeRootDir);
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(maxPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(maxCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //2.damage commit-log and reboot normal
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        long secondLastPhyOffset = messageStore.getMaxPhyOffset();
        long secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);

        messageStore.shutdown();

        //damage last message
        damageCommitLog((DefaultMessageStore) messageStore, secondLastPhyOffset);

        //reboot
        messageStore = buildMessageStore(storeRootDir);
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
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        secondLastPhyOffset = messageStore.getMaxPhyOffset();
        secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);
        messageStore.shutdown();

        //damage last message
        damageCommitLog((DefaultMessageStore) messageStore, secondLastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        UtilAll.ensureDirOK(file.getParent());
        file.createNewFile();

        messageStore = buildMessageStore(storeRootDir);
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

    @Test
    public void testStorePathOK() {
        if (messageStore instanceof DefaultMessageStore) {
            assertTrue(fileExists(((DefaultMessageStore) messageStore).getStorePathPhysic()));
            assertTrue(fileExists(((DefaultMessageStore) messageStore).getStorePathLogic()));
        }
    }

    private boolean fileExists(String path) {
        if (path != null) {
            File f = new File(path);
            return f.exists();
        }
        return false;
    }

    private void damageCommitLog(DefaultMessageStore store, long offset) throws Exception {
        assertThat(store).isNotNull();
        MessageStoreConfig messageStoreConfig = store.getMessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathCommitLog() + File.separator + "00000000000000000000");
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = raf.getChannel()) {
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
        }
    }

    @Test
    public void testPutMsgExceedsMaxLength() {
        messageBody = new byte[4 * 1024 * 1024 + 1];
        MessageExtBrokerInner msg = buildMessage();

        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.MESSAGE_ILLEGAL);
    }

    @Test
    public void testPutMsgBatchExceedsMaxLength() {
        messageBody = new byte[4 * 1024 * 1024 + 1];
        MessageExtBrokerInner msg1 = buildMessage();
        MessageExtBrokerInner msg2 = buildMessage();
        MessageExtBrokerInner msg3 = buildMessage();

        MessageBatch msgBatch = MessageBatch.generateFromList(Arrays.asList(msg1, msg2, msg3));
        msgBatch.setBody(msgBatch.encode());

        MessageExtBatch msgExtBatch = buildMessageBatch(msgBatch);

        try {
            PutMessageResult result = this.messageStore.putMessages(msgExtBatch);
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("message body size exceeded");
        }
    }

    @Test
    public void testPutMsgWhenReplicasNotEnough() {
        MessageStoreConfig messageStoreConfig = ((DefaultMessageStore) this.messageStore).getMessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        messageStoreConfig.setTotalReplicas(2);
        messageStoreConfig.setInSyncReplicas(2);
        messageStoreConfig.setEnableAutoInSyncReplicas(false);
        ((DefaultMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        this.messageStore.setAliveReplicaNumInGroup(1);

        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult result = this.messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH);
        ((DefaultMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(false);
    }

    @Test
    public void testPutMsgWhenAdaptiveDegradation() {
        MessageStoreConfig messageStoreConfig = ((DefaultMessageStore) this.messageStore).getMessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        messageStoreConfig.setTotalReplicas(2);
        messageStoreConfig.setInSyncReplicas(2);
        messageStoreConfig.setEnableAutoInSyncReplicas(true);
        ((DefaultMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        this.messageStore.setAliveReplicaNumInGroup(1);

        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult result = this.messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
        ((DefaultMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(false);
        messageStoreConfig.setEnableAutoInSyncReplicas(false);
    }

    @Test
    public void testGetBulkCommitLogData() {
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;

        messageBody = new byte[2 * 1024 * 1024];

        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msg1 = buildMessage();
            messageStore.putMessage(msg1);
        }

        System.out.printf("%d%n", defaultMessageStore.getMaxPhyOffset());

        List<SelectMappedBufferResult> bufferResultList = defaultMessageStore.getBulkCommitLogData(0, (int) defaultMessageStore.getMaxPhyOffset());
        List<MessageExt> msgList = new ArrayList<>();
        for (SelectMappedBufferResult bufferResult : bufferResultList) {
            msgList.addAll(MessageDecoder.decodesBatch(bufferResult.getByteBuffer(), true, false, false));
            bufferResult.release();
        }

        assertThat(msgList.size()).isEqualTo(10);
    }

    @Test
    public void testPutLongMessage() throws Exception {
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        CommitLog commitLog = ((DefaultMessageStore) messageStore).getCommitLog();
        MessageStoreConfig messageStoreConfig = ((DefaultMessageStore) messageStore).getMessageStoreConfig();
        MessageExtEncoder.PutMessageThreadLocal putMessageThreadLocal = commitLog.getPutMessageThreadLocal().get();

        //body size, topic size, properties size exactly equal to max size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setTopic(new String(new byte[127]));
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE]));
        PutMessageResult encodeResult1 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertTrue(encodeResult1 == null);

        //body size exactly more than max message body size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize() + 1]);
        PutMessageResult encodeResult2 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertTrue(encodeResult2.getPutMessageStatus() == PutMessageStatus.MESSAGE_ILLEGAL);

        //body size exactly equal to max message size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize() + 64 * 1024]);
        PutMessageResult encodeResult3 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertTrue(encodeResult3.getPutMessageStatus() == PutMessageStatus.MESSAGE_ILLEGAL);

        //message properties length more than properties maxSize
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE + 1]));
        PutMessageResult encodeResult4 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertTrue(encodeResult4.getPutMessageStatus() == PutMessageStatus.PROPERTIES_SIZE_EXCEEDED);

        //message length more than buffer length capacity
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setTopic(new String(new byte[Short.MAX_VALUE]));
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE]));
        PutMessageResult encodeResult5 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertTrue(encodeResult5.getPutMessageStatus() == PutMessageStatus.MESSAGE_ILLEGAL);
    }

    @Test
    public void testDynamicMaxMessageSize() {
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        MessageStoreConfig messageStoreConfig = ((DefaultMessageStore) messageStore).getMessageStoreConfig();
        int originMaxMessageSize = messageStoreConfig.getMaxMessageSize();

        messageExtBrokerInner.setBody(new byte[originMaxMessageSize + 10]);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertTrue(putMessageResult.getPutMessageStatus() == PutMessageStatus.MESSAGE_ILLEGAL);

        int newMaxMessageSize = originMaxMessageSize + 10;
        messageStoreConfig.setMaxMessageSize(newMaxMessageSize);
        putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertTrue(putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK);

        messageStoreConfig.setMaxMessageSize(10);
        putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertTrue(putMessageResult.getPutMessageStatus() == PutMessageStatus.MESSAGE_ILLEGAL);

        messageStoreConfig.setMaxMessageSize(originMaxMessageSize);
    }

    @Test
    public void testDeleteTopics() {
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable =
            ((DefaultMessageStore) messageStore).getConsumeQueueTable();
        for (int i = 0; i < 10; i++) {
            ConcurrentMap<Integer, ConsumeQueueInterface> cqTable = new ConcurrentHashMap<>();
            String topicName = "topic-" + i;
            for (int j = 0; j < 4; j++) {
                ConsumeQueue consumeQueue = new ConsumeQueue(topicName, j, messageStoreConfig.getStorePathRootDir(),
                    messageStoreConfig.getMappedFileSizeConsumeQueue(), messageStore);
                cqTable.put(j, consumeQueue);
            }
            consumeQueueTable.put(topicName, cqTable);
        }
        Assert.assertEquals(consumeQueueTable.size(), 10);
        HashSet<String> resultSet = Sets.newHashSet("topic-3", "topic-5");
        messageStore.deleteTopics(Sets.difference(consumeQueueTable.keySet(), resultSet));
        Assert.assertEquals(consumeQueueTable.size(), 2);
        Assert.assertEquals(resultSet, consumeQueueTable.keySet());
    }

    @Test
    public void testCleanUnusedTopic() {
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable =
            ((DefaultMessageStore) messageStore).getConsumeQueueTable();
        for (int i = 0; i < 10; i++) {
            ConcurrentMap<Integer, ConsumeQueueInterface> cqTable = new ConcurrentHashMap<>();
            String topicName = "topic-" + i;
            for (int j = 0; j < 4; j++) {
                ConsumeQueue consumeQueue = new ConsumeQueue(topicName, j, messageStoreConfig.getStorePathRootDir(),
                    messageStoreConfig.getMappedFileSizeConsumeQueue(), messageStore);
                cqTable.put(j, consumeQueue);
            }
            consumeQueueTable.put(topicName, cqTable);
        }
        Assert.assertEquals(consumeQueueTable.size(), 10);
        HashSet<String> resultSet = Sets.newHashSet("topic-3", "topic-5");
        messageStore.cleanUnusedTopic(resultSet);
        Assert.assertEquals(consumeQueueTable.size(), 2);
        Assert.assertEquals(resultSet, consumeQueueTable.keySet());
    }

    @Test
    public void testChangeStoreConfig() {
        Properties properties = new Properties();
        properties.setProperty("enableBatchPush", "true");
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        MixAll.properties2Object(properties, messageStoreConfig);
        assertThat(messageStoreConfig.isEnableBatchPush()).isTrue();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
