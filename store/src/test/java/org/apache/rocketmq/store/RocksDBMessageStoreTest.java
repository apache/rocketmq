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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueue;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.assertj.core.util.Strings;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class RocksDBMessageStoreTest {
    private final String storeMessage = "Once, there was a chance for me!";
    private final String messageTopic = "FooBar";
    private final String storeType = StoreType.DEFAULT_ROCKSDB.getStoreType();
    private int queueTotal = 100;
    private final AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        if (notExecuted()) {
            return;
        }
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @Test(expected = OverlappingFileLockException.class)
    public void test_repeat_restart() throws Exception {
        if (notExecuted()) {
            throw new OverlappingFileLockException();
        }
        queueTotal = 1;
        messageBody = storeMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir") + File.separator + "store");
        messageStoreConfig.setHaListenPort(0);
        MessageStore master = new RocksDBMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());

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
        if (notExecuted()) {
            return;
        }
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore() throws Exception {
        return buildMessageStore(null, "");
    }

    private MessageStore buildMessageStore(String storePathRootDir, String topic) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setStoreType(storeType);
        messageStoreConfig.setHaListenPort(0);
        if (Strings.isNullOrEmpty(storePathRootDir)) {
            UUID uuid = UUID.randomUUID();
            storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid.toString();
        }
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        topicConfigTable.put(topic, new TopicConfig(topic, 1, 1));
        return new RocksDBMessageStore(messageStoreConfig,
            new BrokerStatsManager("simpleTest", true),
            new MyMessageArrivingListener(),
            new BrokerConfig(), topicConfigTable);
    }

    @Test
    public void testWriteAndRead() {
        if (notExecuted()) {
            return;
        }
        long ipv4HostMessages = 10;
        long ipv6HostMessages = 10;
        long totalMessages = ipv4HostMessages + ipv6HostMessages;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < ipv4HostMessages; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < ipv6HostMessages; i++) {
            messageStore.putMessage(buildIPv6HostMessage());
        }

        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        for (long i = 0; i < totalMessages; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMessages, messageStore);
    }

    @Test
    public void testLookMessageByOffset_OffsetIsFirst() {
        if (notExecuted()) {
            return;
        }
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
        if (notExecuted()) {
            return;
        }
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
        if (notExecuted()) {
            return;
        }
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
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

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
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);
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
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);
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
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        long offset = messageStore.getOffsetInQueueByTime(topic, wrongQueueId, appendMessageResults[0].getStoreTimestamp());

        assertThat(offset).isEqualTo(0);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueNotFound2() {
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, 0);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueOffsetNotExist() {
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, -1);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetMessageStoreTimeStamp() {
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        ConsumeQueueInterface consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        int minOffsetInQueue = (int) consumeQueue.getMinOffsetInQueue();
        for (int i = minOffsetInQueue; i < consumeQueue.getMaxOffsetInQueue(); i++) {
            long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, queueId, i);
            assertThat(messageStoreTimeStamp).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void testGetStoreTime_ParamIsNull() {
        if (notExecuted()) {
            return;
        }
        long storeTime = getStoreTime(null);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void testGetStoreTime_EverythingIsOk() {
        if (notExecuted()) {
            return;
        }
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);
        ConsumeQueueInterface consumeQueue = messageStore.getConsumeQueue(topic, queueId);

        for (int i = 0; i < totalCount; i++) {
            CqUnit cqUnit = consumeQueue.get(i);
            long storeTime = getStoreTime(cqUnit);
            assertThat(storeTime).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void testGetStoreTime_PhyOffsetIsLessThanCommitLogMinOffset() {
        if (notExecuted()) {
            return;
        }
        long phyOffset = -10;
        int size = 138;
        CqUnit cqUnit = new CqUnit(0, phyOffset, size, 0);
        long storeTime = getStoreTime(cqUnit);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void testPutMessage_whenMessagePropertyIsTooLong() throws ConsumeQueueException {
        if (notExecuted()) {
            return;
        }
        String topicName = "messagePropertyIsTooLongTest";
        MessageExtBrokerInner illegalMessage = buildSpecifyLengthPropertyMessage("123".getBytes(StandardCharsets.UTF_8), topicName, Short.MAX_VALUE + 1);
        assertEquals(messageStore.putMessage(illegalMessage).getPutMessageStatus(), PutMessageStatus.PROPERTIES_SIZE_EXCEEDED);
        assertEquals(0L, messageStore.getQueueStore().getMaxOffset(topicName, 0).longValue());
        MessageExtBrokerInner normalMessage = buildSpecifyLengthPropertyMessage("123".getBytes(StandardCharsets.UTF_8), topicName, 100);
        assertEquals(messageStore.putMessage(normalMessage).getPutMessageStatus(), PutMessageStatus.PUT_OK);
        assertEquals(1L, messageStore.getQueueStore().getMaxOffset(topicName, 0).longValue());
    }

    private RocksDBMessageStore getDefaultMessageStore() {
        return (RocksDBMessageStore) this.messageStore;
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
            fail("build IPv6 host message error", e);
        }

        try {
            msg.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            fail("build IPv6 host message error", e);
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
    public void testGroupCommit() {
        if (notExecuted()) {
            return;
        }
        long totalMessages = 10;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < totalMessages; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < totalMessages; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMessages, messageStore);
    }

    @Test
    public void testMaxOffset() throws ConsumeQueueException {
        if (notExecuted()) {
            return;
        }
        int firstBatchMessages = 3;
        int queueId = 0;
        messageBody = storeMessage.getBytes();

        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, queueId)).isEqualTo(0);

        for (int i = 0; i < firstBatchMessages; i++) {
            final MessageExtBrokerInner msg = buildMessage();
            msg.setQueueId(queueId);
            messageStore.putMessage(msg);
        }

        Awaitility.await()
                .with()
                .atMost(3, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .until(() -> messageStore.getMaxOffsetInQueue(messageTopic, queueId) == firstBatchMessages);

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

    private void verifyThatMasterIsFunctional(long totalMessages, MessageStore master) {
        for (long i = 0; i < totalMessages; i++) {
            master.putMessage(buildMessage());
        }

        StoreTestUtil.waitCommitLogReput((RocksDBMessageStore) messageStore);

        for (long i = 0; i < totalMessages; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();

        }
    }

    @Test
    public void testPullSize() {
        if (notExecuted()) {
            return;
        }
        String topic = "pullSizeTopic";

        for (int i = 0; i < 32; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        // wait for consume queue build
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .with()
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> messageStore.getMaxOffsetInQueue(topic, 0) >= 32);

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
        if (notExecuted()) {
            return;
        }
        String topic = "recoverTopic";
        messageBody = storeMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        // wait for build consumer queue
        Awaitility.await()
                .with()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> messageStore.getMaxOffsetInQueue(topic, 0) >= 100);

        long maxPhyOffset = messageStore.getMaxPhyOffset();
        long maxCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        //1.just reboot
        messageStore.shutdown();
        String storeRootDir = messageStore.getMessageStoreConfig().getStorePathRootDir();
        messageStore = buildMessageStore(storeRootDir, topic);
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertEquals(maxPhyOffset, messageStore.getMaxPhyOffset());
        assertEquals(maxCqOffset, messageStore.getMaxOffsetInQueue(topic, 0));

        //2.damage commit-log and reboot normal
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        Awaitility.await()
                .with()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> messageStore.getMaxOffsetInQueue(topic, 0) >= 200);

        long secondLastPhyOffset = messageStore.getMaxPhyOffset();
        long secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        // Append a message to corrupt
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);

        messageStore.shutdown();

        // Corrupt the last message
        damageCommitLog((RocksDBMessageStore) messageStore, secondLastPhyOffset);

        //reboot
        messageStore = buildMessageStore(storeRootDir, topic);
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertEquals(secondLastPhyOffset, messageStore.getMaxPhyOffset());
        assertEquals(secondLastCqOffset, messageStore.getMaxOffsetInQueue(topic, 0));

        //3.Corrupt commit-log and reboot abnormal
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        Awaitility.await()
                .with()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> messageStore.getMaxOffsetInQueue(topic, 0) >= 300);

        secondLastPhyOffset = messageStore.getMaxPhyOffset();
        secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);
        messageStore.shutdown();

        //Corrupt the last message
        damageCommitLog((RocksDBMessageStore) messageStore, secondLastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(messageStore.getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        UtilAll.ensureDirOK(file.getParent());
        assertTrue(file.createNewFile());

        messageStore = buildMessageStore(storeRootDir, topic);
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertEquals(secondLastPhyOffset, messageStore.getMaxPhyOffset());
        assertEquals(secondLastCqOffset, messageStore.getMaxOffsetInQueue(topic, 0));

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
        if (notExecuted()) {
            return;
        }
        if (messageStore instanceof RocksDBMessageStore) {
            assertTrue(fileExists(((RocksDBMessageStore) messageStore).getStorePathPhysic()));
            assertTrue(fileExists(((RocksDBMessageStore) messageStore).getStorePathLogic()));
        }
    }

    private boolean fileExists(String path) {
        if (path != null) {
            File f = new File(path);
            return f.exists();
        }
        return false;
    }

    private void damageCommitLog(RocksDBMessageStore store, long offset) throws Exception {
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
        if (notExecuted()) {
            return;
        }
        messageBody = new byte[4 * 1024 * 1024 + 1];
        MessageExtBrokerInner msg = buildMessage();

        PutMessageResult result = messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.MESSAGE_ILLEGAL);
    }

    @Test
    public void testPutMsgBatchExceedsMaxLength() {
        if (notExecuted()) {
            return;
        }
        messageBody = new byte[4 * 1024 * 1024 + 1];
        MessageExtBrokerInner msg1 = buildMessage();
        MessageExtBrokerInner msg2 = buildMessage();
        MessageExtBrokerInner msg3 = buildMessage();

        MessageBatch msgBatch = MessageBatch.generateFromList(Arrays.asList(msg1, msg2, msg3));
        msgBatch.setBody(msgBatch.encode());

        MessageExtBatch msgExtBatch = buildMessageBatch(msgBatch);

        try {
            this.messageStore.putMessages(msgExtBatch);
            fail("Should have raised an exception");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("message body size exceeded");
        }
    }

    @Test
    public void testPutMsgWhenReplicasNotEnough() {
        if (notExecuted()) {
            return;
        }
        MessageStoreConfig messageStoreConfig = this.messageStore.getMessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        messageStoreConfig.setTotalReplicas(2);
        messageStoreConfig.setInSyncReplicas(2);
        messageStoreConfig.setEnableAutoInSyncReplicas(false);
        ((RocksDBMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        this.messageStore.setAliveReplicaNumInGroup(1);

        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult result = this.messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH);
        ((RocksDBMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(false);
    }

    @Test
    public void testPutMsgWhenAdaptiveDegradation() {
        if (notExecuted()) {
            return;
        }
        MessageStoreConfig messageStoreConfig = this.messageStore.getMessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        messageStoreConfig.setTotalReplicas(2);
        messageStoreConfig.setInSyncReplicas(2);
        messageStoreConfig.setEnableAutoInSyncReplicas(true);
        ((RocksDBMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(true);
        this.messageStore.setAliveReplicaNumInGroup(1);

        MessageExtBrokerInner msg = buildMessage();
        PutMessageResult result = this.messageStore.putMessage(msg);
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
        ((RocksDBMessageStore) this.messageStore).getBrokerConfig().setEnableSlaveActingMaster(false);
        messageStoreConfig.setEnableAutoInSyncReplicas(false);
    }

    @Test
    public void testGetBulkCommitLogData() {
        if (notExecuted()) {
            return;
        }
        RocksDBMessageStore defaultMessageStore = (RocksDBMessageStore) messageStore;

        messageBody = new byte[2 * 1024 * 1024];

        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msg1 = buildMessage();
            messageStore.putMessage(msg1);
        }

        List<SelectMappedBufferResult> bufferResultList = defaultMessageStore.getBulkCommitLogData(0, (int) defaultMessageStore.getMaxPhyOffset());
        List<MessageExt> msgList = new ArrayList<>();
        for (SelectMappedBufferResult bufferResult : bufferResultList) {
            msgList.addAll(MessageDecoder.decodesBatch(bufferResult.getByteBuffer(), true, false, false));
            bufferResult.release();
        }

        assertThat(msgList.size()).isEqualTo(10);
    }

    @Test
    public void testPutLongMessage() {
        if (notExecuted()) {
            return;
        }
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        CommitLog commitLog = messageStore.getCommitLog();
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        MessageExtEncoder.PutMessageThreadLocal putMessageThreadLocal = commitLog.getPutMessageThreadLocal().get();

        //body size, topic size, properties size exactly equal to max size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setTopic(new String(new byte[127]));
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE]));
        PutMessageResult encodeResult1 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertNull(encodeResult1);

        //body size exactly more than max message body size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize() + 1]);
        PutMessageResult encodeResult2 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertSame(encodeResult2.getPutMessageStatus(), PutMessageStatus.MESSAGE_ILLEGAL);

        //body size exactly equal to max message size
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize() + 64 * 1024]);
        PutMessageResult encodeResult3 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertSame(encodeResult3.getPutMessageStatus(), PutMessageStatus.MESSAGE_ILLEGAL);

        //message properties length more than properties maxSize
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE + 1]));
        PutMessageResult encodeResult4 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertSame(encodeResult4.getPutMessageStatus(), PutMessageStatus.PROPERTIES_SIZE_EXCEEDED);

        //message length more than buffer length capacity
        messageExtBrokerInner.setBody(new byte[messageStoreConfig.getMaxMessageSize()]);
        messageExtBrokerInner.setTopic(new String(new byte[Short.MAX_VALUE]));
        messageExtBrokerInner.setPropertiesString(new String(new byte[Short.MAX_VALUE]));
        PutMessageResult encodeResult5 = putMessageThreadLocal.getEncoder().encode(messageExtBrokerInner);
        assertSame(encodeResult5.getPutMessageStatus(), PutMessageStatus.MESSAGE_ILLEGAL);
    }

    @Test
    public void testDynamicMaxMessageSize() {
        if (notExecuted()) {
            return;
        }
        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        int originMaxMessageSize = messageStoreConfig.getMaxMessageSize();

        messageExtBrokerInner.setBody(new byte[originMaxMessageSize + 10]);
        PutMessageResult putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertSame(putMessageResult.getPutMessageStatus(), PutMessageStatus.MESSAGE_ILLEGAL);

        int newMaxMessageSize = originMaxMessageSize + 10;
        messageStoreConfig.setMaxMessageSize(newMaxMessageSize);
        putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertSame(putMessageResult.getPutMessageStatus(), PutMessageStatus.PUT_OK);

        messageStoreConfig.setMaxMessageSize(10);
        putMessageResult = messageStore.putMessage(messageExtBrokerInner);
        assertSame(putMessageResult.getPutMessageStatus(), PutMessageStatus.MESSAGE_ILLEGAL);

        messageStoreConfig.setMaxMessageSize(originMaxMessageSize);
    }

    @Test
    public void testDeleteTopics() {
        if (notExecuted()) {
            return;
        }
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable =
            ((RocksDBMessageStore) messageStore).getConsumeQueueTable();
        for (int i = 0; i < 10; i++) {
            ConcurrentMap<Integer, ConsumeQueueInterface> cqTable = new ConcurrentHashMap<>();
            String topicName = "topic-" + i;
            for (int j = 0; j < 4; j++) {
                RocksDBConsumeQueue consumeQueue = new RocksDBConsumeQueue(messageStoreConfig,
                    (RocksDBConsumeQueueStore) messageStore.getQueueStore(), topicName, i);
                cqTable.put(j, consumeQueue);
            }
            consumeQueueTable.put(topicName, cqTable);
        }
        assertEquals(consumeQueueTable.size(), 10);
        HashSet<String> resultSet = Sets.newHashSet("topic-3", "topic-5");
        messageStore.deleteTopics(Sets.difference(consumeQueueTable.keySet(), resultSet));
        assertEquals(consumeQueueTable.size(), 2);
        assertEquals(resultSet, consumeQueueTable.keySet());
    }

    @Test
    public void testCleanUnusedTopic() {
        if (notExecuted()) {
            return;
        }
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> consumeQueueTable =
            ((RocksDBMessageStore) messageStore).getConsumeQueueTable();
        for (int i = 0; i < 10; i++) {
            ConcurrentMap<Integer, ConsumeQueueInterface> cqTable = new ConcurrentHashMap<>();
            String topicName = "topic-" + i;
            for (int j = 0; j < 4; j++) {
                RocksDBConsumeQueue consumeQueue = new RocksDBConsumeQueue(messageStoreConfig,
                    (RocksDBConsumeQueueStore) messageStore.getQueueStore(), topicName, i);
                cqTable.put(j, consumeQueue);
            }
            consumeQueueTable.put(topicName, cqTable);
        }
        assertEquals(consumeQueueTable.size(), 10);
        HashSet<String> resultSet = Sets.newHashSet("topic-3", "topic-5");
        messageStore.cleanUnusedTopic(resultSet);
        assertEquals(consumeQueueTable.size(), 2);
        assertEquals(resultSet, consumeQueueTable.keySet());
    }

    private static class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    private boolean notExecuted() {
        return MixAll.isMac();
    }
}


