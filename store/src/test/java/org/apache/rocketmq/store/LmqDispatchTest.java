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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.junit.Test;
import org.rocksdb.RocksDBException;

public class LmqDispatchTest {

    @Test
    public void testPrepareAndUpdateMixedQueues() throws Exception {
        MessageStore messageStore = mock(MessageStore.class);
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        ConsumeQueueStoreInterface queueStore = mock(ConsumeQueueStoreInterface.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStore.getQueueStore()).thenReturn(queueStore);
        when(messageStoreConfig.isEnableLmq()).thenReturn(true);

        String firstLmq = MixAll.LMQ_PREFIX + "first";
        String secondLmq = MixAll.LMQ_PREFIX + "second";
        when(queueStore.getLmqQueueOffset(firstLmq, MixAll.LMQ_QUEUE_ID)).thenReturn(7L);
        when(queueStore.getLmqQueueOffset(secondLmq, MixAll.LMQ_QUEUE_ID)).thenReturn(11L);

        MessageExtBrokerInner message = new MessageExtBrokerInner();
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH,
            firstLmq + MixAll.LMQ_DISPATCH_SEPARATOR + "normal-topic" + MixAll.LMQ_DISPATCH_SEPARATOR + secondLmq);

        String[] queueNames = LmqDispatch.prepareLmqDispatch(messageStore, message);

        assertArrayEquals(new String[] {firstLmq, "normal-topic", secondLmq}, queueNames);
        assertEquals("7,,11", message.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET));
        verify(queueStore).getLmqQueueOffset(firstLmq, MixAll.LMQ_QUEUE_ID);
        verify(queueStore).getLmqQueueOffset(secondLmq, MixAll.LMQ_QUEUE_ID);

        LmqDispatch.updateLmqOffsets(messageStore, queueNames);
        verify(queueStore).increaseLmqOffset(firstLmq, MixAll.LMQ_QUEUE_ID, (short) 1);
        verify(queueStore).increaseLmqOffset(secondLmq, MixAll.LMQ_QUEUE_ID, (short) 1);
    }

    @Test
    public void testPublicWrapPreservesWaitPropertyBehaviorWhenLmqIsDisabled() throws Exception {
        MessageStore messageStore = mock(MessageStore.class);
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        ConsumeQueueStoreInterface queueStore = mock(ConsumeQueueStoreInterface.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStore.getQueueStore()).thenReturn(queueStore);
        when(messageStoreConfig.isEnableLmq()).thenReturn(false);

        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setWaitStoreMsgOK(true);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH,
            MixAll.LMQ_PREFIX + "first,normal-topic," + MixAll.LMQ_PREFIX + "second");

        LmqDispatch.wrapLmqDispatch(messageStore, message);

        assertEquals(",,", message.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET));
        assertEquals("true", message.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK));
        Map<String, String> serializedProperties =
            MessageDecoder.string2messageProperties(message.getPropertiesString());
        assertFalse(serializedProperties.containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK));
        verify(queueStore, never()).getLmqQueueOffset(anyString(), anyInt());
    }

    @Test
    public void testCommitLogPreservesLegacyPropertyBytesAndIncrementsOffsetOnce() throws Exception {
        AppendFixture fixture = createAppendFixture();
        try {
            String lmqName = MixAll.LMQ_PREFIX + "put-ok";
            MessageExtBrokerInner message = createLmqMessage(fixture.messageStoreConfig, lmqName);
            MessageExtBrokerInner legacyMessage = createLmqMessage(fixture.messageStoreConfig, lmqName);
            MessageAccessor.putProperty(legacyMessage, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, "0");
            legacyMessage.removeWaitStorePropertyString();
            legacyMessage.setPropertiesString(MessageDecoder.messageProperties2String(legacyMessage.getProperties()));
            byte[] expectedProperties = legacyMessage.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            ByteBuffer destination = ByteBuffer.allocate(1024);

            AppendMessageResult result = fixture.callback.doAppend(0, destination, destination.capacity(), message,
                null);

            assertEquals(AppendMessageStatus.PUT_OK, result.getStatus());
            assertEquals(1L, fixture.messageStore.getQueueStore().getLmqQueueOffset(lmqName, MixAll.LMQ_QUEUE_ID));
            assertEquals(legacyMessage.getPropertiesString(), message.getPropertiesString());

            int messageLength = destination.getInt(0);
            byte[] persistedProperties = new byte[expectedProperties.length];
            ByteBuffer persistedPropertiesBuffer = destination.duplicate();
            persistedPropertiesBuffer.position(messageLength - expectedProperties.length);
            persistedPropertiesBuffer.get(persistedProperties);
            assertArrayEquals(expectedProperties, persistedProperties);

            MessageExt persistedMessage = MessageDecoder.decode((ByteBuffer) destination.flip());
            assertNotNull(persistedMessage);
            assertEquals("true", persistedMessage.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK));
            assertEquals("0", persistedMessage.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET));
        } finally {
            fixture.destroy();
        }
    }

    @Test
    public void testCommitLogEndOfFileRetryIncrementsOffsetOnce() throws Exception {
        AppendFixture fixture = createAppendFixture();
        try {
            String lmqName = MixAll.LMQ_PREFIX + "end-of-file";
            MessageExtBrokerInner message = createLmqMessage(fixture.messageStoreConfig, lmqName);
            PutMessageContext putMessageContext = new PutMessageContext("test-topic-0");
            ByteBuffer endOfFileBuffer = ByteBuffer.allocate(8);

            AppendMessageResult endOfFileResult = fixture.callback.doAppend(0, endOfFileBuffer,
                endOfFileBuffer.capacity(), message, putMessageContext);

            assertEquals(AppendMessageStatus.END_OF_FILE, endOfFileResult.getStatus());
            assertTrue(message.isEncodeCompleted());
            assertEquals(0L, fixture.messageStore.getQueueStore().getLmqQueueOffset(lmqName,
                MixAll.LMQ_QUEUE_ID));

            ByteBuffer destination = ByteBuffer.allocate(1024);
            AppendMessageResult putResult = fixture.callback.doAppend(8, destination, destination.capacity(), message,
                putMessageContext);

            assertEquals(AppendMessageStatus.PUT_OK, putResult.getStatus());
            assertEquals(1L, fixture.messageStore.getQueueStore().getLmqQueueOffset(lmqName, MixAll.LMQ_QUEUE_ID));
        } finally {
            fixture.destroy();
        }
    }

    @Test
    public void testCommitLogMapsRocksDbAndConsumeQueueFailures() throws Exception {
        assertPrepareFailureStatus(new ConsumeQueueException(new RocksDBException("rocksdb failure")),
            AppendMessageStatus.ROCKSDB_ERROR);
        assertPrepareFailureStatus(new ConsumeQueueException("consume queue failure"),
            AppendMessageStatus.UNKNOWN_ERROR);
    }

    private void assertPrepareFailureStatus(ConsumeQueueException exception, AppendMessageStatus expectedStatus)
        throws Exception {
        String storePath = newStorePath();
        MessageStoreConfig messageStoreConfig = createMessageStoreConfig(storePath);
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        ConsumeQueueStoreInterface queueStore = mock(ConsumeQueueStoreInterface.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(messageStore.getQueueStore()).thenReturn(queueStore);
        when(queueStore.getLmqQueueOffset(anyString(), anyInt())).thenThrow(exception);

        CommitLog commitLog = new CommitLog(messageStore);
        AppendMessageCallback callback = commitLog.new DefaultAppendMessageCallback(messageStoreConfig);
        MessageExtBrokerInner message = createLmqMessage(messageStoreConfig, MixAll.LMQ_PREFIX + "failure");

        AppendMessageResult result = callback.doAppend(0, ByteBuffer.allocate(1024), 1024, message, null);

        assertEquals(expectedStatus, result.getStatus());
        assertFalse(message.isEncodeCompleted());
        UtilAll.deleteFile(new File(storePath));
    }

    private MessageExtBrokerInner createLmqMessage(MessageStoreConfig messageStoreConfig, String lmqName) {
        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setTopic("test-topic");
        message.setQueueId(0);
        message.setBody("body".getBytes(MessageDecoder.CHARSET_UTF8));
        message.setBornTimestamp(System.currentTimeMillis());
        message.setStoreTimestamp(System.currentTimeMillis());
        message.setBornHost(new InetSocketAddress("127.0.0.1", 12345));
        message.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        message.setWaitStoreMsgOK(true);
        message.putUserProperty("m", "same-bucket-as-wait");
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqName);
        message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));

        MessageExtEncoder encoder = new MessageExtEncoder(messageStoreConfig);
        assertNull(encoder.encode(message));
        message.setEncodedBuff(encoder.getEncoderBuffer());
        return message;
    }

    private AppendFixture createAppendFixture() throws Exception {
        String storePath = newStorePath();
        MessageStoreConfig messageStoreConfig = createMessageStoreConfig(storePath);
        DefaultMessageStore messageStore = new DefaultMessageStore(messageStoreConfig, null, null,
            new BrokerConfig(), new ConcurrentHashMap<>());
        CommitLog commitLog = new CommitLog(messageStore);
        return new AppendFixture(storePath, messageStoreConfig, messageStore,
            commitLog.new DefaultAppendMessageCallback(messageStoreConfig));
    }

    private MessageStoreConfig createMessageStoreConfig(String storePath) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");
        messageStoreConfig.setMappedFileSizeCommitLog(8 * 1024);
        messageStoreConfig.setMaxMessageSize(1024 * 1024);
        messageStoreConfig.setEnableLmq(true);
        return messageStoreConfig;
    }

    private String newStorePath() {
        return System.getProperty("java.io.tmpdir") + File.separator + "lmq-dispatch-" + UUID.randomUUID();
    }

    private static class AppendFixture {
        private final String storePath;
        private final MessageStoreConfig messageStoreConfig;
        private final DefaultMessageStore messageStore;
        private final AppendMessageCallback callback;

        private AppendFixture(String storePath, MessageStoreConfig messageStoreConfig,
            DefaultMessageStore messageStore, AppendMessageCallback callback) {
            this.storePath = storePath;
            this.messageStoreConfig = messageStoreConfig;
            this.messageStore = messageStore;
            this.callback = callback;
        }

        private void destroy() {
            UtilAll.deleteFile(new File(storePath));
        }
    }
}
