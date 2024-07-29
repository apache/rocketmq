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

package org.apache.rocketmq.broker.failover;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EscapeBridgeTest {

    private EscapeBridge escapeBridge;

    @Mock
    private BrokerController brokerController;

    @Mock
    private MessageExtBrokerInner messageExtBrokerInner;

    private BrokerConfig brokerConfig;

    @Mock
    private DefaultMessageStore defaultMessageStore;

    private GetMessageResult getMessageResult;

    @Mock
    private DefaultMQProducer defaultMQProducer;

    @Mock
    private TopicRouteInfoManager topicRouteInfoManager;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    private static final String BROKER_NAME = "broker_a";

    private static final String TEST_TOPIC = "TEST_TOPIC";

    private static final int DEFAULT_QUEUE_ID = 0;


    @Before
    public void before() throws Exception {
        brokerConfig = new BrokerConfig();
        getMessageResult = new GetMessageResult();
        brokerConfig.setBrokerName(BROKER_NAME);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        escapeBridge = new EscapeBridge(brokerController);
        messageExtBrokerInner = new MessageExtBrokerInner();
        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        when(brokerController.getTopicRouteInfoManager()).thenReturn(topicRouteInfoManager);
        when(topicRouteInfoManager.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn("");

        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);

        brokerConfig.setEnableSlaveActingMaster(true);
        brokerConfig.setEnableRemoteEscape(true);
        escapeBridge.start();
        defaultMQProducer.start();
    }

    @After
    public void after() {
        escapeBridge.shutdown();
        brokerController.shutdown();
        defaultMQProducer.shutdown();
    }

    @Test
    public void putMessageTest() {
        messageExtBrokerInner.setTopic(TEST_TOPIC);
        messageExtBrokerInner.setQueueId(DEFAULT_QUEUE_ID);
        messageExtBrokerInner.setBody("Hello World".getBytes(StandardCharsets.UTF_8));
        // masterBroker is null
        final PutMessageResult result1 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result1 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result1.getPutMessageStatus());

        // masterBroker is not null
        messageExtBrokerInner.setBody("Hello World2".getBytes(StandardCharsets.UTF_8));
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        when(brokerController.peekMasterBroker()).thenReturn(null);
        final PutMessageResult result3 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result3 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result3.getPutMessageStatus());
    }

    @Test
    public void asyncPutMessageTest() {

        // masterBroker is null
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        // masterBroker is not null
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        when(brokerController.peekMasterBroker()).thenReturn(null);
        Assertions.assertThatCode(() -> escapeBridge.asyncPutMessage(messageExtBrokerInner)).doesNotThrowAnyException();
    }

    @Test
    public void putMessageToSpecificQueueTest() {
        // masterBroker is null
        final PutMessageResult result1 = escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner);
        assert result1 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result1.getPutMessageStatus());

        // masterBroker is not null
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        Assertions.assertThatCode(() -> escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageTest() {
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> escapeBridge.getMessage(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageAsyncTest() {
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        Assertions.assertThatCode(() -> escapeBridge.putMessage(messageExtBrokerInner)).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageAsyncTest_localStore_getMessageAsync_null() {
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("getMessageResult is null", rst.getMiddle());
        Assert.assertFalse(rst.getRight()); // no retry
    }

    @Test
    public void getMessageAsyncTest_localStore_decodeNothing() throws Exception {
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockGetMessageResult(0, TEST_TOPIC, null)));
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("Can not get msg", rst.getMiddle());
        Assert.assertFalse(rst.getRight()); // no retry
    }

    @Test
    public void getMessageAsyncTest_localStore_message_found() throws Exception {
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        when(defaultMessageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(mockGetMessageResult(2, TEST_TOPIC, "HW".getBytes())));
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false).join();
        Assert.assertNotNull(rst.getLeft());
        Assert.assertEquals(0, rst.getLeft().getQueueOffset());
        Assert.assertTrue(Arrays.equals("HW".getBytes(), rst.getLeft().getBody()));
        Assert.assertFalse(rst.getRight());
    }

    @Test
    public void getMessageAsyncTest_remoteStore_addressNotFound() throws Exception {
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(null);

        // just test address not found, since we have complete tests of getMessageFromRemoteAsync()
        when(topicRouteInfoManager.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn(null);
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageAsync(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME, false).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("brokerAddress not found", rst.getMiddle());
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void getMessageFromRemoteTest() {
        Assertions.assertThatCode(() -> escapeBridge.getMessageFromRemote(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageFromRemoteAsyncTest() {
        Assertions.assertThatCode(() -> escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageFromRemoteAsyncTest_exception_caught() throws Exception {
        when(brokerOuterAPI.pullMessageFromSpecificBrokerAsync(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
                .thenThrow(new RemotingException("mock remoting exception"));
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("Get message from remote failed", rst.getMiddle());
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void getMessageFromRemoteAsyncTest_brokerAddressNotFound() throws Exception {
        when(topicRouteInfoManager.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn(null);
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("brokerAddress not found", rst.getMiddle());
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void getMessageFromRemoteAsyncTest_message_found() throws Exception {
        PullResult pullResult = new PullResult(PullStatus.FOUND, 1, 1, 1, new ArrayList<MessageExt>(1){{add(new MessageExt());}});
        when(brokerOuterAPI.pullMessageFromSpecificBrokerAsync(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(pullResult, "", false))); // right value is ignored
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME).join();
        Assert.assertNotNull(rst.getLeft());
        Assert.assertTrue(StringUtils.isEmpty(rst.getMiddle()));
        Assert.assertFalse(rst.getRight()); // no retry
    }

    @Test
    public void getMessageFromRemoteAsyncTest_message_notFound() throws Exception {
        PullResult pullResult = new PullResult(PullStatus.NO_MATCHED_MSG, 1, 1, 1, null);
        when(brokerOuterAPI.pullMessageFromSpecificBrokerAsync(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(pullResult, "no msg", false)));
        Triple<MessageExt, String, Boolean> rst = escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("no msg", rst.getMiddle());
        Assert.assertFalse(rst.getRight()); // no retry

        when(brokerOuterAPI.pullMessageFromSpecificBrokerAsync(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(Triple.of(null, "other resp code", true)));
        rst = escapeBridge.getMessageFromRemoteAsync(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME).join();
        Assert.assertNull(rst.getLeft());
        Assert.assertEquals("other resp code", rst.getMiddle());
        Assert.assertTrue(rst.getRight()); // need retry
    }

    @Test
    public void decodeMsgListTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        MappedFile mappedFile = new DefaultMappedFile();
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, 10, mappedFile);

        getMessageResult.addMessage(result);
        Assertions.assertThatCode(() -> escapeBridge.decodeMsgList(getMessageResult, false)).doesNotThrowAnyException();
    }

    @Test
    public void decodeMsgListTest_messageNotNull() throws Exception {
        MessageExt msg = new MessageExt();
        msg.setBody("HW".getBytes());
        msg.setTopic("topic");
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 9000));
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 9000));
        ByteBuffer byteBuffer = ByteBuffer.wrap(MessageDecoder.encode(msg, false));
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, 10, new DefaultMappedFile());


        getMessageResult.addMessage(result);
        getMessageResult.getMessageQueueOffset().add(0L);
        List<MessageExt> list = escapeBridge.decodeMsgList(getMessageResult, false); // skip deCompressBody test
        Assert.assertEquals(1, list.size());
        Assert.assertTrue(Arrays.equals(msg.getBody(), list.get(0).getBody()));
    }

    private GetMessageResult mockGetMessageResult(int count, String topic, byte[] body) throws Exception {
        GetMessageResult result = new GetMessageResult();
        for (int i = 0; i < count; i++) {
            MessageExt msg = new MessageExt();
            msg.setBody(body);
            msg.setTopic(topic);
            msg.setBornHost(new InetSocketAddress("127.0.0.1", 9000));
            msg.setStoreHost(new InetSocketAddress("127.0.0.1", 9000));
            ByteBuffer byteBuffer = ByteBuffer.wrap(MessageDecoder.encode(msg, false));
            SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(0, byteBuffer, body.length, new DefaultMappedFile());

            result.addMessage(bufferResult);
            result.getMessageQueueOffset().add(i + 0L);
        }
        return result;
    }

}
