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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
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

        Assertions.assertThatCode(() -> escapeBridge.getMessage(TEST_TOPIC, 0, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void getMessageFromRemoteTest() {
        Assertions.assertThatCode(() -> escapeBridge.getMessageFromRemote(TEST_TOPIC, 1, DEFAULT_QUEUE_ID, BROKER_NAME)).doesNotThrowAnyException();
    }

    @Test
    public void decodeMsgListTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        MappedFile mappedFile = new DefaultMappedFile();
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, 10, mappedFile);

        getMessageResult.addMessage(result);
        Assertions.assertThatCode(() -> escapeBridge.decodeMsgList(getMessageResult)).doesNotThrowAnyException();
    }

}
