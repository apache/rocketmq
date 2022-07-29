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

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
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

    private SendResult sendResult = new SendResult();


    @Before
    public void before() throws Exception {
        brokerConfig = new BrokerConfig();
        getMessageResult = new GetMessageResult();
        brokerConfig.setBrokerName("broker_a");
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        escapeBridge = new EscapeBridge(brokerController);
        messageExtBrokerInner = new MessageExtBrokerInner();
        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(brokerController.getNameServerList()).thenReturn("127.0.0.1:9876");
        brokerConfig.setEnableSlaveActingMaster(true);
        brokerConfig.setEnableRemoteEscape(true);
        escapeBridge.start();
        defaultMQProducer.start();
    }

    @After
    public void after() {
        escapeBridge.shutdown();
        brokerController.shutdown();
    }

    @Test
    public void putMessageTest() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        messageExtBrokerInner.setTopic("TEST_TOPIC");
        messageExtBrokerInner.setQueueId(0);
        messageExtBrokerInner.setBody("Hello World".getBytes(StandardCharsets.UTF_8));
        // masterBroker is null
        final PutMessageResult result1 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result1 != null;
        assert PutMessageStatus.PUT_TO_REMOTE_BROKER_FAIL.equals(result1.getPutMessageStatus());

        // masterBroker is not null
        messageExtBrokerInner.setBody("Hello World2".getBytes(StandardCharsets.UTF_8));
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        final PutMessageResult result2 = escapeBridge.putMessage(messageExtBrokerInner);
        assert result2 == null;

        when(defaultMQProducer.send(messageExtBrokerInner)).thenReturn(sendResult);
        when(brokerController.peekMasterBroker()).thenReturn(null);
        escapeBridge.putMessage(messageExtBrokerInner);
    }

    @Test
    public void asyncPutMessageTest() {

        // masterBroker is null
        escapeBridge.asyncPutMessage(messageExtBrokerInner);

        // masterBroker is not null
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        escapeBridge.asyncPutMessage(messageExtBrokerInner);

        // set brokerConfig
        when(brokerController.peekMasterBroker()).thenReturn(null);

        escapeBridge.asyncPutMessage(messageExtBrokerInner);
    }

    @Test
    public void putMessageToSpecificQueueTest() {
        // masterBroker is null
        escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner);

        // masterBroker is not null
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        escapeBridge.putMessageToSpecificQueue(messageExtBrokerInner);
    }

    @Test
    public void getMessageTest() {
        when(brokerController.peekMasterBroker()).thenReturn(brokerController);
        when(brokerController.getMessageStoreByBrokerName(any())).thenReturn(defaultMessageStore);
        escapeBridge.putMessage(messageExtBrokerInner);

        final MessageExt message = escapeBridge.getMessage("TEST_TOPIC", 0, 0, null);
        assert  message == null;

    }
}
