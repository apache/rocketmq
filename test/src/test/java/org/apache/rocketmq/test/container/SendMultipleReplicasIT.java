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

package org.apache.rocketmq.test.container;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public class SendMultipleReplicasIT extends ContainerIntegrationTestBase {
    private static DefaultMQProducer mqProducer;

    private final byte[] MESSAGE_BODY = ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET);

    public SendMultipleReplicasIT() throws UnsupportedEncodingException {
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        mqProducer = createProducer("SendMultipleReplicasMessageIT_Producer");
        mqProducer.setSendMsgTimeout(15 * 1000);
        mqProducer.start();
    }

    @AfterClass
    public static void afterClass() {
        if (mqProducer != null) {
            mqProducer.shutdown();
        }
    }

    @Test
    public void sendMessageToBrokerGroup() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        awaitUntilSlaveOK();

        // Send message to broker group with three replicas
        Message msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
        SendResult sendResult = mqProducer.send(msg);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
    }

    @Test
    public void sendMessage_Auto_Replicas_Success() throws Exception {
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2
                && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);
        // Broker with 3 replicas configured as 3-2-1 auto replicas mode
        Message msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
        SendResult sendResult = mqProducer.send(msg);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);

        // Remove two slave broker
        removeSlaveBroker(1, brokerContainer2, master1With3Replicas);
        removeSlaveBroker(2, brokerContainer3, master1With3Replicas);
        await().atMost(100, TimeUnit.SECONDS)
            .until(() ->
                ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 0
                    && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 1);

        master1With3Replicas.getMessageStoreConfig().setEnableAutoInSyncReplicas(true);
        List<MessageQueue> mqList = mqProducer.getDefaultMQProducerImpl().fetchPublishMessageQueues(THREE_REPLICAS_TOPIC);
        MessageQueue targetMq = null;
        for (MessageQueue mq : mqList) {
            if (mq.getBrokerName().equals(master1With3Replicas.getBrokerConfig().getBrokerName())) {
                targetMq = mq;
            }
        }

        assertThat(targetMq).isNotNull();
        // Although this broker group only has one slave broker, send will be success in auto mode.
        msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
        sendResult = mqProducer.send(msg, targetMq);
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);

        // Recover the cluster state
        createAndAddSlave(1, brokerContainer2, master1With3Replicas);
        createAndAddSlave(2, brokerContainer3, master1With3Replicas);
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2
                && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);
    }

    @Test
    public void sendMessage_Auto_Replicas_Failed()
        throws Exception {
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2
                && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);
        // Broker with 3 replicas configured as 3-2-1 auto replicas mode
        // Remove two slave broker
        removeSlaveBroker(1, brokerContainer2, master1With3Replicas);
        removeSlaveBroker(2, brokerContainer3, master1With3Replicas);
        await().atMost(100, TimeUnit.SECONDS)
            .until(() ->
                ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 0
                    && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 1);

        // Disable the auto mode
        master1With3Replicas.getMessageStoreConfig().setEnableAutoInSyncReplicas(false);

        List<MessageQueue> mqList = mqProducer.getDefaultMQProducerImpl().fetchPublishMessageQueues(THREE_REPLICAS_TOPIC);
        MessageQueue targetMq = null;
        for (MessageQueue mq : mqList) {
            if (mq.getBrokerName().equals(master1With3Replicas.getBrokerConfig().getBrokerName())) {
                targetMq = mq;
            }
        }

        assertThat(targetMq).isNotNull();

        Message msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
        boolean exceptionCaught = false;
        try {
            mqProducer.send(msg, targetMq);
        } catch (MQBrokerException e) {
            exceptionCaught = true;
        }

        assertThat(exceptionCaught).isTrue();
        // Recover the cluster state
        createAndAddSlave(1, brokerContainer2, master1With3Replicas);
        createAndAddSlave(2, brokerContainer3, master1With3Replicas);
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master1With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2
                && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3);
    }
}
