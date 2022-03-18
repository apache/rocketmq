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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
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
public class GetMaxOffsetFromSlaveIT extends ContainerIntegrationTestBase {
    private static DefaultMQProducer mqProducer;

    private final byte[] MESSAGE_BODY = ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET);

    public GetMaxOffsetFromSlaveIT() throws UnsupportedEncodingException {
    }

    @BeforeClass
    public static void beforeClass() throws MQClientException {
        mqProducer = createProducer(GetMaxOffsetFromSlaveIT.class.getSimpleName() + "_Producer");
        mqProducer.start();
    }

    @AfterClass
    public static void afterClass() {
        if (mqProducer != null) {
            mqProducer.shutdown();
        }
    }

    @Test
    public void testGetMaxOffsetFromSlave() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        awaitUntilSlaveOK();
        mqProducer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);

        for (int i = 0; i < 100; i++) {
            Message msg = new Message(THREE_REPLICAS_TOPIC, MESSAGE_BODY);
            SendResult sendResult = mqProducer.send(msg, 10000);
            assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        }

        Map<Integer, Long> maxOffsetMap = new HashMap<>();
        TopicPublishInfo publishInfo = mqProducer.getDefaultMQProducerImpl().getTopicPublishInfoTable().get(THREE_REPLICAS_TOPIC);
        assertThat(publishInfo).isNotNull();
        for (MessageQueue mq : publishInfo.getMessageQueueList()) {
            maxOffsetMap.put(mq.getQueueId(), mqProducer.getDefaultMQProducerImpl().
                maxOffset(new MessageQueue(THREE_REPLICAS_TOPIC, master3With3Replicas.getBrokerConfig().getBrokerName(), mq.getQueueId())));
        }

        isolateBroker(master3With3Replicas);

        mqProducer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(THREE_REPLICAS_TOPIC);
        assertThat(mqProducer.getDefaultMQProducerImpl().getmQClientFactory().findBrokerAddressInPublish(
            master3With3Replicas.getBrokerConfig().getBrokerName())).isNotNull();

        for (MessageQueue mq : publishInfo.getMessageQueueList()) {
            assertThat(mqProducer.getDefaultMQProducerImpl().maxOffset(
                new MessageQueue(THREE_REPLICAS_TOPIC, master3With3Replicas.getBrokerConfig().getBrokerName(), mq.getQueueId())))
                .isEqualTo(maxOffsetMap.get(mq.getQueueId()));
        }

        cancelIsolatedBroker(master3With3Replicas);
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> ((DefaultMessageStore) master3With3Replicas.getMessageStore()).getHaService().getConnectionCount().get() == 2);
    }
}
