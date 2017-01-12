/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.client;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class SimpleConsumerProducerTest {
    private static final String TOPIC_TEST = "TopicTest-fundmng";

    @Test
    public void producerConsumerTest() throws MQClientException, InterruptedException, IOException {
        System.setProperty("rocketmq.namesrv.domain", "jmenv.tbsite.alipay.net");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("S_fundmng_demo_producer");
        DefaultMQProducer producer = new DefaultMQProducer("P_fundmng_demo_producer");
        //producer.computeClientIP("10.1.33");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC_TEST, null);

        final AtomicLong lastReceivedMills = new AtomicLong(System.currentTimeMillis());

        final AtomicLong consumeTimes = new AtomicLong(0);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
                                                            final ConsumeConcurrentlyContext context) {
                System.out.println("Received" + consumeTimes.incrementAndGet() + "messages !");

                lastReceivedMills.set(System.currentTimeMillis());

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        producer.start();

        for (int i = 0; i < 100; i++) {
            try {
                Message msg = new Message(TOPIC_TEST, ("Hello RocketMQ " + i).getBytes());
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            } catch (Exception e) {
                System.err.println(e.toString());
                TimeUnit.SECONDS.sleep(1);
            }
        }

//        // wait no messages
        while ((System.currentTimeMillis() - lastReceivedMills.get()) < 5000) {
            TimeUnit.MILLISECONDS.sleep(200);
        }

        consumer.shutdown();
        producer.shutdown();
    }
}
