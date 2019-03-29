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

package org.apache.rocketmq.client.trace;

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
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Date;
import java.util.List;

public class SameProcessSameTopicTest {
    private static String topic = "TopicTest";

    public static void startConsume() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer", true);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new Date() + ":" + new String(msg.getBody()) + " consume success! " + msg.getMsgId());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        consumer.start();
    }

    public static void startProduce() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("test_producer",true);

        String[] tags = {"taga","tagb","tagc","tagd"};
        producer.start();
        for (int i = 0; i < 100 ;) {
            try {
                Message msg = new Message(topic /* Topic */, tags[i%4] /* Tag */,
                        ("trace message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                SendResult sendResult = producer.send(msg);

                System.out.printf("%d-%s%n", i,sendResult);
                Thread.sleep(1000);
                i++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    public static void main(String[] args) throws InterruptedException, MQClientException {

        Thread t1 = new Thread() {
            @Override
            public void run() {
                try {
                    startConsume();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }
        };


        Thread t2 = new Thread() {
            @Override
            public void run() {
                try {
                    startProduce();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }
        };


        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.printf("Consumer Started.%n");
    }
}