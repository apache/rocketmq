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
package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class ProducerWithNamespace {

    public static final String NAMESPACE = "InstanceTest";
    public static final String PRODUCER_GROUP = "pidTest";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final int MESSAGE_COUNT = 100;
    public static final String TOPIC = "NAMESPACE_TOPIC";
    public static final String TAG = "tagTest";

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer(NAMESPACE, PRODUCER_GROUP);

        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message message = new Message(TOPIC, TAG, "Hello world".getBytes(StandardCharsets.UTF_8));
            try {
                SendResult result = producer.send(message);
                System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}