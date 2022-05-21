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

package org.apache.rocketmq.thinclient.example;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;
import org.apache.rocketmq.apis.producer.SendReceipt;

public class ProducerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) {
        try {
            String accessPoint = "11.166.42.94:8081";
            String topic = "lingchu_normal_topic";
            String tag = "tagA";
            byte[] body = "Hello RocketMQ".getBytes(StandardCharsets.UTF_8);
            String accessKey = "AccessKey";
            String secretKey = "SecretKey";

            final ClientServiceProvider provider = ClientServiceProvider.loadService();
            StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setAccessPoint(accessPoint)
                .setCredentialProvider(staticSessionCredentialsProvider)
                .build();
            final Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setBody(body)
                .setTag(tag)
                .build();
            Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(topic)
                .build();
            LOGGER.info("Start producer successfully.");
            for (int i = 0; i < 1; i++) {
                final SendReceipt sendReceipt = producer.send(message);
                LOGGER.info("Send message successfully, sendReceipt={}", sendReceipt);
            }
//            Thread.sleep(100000);
            producer.close();
        } catch (Throwable t) {
            LOGGER.info("Exception raised", t);
        }
    }
}
