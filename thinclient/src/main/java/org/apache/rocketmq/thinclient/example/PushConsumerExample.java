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
import java.io.IOException;
import java.util.Collections;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientServiceProvider;
import org.apache.rocketmq.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.ClientException;

public class PushConsumerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) throws InterruptedException {
        String accessPoint = "11.166.42.94:8081";
        String topic = "lingchu_normal_topic";
        String tag = "tagA";
        String consumerGroup = "lingchu_normal_group";
        String accessKey = "AccessKey";
        String secretKey = "secretKey";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        StaticSessionCredentialsProvider staticSessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setAccessPoint(accessPoint)
            .setCredentialProvider(staticSessionCredentialsProvider)
            .build();
        try (PushConsumer ignored = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setMessageListener(messageView -> {
                LOGGER.info("Received message, message={}", messageView);
                return ConsumeResult.OK;
            })
            .build()) {
            LOGGER.info("Start push consumer successfully.");
            Thread.sleep(1000000);
        } catch (IOException | ClientException e) {
            e.printStackTrace();
        }
    }
}
