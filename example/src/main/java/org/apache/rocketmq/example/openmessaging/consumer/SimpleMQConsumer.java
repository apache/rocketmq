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
package org.apache.rocketmq.example.openmessaging.consumer;


import io.openmessaging.api.Consumer;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import java.util.Properties;
import org.apache.rocketmq.example.openmessaging.MQConfig;
import org.apache.rocketmq.ons.api.PropertyKeyConst;

public class SimpleMQConsumer {

    public static void main(String[] args) {
        MessagingAccessPoint messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876");

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.GROUP_ID);
        consumerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        consumerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);

        Consumer consumer = messagingAccessPoint.createConsumer(consumerProperties);
        /*
         * Alternatively, you can use the ONSFactory to create instance directly.
         * <pre>
         * {@code
         * consumerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
         * OrderConsumer consumer  = ONSFactory.createOrderedConsumer(consumerProperties);
         * }
         * </pre>
         */
        consumer.subscribe(MQConfig.TOPIC, MQConfig.TAG, new MessageListenerImpl());
        consumer.start();
        System.out.printf("Consumer start success. %n");

        try {
            Thread.sleep(200000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        consumer.shutdown();
    }
}
