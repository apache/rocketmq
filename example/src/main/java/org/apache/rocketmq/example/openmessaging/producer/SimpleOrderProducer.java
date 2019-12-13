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
package org.apache.rocketmq.example.openmessaging.producer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.exception.OMSRuntimeException;
import io.openmessaging.api.order.OrderProducer;
import java.util.Properties;
import org.apache.rocketmq.example.openmessaging.MQConfig;
import org.apache.rocketmq.ons.api.PropertyKeyConst;

public class SimpleOrderProducer {

    public static void main(String[] args) {
        MessagingAccessPoint messagingAccessPoint = OMS.getMessagingAccessPoint("oms:rocketmq://127.0.0.1:9876");

        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MQConfig.ORDER_GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.AccessKey, MQConfig.ACCESS_KEY);
        producerProperties.setProperty(PropertyKeyConst.SecretKey, MQConfig.SECRET_KEY);
        OrderProducer producer = messagingAccessPoint.createOrderProducer(producerProperties);


        /*
         * Alternatively, you can use the ONSFactory to create instance directly.
         * <pre>
         * {@code
         * producerProperties.setProperty(PropertyKeyConst.NAMESRV_ADDR, MQConfig.NAMESRV_ADDR);
         * OrderProducer producer = ONSFactory.createOrderProducer(producerProperties);
         * }
         * </pre>
         */

        producer.start();
        System.out.printf("Producer Started. %n");

        for (int i = 0; i < 10; i++) {
            Message msg = new Message(MQConfig.ORDER_TOPIC, MQConfig.TAG, "MQ send order message test".getBytes());
            String orderId = "biz_" + i % 10;
            msg.setKey(orderId);
            String shardingKey = String.valueOf(orderId);
            try {
                SendResult sendResult = producer.send(msg, shardingKey);
                assert sendResult != null;
                System.out.printf("Send mq timer message success! Topic is: %s msgId is: %s%n", MQConfig.TOPIC, sendResult.getMessageId());
            } catch (OMSRuntimeException e) {
                System.out.printf("Send mq message failed. Topic is: %s%n", MQConfig.TOPIC);
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
