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
package org.apache.rocketmq.example.openmessaging;

import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;

public class SimplePullConsumer {

    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_CONSUMER";

    public static void main(String[] args) {
        // You need to set the environment variable OMS_RMQ_DIRECT_NAME_SRV=true

        final MessagingAccessPoint messagingAccessPoint =
            OMS.getMessagingAccessPoint(URL);

        messagingAccessPoint.startup();

        final Producer producer = messagingAccessPoint.createProducer();

        final PullConsumer consumer = messagingAccessPoint.createPullConsumer(
            OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, QUEUE));

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        final String queueName = "TopicTest";

        producer.startup();
        Message msg = producer.createBytesMessage(queueName, "Hello Open Messaging".getBytes());
        SendResult sendResult = producer.send(msg);
        System.out.printf("Send Message OK. MsgId: %s%n", sendResult.messageId());
        producer.shutdown();

        consumer.attachQueue(queueName);

        consumer.startup();
        System.out.printf("Consumer startup OK%n");

        // Keep running until we find the one that has just been sent
        boolean stop = false;
        while (!stop) {
            Message message = consumer.receive();
            if (message != null) {
                String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                System.out.printf("Received one message: %s%n", msgId);
                consumer.ack(msgId);

                if (!stop) {
                    stop = msgId.equalsIgnoreCase(sendResult.messageId());
                }

            } else {
                System.out.printf("Return without any message%n");
            }
        }

        consumer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
