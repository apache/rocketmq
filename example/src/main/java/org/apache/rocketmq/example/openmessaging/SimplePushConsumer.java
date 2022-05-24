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
import io.openmessaging.consumer.PushConsumer;

public class SimplePushConsumer {

    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_HELLO_TOPIC";

    public static void main(String[] args) {
        // You need to set the environment variable OMS_RMQ_DIRECT_NAME_SRV=true

        final MessagingAccessPoint messagingAccessPoint = OMS
            .getMessagingAccessPoint(URL);

        final PushConsumer consumer = messagingAccessPoint.
            createPushConsumer(OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "OMS_CONSUMER"));

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.shutdown();
            messagingAccessPoint.shutdown();
        }));

        consumer.attachQueue(QUEUE, (message, context) -> {
            System.out.printf("Received one message: %s%n", message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
            context.ack();
        });

        consumer.startup();
        System.out.printf("Consumer startup OK%n");
    }
}
