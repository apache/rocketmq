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

import io.openmessaging.Future;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.util.concurrent.CountDownLatch;

public class SimpleProducer {

    public static final String URL = "oms:rocketmq://localhost:9876/default:default";
    public static final String QUEUE = "OMS_HELLO_TOPIC";

    public static void main(String[] args) {
        // You need to set the environment variable OMS_RMQ_DIRECT_NAME_SRV=true
        final MessagingAccessPoint messagingAccessPoint =
            OMS.getMessagingAccessPoint(URL);

        final Producer producer = messagingAccessPoint.createProducer();

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        producer.startup();
        System.out.printf("Producer startup OK%n");

        {
            Message message = producer.createBytesMessage(QUEUE, "OMS_HELLO_BODY".getBytes());

            SendResult sendResult = producer.send(message);
            //final Void aVoid = result.get(3000L);
            System.out.printf("Send async message OK, msgId: %s%n", sendResult.messageId());
        }

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        {
            final Future<SendResult> result = producer.sendAsync(producer.createBytesMessage(QUEUE, "OMS_HELLO_BODY".getBytes()));
            result.addListener(future -> {
                if (future.getThrowable() != null) {
                    System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                } else {
                    System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                }
                countDownLatch.countDown();
            });
        }

        {
            producer.sendOneway(producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes()));
            System.out.printf("Send oneway message OK%n");
        }

        try {
            countDownLatch.await();
            // Wait some time for one-way delivery.
            Thread.sleep(500);
        } catch (InterruptedException ignore) {
        }

        producer.shutdown();
    }
}
