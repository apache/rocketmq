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
package org.apache.rocketmq.example.schedule;

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class TimerMessageProducer {

    public static final String PRODUCER_GROUP = "TimerMessageProducerGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TimerTopic";

    public static void main(String[] args) throws Exception {
        // Instantiate a producer to send scheduled messages
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
//        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        // Launch producer
        producer.start();
        int totalMessagesToSend = 10;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message(TOPIC, ("Hello scheduled message " + i).getBytes(StandardCharsets.UTF_8));
            // This message will be delivered to consumer 10 seconds later.
            //message.setDelayTimeSec(10);
            // The effect is the same as the above
            // message.setDelayTimeMs(10_000L);
            // Set the specific delivery time, and the effect is the same as the above
            message.setDeliverTimeMs(System.currentTimeMillis() + 10_000L);
            // Send the message
            SendResult result = producer.send(message);
            System.out.printf(result + "\n");
        }

        // Shutdown producer after use.
        producer.shutdown();
    }
}
