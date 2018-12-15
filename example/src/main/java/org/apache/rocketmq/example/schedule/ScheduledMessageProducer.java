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

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.start();

        for (int i = 0; i < 100; i++) {
            Message message = new Message("ScheduledMsgTest", ("Hello scheduled message " + i).getBytes());
            /*
             * Delay level: 1  / 2 / 3 / 4 / 5 / 6 / 7 / 8 / 9 / 10/ 11/ 12/ 13/ 14/ 15/ 16/ 17/ 18
             * Delay time : 1s /5s /10s/30s/ 1m/ 2m/ 3m/ 4m/ 5m/ 6m/ 7m/ 8m/ 9m/10m/20m/30m/ 1h/ 2h
             * setDelayTimeLevel(3) means this message will be delivered to consumer 10 seconds later.
             */
            message.setDelayTimeLevel(3);
            producer.send(message);
        }

        producer.shutdown();
    }
}
