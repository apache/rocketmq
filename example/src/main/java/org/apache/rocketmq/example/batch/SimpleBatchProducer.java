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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.example.batch;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class SimpleBatchProducer {

    public static final String PRODUCER_GROUP = "BatchProducerGroupName";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "BatchTest";
    public static final String TAG = "Tag";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
//        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        //If you just send messages of no more than 1MiB at a time, it is easy to use batch
        //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(TOPIC, TAG, "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(TOPIC, TAG, "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(TOPIC, TAG, "OrderID003", "Hello world 2".getBytes()));

        SendResult sendResult = producer.send(messages);
        System.out.printf("%s", sendResult);
    }
}
