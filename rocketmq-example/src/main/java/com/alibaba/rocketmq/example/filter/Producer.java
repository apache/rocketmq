/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.example.filter;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.start();

        try {
            for (int i = 0; i < 6000000; i++) {
                Message msg = new Message("TopicFilter7",// topic
                    "TagA",// tag
                    "OrderID001",// key
                    ("Hello MetaQ").getBytes());// body

                msg.putUserProperty("SequenceId", String.valueOf(i));

                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        producer.shutdown();
    }
}
