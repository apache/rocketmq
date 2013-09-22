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
package com.alibaba.rocketmq.research.client;

import java.net.SocketException;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.MixAll;


public class ClientConfigTest {
    public static void main(String[] args) throws SocketException {
        MixAll.printObjectProperties(null, new ClientConfig());
        System.out.println("----------------------------------------------");
        MixAll.printObjectProperties(null, new DefaultMQProducer());
        System.out.println("----------------------------------------------");
        MixAll.printObjectProperties(null, new TransactionMQProducer());
        System.out.println("----------------------------------------------");
        MixAll.printObjectProperties(null, new DefaultMQPushConsumer());
        System.out.println("----------------------------------------------");
        MixAll.printObjectProperties(null, new DefaultMQPullConsumer());
    }
}
