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

package org.apache.rocketmq.example.rpc;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class RequestProducer {
    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "RequestTopic";
    public static final String TAG = "Tag";
    public static final long TTL = 3000L;

    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        //You need to set namesrvAddr to the address of the local namesrv
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        try {
            Message msg = new Message(TOPIC,
                TAG,
                "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            long begin = System.currentTimeMillis();
            Message retMsg = producer.request(msg, TTL);
            long cost = System.currentTimeMillis() - begin;
            System.out.printf("request to <%s> cost: %d replyMessage: %s %n", TOPIC, cost, retMsg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
