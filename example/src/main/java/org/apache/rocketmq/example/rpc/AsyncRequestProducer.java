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
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class AsyncRequestProducer {
    private static final InternalLogger log = ClientLogger.getLog();

    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "RequestTopic";
    public static final String TAG = "Tag";
    public static final long TTL = 3000L;

    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        // If the debugging source code can open comments, you need to set the namesrvAddr to the address of the local namesrvAddr
//        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        try {
            Message msg = new Message(TOPIC, TAG,
                "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            long begin = System.currentTimeMillis();
            producer.request(msg, new RequestCallback() {
                @Override
                public void onSuccess(Message message) {
                    long cost = System.currentTimeMillis() - begin;
                    System.out.printf("request to <%s> cost: %d replyMessage: %s %n", TOPIC, cost, message);
                }

                @Override
                public void onException(Throwable e) {
                    System.err.printf("request to <%s> fail.", TOPIC);
                }
            }, TTL);
        } catch (Exception e) {
            log.warn("", e);
        }
        /* shutdown after your request callback is finished */
//        producer.shutdown();
    }
}
