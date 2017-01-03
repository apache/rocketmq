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
package org.apache.rocketmq.example.simple;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendFuture;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException, UnsupportedEncodingException {

        DefaultMQProducer producer = new DefaultMQProducer("Jodie_Daily_test");
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        for (int i = 0; i < 10000000; i++) {
            try {
                final int index = i;
                Message msg = new Message("Jodie_topic_1023",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Another way to send messages in async, there are some advantages for this way:
         *
         * 0. Supports multiple callbacks
         * 1. Executes the callback(the callback might take too long) in exclusive thread pool
         * 2. Converts to blocking mode(by invoking {@link SendFuture#get()})
         *
         * It's a more efficient mechanism(higher throughput) if your callback will
         * take a short period of time(eg. blocking on I/O)
         */
        Message message = new Message();
        message.setTopic("your topic identifier");
        message.setBody("hello, RocketMQ!".getBytes());

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            SendFuture future = producer.send(message, executor, 1000);
            future.addCallback(new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    // do sth
                }

                @Override
                public void onException(Throwable e) {
                    // do sth
                }
            }).addCallback(new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    // do sth
                }

                @Override
                public void onException(Throwable e) {
                    // do sth
                }
            });
        } catch (RemotingException e) {
            // something wrong
        }

        executor.shutdown();
        producer.shutdown();
    }
}
