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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * This class demonstrates how to send messages to brokers using provided
 * {@link DefaultMQProducer}.
 */
public class Producer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 1000;

    /**
     * Send timeout in milliseconds.
     */
    public static final int SEND_TIMEOUT_MILLIS = 20000;

    /**
     * Sleep time on error in milliseconds.
     */
    public static final int ERROR_SLEEP_MILLIS = 1000;

    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";
    public static final String TAG = "TagA";

    private Producer() {
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        // Uncomment the following line while debugging
        // producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {
                String messageBody = "Hello RocketMQ " + i;
                Message msg = new Message(TOPIC, TAG,
                        messageBody.getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = producer.send(msg, SEND_TIMEOUT_MILLIS);
                /*
                 * There are different ways to send message, if you don't care about the send
                 * result,you can use this way
                 * {@code
                 * producer.sendOneway(msg);
                 * }
                 */

                /*
                 * if you want to get the send result in a synchronize way, you can use this
                 * send method
                 * {@code
                 * SendResult sendResult = producer.send(msg);
                 * System.out.printf("%s%n", sendResult);
                 * }
                 */

                /*
                 * if you want to get the send result in a asynchronize way, you can use this
                 * send method
                 * {@code
                 *
                 * producer.send(msg, new SendCallback() {
                 * 
                 * @Override
                 * public void onSuccess(SendResult sendResult) {
                 * // do something
                 * }
                 *
                 * @Override
                 * public void onException(Throwable e) {
                 * // do something
                 * }
                 * });
                 *
                 * }
                 */

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(ERROR_SLEEP_MILLIS);
            }
        }

        /*
         * Shut down once the producer instance is no longer in use.
         */
        producer.shutdown();
    }
}
