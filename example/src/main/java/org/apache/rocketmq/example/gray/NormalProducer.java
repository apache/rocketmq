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
package org.apache.rocketmq.example.gray;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByGray;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * normal producer example
 */
public class NormalProducer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 100;
    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "topic_check_gary";
    public static final String TAG = "TagA";

    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);

        /*
         * Specify name server addresses.
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         *  producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        // producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        /**
         * It can be configured using Spring, or directly in the code. Here it's done directly for testing purposes.
         */
        producer.setEnableGraySwitch(true);
        boolean enableGraySwitch = producer.isEnableGraySwitch();

        /**
         * You can determine whether the current producer client is in a gray environment based on system environment variables or rules within your system.
         * Assume it's a gray environment.
         */
        producer.setGrayTag(false);

        SelectMessageQueueByGray selectMessageQueueByGray = new SelectMessageQueueByGray();





        /*
         * Launch the instance.
         */
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            try {
                String message;
                if (producer.isGrayTag()) {
                    message = "This is a gray message.";
                } else {
                    message = "This is a normal message.";
                }
                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message(TOPIC /* Topic */,
                        TAG /* Tag */,
                        (message + " *" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                SendResult sendResult = null;
                /*
                 * Call send message to deliver message to one of brokers.
                 */
                if (producer.isEnableGraySwitch()) {
                    sendResult = producer.send(msg, selectMessageQueueByGray, producer.isGrayTag(), 30 * 1000);
                }

                /*
                 * There are different ways to send message, if you don't care about the send result,you can use this way
                 * {@code
                 * producer.sendOneway(msg);
                 * }
                 */

                /*
                 * if you want to get the send result in a synchronize way, you can use this send method
                 * {@code
                 * SendResult sendResult = producer.send(msg);
                 * System.out.printf("%s%n", sendResult);
                 * }
                 */

                /*
                 * if you want to get the send result in a asynchronize way, you can use this send method
                 * {@code
                 *
                 *  producer.send(msg, new SendCallback() {
                 *  @Override
                 *  public void onSuccess(SendResult sendResult) {
                 *      // do something
                 *  }
                 *
                 *  @Override
                 *  public void onException(Throwable e) {
                 *      // do something
                 *  }
                 *});
                 *
                 *}
                 */

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is no longer in use.
         */
        producer.shutdown();
    }

}
