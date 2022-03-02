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
package org.apache.rocketmq.example.ordermessage;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;


/**
 * <p>
 * This example shows RocketMQ keep order message (partially ordered not global order)
 * </p>
 * <p>
 * in the output below,you can see [i=23, orderId=3] consume fail(consumerResult=SUSPEND_CURRENT_QUEUE_A_MOMENT),
 * to keep ordered message consume,consumer will reconsume [i=23, orderId=3] and then consume [i=33, orderId=3]
 * <pre>
 *
 * Consumer Started.
 * ConsumeMessageThread_please_rename_unique_group_name_3_1 Message body: Hello RocketMQ i=3, orderId=3 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_1 Message body: Hello RocketMQ i=7, orderId=7 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_1 Message body: Hello RocketMQ i=13, orderId=3 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_1 Message body: Hello RocketMQ i=17, orderId=7 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_1 Message body: Hello RocketMQ i=23, orderId=3 consumerResult SUSPEND_CURRENT_QUEUE_A_MOMENT
 * ...omit
 * ConsumeMessageThread_please_rename_unique_group_name_3_4 Message body: Hello RocketMQ i=23, orderId=3 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_5 Message body: Hello RocketMQ i=52, orderId=2 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_4 Message body: Hello RocketMQ i=27, orderId=7 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_5 Message body: Hello RocketMQ i=62, orderId=2 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_4 Message body: Hello RocketMQ i=33, orderId=3 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_5 Message body: Hello RocketMQ i=72, orderId=2 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_4 Message body: Hello RocketMQ i=37, orderId=7 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_5 Message body: Hello RocketMQ i=82, orderId=2 consumerResult SUCCESS
 * ConsumeMessageThread_please_rename_unique_group_name_3_4 Message body: Hello RocketMQ i=43, orderId=3 consumerResult SUCCESS
 * ...omit
 * </pre>
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest", "TagA || TagC || TagD");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                ConsumeOrderlyStatus consumerResult = getConsumerResult(consumeTimes, context);
                printMsg(msgs, consumerResult);
                return consumerResult;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    private static ConsumeOrderlyStatus getConsumerResult(AtomicLong consumeTimes, ConsumeOrderlyContext context) {
        consumeTimes.incrementAndGet();
        if ((consumeTimes.get() % 2) == 0) {
            return ConsumeOrderlyStatus.SUCCESS;
        } else if ((consumeTimes.get() % 5) == 0) {
            context.setSuspendCurrentQueueTimeMillis(3000);
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }

        return ConsumeOrderlyStatus.SUCCESS;
    }

    private static void printMsg(List<MessageExt> msgs, ConsumeOrderlyStatus consumerResult) {
        if (null == msgs) {
            return;
        }
        for (MessageExt msg : msgs) {
            System.out.printf("%s Message body: %s consumerResult %s \n", Thread.currentThread().getName(), new String(msg.getBody()), consumerResult);
        }
    }

}
