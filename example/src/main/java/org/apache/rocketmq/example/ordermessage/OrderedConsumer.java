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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class OrderedConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("OrderedMsgTest", "TagA || TagC || TagD");

        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

                /*
                 * The default value of AutoCommit is true and RocketMQ will commit offset automatically
                 * but if you set AutoCommit to false, the offset can only be commited by returning ConsumeOrderlyStatus.COMMIT
                 */
                context.setAutoCommit(false);

                /*
                 * Messages with the same orderID will be consumed sequentially
                 */
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                /*
                 * Returning ConsumeOrderlyStatus.SUCCESS means the messages was consumed successfully,
                 * but the offset will not be committed if AutoCommit is false
                 *
                 * {@code
                 * return ConsumeOrderlyStatus.SUCCESS;
                 * }
                 */

                /*
                 * Returning ConsumeOrderlyStatus.ROLLBACK means the messages after the commit point will be re-consumed
                 *
                 * {@code
                 * return ConsumeOrderlyStatus.ROLLBACK;
                 * }
                 */

                /*
                 * Returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT means
                 * the message consumption of the current queue will be suspended for a while.
                 *
                 * {@code
                 * context.setSuspendCurrentQueueTimeMillis(3000);
                 * return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                 * }
                 */


                /*
                 * Returning ConsumeOrderlyStatus.COMMIT means the max offset of the list will be committed
                 */
                return ConsumeOrderlyStatus.COMMIT;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
