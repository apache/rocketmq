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

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

//import java.lang.reflect.Array;
import java.util.HashMap;
//import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.Queue;
import java.util.Comparator;
import java.util.PriorityQueue;

public class OrderedConsumer {

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        consumer.start();


        Queue<MessageExt> integerPriorityQueue = new PriorityQueue<>(1024,idComparator);

        //Queue<MessageExt> queue = new LinkedList<MessageExt>();




        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");

        for (MessageQueue mq : mqs) {
            try {
                PullResult pullResult =
                        consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 1);

                System.out.printf("%s%n", pullResult);

                for (MessageExt mes : pullResult.getMsgFoundList()) {
                    integerPriorityQueue.add(mes);
                }
                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());


            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        boolean isEmpty = false;
        int queueId = 0;


        while (true) {

            MessageExt lastMessage = integerPriorityQueue.poll();
            System.out.printf("Time: %d%n",lastMessage.getBornTimestamp());
            // you can print or deal  the message, this message is




            queueId = lastMessage.getQueueId();

            for (MessageQueue mq : mqs) {
                if (mq.getQueueId() != queueId)
                {
                    continue;
                }

                try {
                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 1);

                    System.out.printf("QueueID:%d %s%n",mq.getQueueId(), pullResult);

                    for (MessageExt mes : pullResult.getMsgFoundList()) {
                        integerPriorityQueue.add(mes);
                    }
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            isEmpty = true;
                            break ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (isEmpty)
            {
                break;
            }
        }
        consumer.shutdown();
    }



    public static Comparator<MessageExt> idComparator = new Comparator<MessageExt>() {
        @Override
        public int compare(MessageExt c1, MessageExt c2) {
            if (c1.getBornTimestamp() > c2.getBornTimestamp()) {
                return 1;
            }
            else
            {
                return 0;
            }
        }
    };


    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}
