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
package com.alibaba.rocketmq.test.integration.order;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交）
 * 
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class PushConsumerTest extends OrderBaseTest {
    private DefaultMQPushConsumer consumer;


    @Before
    public void before() throws Exception {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setInstanceName(instanceName + System.nanoTime());
        consumer.subscribe(topic, getTagsExpression(0));
        consumer.subscribe("qatest_TopicTest3_virtual_order", getTagsExpression(0));
        // consumer.subscribe(topic, getTagsExpression(3));
    }


    @Test
    public void pushConsumerFromLastOffset() throws MQClientException {
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageListener listener = getRollBackMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void pushConsumerWithAutoCommitFromLastOffset() throws MQClientException {
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        MessageListener listener = getAutoCommitMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void pushConsumerWithAutoCommitFromMinOffset() throws MQClientException {
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        MessageListener listener = getAutoCommitMessageListener();
        consumer.registerMessageListener(listener);

        consumer.start();
        System.out.println("Consumer Started.");
        try {
            System.in.read();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static MessageListenerOrderly getRollBackMessageListener() {
        return new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);


            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.println("MESSAGE BODY========" + new String(msgs.get(0).getBody()));
                context.setAutoCommit(false);
                this.consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 1) == 0) {
                    System.out.println("SUCCESS========Receive New Messages: " + msgs);
                    return ConsumeOrderlyStatus.SUCCESS;
                }
                else if ((this.consumeTimes.get() % 2) == 0) {
                    System.out.println("ROLLBACK========Receive New Messages: " + msgs);
                    return ConsumeOrderlyStatus.ROLLBACK;
                }
                else if ((this.consumeTimes.get() % 3) == 0) {
                    System.out.println("COMMIT========Receive New Messages: " + msgs);
                    return ConsumeOrderlyStatus.COMMIT;
                }
                else if ((this.consumeTimes.get() % 5) == 0) {
                    System.out.println("SUSPEND_CURRENT_QUEUE_A_MOMENT========Receive New Messages: "
                            + msgs.get(0));
                    context.setSuspendCurrentQueueTimeMillis(3000);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
    }


    private static MessageListenerOrderly getAutoCommitMessageListener() {
        return new MessageListenerOrderly() {
            AtomicLong consumeTimes = new AtomicLong(0);


            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.println("MESSAGE BODY========" + new String(msgs.get(0).getBody()));
                this.consumeTimes.incrementAndGet();
                if ((this.consumeTimes.get() % 1) == 0) {
                    System.out.println("SUCCESS========Receive New Messages: " + msgs);
                    return ConsumeOrderlyStatus.SUCCESS;
                }
                else if ((this.consumeTimes.get() % 2) == 0) {
                    System.out.println("ROLLBACK========Receive New Messages: " + msgs);
                    return ConsumeOrderlyStatus.ROLLBACK;
                }
                else if ((this.consumeTimes.get() % 5) == 0) {
                    System.out.println("SUSPEND_CURRENT_QUEUE_A_MOMENT========Receive New Messages: "
                            + msgs.get(0));
                    context.setSuspendCurrentQueueTimeMillis(3000);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
    }

}
