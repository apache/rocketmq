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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.consumer.pull;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.nio.channels.Pipe;
import java.util.*;

public class PullSizeTest extends BaseConf {

    private static Logger logger = Logger.getLogger(PullSizeTest.class);

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static final String PULL_SIZE_TOPIC = "TopicPullTest";

    public static final String PULL_SIZE_GROUP = "pullSizeTest";

    public boolean send() throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(PULL_SIZE_GROUP);
        producer.setNamesrvAddr(nsAddr);
        producer.start();
        int successCount = 0;
        for (int i = 0; i < 1000; i++) {
            try {
                Message msg = new Message(PULL_SIZE_TOPIC, "TagA", ("RocketMQ pull size test index " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
                    successCount++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(500);
            }
        }
        producer.shutdown();
        if (successCount > 800) {
            return true;
        } else {
            return false;
        }
    }


    public void pullMsg() throws MQClientException {
        boolean result = false;
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(PULL_SIZE_GROUP);
        consumer.setNamesrvAddr(nsAddr);
        consumer.start();
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(PULL_SIZE_TOPIC);
        for (MessageQueue mq : mqs) {
            if (result) {
                break;
            }
            try {
                PullResult pullResult = consumer.pull(mq, null, getMessageQueueOffset(mq), 32);
                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                        if (msgFoundList != null) {
                            logger.info("[RECV] received msg queue:" + mq.getBrokerName() + "-" + mq.getQueueId());
                            result |= msgFoundList.size() >= 32;
                        }
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                logger.error("[SEND] send failed", e);
                e.printStackTrace();
            }
        }
        consumer.shutdown();
        Assert.assertTrue(result);
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

    @Test
    public void testPullSize() throws MQClientException, InterruptedException {
        IntegrationTestBase.initTopic(PULL_SIZE_TOPIC, nsAddr, clusterName);
        logger.info("[ROCKETMQ] nameserver and broker init success");
        boolean send = send();
        Assert.assertTrue(send);
        logger.info("[PULLSIZE] send success");
        pullMsg();
    }
}
