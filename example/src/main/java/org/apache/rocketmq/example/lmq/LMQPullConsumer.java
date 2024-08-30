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
package org.apache.rocketmq.example.lmq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

@SuppressWarnings("deprecation")
public class LMQPullConsumer {
    public static final String BROKER_NAME = "broker-a";

    public static final String CONSUMER_GROUP = "CID_LMQ_PULL_1";

    public static final String TOPIC = "TopicLMQParent";

    public static final String LMQ_TOPIC = MixAll.LMQ_PREFIX + "123";

    public static final String NAMESRV_ADDR = "127.0.0.1:9876";

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setRegisterTopics(new HashSet<>(Arrays.asList(TOPIC)));
        consumer.start();

        // use parent topic to fill up broker addr table
        consumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory()
            .updateTopicRouteInfoFromNameServer(TOPIC);

        final MessageQueue lmq = new MessageQueue(LMQ_TOPIC, BROKER_NAME, (int) MixAll.LMQ_QUEUE_ID);
        long offset = consumer.minOffset(lmq);

        consumer.pullBlockIfNotFound(lmq, "*", offset, 32, new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                List<MessageExt> list = pullResult.getMsgFoundList();
                if (list == null || list.isEmpty()) {
                    return;
                }

                for (MessageExt msg : list) {
                    System.out.printf("%s Pull New Messages: %s %n", Thread.currentThread().getName(), msg);
                }
            }

            @Override
            public void onException(Throwable e) {

            }
        });
    }
}
