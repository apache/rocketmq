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
package com.alibaba.rocketmq.test.integration.normal;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class ProducerTest extends NormalBaseTest {
    private DefaultMQProducer producer;


    @Before
    public void before() throws Exception {
        producer = new DefaultMQProducer(producerGroup);
        producer.setInstanceName(instanceName);
        producer.start();
    }


    @Test
    public void producerTest() throws MQClientException, InterruptedException {
        for (int i = 0; i < 1000; i++) {
            try {
                Message msg = new Message(topic,// topic
                    tags[i % tags.length],// tag
                    "KEY" + i,// key
                    ("Hello RocketMQ " + i).getBytes()// body
                        );
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
            catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}
