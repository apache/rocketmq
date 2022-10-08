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

package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQPushConsumerImplTest {
    @Mock
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Rule
    public ExpectedException thrown = ExpectedException.none();


    @Test
    public void checkConfigTest() throws MQClientException {

        //test type
        thrown.expect(MQClientException.class);

        //test message
        thrown.expectMessage("consumeThreadMin (10) is larger than consumeThreadMax (9)");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");

        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(9);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(consumer, null);
        defaultMQPushConsumerImpl.start();
    }

    @Test
    public void testHook() throws Exception {
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(defaultMQPushConsumer, null);
        defaultMQPushConsumerImpl.registerConsumeMessageHook(new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "consumerHook";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                assertThat(context).isNotNull();
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
                assertThat(context).isNotNull();
            }
        });
        defaultMQPushConsumerImpl.registerFilterMessageHook(new FilterMessageHook() {
            @Override
            public String hookName() {
                return "filterHook";
            }

            @Override
            public void filterMessage(FilterMessageContext context) {
                assertThat(context).isNotNull();
            }
        });
        defaultMQPushConsumerImpl.executeHookBefore(new ConsumeMessageContext());
        defaultMQPushConsumerImpl.executeHookAfter(new ConsumeMessageContext());
    }

    @Ignore
    @Test
    public void testPush() throws Exception {
        when(defaultMQPushConsumer.getMessageListener()).thenReturn(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                assertThat(msgs).size().isGreaterThan(0);
                assertThat(context).isNotNull();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(defaultMQPushConsumer, null);
        try {
            defaultMQPushConsumerImpl.start();
        } finally {
            defaultMQPushConsumerImpl.shutdown();
        }
    }
}
