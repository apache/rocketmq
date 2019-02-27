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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

/**
 * test class for ConsumeMessageOrderlyService
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessageOrderlyServiceTest {

    private static final int DEFAULT_THREAD_MAX = 64;
    private static final int DEFAULT_THREAD_MIN = 20;


    private String consumerGroup;
    private String topic = "FooBar-A";
    private String broker = "BrokerA";


    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private DefaultMQPushConsumer pushConsumer;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private RebalancePushImpl rebalancePushImpl;


    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(6 * 1000);
        pushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return null;
            }
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        field.set(pushConsumerImpl, rebalancePushImpl);

        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();

        mQClientFactory = spy(pushConsumerImpl.getmQClientFactory());
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pushConsumerImpl, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        pullAPIWrapper = spy(new PullAPIWrapper(mQClientFactory, consumerGroup, false));
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("pullAPIWrapper");
        field.setAccessible(true);
        field.set(pushConsumerImpl, pullAPIWrapper);

        pushConsumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().setmQClientFactory(mQClientFactory);
        mQClientFactory.registerConsumer(consumerGroup, pushConsumerImpl);

    }


    @After
    public void terminate() {
        pushConsumer.shutdown();
    }


    @Test
    public void testConsumeMessageOrderlyService() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        Assert.assertNotNull(service);
    }


    @Test
    public void testStart() {
        try {
            this.runStartOrShutdown(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testShutdown() {
        try {
            this.runStartOrShutdown(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testUpdateCorePoolSize() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        // 0
        int corePoolSizeMin = 0;
        service.updateCorePoolSize(corePoolSizeMin);
        assertThat(service.getCorePoolSize()).isEqualTo(DEFAULT_THREAD_MIN);

        // 1-64
        int corePoolSizeMiddle = new Random().nextInt(DEFAULT_THREAD_MAX - 1) + 1;
        service.updateCorePoolSize(corePoolSizeMiddle);
        assertThat(service.getCorePoolSize()).isEqualTo(corePoolSizeMiddle);

        // >=64
        int corePoolSizeMax = new Random().nextInt(DEFAULT_THREAD_MAX) + DEFAULT_THREAD_MAX;
        service.updateCorePoolSize(corePoolSizeMax);
        assertThat(service.getCorePoolSize()).isEqualTo(corePoolSizeMiddle);
    }


    @Test
    public void testIncCorePoolSize() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        service.incCorePoolSize();
    }


    @Test
    public void testDecCorePoolSize() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        service.decCorePoolSize();
    }


    @Test
    public void testGetCorePoolSize() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        int corePoolSize = service.getCorePoolSize();
        boolean shortMax = Short.MAX_VALUE >= corePoolSize;
        boolean threadMax = pushConsumer.getDefaultMQPushConsumerImpl().getDefaultMQPushConsumer().getConsumeThreadMax() > corePoolSize;
        Assert.assertTrue(corePoolSize > 0);
        Assert.assertTrue(shortMax);
        Assert.assertTrue(threadMax);
    }


    @Test
    public void testConsumeMessageDirectly_Null() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return null;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_RETURN_NULL);
    }


    @Test
    public void testConsumeMessageDirectly_Success() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_SUCCESS);
    }


    @Test
    public void testConsumeMessageDirectly_Suspend() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_LATER);
    }


    @Test
    public void testConsumeMessageDirectly_Commit() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.COMMIT;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_COMMIT);
    }


    @Test
    public void testConsumeMessageDirectly_Rollback() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.ROLLBACK;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_ROLLBACK);
    }


    @Test
    public void testConsumeMessageDirectly_Exception() {
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                int r = 1 / 0;
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });
        MessageExt msg = new MessageExt();
        ConsumeMessageDirectlyResult resultNull = service.consumeMessageDirectly(msg, broker);
        assertThat(resultNull.getConsumeResult()).isEqualTo(CMResult.CR_THROW_EXCEPTION);
    }


    @Test
    public void testSubmitConsumeRequest() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];
        ConsumeMessageOrderlyService service = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                messageExts[0] = msgs.get(0);
                countDownLatch.countDown();
                return null;
            }
        });
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(service);
        ProcessQueue processQueue = new ProcessQueue();
        MessageQueue messageQueue = new MessageQueue();
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        messageExtList.add(messageExts[0]);
        service.submitConsumeRequest(messageExtList, processQueue, messageQueue, false);
        service.submitConsumeRequest(messageExtList, processQueue, messageQueue, true);
    }


    private void runStartOrShutdown(boolean shutdown) throws Exception {
        pushConsumer.setMessageModel(MessageModel.BROADCASTING);
        ConsumeMessageOrderlyService serviceBroadcasting = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        serviceBroadcasting.start();
        Field fieldB = ConsumeMessageOrderlyService.class.getDeclaredField("stopped");
        fieldB.setAccessible(true);
        Assert.assertEquals(false, fieldB.get(serviceBroadcasting));
        if (shutdown) {
            serviceBroadcasting.shutdown();
            Assert.assertEquals(true, fieldB.get(serviceBroadcasting));
        }

        pushConsumer.setMessageModel(MessageModel.CLUSTERING);
        ConsumeMessageOrderlyService serviceClustering = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        serviceClustering.start();
        Field fieldC = ConsumeMessageOrderlyService.class.getDeclaredField("stopped");
        fieldC.setAccessible(true);
        Assert.assertEquals(false, fieldC.get(serviceClustering));
        if (shutdown) {
            serviceClustering.shutdown();
            Assert.assertEquals(true, fieldC.get(serviceClustering));
        }
    }
}
