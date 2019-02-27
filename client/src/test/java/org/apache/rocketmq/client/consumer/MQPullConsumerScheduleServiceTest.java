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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQPullConsumerScheduleServiceTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private DefaultMQPullConsumer pullConsumer;
    private MQPullConsumerScheduleService pullConsumerScheduleService ;
    private String consumerGroup = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "BrokerA";


    @Before
    public void init() throws Exception {
        pullConsumerScheduleService = new MQPullConsumerScheduleService(consumerGroup) ;
        pullConsumer = pullConsumerScheduleService.getDefaultMQPullConsumer() ;
        pullConsumer.setNamesrvAddr("127.0.0.1:9876");
        pullConsumerScheduleService.setMessageModel(MessageModel.CLUSTERING);
        pullConsumerScheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {
            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    consumer.pull(mq, "*", 1024, 3, new PullCallback() {
                        @Override
                        public void onSuccess(PullResult pullResult) {
                            PullStatus pullStatus = pullResult.getPullStatus();
                            switch (pullStatus) {
                                case FOUND :
                                    assertThat(pullResult).isNotNull();
                                    assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
                                    assertThat(pullResult.getNextBeginOffset()).isEqualTo(1024 + 1);
                                    assertThat(pullResult.getMinOffset()).isEqualTo(123);
                                    assertThat(pullResult.getMaxOffset()).isEqualTo(2048);
                                    assertThat(pullResult.getMsgFoundList()).isEqualTo(new ArrayList<Object>());
                                    break ;
                                case NO_NEW_MSG :
                                    assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
                                    break ;
                                case NO_MATCHED_MSG :
                                case OFFSET_ILLEGAL :
                                default :
                                    break ;
                            }
                        }

                        @Override
                        public void onException(Throwable e) {
                            System.out.println("run is exception");
                        }
                    });
                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (RemotingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                context.setPullNextDelayTimeMillis(100);
            }
        });
        pullConsumerScheduleService.start();

        DefaultMQPullConsumerImpl pullConsumerImpl = pullConsumerScheduleService.getDefaultMQPullConsumer().getDefaultMQPullConsumerImpl();
        PullAPIWrapper pullAPIWrapper = pullConsumerImpl.getPullAPIWrapper();
        Field field = PullAPIWrapper.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pullAPIWrapper, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        when(mQClientFactory.findBrokerAddressInSubscribe(anyString(),anyLong(),anyBoolean())).thenReturn(new FindBrokerResult("127.0.0.1:10911", false));
    }

    @After
    public void terminate() {
        pullConsumerScheduleService.shutdown();
    }


    @Test
    public void testStart_AllbackTable_ShouldNotNull_AfterStart() {
        ConcurrentMap<String, PullTaskCallback> callbackTable = pullConsumerScheduleService.getCallbackTable();
        Assert.assertNotNull(callbackTable);

        PullTaskCallback pullTaskCallback = callbackTable.get(topic);
        Assert.assertNotNull(pullTaskCallback);
    }

    @Test
    public void testStart_PullConsumerShouldNotNUll_AfterStart() {
        Assert.assertNotNull(pullConsumer.getOffsetStore());

        Assert.assertEquals(pullConsumer.getNamesrvAddr(), "127.0.0.1:9876");

        Assert.assertEquals(pullConsumer.getMessageModel(), MessageModel.CLUSTERING);

        Assert.assertEquals(pullConsumer.getConsumerGroup(), consumerGroup);
    }

    @Test
    public void testPullMessage_Success () throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                PullMessageRequestHeader requestHeader = invocation.getArgument(1);
                PullResult pullResult = createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));

                PullCallback pullCallback = invocation.getArgument(4);
                pullCallback.onSuccess(pullResult);
                return null;
            }
        }).when(mQClientAPIImpl).pullMessage(anyString(), any(PullMessageRequestHeader.class), anyLong(), any(CommunicationMode.class), nullable(PullCallback.class));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        PullTaskContext context = new PullTaskContext();
        context.setPullConsumer(pullConsumer);

        PullTaskCallback pullTaskCallback = pullConsumerScheduleService.getCallbackTable().get(topic);
        pullTaskCallback.doPullTask(messageQueue, context);

    }

    @Test
    public void testPullMessage_NotFound() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                PullMessageRequestHeader requestHeader = invocation.getArgument(1);
                PullResult pullResult = createPullResult(requestHeader, PullStatus.NO_NEW_MSG, Collections.singletonList(new MessageExt()));

                PullCallback pullCallback = invocation.getArgument(4);
                pullCallback.onSuccess(pullResult);
                return null;
            }
        }).when(mQClientAPIImpl).pullMessage(anyString(), any(PullMessageRequestHeader.class), anyLong(), any(CommunicationMode.class), nullable(PullCallback.class));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        PullTaskContext context = new PullTaskContext();
        context.setPullConsumer(pullConsumer);

        PullTaskCallback pullTaskCallback = pullConsumerScheduleService.getCallbackTable().get(topic);
        pullTaskCallback.doPullTask(messageQueue, context);
    }
    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
                                           List<MessageExt> messageExtList) throws Exception {
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, new byte[] {});
    }
}
