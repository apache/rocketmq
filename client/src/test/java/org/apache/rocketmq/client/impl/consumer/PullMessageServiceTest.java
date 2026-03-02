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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullMessageServiceTest {

    @Mock
    private MQClientInstance mQClientFactory;

    @Mock
    private ScheduledExecutorService executorService;

    private PullMessageService pullMessageService;

    private final long defaultTimeout = 3000L;

    private final String defaultGroup = "defaultGroup";

    @Before
    public void init() throws Exception {
        pullMessageService = new PullMessageService(mQClientFactory);
        FieldUtils.writeDeclaredField(pullMessageService, "scheduledExecutorService", executorService, true);
        pullMessageService.start();
    }

    @Test
    public void testProcessPullResult() {
        PopRequest popRequest = mock(PopRequest.class);
        pullMessageService.executePopPullRequestLater(popRequest, defaultTimeout);
        pullMessageService.makeStop();
        pullMessageService.executePopPullRequestLater(popRequest, defaultTimeout);
        verify(executorService, times(1))
                .schedule(any(Runnable.class),
                        eq(defaultTimeout),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testExecutePopPullRequestImmediately() throws IllegalAccessException, InterruptedException {
        PopRequest popRequest = mock(PopRequest.class);
        LinkedBlockingQueue<MessageRequest> messageRequestQueue = mock(LinkedBlockingQueue.class);
        FieldUtils.writeDeclaredField(pullMessageService, "messageRequestQueue", messageRequestQueue, true);
        pullMessageService.executePopPullRequestImmediately(popRequest);
        verify(messageRequestQueue, times(1)).put(any(PopRequest.class));
    }

    @Test
    public void testExecuteTaskLater() {
        Runnable runnable = mock(Runnable.class);
        pullMessageService.executeTaskLater(runnable, defaultTimeout);
        pullMessageService.makeStop();
        pullMessageService.executeTaskLater(runnable, defaultTimeout);
        verify(executorService, times(1))
                .schedule(any(Runnable.class),
                        eq(defaultTimeout),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testExecuteTask() {
        Runnable runnable = mock(Runnable.class);
        pullMessageService.executeTask(runnable);
        pullMessageService.makeStop();
        pullMessageService.executeTask(runnable);
        verify(executorService, times(1)).execute(any(Runnable.class));
    }

    @Test
    public void testGetScheduledExecutorService() {
        assertEquals(executorService, pullMessageService.getScheduledExecutorService());
    }

    @Test
    public void testRun() throws InterruptedException, IllegalAccessException {
        LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<>();
        PopRequest popRequest = mock(PopRequest.class);
        when(popRequest.getMessageRequestMode()).thenReturn(MessageRequestMode.POP);
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        messageRequestQueue.put(popRequest);
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = mock(DefaultMQPushConsumerImpl.class);
        when(mQClientFactory.selectConsumer(any())).thenReturn(defaultMQPushConsumerImpl);
        FieldUtils.writeDeclaredField(pullMessageService, "messageRequestQueue", messageRequestQueue, true);
        new Thread(() -> pullMessageService.run()).start();
        TimeUnit.SECONDS.sleep(1);
        pullMessageService.makeStop();
        verify(mQClientFactory, times(1)).selectConsumer(eq(defaultGroup));
        verify(defaultMQPushConsumerImpl).popMessage(any(PopRequest.class));
    }

    @Test
    public void testRunWithNullConsumer() throws InterruptedException, IllegalAccessException {
        LinkedBlockingQueue<MessageRequest> messageRequestQueue = new LinkedBlockingQueue<>();
        PopRequest popRequest = mock(PopRequest.class);
        when(popRequest.getMessageRequestMode()).thenReturn(MessageRequestMode.POP);
        when(popRequest.getConsumerGroup()).thenReturn(defaultGroup);
        messageRequestQueue.put(popRequest);
        FieldUtils.writeDeclaredField(pullMessageService, "messageRequestQueue", messageRequestQueue, true);
        new Thread(() -> pullMessageService.run()).start();
        TimeUnit.SECONDS.sleep(1);
        pullMessageService.makeStop();
        verify(mQClientFactory, times(1)).selectConsumer(eq(defaultGroup));
    }
}
