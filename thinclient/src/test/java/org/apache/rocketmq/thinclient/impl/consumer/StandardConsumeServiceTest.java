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

package org.apache.rocketmq.thinclient.impl.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.hook.MessageInterceptor;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.metrics.MetricMessageInterceptor;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;
import org.apache.rocketmq.thinclient.tool.TestBase;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardConsumeServiceTest extends TestBase {

    @Test
    public void testDispatch0() {
        final ProcessQueue processQueue0 = mock(ProcessQueue.class);
        final ProcessQueue processQueue1 = mock(ProcessQueue.class);

        final MessageQueueImpl messageQueue0 = fakeMessageQueueImpl0();
        final MessageQueueImpl messageQueue1 = fakeMessageQueueImpl1();

        ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable = new ConcurrentHashMap<>();
        processQueueTable.put(messageQueue0, processQueue0);
        processQueueTable.put(messageQueue1, processQueue1);

        final MessageViewImpl messageView0 = fakeMessageViewImpl(messageQueue0);
        final MessageViewImpl messageView1 = fakeMessageViewImpl(messageQueue1);

        when(processQueue0.tryTakeMessage()).thenReturn(Optional.of(messageView0));
        when(processQueue1.tryTakeMessage()).thenReturn(Optional.of(messageView1));

        MessageListener listener = messageView -> ConsumeResult.OK;

        MessageInterceptor interceptor = new MessageInterceptor() {
            @Override
            public void doBefore(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons) {
            }

            @Override
            public void doAfter(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons,
                Duration duration, MessageHookPointsStatus status) {
            }
        };
        final StandardConsumeService service = new StandardConsumeService(FAKE_CLIENT_ID, processQueueTable, listener, SINGLE_THREAD_POOL_EXECUTOR, interceptor, SCHEDULER);
        service.dispatch0();
        verify(processQueue0, times(1)).eraseMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
        verify(processQueue0, times(1)).eraseMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
    }
}