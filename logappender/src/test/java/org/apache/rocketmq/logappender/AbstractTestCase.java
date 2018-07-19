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
package org.apache.rocketmq.logappender;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.logappender.common.ProducerInstance;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic test rocketmq broker and name server init
 */
public class AbstractTestCase {

    private static CopyOnWriteArrayList<Message> messages = new CopyOnWriteArrayList<>();

    @Before
    public void mockLoggerAppender() throws Exception {
        DefaultMQProducer defaultMQProducer = spy(new DefaultMQProducer("loggerAppender"));
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Message message = (Message) invocationOnMock.getArgument(0);
                messages.add(message);
                return null;
            }
        }).when(defaultMQProducer).sendOneway(any(Message.class));
        ProducerInstance spy = mock(ProducerInstance.class);
        Field instance = ProducerInstance.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(ProducerInstance.class, spy);
        doReturn(defaultMQProducer).when(spy).getInstance(anyString(), anyString());
    }

    public void clear() {

    }

    protected int consumeMessages(int count, final String key, int timeout) {
        final AtomicInteger cc = new AtomicInteger(0);
        for (Message message : messages) {
            String body = new String(message.getBody());
            if (body.contains(key)) {
                cc.incrementAndGet();
            }
        }
        return cc.get();
    }
}
