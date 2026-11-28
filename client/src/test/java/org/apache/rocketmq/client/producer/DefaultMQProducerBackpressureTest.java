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
package org.apache.rocketmq.client.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.lock.ReadWriteCASLock;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class DefaultMQProducerBackpressureTest {
    private static final int BYTE_LIMIT = 1024 * 1024;
    private DefaultMQProducer producer;
    private DefaultMQProducerImpl impl;
    private ExecutorService executor;
    private SendCallback callback;

    @Before
    public void setUp() {
        producer = new DefaultMQProducer("BackpressureTest");
        producer.setEnableBackpressureForAsyncMode(true);
        producer.setBackPressureForAsyncSendNum(10);
        producer.setBackPressureForAsyncSendSize(BYTE_LIMIT);
        impl = producer.getDefaultMQProducerImpl();
        // Hold accepted sends before transport so the admission limits fill deterministically.
        executor = mock(ExecutorService.class);
        producer.setAsyncSenderExecutor(executor);
        callback = mock(SendCallback.class);
    }

    @Test
    public void testInterruptedCountWaitReleasesReadLock() throws Exception {
        for (int i = 0; i < 10; i++) {
            send(1);
        }

        interruptBlockedSend(true);

        assertAdmissionState(0, BYTE_LIMIT - 10);
        verify(executor, times(10)).submit(any(Runnable.class));
        verifyNoInteractions(callback);
    }

    @Test
    public void testInterruptedSizeWaitRestoresCountPermitAndReadLock() throws Exception {
        send(BYTE_LIMIT);

        interruptBlockedSend(true);

        assertAdmissionState(9, 0);
        verify(executor).submit(any(Runnable.class));
        verifyNoInteractions(callback);
    }

    @Test
    public void testSizeTimeoutStillRestoresCountPermitAndNotifiesCallback() throws Exception {
        send(BYTE_LIMIT);
        producer.send(new Message("TestTopic", new byte[1]), callback, 50);

        assertAdmissionState(9, 0);
        verify(executor).submit(any(Runnable.class));
        verify(callback).onException(any(RemotingTooMuchRequestException.class));
    }

    @Test
    public void testDefaultAsyncSendReportsInterruptionWithoutLeakingPermits() throws Exception {
        send(BYTE_LIMIT);

        interruptBlockedSend(false);

        assertAdmissionState(9, 0);
        verify(executor).submit(any(Runnable.class));
        verify(callback).onException(any(InterruptedException.class));
    }

    private void send(int size) throws Exception {
        producer.send(new Message("TestTopic", new byte[size]), callback, 10000);
    }

    private void interruptBlockedSend(boolean explicitTimeout) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread sender = new Thread(() -> {
            try {
                if (explicitTimeout) {
                    send(1);
                } else {
                    producer.send(new Message("TestTopic", new byte[1]), callback);
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        }, "interrupted-async-sender");
        sender.start();
        try {
            await().atMost(5, TimeUnit.SECONDS).until(() -> sender.getState() == Thread.State.TIMED_WAITING);
        } finally {
            sender.interrupt();
            sender.join(5000);
        }
        assertThat(sender.isAlive()).isFalse();
        if (explicitTimeout) {
            assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        } else {
            assertThat(failure.get()).isNull();
        }
    }

    private void assertAdmissionState(int countPermits, int sizePermits) throws Exception {
        SoftAssertions assertions = new SoftAssertions();
        assertions.assertThat(impl.getSemaphoreAsyncSendNumAvailablePermits())
            .as("available count permits").isEqualTo(countPermits);
        assertions.assertThat(impl.getSemaphoreAsyncSendSizeAvailablePermits())
            .as("available size permits").isEqualTo(sizePermits);
        for (String field : new String[] {"backPressureForAsyncSendNumLock", "backPressureForAsyncSendSizeLock"}) {
            ReadWriteCASLock lock = (ReadWriteCASLock) FieldUtils.readDeclaredField(producer, field, true);
            // A leaked reader would make the runtime configuration setter spin forever.
            assertions.assertThat(lock.getWriteLock()).as(field + " can be updated").isTrue();
        }
        assertions.assertAll();
    }
}
