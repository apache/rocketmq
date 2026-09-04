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
package org.apache.rocketmq.remoting.netty;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingAbstractTest {
    @Spy
    private NettyRemotingAbstract remotingAbstract = new NettyRemotingClient(new NettyClientConfig());

    @Test
    public void testProcessResponseCommand() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                assertThat(semaphore.availablePermits()).isEqualTo(0);
            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    @Test
    public void testProcessResponseCommand_NullCallBack() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, null,
            new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testProcessResponseCommand_RunCallBackInCurrentThread() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                assertThat(semaphore.availablePermits()).isEqualTo(0);
            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);
        when(remotingAbstract.getCallbackExecutor()).thenReturn(null);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back finished in current thread
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    @Test
    public void testScanResponseTable() {
        int dummyId = 1;
        // mock timeout
        ResponseFuture responseFuture = new ResponseFuture(null, dummyId, -1000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {

            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, null);
        remotingAbstract.responseTable.putIfAbsent(dummyId, responseFuture);
        remotingAbstract.scanResponseTable();
        assertNull(remotingAbstract.responseTable.get(dummyId));
    }

    @Test
    public void testProcessRequestCommand() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(1, null);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, request, 3000,
            new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    assertThat(semaphore.availablePermits()).isEqualTo(0);
                }

                @Override
                public void operationFail(Throwable throwable) {

                }
            }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);
        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    @Test
    public void testNettyEventExecutorShutdownDoesNotWaitForPollTimeout() throws InterruptedException {
        // NettyEventExecutor blocks in eventQueue.poll(3000ms) rather than waitForRunning, so the
        // wakeup() performed by ServiceThread.shutdown() cannot interrupt it: the thread only
        // notices the stopped flag once the poll expires, making every shutdown wait up to 3s.
        // A broker or client shutdown pays this for each remoting instance it owns.
        NettyRemotingAbstract remoting = new NettyRemotingClient(new NettyClientConfig());
        remoting.nettyEventExecutor.start();

        // let the thread reach the blocking poll
        TimeUnit.MILLISECONDS.sleep(300);

        long begin = System.currentTimeMillis();
        remoting.nettyEventExecutor.shutdown();
        long elapsed = System.currentTimeMillis() - begin;

        assertThat(remoting.nettyEventExecutor.isStopped()).isTrue();
        assertThat(elapsed).isLessThan(1000);
    }
}