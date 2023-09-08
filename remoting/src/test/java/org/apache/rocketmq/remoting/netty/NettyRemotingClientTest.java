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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingClientTest {
    @Spy
    private NettyRemotingClient remotingClient = new NettyRemotingClient(new NettyClientConfig());

    @Test
    public void testSetCallbackExecutor() throws NoSuchFieldException, IllegalAccessException {        
        ExecutorService customized = Executors.newCachedThreadPool();
        remotingClient.setCallbackExecutor(customized);
        assertThat(remotingClient.getCallbackExecutor()).isEqualTo(customized);
    }

    @Test
    public void testInvokeResponse() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        RemotingCommand actual = future.get();
        assertThat(actual).isEqualTo(response);
    }

    @Test
    public void testRemotingSendRequestException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            callback.operationFail(new RemotingSendRequestException(null));
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause()).isInstanceOf(RemotingSendRequestException.class);
    }

    @Test
    public void testRemotingTimeoutException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            callback.operationFail(new RemotingTimeoutException(""));
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause()).isInstanceOf(RemotingTimeoutException.class);
    }

    @Test
    public void testRemotingException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            callback.operationFail(new RemotingException(null));
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause()).isInstanceOf(RemotingException.class);
    }

    @Test
    public void testInvokeOnewayException() throws Exception {
        String addr = "0.0.0.0";
        try {
            remotingClient.invokeOneway(addr, null, 1000);
        } catch (RemotingConnectException e) {
            assertThat(e.getMessage()).contains(addr);
        }
    }
}
