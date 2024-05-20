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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.local.LocalChannel;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingClientTest {
    @Spy
    private NettyRemotingClient remotingClient = new NettyRemotingClient(new NettyClientConfig());
    @Mock
    private RPCHook rpcHookMock;

    @Test
    public void testSetCallbackExecutor() {
        ExecutorService customized = Executors.newCachedThreadPool();
        remotingClient.setCallbackExecutor(customized);
        assertThat(remotingClient.getCallbackExecutor()).isEqualTo(customized);
    }

    @Test
    public void testInvokeResponse() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
        responseFuture.setResponseCommand(response);
        CompletableFuture<RemotingCommand> future0 = new CompletableFuture<>();
        future0.complete(responseFuture.getResponseCommand());
        doReturn(future0).when(remotingClient).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        RemotingCommand actual = future.get();
        assertThat(actual).isEqualTo(response);
    }

    @Test
    public void testRemotingSendRequestException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        CompletableFuture<RemotingCommand> future0 = new CompletableFuture<>();
        future0.completeExceptionally(new RemotingSendRequestException(null));
        doReturn(future0).when(remotingClient).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause()).isInstanceOf(RemotingSendRequestException.class);
    }

    @Test
    public void testRemotingTimeoutException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        CompletableFuture<RemotingCommand> future0 = new CompletableFuture<>();
        future0.completeExceptionally(new RemotingTimeoutException(""));
        doReturn(future0).when(remotingClient).invoke(anyString(), any(RemotingCommand.class), anyLong());

        CompletableFuture<RemotingCommand> future = remotingClient.invoke("0.0.0.0", request, 1000);
        Throwable thrown = catchThrowable(future::get);
        assertThat(thrown.getCause()).isInstanceOf(RemotingTimeoutException.class);
    }

    @Test
    public void testRemotingException() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        CompletableFuture<RemotingCommand> future0 = new CompletableFuture<>();
        future0.completeExceptionally(new RemotingException(""));
        doReturn(future0).when(remotingClient).invoke(anyString(), any(RemotingCommand.class), anyLong());

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

    @Test
    public void testInvoke0() throws ExecutionException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        Channel channel = new MockChannel() {
            @Override
            public ChannelFuture writeAndFlush(Object msg) {
                ResponseFuture responseFuture = remotingClient.responseTable.get(request.getOpaque());
                responseFuture.setResponseCommand(response);
                responseFuture.executeInvokeCallback();
                return super.writeAndFlush(msg);
            }
        };
        CompletableFuture<ResponseFuture> future = remotingClient.invoke0(channel, request, 1000L);
        assertThat(future.get().getResponseCommand()).isEqualTo(response);
    }

    @Test
    public void testInvoke0WithException() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        Channel channel = new MockChannel() {
            @Override
            public ChannelFuture writeAndFlush(Object msg) {
                ResponseFuture responseFuture = remotingClient.responseTable.get(request.getOpaque());
                responseFuture.executeInvokeCallback();
                return super.writeAndFlush(msg);
            }
        };
        CompletableFuture<ResponseFuture> future = remotingClient.invoke0(channel, request, 1000L);
        assertThatThrownBy(future::get).getCause().isInstanceOf(RemotingException.class);
    }

    @Test
    public void testInvokeSync() throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        remotingClient.registerRPCHook(rpcHookMock);

        Channel channel = new LocalChannel();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, 1000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }
        }, new SemaphoreReleaseOnlyOnce(new Semaphore(1)));
        responseFuture.setResponseCommand(response);
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        future.complete(responseFuture);

        doReturn(future).when(remotingClient).invoke0(any(Channel.class), any(RemotingCommand.class), anyLong());
        RemotingCommand actual = remotingClient.invokeSyncImpl(channel, request, 1000);
        assertThat(actual).isEqualTo(response);

        verify(rpcHookMock).doBeforeRequest(anyString(), eq(request));
        verify(rpcHookMock).doAfterResponse(anyString(), eq(request), eq(response));
    }

    @Test
    public void testInvokeAsync() {
        remotingClient.registerRPCHook(rpcHookMock);
        Channel channel = new LocalChannel();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, 1000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }
        }, new SemaphoreReleaseOnlyOnce(new Semaphore(1)));
        responseFuture.setResponseCommand(response);
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        future.complete(responseFuture);

        doReturn(future).when(remotingClient).invoke0(any(Channel.class), any(RemotingCommand.class), anyLong());

        InvokeCallback callback = mock(InvokeCallback.class);
        remotingClient.invokeAsyncImpl(channel, request, 1000, callback);
        verify(callback, times(1)).operationSucceed(eq(response));
        verify(callback, times(1)).operationComplete(eq(responseFuture));
        verify(callback, never()).operationFail(any());

        verify(rpcHookMock).doBeforeRequest(anyString(), eq(request));
        verify(rpcHookMock).doAfterResponse(anyString(), eq(request), eq(response));
    }

    @Test
    public void testInvokeAsyncFail() {
        remotingClient.registerRPCHook(rpcHookMock);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        Channel channel = new LocalChannel();
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        future.completeExceptionally(new RemotingException(null));

        doReturn(future).when(remotingClient).invoke0(any(Channel.class), any(RemotingCommand.class), anyLong());

        InvokeCallback callback = mock(InvokeCallback.class);
        remotingClient.invokeAsyncImpl(channel, request, 1000, callback);
        verify(callback, never()).operationSucceed(any());
        verify(callback, times(1)).operationComplete(any());
        verify(callback, times(1)).operationFail(any());

        verify(rpcHookMock).doBeforeRequest(anyString(), eq(request));
        verify(rpcHookMock, never()).doAfterResponse(anyString(), eq(request), any());
    }

    @Test
    public void testInvokeImpl() throws ExecutionException, InterruptedException {
        remotingClient.registerRPCHook(rpcHookMock);
        Channel channel = new LocalChannel();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, 1000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }
        }, new SemaphoreReleaseOnlyOnce(new Semaphore(1)));
        responseFuture.setResponseCommand(response);
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        future.complete(responseFuture);

        doReturn(future).when(remotingClient).invoke0(any(Channel.class), any(RemotingCommand.class), anyLong());

        CompletableFuture<ResponseFuture> future0 = remotingClient.invokeImpl(channel, request, 1000);
        assertThat(future0.get()).isEqualTo(responseFuture);

        verify(rpcHookMock).doBeforeRequest(anyString(), eq(request));
        verify(rpcHookMock).doAfterResponse(anyString(), eq(request), eq(response));
    }

    @Test
    public void testInvokeImplFail() {
        remotingClient.registerRPCHook(rpcHookMock);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);

        Channel channel = new LocalChannel();
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        future.completeExceptionally(new RemotingException(null));

        doReturn(future).when(remotingClient).invoke0(any(Channel.class), any(RemotingCommand.class), anyLong());

        assertThatThrownBy(() -> remotingClient.invokeImpl(channel, request, 1000).get()).getCause().isInstanceOf(RemotingException.class);

        verify(rpcHookMock).doBeforeRequest(anyString(), eq(request));
        verify(rpcHookMock, never()).doAfterResponse(anyString(), eq(request), any());
    }

    @Test
    public void testIsAddressReachableFail() throws NoSuchFieldException, IllegalAccessException {
        Bootstrap bootstrap = spy(Bootstrap.class);
        Field field = NettyRemotingClient.class.getDeclaredField("bootstrap");
        field.setAccessible(true);
        field.set(remotingClient, bootstrap);
        assertThat(remotingClient.isAddressReachable("0.0.0.0:8080")).isFalse();
        verify(bootstrap).connect(eq("0.0.0.0"), eq(8080));
        assertThat(remotingClient.isAddressReachable("[fe80::]:8080")).isFalse();
        verify(bootstrap).connect(eq("[fe80::]"), eq(8080));
    }
}
