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

package org.apache.rocketmq.proxy.processor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class DefaultMessagingProcessorTest extends BaseProcessorTest {

    private DefaultMessagingProcessor defaultMessagingProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.defaultMessagingProcessor = new DefaultMessagingProcessor(this.serviceManager);
    }

    @Test
    public void testRequestShouldRestoreOpaqueWhenForwardFails() {
        CompletableFuture<RemotingCommand> forwardFuture = new CompletableFuture<>();
        forwardFuture.completeExceptionally(new RuntimeException("mock forward failed"));
        AtomicInteger forwardOpaque = new AtomicInteger(-1);
        when(this.messageService.request(any(), anyString(), any(), anyLong())).thenAnswer(invocation -> {
            forwardOpaque.set(((RemotingCommand) invocation.getArgument(2)).getOpaque());
            return forwardFuture;
        });

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        int originalOpaque = 12345;
        request.setOpaque(originalOpaque);

        Assert.assertThrows(CompletionException.class,
            () -> this.defaultMessagingProcessor.request(createContext(), "broker-a", request, 3000).join());

        Assert.assertNotEquals(originalOpaque, forwardOpaque.get());
        Assert.assertEquals(originalOpaque, request.getOpaque());
    }

    @Test
    public void testRequestOnewayShouldRestoreOpaqueWhenForwardFails() {
        CompletableFuture<Void> forwardFuture = new CompletableFuture<>();
        forwardFuture.completeExceptionally(new RuntimeException("mock oneway forward failed"));
        when(this.messageService.requestOneway(any(), anyString(), any(), anyLong())).thenReturn(forwardFuture);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        int originalOpaque = 12345;
        request.setOpaque(originalOpaque);

        Assert.assertThrows(CompletionException.class,
            () -> this.defaultMessagingProcessor.requestOneway(createContext(), "broker-a", request, 3000).join());

        Assert.assertEquals(originalOpaque, request.getOpaque());
    }

    @Test
    public void testRequestShouldRestoreOpaqueWhenForwardThrows() {
        when(this.messageService.request(any(), anyString(), any(), anyLong()))
            .thenThrow(new RuntimeException("mock forward throws"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        int originalOpaque = 12345;
        request.setOpaque(originalOpaque);

        Assert.assertThrows(RuntimeException.class,
            () -> this.defaultMessagingProcessor.request(createContext(), "broker-a", request, 3000));

        Assert.assertEquals(originalOpaque, request.getOpaque());
    }

    @Test
    public void testRequestOnewayShouldRestoreOpaqueWhenForwardThrows() {
        when(this.messageService.requestOneway(any(), anyString(), any(), anyLong()))
            .thenThrow(new RuntimeException("mock oneway forward throws"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        int originalOpaque = 12345;
        request.setOpaque(originalOpaque);

        Assert.assertThrows(RuntimeException.class,
            () -> this.defaultMessagingProcessor.requestOneway(createContext(), "broker-a", request, 3000));

        Assert.assertEquals(originalOpaque, request.getOpaque());
    }
}
