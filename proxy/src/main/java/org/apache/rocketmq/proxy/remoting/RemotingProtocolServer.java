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

package org.apache.rocketmq.proxy.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class RemotingProtocolServer implements StartAndShutdown, RemotingProxyOutClient {

    private final MessagingProcessor messagingProcessor;
    private RemotingServer defaultRemotingServer;

    public RemotingProtocolServer(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
    }

    protected void init() {

    }

    protected void registerRemotingServer(RemotingServer remotingServer) {

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public CompletableFuture<RemotingCommand> invokeToClient(Channel channel, RemotingCommand request,
        long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            this.defaultRemotingServer.invokeAsync(channel, request, timeoutMillis, responseFuture -> {
                if (responseFuture.getResponseCommand() == null) {
                    future.completeExceptionally(new MQClientException("response is null after send request to client", responseFuture.getCause()));
                    return;
                }
                future.complete(responseFuture.getResponseCommand());
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
