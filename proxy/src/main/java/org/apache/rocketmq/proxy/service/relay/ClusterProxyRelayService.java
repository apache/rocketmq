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
package org.apache.rocketmq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

/**
 * Relay service used when the proxy runs in CLUSTER mode, i.e. there is no co-located broker
 * whose {@code NettyRemotingAbstract} could receive the relayed answer (that is what
 * {@link LocalProxyRelayService} does).
 *
 * <p>In CLUSTER mode the proxy process itself is the caller: it writes a
 * {@link ProxyRelayRequest} into a {@link ProxyChannel} and waits on the future carried by that
 * request. This class therefore hands back a real, non-null relay future (returning {@code null}
 * used to make {@code GrpcChannelManager#addResponseFuture} store a null and the caller never
 * observe any result) and bridges it to the caller's future, including the exceptional path and
 * the {@code GrpcChannelManager#scanExpireResultFuture} timeout completion.
 *
 * <p>A plain {@link RemotingCommand} (not a {@link ProxyRelayRequest}) has no in-process waiter;
 * the relay future is still returned so that the channel implementation can register it for the
 * client answer, but nobody observes the outcome.
 */
public class ClusterProxyRelayService extends AbstractProxyRelayService {

    public ClusterProxyRelayService(TransactionService transactionService) {
        super(transactionService);
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(
        ProxyContext context, RemotingCommand command,
        GetConsumerRunningInfoRequestHeader header) {
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> relayFuture = new CompletableFuture<>();
        bridgeToCaller(command, relayFuture);
        return relayFuture;
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> processConsumeMessageDirectly(
        ProxyContext context, RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header) {
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> relayFuture = new CompletableFuture<>();
        bridgeToCaller(command, relayFuture);
        return relayFuture;
    }

    /**
     * Propagates the relay outcome to the in-process caller that attached its future to the
     * command. Both normal and exceptional completions are forwarded so the caller never has to
     * fall back on its own timeout.
     */
    private static <T> void bridgeToCaller(RemotingCommand command,
        CompletableFuture<ProxyRelayResult<T>> relayFuture) {
        if (!(command instanceof ProxyRelayRequest)) {
            return;
        }
        CompletableFuture<ProxyRelayResult<T>> callerFuture =
            ((ProxyRelayRequest) command).typedResponseFuture();
        if (callerFuture == null) {
            return;
        }
        relayFuture.whenComplete((relayResult, throwable) -> {
            if (throwable != null) {
                callerFuture.completeExceptionally(throwable);
            } else {
                callerFuture.complete(relayResult);
            }
        });
    }
}
