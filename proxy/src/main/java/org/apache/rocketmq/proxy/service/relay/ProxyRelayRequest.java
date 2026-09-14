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

import com.alibaba.fastjson2.annotation.JSONField;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;

/**
 * A {@link RemotingCommand} that travels through the in-process relay pipeline
 * ({@code ProxyChannel#writeAndFlush}) instead of a real Netty socket, and therefore has to
 * carry the caller's {@link CompletableFuture} along with the wire fields.
 *
 * <p>Why this exists: {@code ProxyChannel#writeAndFlush(Object)} can only return a Netty
 * {@code ChannelFuture}, so an in-process caller (CLUSTER-mode proxy admin) that writes a
 * {@code GET_CONSUMER_RUNNING_INFO} / {@code CONSUME_MESSAGE_DIRECTLY} command into a
 * {@code ProxyChannel} has no way to observe the client's answer. The relay future created by
 * {@link ProxyRelayService} is consumed by the concrete channel implementation (it is handed to
 * {@code GrpcChannelManager#addResponseFuture}) and never surfaces again. Attaching the caller's
 * future to the command lets {@link ClusterProxyRelayService} bridge the two.
 *
 * <p><b>Payload typing.</b> The future is stored type-erased. The request code fully determines
 * the payload type and is the only contract between the caller and the relay:
 * <ul>
 *   <li>{@code RequestCode.GET_CONSUMER_RUNNING_INFO} &rarr; a future of {@link ProxyRelayResult}
 *       whose payload is a {@link ConsumerRunningInfo}</li>
 *   <li>{@code RequestCode.CONSUME_MESSAGE_DIRECTLY} &rarr; a future of {@link ProxyRelayResult}
 *       whose payload is a {@link ConsumeMessageDirectlyResult}</li>
 * </ul>
 *
 * <p>This command is strictly in-process: it is never encoded onto a socket, so the non-wire
 * field below is excluded from the fastjson2 view of the command.
 */
public class ProxyRelayRequest extends RemotingCommand {

    /**
     * The in-process caller's future, kept type-erased (see the class javadoc for the code to
     * payload mapping). Never serialized.
     */
    @JSONField(serialize = false)
    private final transient CompletableFuture<?> responseFuture;

    protected ProxyRelayRequest(CompletableFuture<?> responseFuture) {
        this.responseFuture = responseFuture;
    }

    /**
     * Mirrors {@link RemotingCommand#createRequestCommand(int, CommandCustomHeader)}. The only
     * difference is that the private {@code customHeader} field of the superclass is populated
     * through the public {@link #writeCustomHeader(CommandCustomHeader)} hook, so that
     * {@code ProxyChannel#writeAndFlush} can read it back with {@code readCustomHeader()}.
     *
     * @param code           {@code RequestCode.GET_CONSUMER_RUNNING_INFO} or
     *                       {@code RequestCode.CONSUME_MESSAGE_DIRECTLY}
     * @param customHeader   the matching request header
     * @param responseFuture the caller's future, typed as documented on the class
     */
    public static ProxyRelayRequest createRequestCommand(int code, CommandCustomHeader customHeader,
        CompletableFuture<?> responseFuture) {
        ProxyRelayRequest cmd = new ProxyRelayRequest(responseFuture);
        cmd.setCode(code);
        cmd.writeCustomHeader(customHeader);
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * Type-erased view of the caller's future. Prefer {@link #typedResponseFuture()} unless the
     * caller only needs to know whether a future was attached at all.
     */
    public CompletableFuture<?> getResponseFuture() {
        return responseFuture;
    }

    /**
     * Typed view of the caller's future, e.g.
     * {@code CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>>}.
     *
     * @param <T> the payload type implied by the request code
     */
    public <T> CompletableFuture<ProxyRelayResult<T>> typedResponseFuture() {
        return castResponseFuture(this.responseFuture);
    }

    /**
     * The single unavoidable unchecked cast of this design.
     *
     * <p>It is safe because both ends of the future live inside the same proxy process and are
     * written by the same feature: the caller creates
     * {@code CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>>} (or the
     * {@link ConsumeMessageDirectlyResult} twin), pairs it with the matching request code in
     * {@link #createRequestCommand}, and the relay only ever completes it with the payload of the
     * same {@link ProxyRelayService} method that the request code selected. The command never
     * crosses a process boundary, so no untrusted producer can violate the pairing.
     */
    @SuppressWarnings("unchecked")
    private static <T> CompletableFuture<ProxyRelayResult<T>> castResponseFuture(CompletableFuture<?> raw) {
        return (CompletableFuture<ProxyRelayResult<T>>) raw;
    }
}
