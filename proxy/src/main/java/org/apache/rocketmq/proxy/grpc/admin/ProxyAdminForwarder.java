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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.AdminGrpc;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/**
 * Forwards a Proxy Admin RPC to the peer proxy that actually owns the target client.
 *
 * <p>A gRPC client keeps a single telemetry stream, so only the proxy holding that stream can ask
 * the client for a thread dump, a verify-message run or its running info. {@code
 * GrpcChannelManager#getChannel(clientId)} is therefore local-only and returns {@code null} for a
 * client connected elsewhere. {@link ConsumerManager#findChannel(String, String)} on the other hand
 * also sees clients registered on peer proxies: {@code HeartbeatSyncer} replicates them as
 * {@link RemoteChannel} instances whose {@link RemoteChannel#getRemoteProxyIp()} is the owning
 * proxy's {@code ProxyConfig.localServeAddr}. That is exactly the routing hint this class uses.
 *
 * <p><b>Peer admin port assumption.</b> The admin port is not published anywhere in the heartbeat
 * sync payload ({@code HeartbeatSyncerData} carries {@code remotingListenPort} and
 * {@code grpcServerPort}, not the admin port), so this class assumes the admin port is uniform
 * across the cluster and dials the peer at {@code <remoteProxyIp>:<this proxy's own
 * ProxyConfig.getGrpcAdminServerPort()>}. A cluster that runs heterogeneous admin ports would need
 * the port added to the heartbeat sync record first.
 *
 * <p><b>TLS.</b> Every proxy gRPC server installs {@code ProxyAndTlsProtocolNegotiator}. In the
 * default configuration ({@code tlsTestModeEnable=true}) each JVM generates a fresh
 * {@code SelfSignedCertificate}, so no peer can possibly validate it; because
 * {@code TlsSystemConfig.tlsMode} defaults to {@code permissive} the plaintext handshake is
 * accepted and used. Otherwise an {@link InsecureTrustManagerFactory}-based client SslContext is
 * used: there is no cluster-wide CA distribution for service-to-service calls, and the peer is
 * addressed by bare IP, which no sane certificate carries as a SAN.
 *
 * <p><b>Auth.</b> {@code ProxyAdminAuthInterceptor} derives the caller identity from the inbound
 * {@code authorization} + {@code x-mq-date-time} metadata and there is no service-to-service
 * identity in the open-source proxy. A forwarded call therefore impersonates the original caller:
 * every inbound metadata key is copied verbatim onto the outbound call. The caller must already be
 * authorized for the admin resource on the entry proxy, and the peer re-runs the very same
 * authentication/authorization evaluation against those copied credentials.
 *
 * <p><b>Loop prevention.</b> A forwarded call carries {@link GrpcConstants#ADMIN_FORWARDED}. The
 * receiving proxy detects it with {@link #isInboundForwarded()} and must serve the call locally
 * instead of forwarding again, which also stops a two-proxy ping-pong when both sides see the
 * client as remote.
 */
public class ProxyAdminForwarder implements StartAndShutdown {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * Wire value of {@link GrpcConstants#ADMIN_FORWARDED}.
     */
    private static final String FORWARDED_VALUE = "true";

    /**
     * gRPC reserves the {@code grpc-} prefix; {@link Metadata.Key#of} rejects such names.
     */
    private static final String RESERVED_KEY_PREFIX = "grpc-";

    private static final long SHUTDOWN_GRACE_SECONDS = 5L;

    private final ServiceManager serviceManager;

    /**
     * One channel + stub per remote proxy address ({@code <ip>:<adminPort>}). Channels are created
     * lazily and are long-lived: a proxy set is small and stable, so re-handshaking per RPC would
     * only add latency.
     */
    private final ConcurrentMap<String, PeerConnection> peerConnections = new ConcurrentHashMap<>();

    public ProxyAdminForwarder(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    /**
     * True when this call already arrived forwarded from a peer proxy; such a call must never be
     * forwarded again.
     */
    public boolean isInboundForwarded() {
        Metadata inbound = GrpcConstants.METADATA.get(Context.current());
        if (inbound == null) {
            return false;
        }
        return FORWARDED_VALUE.equalsIgnoreCase(inbound.get(GrpcConstants.ADMIN_FORWARDED));
    }

    /**
     * If the client identified by (group, clientId) is registered on another proxy, forward the RPC
     * there and return true — the caller must then return immediately and let the peer own the
     * observer. Returns false when the client is local, unknown, or forwarding is not possible, so
     * the caller proceeds with its normal local handling.
     *
     * <p>This method never throws. Every "cannot forward" condition degrades to {@code false}; if
     * the decision to forward was already taken and the outbound call then fails, the failure is
     * propagated to {@code responseObserver#onError} and {@code true} is returned — see
     * {@link #invokeOnPeer}.
     *
     * @param group            consumer group the client is registered under
     * @param clientId         client id, as reported by the client itself
     * @param responseObserver the observer of the inbound admin RPC; ownership moves to the peer
     *                         when this method returns true
     * @param invocation       performs the actual unary call on the peer's stub, e.g.
     *                         {@code (stub, observer) -> stub.printThreadStackTrace(request, observer)}
     */
    public <T> boolean forwardIfRemote(String group, String clientId,
        StreamObserver<T> responseObserver,
        BiConsumer<AdminGrpc.AdminStub, StreamObserver<T>> invocation) {
        if (StringUtils.isBlank(clientId)) {
            return false;
        }

        // Loop guard first: a forwarded call must be answered locally even if this proxy still
        // sees the client as remote (stale heartbeat sync), otherwise the two proxies bounce the
        // call until it dies on a deadline.
        if (isInboundForwarded()) {
            log.info("admin call already forwarded by a peer, serving locally. group:{}, clientId:{}",
                group, clientId);
            return false;
        }

        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        if (proxyConfig == null) {
            log.warn("proxy config is not initialized, cannot forward. group:{}, clientId:{}", group, clientId);
            return false;
        }
        Integer adminPort = proxyConfig.getGrpcAdminServerPort();
        if (adminPort == null || adminPort <= 0 || !proxyConfig.isGrpcAdminServerEnable()) {
            log.warn("proxy admin server is not enabled on this cluster, cannot forward. "
                + "group:{}, clientId:{}, adminPort:{}", group, clientId, adminPort);
            return false;
        }

        ClientChannelInfo channelInfo = findClientChannel(group, clientId);
        if (channelInfo == null) {
            return false;
        }
        Channel channel = channelInfo.getChannel();
        if (!ChannelHelper.isRemote(channel)) {
            // A locally connected client: the caller can reach it through its own channel manager.
            return false;
        }
        RemoteChannel remoteChannel = (RemoteChannel) channel;
        String remoteProxyIp = remoteChannel.getRemoteProxyIp();
        if (StringUtils.isBlank(remoteProxyIp)) {
            log.warn("remote channel carries no owning proxy address, cannot forward. "
                + "group:{}, clientId:{}, channel:{}", group, clientId, remoteChannel);
            return false;
        }
        // Self-forward guard: RemoteChannel.remoteProxyIp is the peer's localServeAddr, so a plain
        // string comparison detects "this proxy" and prevents a forward to ourselves.
        if (StringUtils.equals(remoteProxyIp, proxyConfig.getLocalServeAddr())) {
            log.info("client is owned by this proxy, serving locally. group:{}, clientId:{}", group, clientId);
            return false;
        }

        return invokeOnPeer(remoteProxyIp, adminPort, group, clientId, responseObserver, invocation);
    }

    /**
     * Runs {@code invocation} against the peer stub and hands the inbound observer to the peer's
     * response stream.
     *
     * <p>Error translation: this class only holds the caller's {@code StreamObserver}, not the concrete
     * response builder, so it cannot synthesize a business-level {@code Status} message inside the
     * response body. A failed forward is therefore propagated as a gRPC {@code onError} carrying the
     * peer's status (or {@code Status.INTERNAL} when the call could not even be issued), and
     * {@code true} is returned so the caller stops and lets that error reach the client. Callers
     * that want an in-band error instead should pre-check with {@code isInboundForwarded()} /
     * channel locality themselves.
     */
    private <T> boolean invokeOnPeer(String remoteProxyIp, int adminPort, String group, String clientId,
        StreamObserver<T> responseObserver,
        BiConsumer<AdminGrpc.AdminStub, StreamObserver<T>> invocation) {
        String target = remoteProxyIp + ":" + adminPort;
        try {
            PeerConnection peer = this.peerConnections.computeIfAbsent(target,
                key -> createPeerConnection(remoteProxyIp, adminPort));
            AdminGrpc.AdminStub stub = peer.stub.withInterceptors(
                MetadataUtils.newAttachHeadersInterceptor(buildOutboundMetadata()));
            log.info("forwarding admin call to peer proxy. target:{}, group:{}, clientId:{}",
                target, group, clientId);
            invocation.accept(stub, responseObserver);
            return true;
        } catch (Throwable t) {
            log.error("forward admin call to peer proxy failed. target:{}, group:{}, clientId:{}",
                target, group, clientId, t);
            responseObserver.onError(Status.INTERNAL
                .withDescription("forward admin call to peer proxy " + target + " failed: " + t.getMessage())
                .withCause(t)
                .asRuntimeException());
            return true;
        }
    }

    private ClientChannelInfo findClientChannel(String group, String clientId) {
        ConsumerManager consumerManager = this.serviceManager == null
            ? null : this.serviceManager.getConsumerManager();
        if (consumerManager == null) {
            return null;
        }
        try {
            return consumerManager.findChannel(group, clientId);
        } catch (Throwable t) {
            log.warn("find client channel failed. group:{}, clientId:{}", group, clientId, t);
            return null;
        }
    }

    /**
     * Builds the metadata for the outbound call: a verbatim copy of the inbound metadata (so the
     * peer can authenticate/authorize the original caller, see the class javadoc) plus the
     * loop-prevention marker.
     */
    private Metadata buildOutboundMetadata() {
        Metadata outbound = new Metadata();
        Metadata inbound = GrpcConstants.METADATA.get(Context.current());
        if (inbound != null) {
            for (String name : inbound.keys()) {
                if (!isCopyable(name)) {
                    continue;
                }
                try {
                    Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
                    for (String value : inbound.getAll(key)) {
                        outbound.put(key, value);
                    }
                } catch (Throwable t) {
                    // Defensive: an exotic inbound key must not abort the whole forward.
                    log.warn("skip inbound metadata key that cannot be forwarded. key:{}", name, t);
                }
            }
        }
        // discardAll first: Metadata#put appends, and a stale marker from the inbound copy would
        // otherwise leave the header duplicated.
        outbound.discardAll(GrpcConstants.ADMIN_FORWARDED);
        outbound.put(GrpcConstants.ADMIN_FORWARDED, FORWARDED_VALUE);
        return outbound;
    }

    private static boolean isCopyable(String name) {
        if (StringUtils.isBlank(name)) {
            return false;
        }
        // Binary headers cannot be read back through ASCII_STRING_MARSHALLER; grpc-* names are
        // reserved and rejected by Metadata.Key#of.
        return !name.endsWith(Metadata.BINARY_HEADER_SUFFIX) && !name.startsWith(RESERVED_KEY_PREFIX);
    }

    private PeerConnection createPeerConnection(String host, int port) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        if (usePlaintext()) {
            builder.usePlaintext();
        } else {
            builder.sslContext(insecureClientSslContext());
        }
        ManagedChannel channel = builder.build();
        log.info("created proxy admin client channel to peer proxy. target:{}:{}", host, port);
        return new PeerConnection(channel, AdminGrpc.newStub(channel));
    }

    private static boolean usePlaintext() {
        // ENFORCING is the only tlsMode in which the peer's negotiator rejects a plaintext
        // handshake, so it always wins over tlsTestModeEnable.
        if (TlsMode.ENFORCING.equals(TlsSystemConfig.tlsMode)) {
            return false;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        // tlsTestModeEnable=true mints a fresh SelfSignedCertificate per JVM; no peer can validate
        // it, and permissive mode (the default) accepts plaintext, so skip TLS entirely.
        return proxyConfig == null || proxyConfig.isTlsTestModeEnable();
    }

    private static SslContext insecureClientSslContext() {
        try {
            return GrpcSslContexts.configure(
                GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException("build proxy admin client SslContext failed", e);
        }
    }

    @Override
    public void start() throws Exception {
        // Peer connections are created lazily on first forward; nothing to warm up.
    }

    @Override
    public void shutdown() throws Exception {
        for (PeerConnection peer : this.peerConnections.values()) {
            try {
                peer.channel.shutdown();
                if (!peer.channel.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                    peer.channel.shutdownNow();
                }
            } catch (Throwable t) {
                log.warn("shutdown proxy admin client channel failed. channel:{}", peer.channel, t);
                peer.channel.shutdownNow();
            }
        }
        this.peerConnections.clear();
    }

    private static final class PeerConnection {
        private final ManagedChannel channel;
        private final AdminGrpc.AdminStub stub;

        private PeerConnection(ManagedChannel channel, AdminGrpc.AdminStub stub) {
            this.channel = channel;
            this.stub = stub;
        }
    }
}
