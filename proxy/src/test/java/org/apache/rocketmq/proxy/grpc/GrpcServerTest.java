/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc;

import io.grpc.Server;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcServerTest {

    @Test
    public void shutdownForcesServerAndClosesOwnedEventLoopsAfterTimeout() throws Exception {
        Server server = mock(Server.class);
        when(server.shutdown()).thenReturn(server);
        when(server.awaitTermination(1, TimeUnit.SECONDS)).thenReturn(false, true);
        EventLoopGroup bossEventLoopGroup = mock(EventLoopGroup.class);
        EventLoopGroup workerEventLoopGroup = mock(EventLoopGroup.class);
        Future<?> bossTermination = mock(Future.class);
        Future<?> workerTermination = mock(Future.class);
        doReturn(bossTermination).when(bossEventLoopGroup).shutdownGracefully(0, 1, TimeUnit.SECONDS);
        doReturn(workerTermination).when(workerEventLoopGroup).shutdownGracefully(0, 1, TimeUnit.SECONDS);
        TlsCertificateManager tlsCertificateManager = mock(TlsCertificateManager.class);
        GrpcServer grpcServer = new GrpcServer(
            server,
            1,
            TimeUnit.SECONDS,
            tlsCertificateManager,
            bossEventLoopGroup,
            workerEventLoopGroup
        );

        grpcServer.shutdown();

        verify(server).shutdownNow();
        verify(server, times(2)).awaitTermination(1, TimeUnit.SECONDS);
        verify(bossEventLoopGroup).shutdownGracefully(0, 1, TimeUnit.SECONDS);
        verify(workerEventLoopGroup).shutdownGracefully(0, 1, TimeUnit.SECONDS);
        verify(bossTermination).awaitUninterruptibly(1, TimeUnit.SECONDS);
        verify(workerTermination).awaitUninterruptibly(1, TimeUnit.SECONDS);
    }
}
