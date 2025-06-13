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
package org.apache.rocketmq.proxy.grpc;

import io.grpc.Server;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GrpcServerLifeCycleTest {

    @Mock
    Server server;
    @Mock
    FileWatchService fileWatchService;

    private GrpcServer grpcServer;

    @BeforeEach
    void before() throws Exception {
        grpcServer = new GrpcServer(server, 5, TimeUnit.SECONDS) {
            @Override
            protected FileWatchService initGrpcCertKeyWatchService() {
                return fileWatchService;
            }
        };
    }

    @Test
    void start_shouldStartFileWatchService() throws Exception {
        grpcServer.start();

        verify(server).start();
        verify(fileWatchService).start();
    }

    @Test
    void shutdown_shouldShutdownFileWatchService() {
        grpcServer.shutdown();

        verify(server).shutdown();
        verify(fileWatchService).shutdown();
    }
}
