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
