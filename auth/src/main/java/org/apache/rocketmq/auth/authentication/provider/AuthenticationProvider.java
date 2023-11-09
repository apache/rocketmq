package org.apache.rocketmq.auth.authentication.provider;

import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthenticationProvider<AuthenticationContext> {

    void initialize(AuthConfig config, Supplier<?> metadataService);

    CompletableFuture<Void> authenticate(AuthenticationContext context);

    AuthenticationContext newContext(Metadata metadata);

    AuthenticationContext newContext(RemotingCommand command);
}
