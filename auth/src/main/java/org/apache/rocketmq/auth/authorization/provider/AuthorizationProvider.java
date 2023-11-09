package org.apache.rocketmq.auth.authorization.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthorizationProvider {

    void initialize(AuthConfig config);

    void initialize(AuthConfig config, Supplier<?> metadataService);

    CompletableFuture<Void> authorize(AuthorizationContext context);

    List<AuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message);

    List<AuthorizationContext> newContexts(RemotingCommand command, String remoteAddr);
}
