package org.apache.rocketmq.auth.authorization.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authorization.builder.AuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.pipeline.AclAuthorizationPipe;
import org.apache.rocketmq.auth.authorization.pipeline.UserAuthorizationPipe;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.pipeline.DefaultPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthorizationProvider implements AuthorizationProvider {

    protected DefaultPipeline<AuthorizationContext, CompletableFuture<Void>> pipeline;

    protected AuthorizationContextBuilder authorizationContextBuilder;

    @Override
    public void initialize(AuthConfig config) {
        this.initialize(config, null);
    }

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        pipeline = DefaultPipeline.of(new UserAuthorizationPipe(config, metadataService))
            .addNext(new AclAuthorizationPipe(config, metadataService));
        this.authorizationContextBuilder = new AuthorizationContextBuilder(config);
    }

    @Override
    public CompletableFuture<Void> authorize(AuthorizationContext context) {
        return pipeline.process(context);
    }

    @Override
    public List<AuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message) {
        return this.authorizationContextBuilder.build(metadata, message);
    }

    @Override
    public List<AuthorizationContext> newContexts(RemotingCommand command, String remoteAddr) {
        return this.authorizationContextBuilder.build(command, remoteAddr);
    }
}
