package org.apache.rocketmq.auth.authentication.provider;

import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.builder.AuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.pipeline.DefaultAuthenticationPipe;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.pipeline.DefaultPipeline;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext> {

    protected DefaultPipeline<DefaultAuthenticationContext, CompletableFuture<Void>> pipeline;

    protected AuthenticationContextBuilder<DefaultAuthenticationContext> authenticationContextBuilder;

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.pipeline = DefaultPipeline.of(new DefaultAuthenticationPipe(config, metadataService));
        this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    }

    @Override
    public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
        return this.pipeline.process(context);
    }

    @Override
    public DefaultAuthenticationContext newContext(Metadata metadata) {
        return this.authenticationContextBuilder.build(metadata);
    }

    @Override
    public DefaultAuthenticationContext newContext(RemotingCommand command) {
        return this.authenticationContextBuilder.build(command);
    }
}
