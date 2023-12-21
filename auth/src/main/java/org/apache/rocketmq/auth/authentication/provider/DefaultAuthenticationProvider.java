package org.apache.rocketmq.auth.authentication.provider;

import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.builder.AuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.pipeline.DefaultAuthenticationHandler;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext> {

    protected HandlerChain<DefaultAuthenticationContext, CompletableFuture<Void>> handlerChain;

    protected AuthenticationContextBuilder<DefaultAuthenticationContext> authenticationContextBuilder;

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.handlerChain = HandlerChain.of(new DefaultAuthenticationHandler(config, metadataService));
        this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    }

    @Override
    public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
        return this.handlerChain.handle(context);
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
