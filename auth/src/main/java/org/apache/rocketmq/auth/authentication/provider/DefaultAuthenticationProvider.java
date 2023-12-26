package org.apache.rocketmq.auth.authentication.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.builder.AuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.builder.DefaultAuthenticationContextBuilder;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.handler.DefaultAuthenticationHandler;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthenticationProvider implements AuthenticationProvider<DefaultAuthenticationContext> {

    protected AuthConfig authConfig;
    protected Supplier<?> metadataService;
    protected AuthenticationContextBuilder<DefaultAuthenticationContext> authenticationContextBuilder;

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.authConfig = config;
        this.metadataService = metadataService;
        this.authenticationContextBuilder = new DefaultAuthenticationContextBuilder();
    }

    @Override
    public CompletableFuture<Void> authenticate(DefaultAuthenticationContext context) {
        return this.newHandlerChain().handle(context);
    }

    @Override
    public DefaultAuthenticationContext newContext(Metadata metadata, GeneratedMessageV3 request) {
        return this.authenticationContextBuilder.build(metadata, request);
    }

    @Override
    public DefaultAuthenticationContext newContext(RemotingCommand command) {
        return this.authenticationContextBuilder.build(command);
    }

    protected HandlerChain<DefaultAuthenticationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<DefaultAuthenticationContext, CompletableFuture<Void>>create()
            .addNext(new DefaultAuthenticationHandler(this.authConfig, metadataService));
    }
}
