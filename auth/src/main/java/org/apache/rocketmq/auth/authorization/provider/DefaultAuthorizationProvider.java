package org.apache.rocketmq.auth.authorization.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authorization.builder.AuthorizationContextBuilder;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.handler.AclAuthorizationHandler;
import org.apache.rocketmq.auth.authorization.handler.UserAuthorizationHandler;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.HandlerChain;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultAuthorizationProvider implements AuthorizationProvider {

    protected AuthConfig authConfig;
    protected Supplier<?> metadataService;
    protected AuthorizationContextBuilder authorizationContextBuilder;

    @Override
    public void initialize(AuthConfig config) {
        this.initialize(config, null);
    }

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        this.authConfig = config;
        this.metadataService = metadataService;
        this.authorizationContextBuilder = new AuthorizationContextBuilder(config);
    }

    @Override
    public CompletableFuture<Void> authorize(AuthorizationContext context) {
        return this.newHandlerChain().handle(context);
    }

    @Override
    public List<AuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message) {
        return this.authorizationContextBuilder.build(metadata, message);
    }

    @Override
    public List<AuthorizationContext> newContexts(RemotingCommand command, String remoteAddr) {
        return this.authorizationContextBuilder.build(command, remoteAddr);
    }

    protected HandlerChain<AuthorizationContext, CompletableFuture<Void>> newHandlerChain() {
        return HandlerChain.<AuthorizationContext, CompletableFuture<Void>>create()
            .addNext(new UserAuthorizationHandler(authConfig, metadataService))
            .addNext(new AclAuthorizationHandler(authConfig, metadataService));
    }
}
