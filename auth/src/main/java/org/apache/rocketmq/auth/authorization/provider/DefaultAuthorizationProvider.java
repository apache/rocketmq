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

    protected HandlerChain<AuthorizationContext, CompletableFuture<Void>> handlerChain;

    protected AuthorizationContextBuilder authorizationContextBuilder;

    @Override
    public void initialize(AuthConfig config) {
        this.initialize(config, null);
    }

    @Override
    public void initialize(AuthConfig config, Supplier<?> metadataService) {
        handlerChain = HandlerChain.<AuthorizationContext, CompletableFuture<Void>>create()
            .addNext(new UserAuthorizationHandler(config, metadataService))
            .addNext(new AclAuthorizationHandler(config, metadataService));
        this.authorizationContextBuilder = new AuthorizationContextBuilder(config);
    }

    @Override
    public CompletableFuture<Void> authorize(AuthorizationContext context) {
        return handlerChain.handle(context);
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
