package org.apache.rocketmq.auth.authorization.handler;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.chain.Handler;
import org.apache.rocketmq.common.chain.HandlerChain;

public class UserAuthorizationHandler implements Handler<DefaultAuthorizationContext, CompletableFuture<Void>> {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public UserAuthorizationHandler(AuthConfig config, Supplier<?> metadataService) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> handle(DefaultAuthorizationContext context, HandlerChain<DefaultAuthorizationContext, CompletableFuture<Void>> chain) {
        if (!context.getSubject().isSubject(SubjectType.USER)) {
            return chain.handle(context);
        }
        return this.getUser(context.getSubject()).thenCompose(user -> {
            if (user.getUserType() == UserType.SUPER) {
                return CompletableFuture.completedFuture(null);
            }
            return chain.handle(context);
        });
    }

    private CompletableFuture<User> getUser(Subject subject) {
        User user = (User) subject;
        return authenticationMetadataProvider.getUser(user.getUsername()).thenApply(result -> {
            if (result == null) {
                throw new AuthorizationException("User:{} not found", user.getUsername());
            }
            if (user.getUserStatus() == UserStatus.DISABLE) {
                throw new AuthenticationException("User:{} is disabled", user.getUsername());
            }
            return result;
        });
    }
}
