package org.apache.rocketmq.auth.authorization.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.pipeline.DefaultPipe;

public class UserAuthorizationPipe extends DefaultPipe<AuthorizationContext, CompletableFuture<Void>> {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public UserAuthorizationPipe(AuthConfig config, Supplier<?> metadataService) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> doProcess(AuthorizationContext context) {
        if (!context.getSubject().isSubject(SubjectType.USER)) {
            return this.doNext(context);
        }
        return this.getUser(context.getSubject()).thenCompose(user -> {
            if (user.getUserType() == UserType.SUPER) {
                return CompletableFuture.completedFuture(null);
            }
            return this.doNext(context);
        });
    }

    private CompletableFuture<User> getUser(Subject subject) {
        User user = (User) subject;
        return authenticationMetadataProvider.getUser(user.getUsername()).thenApply(result -> {
            if (result == null) {
                throw new AuthorizationException("user not found");
            }
            return result;
        });
    }
}
