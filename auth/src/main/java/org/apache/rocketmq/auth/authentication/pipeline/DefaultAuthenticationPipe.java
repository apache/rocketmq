package org.apache.rocketmq.auth.authentication.pipeline;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclSigner;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.pipeline.DefaultPipe;

public class DefaultAuthenticationPipe extends DefaultPipe<DefaultAuthenticationContext, CompletableFuture<Void>> {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public DefaultAuthenticationPipe(AuthConfig config, Supplier<?> metadataService) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> doProcess(DefaultAuthenticationContext context) {
        return getUser(context).thenAccept(user -> doAuthenticate(context, user));
    }

    protected CompletableFuture<User> getUser(DefaultAuthenticationContext context) {
        if (StringUtils.isEmpty(context.getUsername())) {
            throw new AuthenticationException("username cannot be null");
        }
        return this.authenticationMetadataProvider.getUser(context.getUsername());
    }

    protected void doAuthenticate(DefaultAuthenticationContext context, User user) {
        if (user == null) {
            throw new AuthenticationException("user not found");
        }
        String signature = AclSigner.calSignature(context.getContent(), user.getPassword());
        if (!StringUtils.equals(signature, context.getSignature())) {
            throw new AuthenticationException("check signature failed");
        }
    }
}
