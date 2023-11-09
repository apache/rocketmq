package org.apache.rocketmq.proxy.auth;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

public class ProxyAuthenticationMetadataProvider implements AuthenticationMetadataProvider {

    protected AuthConfig authConfig;
    protected MetadataService metadataService;

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        if (metadataService != null) {
            this.metadataService = (MetadataService) metadataService.get();
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        return null;
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        return this.metadataService.getUser(null, username);
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        return null;
    }
}
