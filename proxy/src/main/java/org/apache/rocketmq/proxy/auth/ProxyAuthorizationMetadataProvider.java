package org.apache.rocketmq.proxy.auth;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

public class ProxyAuthorizationMetadataProvider implements AuthorizationMetadataProvider {

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
    public CompletableFuture<Void> createAcl(Acl acl) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteAcl(Subject subject) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateAcl(Acl acl) {
        return null;
    }

    @Override
    public CompletableFuture<Acl> getAcl(Subject subject) {
        return this.metadataService.getAcl(null, subject);
    }

    @Override
    public CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter) {
        return null;
    }
}
