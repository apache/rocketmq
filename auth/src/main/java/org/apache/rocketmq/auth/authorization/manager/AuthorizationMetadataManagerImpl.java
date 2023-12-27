package org.apache.rocketmq.auth.authorization.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.common.utils.IPAddressUtils;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class AuthorizationMetadataManagerImpl implements AuthorizationMetadataManager {

    private final AuthorizationMetadataProvider authorizationMetadataProvider;

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    public AuthorizationMetadataManagerImpl(AuthConfig authConfig) {
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(authConfig);
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(authConfig);
    }

    @Override
    public void shutdown() {
        if (this.authenticationMetadataProvider != null) {
            this.authenticationMetadataProvider.shutdown();
        }
        if (this.authorizationMetadataProvider != null) {
            this.authorizationMetadataProvider.shutdown();
        }
    }

    @Override
    public CompletableFuture<Void> createAcl(Acl acl) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            validate(acl, true);

            initAcl(acl);

            CompletableFuture<? extends Subject> subjectFuture;
            if (acl.getSubject().isSubject(SubjectType.USER)) {
                User user = (User) acl.getSubject();
                subjectFuture = this.getAuthenticationMetadataProvider().getUser(user.getUsername());
            } else {
                subjectFuture = CompletableFuture.completedFuture(acl.getSubject());
            }
            CompletableFuture<Acl> aclFuture = this.getAuthorizationMetadataProvider().getAcl(acl.getSubject());

            return subjectFuture.thenCombine(aclFuture, (subject, oldAcl) -> {
                if (subject == null) {
                    throw new AuthorizationException("The subject is not exist");
                }
                if (oldAcl != null) {
                    throw new AuthorizationException("The acl is existed");
                }
                return acl;
            }).thenCompose(this.getAuthorizationMetadataProvider()::createAcl);

        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> updateAcl(Acl acl) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            validate(acl, false);

            initAcl(acl);

            CompletableFuture<? extends Subject> subjectFuture;
            if (acl.getSubject().isSubject(SubjectType.USER)) {
                User user = (User) acl.getSubject();
                subjectFuture = this.getAuthenticationMetadataProvider().getUser(user.getUsername());
            } else {
                subjectFuture = CompletableFuture.completedFuture(acl.getSubject());
            }
            CompletableFuture<Acl> aclFuture = this.getAuthorizationMetadataProvider().getAcl(acl.getSubject());

            return subjectFuture.thenCombine(aclFuture, (subject, oldAcl) -> {
                if (subject == null) {
                    throw new AuthorizationException("The subject is not exist");
                }
                if (oldAcl == null) {
                    throw new AuthorizationException("The acl is not exist");
                }
                return oldAcl;
            }).thenCompose(oldAcl -> {
                oldAcl.updatePolicy(acl.getPolicies());
                return this.getAuthorizationMetadataProvider().updateAcl(oldAcl);
            });

        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    private static void initAcl(Acl acl) {
        acl.getPolicies().forEach(policy -> {
            if (policy.getPolicyType() == null) {
                policy.setPolicyType(PolicyType.CUSTOM);
            }
        });
    }

    @Override
    public CompletableFuture<Void> deleteAcl(Subject subject, PolicyType policyType, List<Resource> resources) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (subject == null) {
                throw new AuthorizationException("The subject is null");
            }
            if (policyType == null) {
                policyType = PolicyType.CUSTOM;
            }

            CompletableFuture<? extends Subject> subjectFuture;
            if (subject.isSubject(SubjectType.USER)) {
                User user = (User) subject;
                subjectFuture = this.getAuthenticationMetadataProvider().getUser(user.getUsername());
            } else {
                subjectFuture = CompletableFuture.completedFuture(subject);
            }
            CompletableFuture<Acl> aclFuture = this.getAuthorizationMetadataProvider().getAcl(subject);

            PolicyType finalPolicyType = policyType;
            return subjectFuture.thenCombine(aclFuture, (sub, oldAcl) -> {
                if (sub == null) {
                    throw new AuthorizationException("The subject is not exist");
                }
                if (oldAcl == null) {
                    throw new AuthorizationException("The acl is not exist");
                }
                return oldAcl;
            }).thenCompose(oldAcl -> {
                if (CollectionUtils.isNotEmpty(resources)) {
                    oldAcl.deletePolicy(finalPolicyType, resources);
                }
                if (CollectionUtils.isEmpty(resources) || CollectionUtils.isEmpty(oldAcl.getPolicies())) {
                    return this.getAuthorizationMetadataProvider().deleteAcl(subject);
                }
                return this.getAuthorizationMetadataProvider().updateAcl(oldAcl);
            });

        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Acl> getAcl(Subject subject) {
        CompletableFuture<? extends Subject> subjectFuture;
        if (subject.isSubject(SubjectType.USER)) {
            User user = (User) subject;
            subjectFuture = this.getAuthenticationMetadataProvider().getUser(user.getUsername());
        } else {
            subjectFuture = CompletableFuture.completedFuture(subject);
        }
        return subjectFuture.thenCompose(sub -> {
            if (sub == null) {
                throw new AuthorizationException("The subject is not exist");
            }
            return this.getAuthorizationMetadataProvider().getAcl(subject);
        });
    }

    @Override
    public CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter) {
        return this.getAuthorizationMetadataProvider().listAcl(subjectFilter, resourceFilter);
    }

    private void validate(Acl acl, boolean isCreate) {
        Subject subject = acl.getSubject();
        if (subject.getSubjectType() == null) {
            throw new AuthorizationException("The subject type is null");
        }
        List<Policy> policies = acl.getPolicies();
        if (CollectionUtils.isEmpty(policies)) {
            throw new AuthorizationException("The policies is empty");
        }
        for (Policy policy : policies) {
            this.validate(policy, isCreate);
        }
    }

    private void validate(Policy policy, boolean isCreate) {
        List<PolicyEntry> policyEntries = policy.getEntries();
        if (CollectionUtils.isEmpty(policyEntries)) {
            throw new AuthorizationException("The policy entries is empty");
        }
        for (PolicyEntry policyEntry : policyEntries) {
            this.validate(policyEntry, isCreate);
        }
    }

    private void validate(PolicyEntry entry, boolean isCreate) {
        Resource resource = entry.getResource();
        if (resource == null) {
            throw new AuthorizationException("The resource is null");
        }
        if (resource.getResourceType() == null) {
            throw new AuthorizationException("The resource type is null");
        }
        if (resource.getResourcePattern() == null) {
            throw new AuthorizationException("The resource pattern is null");
        }
        if (CollectionUtils.isEmpty(entry.getActions())) {
            throw new AuthorizationException("The actions is empty");
        }
        if (entry.getActions().contains(Action.ANY)) {
            throw new AuthorizationException("The actions can not be Any");
        }
        Environment environment = entry.getEnvironment();
        if (environment != null && CollectionUtils.isNotEmpty(environment.getSourceIps())) {
            for (String sourceIp : environment.getSourceIps()) {
                if (StringUtils.isBlank(sourceIp)) {
                    throw new AuthorizationException("The source ip is empty");
                }
                if (!IPAddressUtils.isValidIPOrCidr(sourceIp)) {
                    throw new AuthorizationException("The source ip is invalid");
                }
            }
        }
        if (entry.getDecision() == null) {
            throw new AuthorizationException("The decision is null");
        }
    }

    private void handleException(Exception e, CompletableFuture<?> result) {
        Throwable throwable = ExceptionUtils.getRealException(e);
        if (throwable instanceof AuthorizationException) {
            result.completeExceptionally(throwable);
        } else {
            result.completeExceptionally(new AuthorizationException(ResponseCode.SYSTEM_ERROR, throwable));
        }
    }

    private AuthorizationMetadataProvider getAuthorizationMetadataProvider() {
        if (authenticationMetadataProvider == null) {
            throw new IllegalStateException("The authenticationMetadataProvider is not configured");
        }
        return authorizationMetadataProvider;
    }

    private AuthenticationMetadataProvider getAuthenticationMetadataProvider() {
        if (authorizationMetadataProvider == null) {
            throw new IllegalStateException("The authorizationMetadataProvider is not configured");
        }
        return authenticationMetadataProvider;
    }
}
