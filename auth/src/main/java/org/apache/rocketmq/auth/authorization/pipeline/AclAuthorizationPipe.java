package org.apache.rocketmq.auth.authorization.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.enums.Decision;
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
import org.apache.rocketmq.common.pipeline.DefaultPipe;
import org.apache.rocketmq.common.resource.ResourcePattern;

public class AclAuthorizationPipe extends DefaultPipe<AuthorizationContext, CompletableFuture<Void>> {

    private final AuthorizationMetadataProvider authorizationMetadataProvider;

    public AclAuthorizationPipe(AuthConfig config) {
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(config);
    }

    public AclAuthorizationPipe(AuthConfig config, Supplier<?> metadataService) {
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(config, metadataService);
    }

    @Override
    public CompletableFuture<Void> doProcess(AuthorizationContext context) {
        return authorizationMetadataProvider.getAcl(context.getSubject())
            .thenAccept(acl -> {
                if (acl == null) {
                    throwException(context, "no matched acl policies");
                }

                // 1. get the defined acl entries which match the request.
                PolicyEntry matchedEntry = matchPolicyEntries(context, acl);

                // 2. if no matched acl entries, return deny
                if (matchedEntry == null) {
                    throwException(context, "no matched acl policies");
                }

                // 3. judge is the entries has denied decision.
                if (matchedEntry.getDecision() == Decision.DENY) {
                    throwException(context, "the acl policy's decision is deny");
                }
            });
    }

    private PolicyEntry matchPolicyEntries(AuthorizationContext context, Acl acl) {
        List<PolicyEntry> policyEntries = new ArrayList<>();

        Policy policy = acl.getPolicy(PolicyType.CUSTOM);
        if (policy != null) {
            List<PolicyEntry> entries = matchPolicyEntries(context, policy.getEntries());
            if (CollectionUtils.isNotEmpty(entries)) {
                policyEntries.addAll(entries);
            }
        }

        if (CollectionUtils.isEmpty(policyEntries)) {
            policy = acl.getPolicy(PolicyType.DEFAULT);
            if (policy != null) {
                List<PolicyEntry> entries = matchPolicyEntries(context, policy.getEntries());
                if (CollectionUtils.isNotEmpty(entries)) {
                    policyEntries.addAll(entries);
                }
            }
        }

        if (CollectionUtils.isEmpty(policyEntries)) {
            return null;
        }

        policyEntries.sort((o1, o2) -> {
            int compare = 0;
            Resource r1 = o1.getResource();
            Resource r2 = o2.getResource();
            if (r1.getResourcePattern() == r2.getResourcePattern()) {
                if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                    String n1 = r1.getResourceName();
                    String n2 = r2.getResourceName();
                    compare = Integer.compare(n1.length(), n2.length());
                }
            } else {
                if (r1.getResourcePattern() == ResourcePattern.LITERAL) {
                    compare = 1;
                }
                if (r1.getResourcePattern() == ResourcePattern.LITERAL) {
                    compare = -1;
                }
                if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                    compare = 1;
                }
                if (r1.getResourcePattern() == ResourcePattern.PREFIXED) {
                    compare = -1;
                }
            }

            if (compare != 0) {
                return compare;
            }

            // the decision deny has higher priority
            Decision d1 = o1.getDecision();
            Decision d2 = o2.getDecision();
            return d1 == Decision.DENY ? 1 : d2 == Decision.DENY ? -1 : 0;
        });

        return policyEntries.get(0);
    }

    private List<PolicyEntry> matchPolicyEntries(AuthorizationContext context, List<PolicyEntry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return null;
        }
        return entries.stream()
            .filter(entry -> entry.isMatchResource(context.getResource()))
            .filter(entry -> entry.isMatchAction(context.getActions()))
            .filter(entry -> entry.isMatchEnvironment(Environment.of(context.getSourceIp())))
            .collect(Collectors.toList());
    }

    private static void throwException(AuthorizationContext context, String detail) {
        throw new AuthorizationException("The {} does not have permission to access the resource {}, " + detail,
            context.getSubject().toSubjectKey(), context.getResource().toResourceKey());
    }
}
