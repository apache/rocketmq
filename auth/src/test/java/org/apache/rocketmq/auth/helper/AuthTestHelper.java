package org.apache.rocketmq.auth.helper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider;
import org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider;
import org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthTestHelper {

    public static AuthConfig createDefaultConfig() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("test-" + System.nanoTime());
        authConfig.setAuthConfigPath("~/config");
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationProvider(DefaultAuthenticationProvider.class.getName());
        authConfig.setAuthenticationMetadataProvider(LocalAuthenticationMetadataProvider.class.getName());
        authConfig.setAuthorizationEnabled(true);
        authConfig.setAuthorizationProvider(DefaultAuthorizationProvider.class.getName());
        authConfig.setAuthorizationMetadataProvider(LocalAuthorizationMetadataProvider.class.getName());
        return authConfig;
    }

    public static Acl buildAcl(String subjectKey, String resources, String actions, String sourceIps,
        Decision decision) {
        Subject subject = Subject.of(subjectKey);
        List<Resource> resourceList = Arrays.stream(StringUtils.split(resources, ",")).map(Resource::parseResource).collect(Collectors.toList());
        List<Action> actionList = Arrays.stream(StringUtils.split(actions, ",")).map(Action::getByName).collect(Collectors.toList());
        Environment environment = null;
        if (StringUtils.isNotBlank(sourceIps)) {
            environment = Environment.of(Arrays.stream(StringUtils.split(sourceIps, ",")).collect(Collectors.toList()));
        }
        return Acl.of(subject, resourceList, actionList, environment, decision);
    }

    public static boolean isEquals(Acl acl1, Acl acl2) {
        if (acl1 == null && acl2 == null) {
            return true;
        }
        if (acl1 == null || acl2 == null) {
            return false;
        }
        Subject subject1 = acl1.getSubject();
        Subject subject2 = acl2.getSubject();
        if (!isEquals(subject1, subject2)) {
            return false;
        }
        Map<PolicyType, Policy> policyMap1 = new HashMap<>();
        Map<PolicyType, Policy> policyMap2 = new HashMap<>();
        if (CollectionUtils.isNotEmpty(acl1.getPolicies())) {
            acl1.getPolicies().forEach(policy -> {
                if (policy.getPolicyType() == null) {
                    policy.setPolicyType(PolicyType.CUSTOM);
                }
                policyMap1.put(policy.getPolicyType(), policy);
            });
        }
        if (CollectionUtils.isNotEmpty(acl2.getPolicies())) {
            acl2.getPolicies().forEach(policy -> {
                if (policy.getPolicyType() == null) {
                    policy.setPolicyType(PolicyType.CUSTOM);
                }
                policyMap2.put(policy.getPolicyType(), policy);
            });
        }
        if (policyMap1.size() != policyMap2.size()) {
            return false;
        }
        Policy customPolicy1 = policyMap1.get(PolicyType.CUSTOM);
        Policy customPolicy2 = policyMap2.get(PolicyType.CUSTOM);
        if (!isEquals(customPolicy1, customPolicy2)) {
            return false;
        }

        Policy defaultPolicy1 = policyMap1.get(PolicyType.DEFAULT);
        Policy defaultPolicy2 = policyMap2.get(PolicyType.DEFAULT);
        if (!isEquals(defaultPolicy1, defaultPolicy2)) {
            return false;
        }

        return true;
    }

    private static boolean isEquals(Policy policy1, Policy policy2) {
        if (policy1 == null && policy2 == null) {
            return true;
        }
        if (policy1 == null || policy2 == null) {
            return false;
        }
        if (policy1.getPolicyType() != policy2.getPolicyType()) {
            return false;
        }
        Map<String, PolicyEntry> policyEntryMap1 = new HashMap<>();
        Map<String, PolicyEntry> policyEntryMap2 = new HashMap<>();
        if (CollectionUtils.isNotEmpty(policy1.getEntries())) {
            policy1.getEntries().forEach(policyEntry -> {
                policyEntryMap1.put(policyEntry.getResource().toResourceKey(), policyEntry);
            });
        }
        if (CollectionUtils.isNotEmpty(policy2.getEntries())) {
            policy2.getEntries().forEach(policyEntry -> {
                policyEntryMap2.put(policyEntry.getResource().toResourceKey(), policyEntry);
            });
        }
        if (policyEntryMap1.size() != policyEntryMap2.size()) {
            return false;
        }

        for (String resourceKey : policyEntryMap1.keySet()) {
            if (!isEquals(policyEntryMap1.get(resourceKey), policyEntryMap2.get(resourceKey))) {
                return false;
            }
        }

        for (String resourceKey : policyEntryMap2.keySet()) {
            if (!isEquals(policyEntryMap1.get(resourceKey), policyEntryMap2.get(resourceKey))) {
                return false;
            }
        }

        return true;
    }

    private static boolean isEquals(PolicyEntry entry1, PolicyEntry entry2) {
        Resource resource1 = entry1.getResource();
        Resource resource2 = entry2.getResource();
        if (!Objects.equals(resource1, resource2)) {
            return false;
        }
        CollectionUtils.isEqualCollection(entry1.getActions(), entry2.getActions());
        return true;
    }

    private static boolean isEquals(Subject subject1, Subject subject2) {
        if (subject1 == null && subject2 == null) {
            return true;
        }
        if (subject1 == null || subject2 == null) {
            return false;
        }
        return subject1.getSubjectType() == subject2.getSubjectType()
            && StringUtils.equals(subject1.toSubjectKey(), subject2.toSubjectKey());
    }

    public static void handleException(Throwable e) {
        Throwable throwable = ExceptionUtils.getRealException(e);
        if (throwable instanceof AuthenticationException) {
            throw (AuthenticationException) throwable;
        }
        if (throwable instanceof AuthorizationException) {
            throw (AuthorizationException) throwable;
        }
        throw new RuntimeException(e);
    }
}
