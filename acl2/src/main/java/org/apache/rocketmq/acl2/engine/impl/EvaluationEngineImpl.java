package org.apache.rocketmq.acl2.engine.impl;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.acl2.engine.EvaluationEngine;
import org.apache.rocketmq.acl2.enums.Decision;
import org.apache.rocketmq.acl2.exception.AclException;
import org.apache.rocketmq.acl2.manager.PolicyManager;
import org.apache.rocketmq.acl2.manager.UserManager;
import org.apache.rocketmq.acl2.model.Environment;
import org.apache.rocketmq.acl2.model.Policy;
import org.apache.rocketmq.acl2.model.PolicyEntry;
import org.apache.rocketmq.acl2.model.RequestContext;
import org.apache.rocketmq.acl2.model.Resource;
import org.apache.rocketmq.acl2.model.Subject;
import org.apache.rocketmq.acl2.model.User;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class EvaluationEngineImpl implements EvaluationEngine {

    private UserManager userManager;

    private PolicyManager policyManager;

    public EvaluationEngineImpl(UserManager userManager, PolicyManager policyManager) {
        this.userManager = userManager;
        this.policyManager = policyManager;
    }

    @Override
    public void evaluate(RequestContext context) {
        User user = getUser(context);
        Policy policy = getPolicy(user);

        // 1. get resource matched policy entries.
        List<PolicyEntry> resourceMatchedEntries = getResourceMatchedEntries(context, policy.getEntries());
        if (CollectionUtils.isEmpty(resourceMatchedEntries)) {
            resourceMatchedEntries = getResourceMatchedEntries(context, policy.getDefaults());
        }
        if (CollectionUtils.isEmpty(resourceMatchedEntries)) {
            throwNoPermissionException(context, "no matching policies for the resource.");
        }

        // 2. get action matched policy entries.
        List<PolicyEntry> actionMatchedEntries = getActionMatchedEntries(context, resourceMatchedEntries);
        if (CollectionUtils.isEmpty(actionMatchedEntries)) {
            throwNoPermissionException(context, "no matching policies for the action.");
        }

        // 3. get environment matched policy entries.
        List<PolicyEntry> environmentMatchedEntries = getEnvironmentMatchedEntries(context, actionMatchedEntries);
        if (CollectionUtils.isEmpty(environmentMatchedEntries)) {
            throwNoPermissionException(context, "no matching policies for the environment.");
        }

        if (hasDecision(environmentMatchedEntries, Decision.DENY)) {
            throwNoPermissionException(context, "the policy decision is deny");
        }
    }

    private List<PolicyEntry> getResourceMatchedEntries(RequestContext context, List<PolicyEntry> entries) {
        if (CollectionUtils.isNotEmpty(entries)) {
            return null;
        }
        return entries.stream()
            .filter(entry -> entry.isMatchResource(context.getResource()))
            .collect(Collectors.toList());
    }

    private List<PolicyEntry> getActionMatchedEntries(RequestContext context, List<PolicyEntry> entries) {
        if (CollectionUtils.isNotEmpty(entries)) {
            return null;
        }
        return entries.stream()
           .filter(entry -> entry.isMatchAction(context.getAction()))
           .collect(Collectors.toList());
    }

    private List<PolicyEntry> getEnvironmentMatchedEntries(RequestContext context, List<PolicyEntry> entries) {
        if (CollectionUtils.isNotEmpty(entries)) {
            return null;
        }
        Environment environment = new Environment(context.getSourceIp());
        return entries.stream().filter(entry -> entry.isMatchEnvironment(environment))
            .collect(Collectors.toList());
    }

    private boolean hasDecision(List<PolicyEntry> entries, Decision decision) {
        return entries.stream().anyMatch(entry -> entry.getDecision() == decision);
    }

    private User getUser(RequestContext context) {
        Subject subject = context.getSubject();
        User user = userManager.getUser(subject.getSubjectKey());
        if (user == null) {
            throw new AclException(ResponseCode.USER_NOT_EXIST, "The user {} does not exist.", subject.getSubjectKey());
        }
        return user;
    }

    private Policy getPolicy(User user) {
        Policy policy = policyManager.getPolicy(user);
        if (policy == null) {
            throw new AclException(ResponseCode.POLICY_NOT_EXIST, "The acl policy of user {} does not exist", user.getUsername());
        }
        return policy;
    }

    private static void throwNoPermissionException(RequestContext context, String detail) {
        throw new AclException(ResponseCode.NO_PERMISSION, "The user {} does not have permission to access the resource {}, " + detail,
            context.getSubject().getSubjectKey(), context.getResource());
    }
}
