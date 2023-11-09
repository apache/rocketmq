package org.apache.rocketmq.broker.auth.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;

public class AclConverter {

    public static Acl convertAcl(AclInfo aclInfo) {
        if (aclInfo == null) {
            return null;
        }
        Subject subject = Subject.parseSubject(aclInfo.getSubject());
        List<Policy> policies = new ArrayList<>();
        for (AclInfo.PolicyInfo policy : aclInfo.getPolicies()) {
            PolicyType policyType = PolicyType.getByCode(policy.getPolicyType());

            List<AclInfo.PolicyEntryInfo> entryInfos = policy.getEntries();
            if (CollectionUtils.isEmpty(entryInfos)) {
                continue;
            }
            List<PolicyEntry> entries = new ArrayList<>();
            for (AclInfo.PolicyEntryInfo entryInfo : entryInfos) {
                Resource resource = Resource.parseResource(entryInfo.getResource());

                List<Action> actions = new ArrayList<>();
                for (String a : entryInfo.getActions()) {
                    Action action = Action.getByCode(a);
                    if (action == null) {
                        continue;
                    }
                    actions.add(action);
                }

                Environment environment = new Environment();
                if (CollectionUtils.isNotEmpty(entryInfo.getSourceIps())) {
                    environment.setSourceIps(entryInfo.getSourceIps());
                }

                Decision decision = Decision.getByCode(entryInfo.getDecision());

                entries.add(PolicyEntry.of(resource, actions, environment, decision));
            }

            policies.add(Policy.of(policyType, entries));
        }

        return Acl.of(subject, policies);
    }

    public static List<AclInfo> convertAcls(List<Acl> acls) {
        if (CollectionUtils.isEmpty(acls)) {
            return null;
        }
        return acls.stream().map(AclConverter::convertAcl)
            .collect(Collectors.toList());
    }

    public static AclInfo convertAcl(Acl acl) {
        if (acl == null) {
            return null;
        }
        AclInfo aclInfo = new AclInfo();
        aclInfo.setSubject(acl.getSubject().toSubjectKey());
        if (CollectionUtils.isEmpty(acl.getPolicies())) {
            return aclInfo;
        }
        List<AclInfo.PolicyInfo> policyInfos = acl.getPolicies().stream()
            .map(AclConverter::convertPolicy)
            .collect(Collectors.toList());
        aclInfo.setPolicies(policyInfos);
        return aclInfo;
    }

    private static AclInfo.PolicyInfo convertPolicy(Policy policy) {
        AclInfo.PolicyInfo policyInfo = new AclInfo.PolicyInfo();
        if (policy.getPolicyType() != null) {
            policyInfo.setPolicyType(policy.getPolicyType().getCode());
        }
        if (CollectionUtils.isEmpty(policy.getEntries())) {
            return policyInfo;
        }
        List<AclInfo.PolicyEntryInfo> entryInfos = policy.getEntries().stream()
            .map(AclConverter::convertPolicyEntry).collect(Collectors.toList());
        policyInfo.setEntries(entryInfos);
        return policyInfo;
    }

    private static AclInfo.PolicyEntryInfo convertPolicyEntry(PolicyEntry entry) {
        AclInfo.PolicyEntryInfo entryInfo = new AclInfo.PolicyEntryInfo();
        entryInfo.setResource(entry.toResourceStr());
        entryInfo.setActions(entry.toActionsStr());
        if (entry.getEnvironment() != null) {
            entryInfo.setSourceIps(entry.getEnvironment().getSourceIps());
        }
        entryInfo.setDecision(entry.getDecision().getCode());
        return entryInfo;
    }
}
